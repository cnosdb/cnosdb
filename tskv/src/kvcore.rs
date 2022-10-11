use std::time::Duration;
use std::{collections::HashMap, panic, sync::Arc};

use flatbuffers::FlatBufferBuilder;
use futures::stream::SelectNextSome;
use futures::FutureExt;
use libc::printf;
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};
use tokio::{
    runtime::Runtime,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::Instant,
};

use metrics::{incr_compaction_failed, incr_compaction_success, sample_tskv_compaction_duration};
use models::schema::TableSchema;
use models::{
    utils::unite_id, FieldId, FieldInfo, InMemPoint, SeriesId, SeriesKey, Tag, Timestamp, ValueType,
};
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use trace::{debug, error, info, trace, warn};

use crate::{
    compaction::{self, run_flush_memtable_job, CompactReq, FlushReq},
    context::GlobalContext,
    database,
    engine::Engine,
    error::{self, Result},
    file_manager::{self, FileManager},
    file_utils,
    index::{db_index, IndexResult},
    kv_option::Options,
    memcache::{DataType, MemCache},
    record_file::Reader,
    summary,
    summary::{Summary, SummaryProcessor, SummaryTask, VersionEdit},
    tseries_family::{SuperVersion, TimeRange, Version},
    tsm::{DataBlock, TsmTombstone, MAX_BLOCK_VALUES},
    version_set,
    version_set::VersionSet,
    wal::{self, WalEntryType, WalManager, WalTask},
    Error, Task, TseriesFamilyId,
};

#[derive(Debug)]
pub struct TsKv {
    options: Arc<Options>,
    global_ctx: Arc<GlobalContext>,
    version_set: Arc<RwLock<VersionSet>>,

    runtime: Arc<Runtime>,
    wal_sender: UnboundedSender<WalTask>,
    flush_task_sender: UnboundedSender<FlushReq>,
    compact_task_sender: UnboundedSender<TseriesFamilyId>,
    summary_task_sender: UnboundedSender<SummaryTask>,
}

impl TsKv {
    pub async fn open(opt: Options, runtime: Arc<Runtime>) -> Result<TsKv> {
        let shared_options = Arc::new(opt);
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel();
        let (compact_task_sender, compact_task_receiver) = mpsc::unbounded_channel();
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (summary_task_sender, summary_task_receiver) = mpsc::unbounded_channel();
        let (version_set, summary) =
            Self::recover_summary(shared_options.clone(), flush_task_sender.clone()).await;
        let wal_cfg = shared_options.wal.clone();
        let core = Self {
            version_set,
            global_ctx: summary.global_context(),
            runtime,
            wal_sender,
            options: shared_options,
            flush_task_sender: flush_task_sender.clone(),
            compact_task_sender: compact_task_sender.clone(),
            summary_task_sender: summary_task_sender.clone(),
        };

        core.recover_wal().await;
        core.run_wal_job(wal_receiver);
        core.run_flush_job(
            flush_task_receiver,
            summary.global_context(),
            summary.version_set(),
            summary_task_sender.clone(),
            compact_task_sender.clone(),
        );
        core.run_compact_job(
            compact_task_receiver,
            summary.global_context(),
            summary.version_set(),
            summary_task_sender.clone(),
        );
        core.run_summary_job(summary, summary_task_receiver);
        Ok(core)
    }

    async fn recover_summary(
        opt: Arc<Options>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) -> (Arc<RwLock<VersionSet>>, Summary) {
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir)
                .context(error::IOSnafu)
                .unwrap();
        }
        let summary_file = file_utils::make_summary_file(&summary_dir, 0);
        let summary = if file_manager::try_exists(&summary_file) {
            Summary::recover(opt.clone(), flush_task_sender)
                .await
                .unwrap()
        } else {
            Summary::new(opt.clone(), flush_task_sender).await.unwrap()
        };
        let version_set = summary.version_set();

        (version_set, summary)
    }

    async fn recover_wal(&self) {
        let wal_manager = WalManager::new(self.options.wal.clone());

        wal_manager
            .recover(self, self.global_ctx.clone())
            .await
            .unwrap();
    }

    pub fn read_point(
        &self,
        db: &str,
        time_range: &TimeRange,
        field_id: FieldId,
    ) -> Vec<DataBlock> {
        let version = {
            let version_set = self.version_set.read();
            if let Some(tsf) = version_set.get_tsfamily_by_name(db) {
                tsf.read().super_version()
            } else {
                warn!("ts_family with db name '{}' not found.", db);
                return vec![];
            }
        };

        let mut data = vec![];
        // get data from memcache
        if let Some(mem_entry) = version.caches.mut_cache.read().get(&field_id) {
            data.append(&mut mem_entry.read().read_cell(time_range));
        }

        // get data from im_memcache
        for mem_cache in version.caches.immut_cache.iter() {
            if mem_cache.read().flushed {
                continue;
            }
            if let Some(mem_entry) = mem_cache.read().get(&field_id) {
                data.append(&mut mem_entry.read().read_cell(time_range));
            }
        }

        // get data from levelinfo
        for level_info in version.version.levels_info.iter() {
            if level_info.level == 0 {
                continue;
            }
            data.append(&mut level_info.read_column_file(
                version.ts_family_id,
                field_id,
                time_range,
            ));
        }

        // get data from delta
        let level_info = version.version.levels_info();
        data.append(&mut level_info[0].read_column_file(
            version.ts_family_id,
            field_id,
            time_range,
        ));

        data
    }

    fn run_wal_job(&self, mut receiver: UnboundedReceiver<WalTask>) {
        warn!("job 'WAL' starting.");
        let wal_opt = self.options.wal.clone();
        let mut wal_manager = WalManager::new(wal_opt);
        let f = async move {
            while let Some(x) = receiver.recv().await {
                match x {
                    WalTask::Write { points, cb } => {
                        // write wal
                        let ret = wal_manager.write(WalEntryType::Write, &points).await;
                        let send_ret = cb.send(ret);
                        match send_ret {
                            Ok(wal_result) => {}
                            Err(err) => {
                                warn!("send WAL write result failed.")
                            }
                        }
                    }
                }
            }
        };
        self.runtime.spawn(f);
        info!("job 'WAL' started.");
    }

    fn run_flush_job(
        &self,
        mut receiver: UnboundedReceiver<FlushReq>,
        ctx: Arc<GlobalContext>,
        version_set: Arc<RwLock<VersionSet>>,
        summary_task_sender: UnboundedSender<SummaryTask>,
        compact_task_sender: UnboundedSender<TseriesFamilyId>,
    ) {
        let f = async move {
            while let Some(x) = receiver.recv().await {
                run_flush_memtable_job(
                    x,
                    ctx.clone(),
                    version_set.clone(),
                    summary_task_sender.clone(),
                    compact_task_sender.clone(),
                )
                .unwrap();
            }
        };
        self.runtime.spawn(f);
        info!("Flush task handler started");
    }

    fn run_compact_job(
        &self,
        mut receiver: UnboundedReceiver<TseriesFamilyId>,
        ctx: Arc<GlobalContext>,
        version_set: Arc<RwLock<VersionSet>>,
        summary_task_sender: UnboundedSender<SummaryTask>,
    ) {
        self.runtime.spawn(async move {
            while let Some(ts_family_id) = receiver.recv().await {
                if let Some(tsf) = version_set.read().get_tsfamily_by_tf_id(ts_family_id) {
                    info!("Starting compaction on ts_family {}", ts_family_id);
                    let start = Instant::now();
                    if let Some(compact_req) = tsf.read().pick_compaction() {
                        let database = compact_req.database.clone();
                        let compact_ts_family = compact_req.ts_family_id;
                        let out_level = compact_req.out_level;
                        match compaction::run_compaction_job(compact_req, ctx.clone()) {
                            Ok(Some(version_edit)) => {
                                incr_compaction_success();
                                let (summary_tx, summary_rx) = oneshot::channel();
                                let ret = summary_task_sender.send(SummaryTask {
                                    edits: vec![version_edit],
                                    cb: summary_tx,
                                });
                                sample_tskv_compaction_duration(
                                    database.as_str(),
                                    compact_ts_family.to_string().as_str(),
                                    out_level.to_string().as_str(),
                                    start.elapsed().as_secs_f64(),
                                )
                                // TODO Handle summary result using summary_rx.
                            }
                            Ok(None) => {
                                info!("There is nothing to compact.");
                            }
                            Err(e) => {
                                incr_compaction_failed();
                                error!("Compaction job failed: {}", e);
                            }
                        }
                    }
                }
            }
        });
    }

    fn run_summary_job(
        &self,
        summary: Summary,
        mut summary_task_receiver: UnboundedReceiver<SummaryTask>,
    ) {
        let f = async move {
            let mut summary_processor = SummaryProcessor::new(Box::new(summary));
            while let Some(x) = summary_task_receiver.recv().await {
                debug!("Apply Summary task");
                summary_processor.batch(x);
                summary_processor.apply().await;
            }
        };
        self.runtime.spawn(f);
        info!("Summary task handler started");
    }

    // fn run_timer_job(&self, pub_sender: Sender<()>) {
    //     let f = async move {
    //         let interval = Duration::from_secs(1);
    //         let ticker = crossbeam_channel::tick(interval);
    //         loop {
    //             ticker.recv().unwrap();
    //             let _ = pub_sender.send(());
    //         }
    //     };
    //     self.runtime.spawn(f);
    // }

    // pub async fn query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
    //     Ok(None)
    // }

    // Compact TSM files in database into bigger TSM files.
    pub fn compact(&self, database: &str) {
        if let Some(db) = self.version_set.read().get_db(database) {
            // TODO: stop current and prevent next flush and compaction.
            for (ts_family_id, ts_family) in db.read().ts_families() {
                if let Some(compact_req) = ts_family.read().pick_compaction() {
                    match compaction::run_compaction_job(compact_req, self.global_ctx.clone()) {
                        Ok(Some(version_edit)) => {
                            let (summary_tx, summary_rx) = oneshot::channel();
                            let ret = self.summary_task_sender.send(SummaryTask {
                                edits: vec![version_edit],
                                cb: summary_tx,
                            });

                            // let _ = summary_rx.await;
                        }
                        Ok(None) => {
                            info!("There is nothing to compact.");
                        }
                        Err(e) => {
                            error!("Compaction job failed: {}", e);
                        }
                    }
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse> {
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = String::from_utf8(fb_points.database().unwrap().to_vec())
            .map_err(|err| Error::ErrCharacterSet)?;

        let db_warp = self.version_set.read().get_db(&db_name);
        let db = match db_warp {
            Some(database) => database,
            None => self.version_set.write().create_db(&db_name),
        };
        let write_group = db.read().build_write_group(fb_points.points().unwrap())?;

        let mut seq = 0;
        if self.options.wal.enabled {
            let (cb, rx) = oneshot::channel();
            self.wal_sender
                .send(WalTask::Write { cb, points })
                .map_err(|err| Error::Send)?;
            (seq, _) = rx.await.context(error::ReceiveSnafu)??;
        }

        let opt_tsf = db.read().get_tsfamily_random();
        let tsf = match opt_tsf {
            Some(v) => v,
            None => db.write().add_tsfamily(
                0,
                seq,
                self.global_ctx.file_id_next(),
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
            ),
        };

        tsf.read().put_points(seq, write_group);
        tsf.write().check_to_flush();
        Ok(WritePointsRpcResponse {
            version: 1,
            points: vec![],
        })
    }

    async fn write_from_wal(
        &self,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse> {
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = String::from_utf8(fb_points.database().unwrap().to_vec())
            .map_err(|err| Error::ErrCharacterSet)?;

        let db = self.version_set.write().create_db(&db_name);

        let write_group = db.read().build_write_group(fb_points.points().unwrap())?;

        let opt_tsf = db.read().get_tsfamily_random();
        let tsf = match opt_tsf {
            Some(v) => v,
            None => db.write().add_tsfamily(
                0,
                seq,
                self.global_ctx.file_id_next(),
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
            ),
        };

        tsf.read().put_points(seq, write_group);

        return Ok(WritePointsRpcResponse {
            version: 1,
            points: vec![],
        });
    }

    fn read(
        &self,
        db: &str,
        sids: Vec<SeriesId>,
        time_range: &TimeRange,
        fields: Vec<u32>,
    ) -> HashMap<SeriesId, HashMap<u32, Vec<DataBlock>>> {
        // get data block
        let mut ans = HashMap::new();
        for sid in sids {
            let sid_entry = ans.entry(sid).or_insert_with(HashMap::new);
            for field_id in fields.iter() {
                let field_id_entry = sid_entry.entry(*field_id).or_insert(vec![]);
                let fid = unite_id((*field_id).into(), sid);
                field_id_entry.append(&mut self.read_point(db, time_range, fid));
            }
        }

        // sort data block, max block size 1000
        let mut final_ans = HashMap::new();
        for i in ans {
            let sid_entry = final_ans.entry(i.0).or_insert_with(HashMap::new);
            for j in i.1 {
                let field_id_entry = sid_entry.entry(j.0).or_insert(vec![]);
                field_id_entry.append(&mut DataBlock::merge_blocks(j.1, MAX_BLOCK_VALUES));
            }
        }

        final_ans
    }

    fn drop_database(&self, database: &str) -> Result<()> {
        let database = database.to_string();

        if let Some(db) = self.version_set.write().delete_db(&database) {
            let mut db_wlock = db.write();
            let ts_family_ids: Vec<TseriesFamilyId> = db_wlock
                .ts_families()
                .iter()
                .map(|(tsf_id, tsf)| *tsf_id)
                .collect();
            for ts_family_id in ts_family_ids {
                db_wlock.del_tsfamily(ts_family_id, self.summary_task_sender.clone());
            }
        }

        let idx_dir = self.options.storage.index_dir(&database);
        if let Err(e) = std::fs::remove_dir_all(&idx_dir) {
            error!("Failed to remove dir '{}'", idx_dir.display());
        }
        let db_dir = self.options.storage.database_dir(&database);
        if let Err(e) = std::fs::remove_dir_all(&db_dir) {
            error!("Failed to remove dir '{}'", db_dir.display());
        }

        Ok(())
    }

    fn create_table(&self, schema: &TableSchema) {
        // todo : remove this create db after impl create db sql
        self.version_set.write().create_db(&schema.db);
        if let Some(db) = self.version_set.write().get_db(&schema.db) {
            match db.read().get_index().create_table(schema) {
                Ok(_) => {}
                Err(e) => error!("failed create database '{}'", e),
            }
        } else {
            error!("Database {}, not found", schema.db);
        }
    }

    fn drop_table(&self, database: &str, table: &str) -> Result<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.

        let version_set = self.version_set.clone();
        let database = database.to_string();
        let table = table.to_string();
        let handle = std::thread::spawn(move || {
            database::delete_table_async(database.to_string(), table.to_string(), version_set)
        });
        let recv_ret = match handle.join() {
            Ok(ret) => ret,
            Err(e) => panic::resume_unwind(e),
        };

        // TODO Release global DropTable flag.
        recv_ret
    }

    fn delete_series(
        &self,
        database: &str,
        series_ids: &[SeriesId],
        field_ids: &[FieldId],
        time_range: &TimeRange,
    ) -> Result<()> {
        let storage_field_ids: Vec<u64> = series_ids
            .iter()
            .flat_map(|sid| field_ids.iter().map(|fid| unite_id(*fid, *sid)))
            .collect();

        if let Some(db) = self.version_set.read().get_db(database) {
            for (ts_family_id, ts_family) in db.read().ts_families().iter() {
                // TODO Cancel current and prevent next flush or compaction in TseriesFamily provisionally.

                ts_family
                    .write()
                    .delete_cache(&storage_field_ids, time_range);

                let version = ts_family.read().super_version();
                for column_file in version.version.column_files(&storage_field_ids, time_range) {
                    column_file.add_tombstone(&storage_field_ids, time_range)?;
                }

                // TODO Start next flush or compaction.
            }
        }

        Ok(())
    }

    fn get_table_schema(&self, name: &str, tab: &str) -> Result<Option<TableSchema>> {
        if let Some(db) = self.version_set.read().get_db(name) {
            let val = db
                .read()
                .get_table_schema(tab)
                .context(error::IndexErrSnafu)?;
            return Ok(val);
        }

        Ok(None)
    }

    fn get_series_id_list(&self, name: &str, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u64>> {
        if let Some(db) = self.version_set.read().get_db(name) {
            return db.read().get_index().get_series_id_list(tab, tags);
        }

        Ok(vec![])
    }

    fn get_series_key(&self, name: &str, sid: u64) -> IndexResult<Option<SeriesKey>> {
        if let Some(db) = self.version_set.read().get_db(name) {
            return db.read().get_series_key(sid);
        }

        Ok(None)
    }

    fn get_db_version(&self, db: &str) -> Option<Arc<SuperVersion>> {
        let version_set = self.version_set.read();
        if let Some(tsf) = version_set.get_tsfamily_by_name(db) {
            return Some(tsf.read().super_version());
        } else {
            warn!("ts_family with db name '{}' not found.", db);
            None
        }
    }
}

#[cfg(test)]
mod test {
    use config::get_config;
    use flatbuffers::{FlatBufferBuilder, WIPOffset};
    use models::utils::now_timestamp;
    use models::{InMemPoint, SeriesId, SeriesKey, Timestamp};
    use protos::{models::Points, models_helper};
    use std::collections::HashMap;
    use std::sync::{atomic, Arc};
    use tokio::runtime::{self, Runtime};

    use crate::{engine::Engine, error, tsm::DataBlock, Options, TimeRange, TsKv};
    use std::sync::atomic::{AtomicI64, Ordering};
    use tokio::sync::watch;

    #[tokio::test]
    #[ignore]
    async fn test_compact() {
        trace::init_default_global_tracing("tskv_log", "tskv.log", "info");
        let config = get_config("/tmp/test/config/config.toml");
        let opt = Options::from(&config);
        let tskv = TsKv::open(opt, Arc::new(Runtime::new().unwrap()))
            .await
            .unwrap();
        tskv.compact("public");
    }

    async fn prepare(tskv: &TsKv, database: &str, table: &str, time_range: &TimeRange) {
        let mut fbb = FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 10);
        fbb.finish(points, None);
        let points_data = fbb.finished_data();

        let write_batch = protos::kv_service::WritePointsRpcRequest {
            version: 1,
            points: points_data.to_vec(),
        };
        tskv.write(write_batch).await.unwrap();

        {
            let table_schema = tskv.get_table_schema(database, table).unwrap().unwrap();
            let series_ids = tskv.get_series_id_list(database, table, &[]).unwrap();

            let field_ids: Vec<u32> = table_schema.fields.iter().map(|f| f.1.id as u32).collect();
            let result: HashMap<SeriesId, HashMap<u32, Vec<DataBlock>>> =
                tskv.read(database, series_ids, time_range, field_ids);
            println!("Result items: {}", result.len());
        }
    }
    #[test]
    #[ignore]
    fn test_drop_table_database() {
        let base_dir = "/tmp/test/tskv/drop_table".to_string();
        let _ = std::fs::remove_dir_all(&base_dir);
        trace::init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let runtime = Arc::new(
            runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );

        runtime.clone().block_on(async move {
            let mut config = get_config("../config/config.toml").clone();
            config.storage.path = base_dir;
            // TODO Add test case for `max_buffer_size = 0`.
            // config.cache.max_buffer_size = 0;
            let opt = Options::from(&config);
            let tskv = TsKv::open(opt, runtime).await.unwrap();

            let database = "db";
            let table = "table";
            let time_range = TimeRange::new(i64::MIN, i64::MAX);

            prepare(&tskv, database, table, &time_range).await;

            tskv.drop_table(database, table).unwrap();

            {
                let table_schema = tskv.get_table_schema(database, table).unwrap();
                assert!(table_schema.is_none());

                let series_ids = tskv.get_series_id_list(database, table, &[]).unwrap();
                assert!(series_ids.is_empty());
            }

            tskv.drop_database(database).unwrap();

            {
                let db = tskv.version_set.read().get_db(database);
                assert!(db.is_none());

                let table_schema = tskv.get_table_schema(database, table).unwrap();
                assert!(table_schema.is_none());

                let series_ids = tskv.get_series_id_list(database, table, &[]).unwrap();
                assert!(series_ids.is_empty());
            }
        });
    }
}
