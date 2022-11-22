use std::time::Duration;
use std::{collections::HashMap, panic, sync::Arc};

use crate::tsm::codec::get_str_codec;
use datafusion::prelude::Column;
use flatbuffers::FlatBufferBuilder;
use futures::stream::SelectNextSome;
use futures::FutureExt;
use libc::printf;
use models::predicate::domain::{ColumnDomains, PredicateRef};
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};
use tokio::{
    runtime::Runtime,
    sync::{
        broadcast::{self, Receiver as BroadcastReceiver, Sender as BroadcastSender},
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::Instant,
};

use crate::error::SendSnafu;
use metrics::{incr_compaction_failed, incr_compaction_success, sample_tskv_compaction_duration};
use models::codec::Encoding;
use models::schema::{DatabaseSchema, TableColumn, TableSchema};
use models::{
    utils::unite_id, ColumnId, FieldId, FieldInfo, InMemPoint, SeriesId, SeriesKey, Tag, Timestamp,
    ValueType,
};
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use trace::{debug, error, info, trace, warn};

use crate::database::Database;
use crate::file_system::file_manager::{self, FileManager};
use crate::index::index_manger;
use crate::{
    compaction::{self, run_flush_memtable_job, CompactReq, FlushReq},
    context::GlobalContext,
    database,
    engine::Engine,
    error::{self, IndexErrSnafu, Result},
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
    close_sender: BroadcastSender<UnboundedSender<()>>,
}

impl TsKv {
    pub async fn open(opt: Options, runtime: Arc<Runtime>) -> Result<TsKv> {
        let shared_options = Arc::new(opt);
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel();
        let (compact_task_sender, compact_task_receiver) = mpsc::unbounded_channel();
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (summary_task_sender, summary_task_receiver) = mpsc::unbounded_channel();
        let (close_sender, _close_receiver) = broadcast::channel(1);
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
            close_sender,
        };

        let wal_manager = core.recover_wal().await;
        core.run_wal_job(wal_manager, wal_receiver);
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

    pub async fn close(&self) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        if let Err(e) = self.close_sender.send(tx) {
            error!("Failed to broadcast close signal: {:?}", e);
        }
        while let Some(_x) = rx.recv().await {
            continue;
        }
        info!("TsKv closed");
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

    async fn recover_wal(&self) -> WalManager {
        let wal_manager = WalManager::open(self.options.wal.clone()).await.unwrap();

        wal_manager
            .recover(self, self.global_ctx.clone())
            .await
            .unwrap();

        wal_manager
    }

    fn run_wal_job(&self, mut wal_manager: WalManager, mut receiver: UnboundedReceiver<WalTask>) {
        warn!("job 'WAL' starting.");
        let mut close_receiver = self.close_sender.subscribe();
        let f = async move {
            loop {
                tokio::select! {
                    wal_task = receiver.recv() => {
                        match wal_task {
                            Some(WalTask::Write { points, cb }) => {
                                // write wal
                                let ret = wal_manager.write(WalEntryType::Write, points).await;
                                let send_ret = cb.send(ret);
                                match send_ret {
                                    Ok(wal_result) => {}
                                    Err(err) => {
                                        warn!("send WAL write result failed.")
                                    }
                                }
                            }
                            _ => {
                                break;
                            }
                        }
                    }
                    close_task = close_receiver.recv() => {
                        info!("job 'WAL' closing.");
                        if let Err(e) = wal_manager.close().await {
                            error!("Failed to close wal: {:?}", e);
                        }
                        info!("job 'WAL' closed.");
                        if let Ok(tx) = close_task {
                            if let Err(e) = tx.send(()) {
                                error!("Failed to send wal closed signal: {:?}", e);
                            }
                        }
                        break;
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
                .await
                .unwrap()
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
                    let req = tsf.read().pick_compaction();
                    if let Some(compact_req) = req {
                        let database = compact_req.database.clone();
                        let compact_ts_family = compact_req.ts_family_id;
                        let out_level = compact_req.out_level;
                        match compaction::run_compaction_job(compact_req, ctx.clone()).await {
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
    pub async fn compact(&self, database: &str) {
        let db = self.version_set.read().get_db(database);
        if let Some(db) = db {
            // TODO: stop current and prevent next flush and compaction.
            let read_db = db.read();
            for (ts_family_id, ts_family) in read_db.ts_families() {
                let req = ts_family.read().pick_compaction();
                if let Some(compact_req) = req {
                    match compaction::run_compaction_job(compact_req, self.global_ctx.clone()).await
                    {
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
            None => self
                .version_set
                .write()
                .create_db(DatabaseSchema::new(&db_name)),
        };
        let write_group = db.read().build_write_group(fb_points.points().unwrap())?;

        let mut seq = 0;
        if self.options.wal.enabled {
            let (cb, rx) = oneshot::channel();
            let mut enc_points = Vec::with_capacity(points.len() / 2);
            get_str_codec(Encoding::Snappy)
                .encode(&[&points], &mut enc_points)
                .map_err(|_| Error::Send)?;
            self.wal_sender
                .send(WalTask::Write {
                    cb,
                    points: Arc::new(enc_points),
                })
                .map_err(|err| Error::Send)?;
            seq = rx.await.context(error::ReceiveSnafu)??.0;
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

        let db = self
            .version_set
            .write()
            .create_db(DatabaseSchema::new(&db_name));

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

    fn create_database(&self, schema: &DatabaseSchema) -> Result<()> {
        if self.version_set.read().db_exists(&schema.name) {
            return Err(Error::DatabaseAlreadyExists {
                database: schema.name.clone(),
            });
        }
        self.version_set.write().create_db(schema.clone());
        Ok(())
    }

    fn list_databases(&self) -> Result<Vec<String>> {
        let version_set = self.version_set.read();
        let dbs = version_set.get_all_db();

        let mut db = Vec::new();
        for (name, _) in dbs.iter() {
            db.push(name.clone())
        }

        Ok(db)
    }

    fn list_tables(&self, database: &str) -> Result<Vec<String>> {
        if let Some(db) = self.version_set.read().get_db(database) {
            Ok(db.read().get_index().list_tables())
        } else {
            error!("Database {}, not found", database);
            Err(Error::DatabaseNotFound {
                database: database.to_string(),
            })
        }
    }

    fn get_db_schema(&self, name: &str) -> Option<DatabaseSchema> {
        self.version_set.read().get_db_schema(name)
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
            index_manger(db_wlock.get_index().path())
                .write()
                .remove_db_index(&database);
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

    fn create_table(&self, schema: &TableSchema) -> Result<()> {
        if let Some(db) = self.version_set.write().get_db(&schema.db) {
            db.read()
                .get_index()
                .create_table(schema)
                .map_err(|e| {
                    error!("failed create database '{}'", e);
                    e
                })
                .context(IndexErrSnafu)
        } else {
            error!("Database {}, not found", schema.db);
            Err(Error::DatabaseNotFound {
                database: schema.db.clone(),
            })
        }
    }

    fn drop_table(&self, database: &str, table: &str) -> Result<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.
        let version_set = self.version_set.clone();
        let database = database.to_string();
        let table = table.to_string();
        self.runtime
            .block_on(database::delete_table_async(database, table, version_set))?;
        Ok(())
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
        let res = self.version_set.read().get_db(database);
        if let Some(db) = res {
            let readable_db = db.read();
            for (ts_family_id, ts_family) in readable_db.ts_families() {
                // TODO Cancel current and prevent next flush or compaction in TseriesFamily provisionally.
                ts_family
                    .write()
                    .delete_cache(&storage_field_ids, time_range);
                let version = ts_family.read().super_version();
                for column_file in version.version.column_files(&storage_field_ids, time_range) {
                    self.runtime
                        .block_on(column_file.add_tombstone(&storage_field_ids, time_range))?;
                }
            }
        };
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

    fn get_series_id_by_filter(
        &self,
        name: &str,
        tab: &str,
        filter: &ColumnDomains<String>,
    ) -> IndexResult<Vec<u64>> {
        let result = if let Some(db) = self.version_set.read().get_db(name) {
            if filter.is_all() {
                // Match all records
                debug!("pushed tags filter is All.");
                db.read().get_index().get_series_id_list(tab, &[])
            } else if filter.is_none() {
                // Does not match any record, return null
                debug!("pushed tags filter is None.");
                Ok(vec![])
            } else {
                // No error will be reported here
                debug!("pushed tags filter is {:?}.", filter);
                let domains = unsafe { filter.domains_unsafe() };
                db.read()
                    .get_index()
                    .get_series_ids_by_domains(tab, domains)
            }
        } else {
            Ok(vec![])
        };

        result
    }

    fn get_series_key(&self, name: &str, sid: u64) -> IndexResult<Option<SeriesKey>> {
        if let Some(db) = self.version_set.read().get_db(name) {
            return db.read().get_series_key(sid);
        }

        Ok(None)
    }

    fn get_db_version(&self, db: &str) -> Result<Option<Arc<SuperVersion>>> {
        let version_set = self.version_set.read();
        if !version_set.db_exists(db) {
            return Err(Error::DatabaseNotFound {
                database: db.to_string(),
            });
        }
        if let Some(tsf) = version_set.get_tsfamily_by_name(db) {
            Ok(Some(tsf.read().super_version()))
        } else {
            warn!("ts_family with db name '{}' not found.", db);
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use config::get_config;
    use flatbuffers::{FlatBufferBuilder, WIPOffset};
    use models::utils::now_timestamp;
    use models::{ColumnId, InMemPoint, SeriesId, SeriesKey, Timestamp};
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
        tskv.compact("public").await;
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

            let field_ids: Vec<ColumnId> = table_schema.columns().iter().map(|f| f.id).collect();
            // let result: HashMap<SeriesId, HashMap<ColumnId, Vec<DataBlock>>> =
            // tskv.read(database, series_ids, time_range, field_ids);
            // println!("Result items: {}", result.len());
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
