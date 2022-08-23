use std::{collections::HashMap, io::Result as IoResultExt, sync, sync::Arc, thread::JoinHandle};

use futures::stream::SelectNextSome;
use libc::printf;
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};

use ::models::{FieldInfo, InMemPoint, SeriesInfo, Tag, ValueType};
use models::{FieldId, SeriesId, SeriesKey, Timestamp};
use protos::models::Points;
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use trace::{debug, error, info, trace, warn};

use crate::engine::Engine;

use crate::database;
use crate::index::utils::unite_id;
use crate::index::IndexResult;
use crate::memcache::MemRaw;
use crate::tsm::{DataBlock, MAX_BLOCK_VALUES};
use crate::{
    compaction::{self, run_flush_memtable_job, CompactReq, FlushReq},
    context::GlobalContext,
    error::{self, Result},
    file_manager::{self, FileManager},
    file_utils,
    index::db_index,
    kv_option::{DBOptions, Options, QueryOption, TseriesFamDesc, TseriesFamOpt, WalConfig},
    memcache::{DataType, MemCache},
    record_file::Reader,
    summary,
    summary::{Summary, SummaryProcessor, SummaryTask, VersionEdit},
    tseries_family::{SuperVersion, TimeRange, Version},
    tsm::TsmTombstone,
    version_set,
    version_set::VersionSet,
    wal::{self, WalEntryType, WalManager, WalTask},
    Error, Task, TseriesFamilyId,
};

pub struct Entry {
    pub series_id: u64,
}

#[derive(Debug)]
pub struct TsKv {
    options: Arc<Options>,
    global_ctx: Arc<GlobalContext>,
    version_set: Arc<RwLock<VersionSet>>,

    wal_sender: UnboundedSender<WalTask>,

    flush_task_sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>,
    compact_task_sender: UnboundedSender<TseriesFamilyId>,
    summary_task_sender: UnboundedSender<SummaryTask>,
}

impl TsKv {
    pub async fn open(opt: Options, ts_family_num: u32) -> Result<TsKv> {
        let shared_options = Arc::new(opt);
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel();
        let (compact_task_sender, compact_task_receiver) = mpsc::unbounded_channel();

        let (version_set, summary) = Self::recover_summary(
            shared_options.clone(),
            ts_family_num,
            shared_options.ts_family.clone(),
        )
        .await;

        let wal_cfg = shared_options.wal.clone();

        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (summary_task_sender, summary_task_receiver) = mpsc::unbounded_channel();
        let core = Self {
            version_set,
            global_ctx: summary.global_context(),
            wal_sender,
            flush_task_sender,
            options: shared_options,
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
        core.run_summary_job(summary, summary_task_receiver, summary_task_sender);

        Ok(core)
    }

    async fn recover_summary(
        opt: Arc<Options>,
        ts_family_num: u32,
        ts_family_opt: Arc<TseriesFamOpt>,
    ) -> (Arc<RwLock<VersionSet>>, Summary) {
        if !file_manager::try_exists(&opt.db.db_path) {
            std::fs::create_dir_all(&opt.db.db_path)
                .context(error::IOSnafu)
                .unwrap();
        }
        let summary_file = file_utils::make_summary_file(&opt.db.db_path, 0);
        let summary = if file_manager::try_exists(&summary_file) {
            Summary::recover(opt.db.clone(), ts_family_num, ts_family_opt)
                .await
                .unwrap()
        } else {
            Summary::new(opt.db.clone(), ts_family_num, ts_family_opt)
                .await
                .unwrap()
        };
        let version_set = summary.version_set();

        (version_set, summary)
    }

    async fn recover_wal(&self) {
        let wal_manager = WalManager::new(self.options.wal.clone());

        wal_manager
            .recover(
                self,
                self.global_ctx.clone(),
                self.flush_task_sender.clone(),
            )
            .await
            .unwrap();
    }

    pub fn read_point(
        &self,
        db: &String,
        time_range: &TimeRange,
        field_id: FieldId,
    ) -> Vec<DataBlock> {
        let mut data = vec![];
        let mut super_version: Option<Arc<SuperVersion>> = None;
        {
            let version_set = self.version_set.read();
            if let Some(tsf) = version_set.get_tsfamily_by_name(db) {
                super_version = Some(tsf.read().super_version());
            } else {
                warn!("ts_family with db name{} not found.", db);
            }
        };
        if let Some(sv) = super_version {
            // get data from memcache
            if let Some(mem_entry) = sv.caches.mut_cache.read().data_cache.get(&field_id) {
                data.append(&mut mem_entry.read_cell(time_range));
            }

            // get data from delta_memcache
            if let Some(mem_entry) = sv.caches.delta_mut_cache.read().data_cache.get(&field_id) {
                data.append(&mut mem_entry.read_cell(time_range));
            }

            // get data from immut_delta_memcache
            for mem_cache in sv.caches.delta_immut_cache.iter() {
                if mem_cache.read().flushed {
                    continue;
                }
                if let Some(mem_entry) = mem_cache.read().data_cache.get(&field_id) {
                    data.append(&mut mem_entry.read_cell(time_range));
                }
            }

            // get data from im_memcache
            for mem_cache in sv.caches.immut_cache.iter() {
                if mem_cache.read().flushed {
                    continue;
                }
                if let Some(mem_entry) = mem_cache.read().data_cache.get(&field_id) {
                    data.append(&mut mem_entry.read_cell(time_range));
                }
            }

            // get data from levelinfo
            for level_info in sv.version.levels_info.iter() {
                if level_info.level == 0 {
                    continue;
                }
                data.append(&mut level_info.read_column_file(sv.ts_family_id, field_id, time_range))
            }

            // get data from delta
            let level_info = sv.version.levels_info();
            data.append(&mut level_info[0].read_column_file(sv.ts_family_id, field_id, time_range))
        }
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
        tokio::spawn(f);
        warn!("job 'WAL' started.");
    }

    fn run_flush_job(
        &self,
        mut receiver: UnboundedReceiver<Arc<Mutex<Vec<FlushReq>>>>,
        ctx: Arc<GlobalContext>,
        version_set: Arc<RwLock<VersionSet>>,
        summary_task_sender: UnboundedSender<SummaryTask>,
        compact_task_sender: UnboundedSender<TseriesFamilyId>,
    ) {
        let f = async move {
            while let Some(x) = receiver.recv().await {
                run_flush_memtable_job(
                    x.clone(),
                    ctx.clone(),
                    HashMap::new(),
                    version_set.clone(),
                    summary_task_sender.clone(),
                    compact_task_sender.clone(),
                )
                .await
                .unwrap();
            }
        };
        tokio::spawn(f);
        warn!("Flush task handler started");
    }

    fn run_compact_job(
        &self,
        mut receiver: UnboundedReceiver<TseriesFamilyId>,
        ctx: Arc<GlobalContext>,
        version_set: Arc<RwLock<VersionSet>>,
        summary_task_sender: UnboundedSender<SummaryTask>,
    ) {
        tokio::spawn(async move {
            while let Some(ts_family_id) = receiver.recv().await {
                if let Some(tsf) = version_set.read().get_tsfamily_by_tf_id(ts_family_id) {
                    info!("Starting compaction on ts_family {}", ts_family_id);
                    if let Some(compact_req) = tsf.read().pick_compaction() {
                        match compaction::run_compaction_job(compact_req, ctx.clone()) {
                            Ok(Some(version_edit)) => {
                                let (summary_tx, summary_rx) = oneshot::channel();
                                let ret = summary_task_sender.send(SummaryTask {
                                    edits: vec![version_edit],
                                    cb: summary_tx,
                                });
                                // TODO Handle summary result using summary_rx.
                            }
                            Ok(None) => {
                                info!("There is nothing to compact.");
                            }
                            Err(e) => {
                                error!("Compaction job failed: {:?}", e);
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
        summary_task_sender: UnboundedSender<SummaryTask>,
    ) {
        let f = async move {
            let mut summary_processor = summary::SummaryProcessor::new(Box::new(summary));
            while let Some(x) = summary_task_receiver.recv().await {
                debug!("Apply Summary task");
                summary_processor.batch(x);
                summary_processor.apply().await;
            }
        };
        tokio::spawn(f);
        warn!("Summary task handler started");
    }

    pub fn start(tskv: Arc<TsKv>, mut req_rx: UnboundedReceiver<Task>) {
        warn!("job 'main' starting.");
        let f = async move {
            while let Some(command) = req_rx.recv().await {
                match command {
                    Task::WritePoints { req, tx } => {
                        debug!("writing points.");
                        match tskv.write(req).await {
                            Ok(resp) => {
                                let _ret = tx.send(Ok(resp));
                            }
                            Err(err) => {
                                info!("write points error {:?}", err);
                                let _ret = tx.send(Err(err));
                            }
                        }
                        debug!("write points completed.");
                    }
                    _ => panic!("unimplemented."),
                }
            }
        };

        tokio::spawn(f);
        warn!("job 'main' started.");
    }

    // pub async fn query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
    //     Ok(None)
    // }
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn write(&self, write_batch: WritePointsRpcRequest) -> Result<WritePointsRpcResponse> {
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = String::from_utf8(fb_points.database().unwrap().to_vec())
            .map_err(|err| Error::ErrCharacterSet)?;

        let db = self.version_set.write().create_db(&db_name);
        let mut db = db.write();
        let mem_points = db.build_mem_points(fb_points.points().unwrap())?;

        let (cb, rx) = oneshot::channel();
        self.wal_sender
            .send(WalTask::Write { cb, points })
            .map_err(|err| Error::Send)?;
        let (seq, _) = rx.await.context(error::ReceiveSnafu)??;

        let tsf = match db.get_tsfamily_random() {
            Some(v) => v,
            None => db.add_tsfamily(
                0,
                seq,
                self.global_ctx.file_id_next(),
                self.options.ts_family.clone(),
                self.summary_task_sender.clone(),
            ),
        };

        tsf.write()
            .put_points(seq, &mem_points, self.flush_task_sender.clone())
            .await;

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
        let mut db = db.write();
        let mem_points = db.build_mem_points(fb_points.points().unwrap())?;

        let tsf = match db.get_tsfamily_random() {
            Some(v) => v,
            None => db.add_tsfamily(
                0,
                seq,
                self.global_ctx.file_id_next(),
                self.options.ts_family.clone(),
                self.summary_task_sender.clone(),
            ),
        };

        tsf.write()
            .put_points(seq, &mem_points, self.flush_task_sender.clone())
            .await;

        return Ok(WritePointsRpcResponse {
            version: 1,
            points: vec![],
        });
    }

    fn read(
        &self,
        db: &String,
        sids: Vec<SeriesId>,
        time_range: &TimeRange,
        fields: Vec<u32>,
    ) -> HashMap<SeriesId, HashMap<u32, Vec<DataBlock>>> {
        // get data block
        let mut ans = HashMap::new();
        for sid in sids {
            let sid_entry = ans.entry(sid).or_insert(HashMap::new());
            for field_id in fields.iter() {
                let field_id_entry = sid_entry.entry(*field_id).or_insert(vec![]);
                let fid = unite_id((*field_id).into(), sid);
                field_id_entry.append(&mut self.read_point(db, time_range, fid));
            }
        }

        // sort data block, max block size 1000
        let mut final_ans = HashMap::new();
        for i in ans {
            let sid_entry = final_ans.entry(i.0).or_insert(HashMap::new());
            for j in i.1 {
                let field_id_entry = sid_entry.entry(j.0).or_insert(vec![]);
                field_id_entry.append(&mut DataBlock::merge_blocks(j.1, MAX_BLOCK_VALUES));
            }
        }

        final_ans
    }

    //todo...
    async fn delete_series(
        &self,
        name: &String,
        sids: Vec<SeriesId>,
        min: Timestamp,
        max: Timestamp,
    ) -> Result<()> {
        let vs = self.version_set.read();
        let db = vs.get_db(name);
        if let None = db {
            return Ok(());
        }
        let db = db.unwrap();

        let series_infos = db.read().get_index().write().get_series_info_list(&sids);

        let timerange = TimeRange {
            max_ts: max,
            min_ts: min,
        };
        let path = self.options.db.db_path.clone();
        for mut series_info in series_infos {
            let mut super_version: Option<Arc<SuperVersion>> = None;
            {
                if let Some(tsf) = db.read().get_tsfamily_random() {
                    tsf.write()
                        .delete_cache(&TimeRange {
                            min_ts: min,
                            max_ts: max,
                        })
                        .await;
                    super_version = Some(tsf.read().super_version())
                }
            };

            if let Some(sv) = super_version {
                for level in sv.version.levels_info() {
                    if level.time_range.overlaps(&timerange) {
                        for column_file in level.files.iter() {
                            if column_file.time_range().overlaps(&timerange) {
                                let field_ids: Vec<FieldId> = series_info
                                    .field_infos()
                                    .iter()
                                    .map(|f| f.field_id())
                                    .collect();
                                let mut tombstone =
                                    TsmTombstone::open_for_write(&path, column_file.file_id())?;
                                tombstone.add_range(&field_ids, min, max)?;
                                tombstone.flush()?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn get_table_schema(&self, name: &String, tab: &String) -> Result<Option<Vec<FieldInfo>>> {
        if let Some(db) = self.version_set.read().get_db(name) {
            let val = db
                .read()
                .get_index()
                .write()
                .get_table_schema(tab)
                .context(error::IndexErrSnafu)?;
            return Ok(val);
        }

        Ok(None)
    }

    async fn get_series_id_list(
        &self,

        name: &String,
        tab: &String,
        tags: &Vec<Tag>,
    ) -> IndexResult<Vec<u64>> {
        if let Some(db) = self.version_set.read().get_db(name) {
            return db
                .read()
                .get_index()
                .write()
                .get_series_id_list(tab, tags)
                .await;
        }

        Ok(vec![])
    }

    fn get_series_key(&self, name: &String, sid: u64) -> IndexResult<Option<SeriesKey>> {
        if let Some(db) = self.version_set.read().get_db(name) {
            return db.read().get_index().write().get_series_key(sid);
        }

        Ok(None)
    }
}
