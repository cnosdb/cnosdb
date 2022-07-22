use std::{collections::HashMap, io::Result as IoResultExt, sync, sync::Arc, thread::JoinHandle};

use ::models::{FieldInfo, InMemPoint, SeriesInfo, Tag, ValueType};
use futures::stream::SelectNextSome;
use models::{FieldId, SeriesId, Timestamp};
use parking_lot::{Mutex, RwLock};
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use snafu::ResultExt;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use trace::{debug, error, info, trace, warn};

use crate::{
    compaction::{run_flush_memtable_job, FlushReq},
    context::GlobalContext,
    error::{self, Result},
    file_manager::{self, FileManager},
    file_utils,
    forward_index::ForwardIndex,
    kv_option::{DBOptions, Options, QueryOption, TseriesFamDesc, TseriesFamOpt, WalConfig},
    memcache::{DataType, MemCache},
    record_file::Reader,
    runtime::WorkerQueue,
    summary::{Summary, SummaryProcesser, SummaryTask, VersionEdit},
    tseries_family::{TimeRange, Version},
    tsm::TsmTombstone,
    version_set,
    version_set::VersionSet,
    wal::{self, WalEntryType, WalManager, WalTask},
    Error, Task,
};

pub struct Entry {
    pub series_id: u64,
}

pub struct TsKv {
    options: Arc<Options>,
    kvctx: Arc<KvContext>,
    version_set: Arc<RwLock<VersionSet>>,

    wal_sender: UnboundedSender<WalTask>,
    forward_index: Arc<RwLock<ForwardIndex>>,

    flush_task_sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>,
    summary_task_sender: UnboundedSender<SummaryTask>,
}

impl TsKv {
    pub async fn open(opt: Options) -> Result<Self> {
        let shared_options = Arc::new(opt);
        let kvctx = Arc::new(KvContext::new(shared_options.clone()));
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel();
        let (version_set, summary) =
            Self::recover(shared_options.clone(), flush_task_sender.clone()).await;
        let mut fidx = ForwardIndex::new(&shared_options.forward_index_conf.path);
        fidx.load_cache_file().await.map_err(|err| Error::LogRecordErr { source: err })?;
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (summary_task_sender, summary_task_receiver) = mpsc::unbounded_channel();
        let core = Self { options: shared_options,
                          kvctx,
                          forward_index: Arc::new(RwLock::new(fidx)),
                          version_set,
                          wal_sender,
                          flush_task_sender,
                          summary_task_sender: summary_task_sender.clone() };
        core.run_wal_job(wal_receiver);
        core.run_flush_job(flush_task_receiver,
                           summary.global_context(),
                           summary.version_set(),
                           summary_task_sender.clone());
        core.run_summary_job(summary, summary_task_receiver, summary_task_sender);

        Ok(core)
    }

    async fn recover(opt: Arc<Options>,
                     flush_task_sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>)
                     -> (Arc<RwLock<VersionSet>>, Summary) {
        if !file_manager::try_exists(&opt.db.db_path) {
            std::fs::create_dir_all(&opt.db.db_path).context(error::IOSnafu).unwrap();
        }
        let summary_file = file_utils::make_summary_file(&opt.db.db_path, 0);
        let summary = if file_manager::try_exists(&summary_file) {
            Summary::recover(&opt.db).await.unwrap()
        } else {
            Summary::new(&opt.db).await.unwrap()
        };
        let version_set = summary.version_set().clone();
        let wal_manager = WalManager::new(opt.wal.clone());
        wal_manager.recover(version_set.clone(),
                            summary.global_context().clone(),
                            flush_task_sender)
                   .await
                   .unwrap();

        (version_set.clone(), summary)
    }

    pub async fn write(&self,
                       write_batch: WritePointsRpcRequest)
                       -> Result<WritePointsRpcResponse> {
        let shared_write_batch = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch)
            .context(error::InvalidFlatbufferSnafu)?;

        // get or create forward index
        for point in fb_points.points().unwrap() {
            let info = SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu)?;
            self.forward_index
                .write()
                .add_series_info_if_not_exists(info)
                .await
                .context(error::ForwardIndexErrSnafu)?;
        }

        // write wal
        let (cb, rx) = oneshot::channel();
        self.wal_sender
            .send(WalTask::Write { points: shared_write_batch.clone(), cb })
            .map_err(|err| Error::Send)?;
        let (seq, _) = rx.await.context(error::ReceiveSnafu)??;

        // write memcache
        if let Some(points) = fb_points.points() {
            let mut version_set = self.version_set.write();
            for point in points.iter() {
                let p = InMemPoint::from(point);
                let sid = p.series_id();
                if let Some(tsf) = version_set.get_tsfamily(sid) {
                    for f in p.fields().iter() {
                        tsf.put_mutcache(f.field_id(),
                                         &f.value,
                                         f.value_type,
                                         seq,
                                         point.timestamp() as i64,
                                         self.flush_task_sender.clone())
                           .await;
                    }
                } else {
                    warn!("ts_family for sid {} not found.", sid);
                }
            }
        }

        // let _ = self.kvctx.shard_write(0, write_batch).await;
        Ok(WritePointsRpcResponse { version: 1, points: vec![] })
    }

    pub async fn read_point(&self, sid: SeriesId, time_range: &TimeRange, field_id: FieldId) {
        let version_set = self.version_set.read();
        if let Some(tsf) = version_set.get_tsfamily_immut(sid) {
            // get data from memcache
            if let Some(mem_entry) = tsf.cache().read().data_cache.get(&field_id) {
                info!("memcache::{}::{}", sid.clone(), field_id);
                mem_entry.read_cell(time_range);
            }

            // get data from delta_memcache
            if let Some(mem_entry) = tsf.delta_cache().read().data_cache.get(&field_id) {
                info!("delta memcache::{}::{}", sid.clone(), field_id);
                mem_entry.read_cell(time_range);
            }

            // get data from immut_delta_memcache
            for mem_cache in tsf.delta_immut_cache().iter() {
                if mem_cache.read().flushed {
                    continue;
                }
                if let Some(mem_entry) = mem_cache.read().data_cache.get(&field_id) {
                    info!("delta im_memcache::{}::{}", sid.clone(), field_id);
                    mem_entry.read_cell(time_range);
                }
            }

            // get data from im_memcache
            for mem_cache in tsf.im_cache().iter() {
                if mem_cache.read().flushed {
                    continue;
                }
                if let Some(mem_entry) = mem_cache.read().data_cache.get(&field_id) {
                    info!("im_memcache::{}::{}", sid.clone(), field_id);
                    mem_entry.read_cell(time_range);
                }
            }

            // get data from levelinfo
            for level_info in tsf.version().read().levels_info.iter() {
                if level_info.level == 0 {
                    continue;
                }
                info!("levelinfo::{}::{}", sid.clone(), field_id);
                level_info.read_columnfile(tsf.tf_id(), field_id, time_range);
            }

            // get data from delta
            let level_info = &tsf.version().read().levels_info;
            if !level_info.is_empty() {
                info!("delta::{}::{}", sid.clone(), field_id);
                level_info[0].read_columnfile(tsf.tf_id(), field_id, time_range);
            }
        } else {
            warn!("ts_family with sid {} not found.", sid);
        }
    }

    pub async fn read(&self, sids: Vec<SeriesId>, time_range: &TimeRange, fields: Vec<FieldId>) {
        for sid in sids {
            for field_id in fields.iter() {
                self.read_point(sid, time_range, *field_id).await;
            }
        }
    }

    pub async fn delete_series(&self,
                               sids: Vec<SeriesId>,
                               min: Timestamp,
                               max: Timestamp)
                               -> Result<()> {
        let series_infos = self.forward_index.read().get_series_info_list(&sids);
        let timerange = TimeRange { max_ts: max, min_ts: min };
        let path = self.options.db.db_path.clone();
        for series_info in series_infos {
            let vs = self.version_set.read();
            if let Some(tsf) = vs.get_tsfamily_immut(series_info.series_id()) {
                tsf.delete_cache(&TimeRange { min_ts: min, max_ts: max }).await;
                let version = tsf.version().read();
                for level in version.levels_info() {
                    if level.ts_range.overlaps(&timerange) {
                        for column_file in level.files.iter() {
                            if column_file.range().overlaps(&timerange) {
                                let field_ids: Vec<FieldId> = series_info.field_infos()
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

    pub async fn insert_cache(&self, seq: u64, buf: &[u8]) -> Result<()> {
        let ps =
            flatbuffers::root::<fb_models::Points>(buf).context(error::InvalidFlatbufferSnafu)?;
        if let Some(points) = ps.points() {
            let mut version_set = self.version_set.write();
            for point in points.iter() {
                let p = InMemPoint::from(point);
                // use sid to dispatch to tsfamily
                // so if you change the colume name
                // please keep the series id
                let sid = p.series_id();
                if let Some(tsf) = version_set.get_tsfamily(sid) {
                    for f in p.fields().iter() {
                        tsf.put_mutcache(f.field_id(),
                                         &f.value,
                                         f.value_type,
                                         seq,
                                         point.timestamp() as i64,
                                         self.flush_task_sender.clone())
                           .await
                    }
                }
            }
        }

        Ok(())
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
                        let ret = wal_manager.write(wal::WalEntryType::Write, &points).await;
                        let send_ret = cb.send(ret);
                        match send_ret {
                            Ok(wal_result) => {},
                            Err(err) => {
                                warn!("send WAL write result failed.")
                            },
                        }
                    },
                }
            }
        };
        tokio::spawn(f);
        warn!("job 'WAL' started.");
    }

    fn run_flush_job(&self,
                     mut receiver: UnboundedReceiver<Arc<Mutex<Vec<FlushReq>>>>,
                     ctx: Arc<GlobalContext>,
                     version_set: Arc<RwLock<VersionSet>>,
                     sender: UnboundedSender<SummaryTask>) {
        let f = async move {
            while let Some(x) = receiver.recv().await {
                run_flush_memtable_job(x.clone(),
                                       ctx.clone(),
                                       HashMap::new(),
                                       version_set.clone(),
                                       sender.clone()).await
                                                      .unwrap();
            }
        };
        tokio::spawn(f);
        warn!("Flush task handler started");
    }

    fn run_summary_job(&self,
                       summary: Summary,
                       mut summary_task_receiver: UnboundedReceiver<SummaryTask>,
                       summary_task_sender: UnboundedSender<SummaryTask>) {
        let f = async move {
            let mut summary_processer = SummaryProcesser::new(Box::new(summary));
            while let Some(x) = summary_task_receiver.recv().await {
                debug!("Apply Summary task");
                summary_processer.batch(x);
                summary_processer.apply().await;
            }
        };
        tokio::spawn(f);
        warn!("Summary task handler started");
    }

    pub fn start(tskv: TsKv, mut req_rx: UnboundedReceiver<Task>) {
        warn!("job 'main' starting.");
        let f = async move {
            while let Some(command) = req_rx.recv().await {
                match command {
                    Task::WritePoints { req, tx } => {
                        warn!("writing points.");
                        match tskv.write(req).await {
                            Ok(resp) => {
                                let _ret = tx.send(Ok(resp));
                            },
                            Err(err) => {
                                let _ret = tx.send(Err(err));
                            },
                        }
                        warn!("write points completed.");
                    },
                    _ => panic!("unimplented."),
                }
            }
        };

        tokio::spawn(f);
        warn!("job 'main' started.");
    }

    pub fn version_set(&self) -> Arc<RwLock<VersionSet>> {
        self.version_set.clone()
    }
    pub async fn query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}

#[allow(dead_code)]
enum KvStatus {
    Init,
    Recover,
    Runing,
    Closed,
}

#[allow(dead_code)]
pub(crate) struct KvContext {
    front_handler: Arc<WorkerQueue>,
    handler: Vec<JoinHandle<()>>,
    status: KvStatus,
}

#[allow(dead_code)]
impl KvContext {
    pub fn new(opt: Arc<Options>) -> Self {
        let front_work_queue = Arc::new(WorkerQueue::new(opt.db.front_cpu));
        let worker_handle = Vec::with_capacity(opt.db.back_cpu);
        Self { front_handler: front_work_queue, handler: worker_handle, status: KvStatus::Init }
    }

    pub fn recover(&mut self) -> Result<()> {
        // todo: index wal summary shardinfo recover here.
        self.status = KvStatus::Recover;
        Ok(())
    }

    pub async fn shard_write(&self,
                             partion_id: usize,
                             mem: Arc<RwLock<MemCache>>,
                             entry: WritePointsRpcRequest)
                             -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.front_handler.add_task(partion_id, async move {
                               let ps =
                                   flatbuffers::root::<fb_models::Points>(&entry.points).unwrap();
                               let err = 0;
                               // todo
                               let _ = tx.send(err);
                           })?;
        rx.await.unwrap();
        Ok(())
    }
    pub async fn shard_query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use chrono::Local;
    use config::GLOBAL_CONFIG;
    use futures::{channel::oneshot, future::join_all, SinkExt};
    use models::{FieldInfo, SeriesInfo, Tag, ValueType};
    use protos::{
        kv_service, kv_service::WritePointsRpcResponse, models as fb_models, models_helper,
    };
    use serial_test::serial;
    use snafu::ResultExt;
    use tokio::sync::{mpsc, oneshot::channel};
    use trace::{debug, error, info, init_default_global_tracing, warn};

    use crate::{
        error,
        kv_option::{TseriesFamDesc, TseriesFamOpt, WalConfig},
        summary::{Summary, VersionEdit},
        tseries_family::TimeRange,
        Error, Task, TsKv,
    };

    async fn get_tskv() -> TsKv {
        let opt = crate::kv_option::Options { wal: Arc::new(WalConfig { dir: String::from("/tmp/test/wal"),
                                                                        ..Default::default() }),
                                              ..Default::default() };

        TsKv::open(opt).await.unwrap()
    }

    #[tokio::test]
    #[serial]
    async fn test_init() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        dbg!("Ok");
    }

    #[tokio::test]
    #[serial]
    async fn test_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request).await.unwrap();
    }

    // remove repeat sid and fields_id
    pub fn remove_duplicates(nums: &mut [u64]) -> usize {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        if nums.len() <= 1 {
            return nums.len() as usize;
        }
        let mut l = 1;
        for r in 1..nums.len() {
            if nums[r] != nums[l - 1] {
                nums[l] = nums[r];
                l += 1;
            }
        }
        l as usize
    }

    // tips : to test all read method, we can use a small MAX_MEMCACHE_SIZE
    #[tokio::test]
    #[serial]
    async fn test_read() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 20);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request.clone()).await.unwrap();

        let shared_write_batch = Arc::new(request.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch).context(error::InvalidFlatbufferSnafu).unwrap();
        let mut sids = vec![];
        let mut fields_id = vec![];
        for point in fb_points.points().unwrap() {
            let mut info =
                SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu).unwrap();
            info.finish();
            sids.push(info.series_id());
            for field in info.field_infos().iter() {
                fields_id.push(field.field_id());
            }
        }

        sids.sort_unstable();
        fields_id.sort_unstable();
        let l = remove_duplicates(&mut sids);
        sids = sids[0..l].to_owned();
        let l = remove_duplicates(&mut fields_id);
        fields_id = fields_id[0..l].to_owned();
        tskv.read(sids, &TimeRange::new(Local::now().timestamp_millis() + 100, 0), fields_id).await;
    }

    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_delete_cache() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;
        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 5);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request.clone()).await.unwrap();

        let shared_write_batch = Arc::new(request.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch).context(error::InvalidFlatbufferSnafu).unwrap();
        let mut sids = vec![];
        let mut fields_id = vec![];
        for point in fb_points.points().unwrap() {
            let mut info =
                SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu).unwrap();
            info.finish();
            sids.push(info.series_id());
            for field in info.field_infos().iter() {
                fields_id.push(field.field_id());
            }
        }

        sids.sort_unstable();
        fields_id.sort_unstable();
        let l = remove_duplicates(&mut sids);
        sids = sids[0..l].to_owned();
        let l = remove_duplicates(&mut fields_id);
        fields_id = fields_id[0..l].to_owned();
        tskv.read(sids.clone(),
                  &TimeRange::new(Local::now().timestamp_millis() + 100, 0),
                  fields_id.clone())
            .await;
        info!("delete delta data");
        tskv.delete_series(sids.clone(), 1, 1).await.unwrap();
        tskv.read(sids.clone(),
                  &TimeRange::new(Local::now().timestamp_millis() + 100, 0),
                  fields_id.clone())
            .await;
    }

    #[tokio::test]
    #[serial]
    #[ignore]
    async fn test_big_write() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        for i in 0..100 {
            let database = "db".to_string();
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points = models_helper::create_big_random_points(&mut fbb, 10);
            fbb.finish(points, None);
            let points = fbb.finished_data().to_vec();

            let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

            tskv.write(request).await.unwrap();
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_insert_cache() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data();

        tskv.insert_cache(1, points).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_start() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;

        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (tx, rx) = channel();

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_with_delta(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let req = kv_service::WritePointsRpcRequest { version: 1, database, points };

        wal_sender.send(Task::WritePoints { req, tx }).unwrap();

        TsKv::start(tskv, wal_receiver);

        match rx.await {
            Ok(Ok(resp)) => info!("successful"),
            _ => panic!("wrong"),
        };
    }

    #[tokio::test]
    #[serial]
    async fn test_add_del_tsf() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;
        let opt = crate::kv_option::Options { wal: Arc::new(WalConfig { dir: String::from("/tmp/test/wal"),
                                                                        ..Default::default() }),
                                              ..Default::default() };
        let shared_options = Arc::new(opt);
        let summary = Summary::new(&shared_options.db).await.unwrap();
        summary.global_context().next_tsf_id();
        let tf_id = summary.global_context().max_tsf_id();

        tskv.version_set()
            .write()
            .add_tsfamily(tf_id,
                          "hello".to_string(),
                          0,
                          0,
                          Arc::new(TseriesFamOpt::default()),
                          tskv.summary_task_sender.clone())
            .await;
        assert_eq!(tskv.version_set().read().tsf_num(), (GLOBAL_CONFIG.tsfamily_num + 1) as usize);

        tskv.version_set()
            .write()
            .del_tsfamily(tf_id, "hello".to_string(), tskv.summary_task_sender.clone());
        assert_eq!(tskv.version_set().read().tsf_num(), GLOBAL_CONFIG.tsfamily_num as usize);

        info!("success");
    }

    #[tokio::test]
    #[serial]
    async fn test_flush_delta() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;
        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points_include_delta(&mut fbb, 20);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_log() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let tskv = get_tskv().await;
        info!("hello");
        warn!("hello");
        debug!("hello");
        error!("hello"); //maybe we can use panic directly
        // panic!("hello");
    }
}
