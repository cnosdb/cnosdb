use std::{
    borrow::BorrowMut, cell::RefCell, collections::HashMap, ops::DerefMut, sync, sync::Arc,
    thread::JoinHandle,
};

use ::models::{FieldInfo, InMemPoint, SeriesInfo, Tag, ValueType};
use models::{FieldID, SeriesID};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use snafu::ResultExt;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot, RwLock,
    },
};

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
    summary::{Summary, VersionEdit},
    tseries_family::{TimeRange, Version},
    tsm::{BlockReader, TsmBlockReader, TsmIndexReader},
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
}

impl TsKv {
    pub async fn open(opt: Options) -> Result<Self> {
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel();
        let shared_options = Arc::new(opt);
        let kvctx = Arc::new(KvContext::new(shared_options.clone()));
        let version_set = Self::recover(shared_options.clone(), flush_task_sender.clone()).await;
        let mut fidx = ForwardIndex::new(&shared_options.forward_index_conf.path);
        fidx.load_cache_file().await.map_err(|err| Error::LogRecordErr { source: err })?;
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let summary = Summary::recover(&[TseriesFamDesc { name: "default".to_string(),
                                                          opt: TseriesFamOpt::default() }],
                                       &shared_options.db).await
                                                          .unwrap();
        let core = Self { options: shared_options,
                          kvctx,
                          forward_index: Arc::new(RwLock::new(fidx)),
                          version_set,
                          wal_sender,
                          flush_task_sender };
        core.run_wal_job(wal_receiver);
        core.run_flush_job(flush_task_receiver, summary.global_context(), core.version_set.clone());

        Ok(core)
    }

    async fn recover(opt: Arc<Options>,
                     flush_task_sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>)
                     -> Arc<RwLock<VersionSet>> {
        if !file_manager::try_exists(&opt.db.db_path) {
            std::fs::create_dir_all(&opt.db.db_path).context(error::IOSnafu).unwrap();
        }
        let summary_file = file_utils::make_summary_file(&opt.db.db_path, 0);
        let summary = if file_manager::try_exists(&summary_file) {
            Summary::recover(&[TseriesFamDesc { name: "default".to_string(),
                                                opt: TseriesFamOpt::default() }],
                             &opt.db).await
                                     .unwrap()
        } else {
            Summary::new(&[TseriesFamDesc { name: "default".to_string(),
                                            opt: TseriesFamOpt::default() }],
                         &opt.db).await
                                 .unwrap()
        };
        let version_set = summary.version_set();
        let wal_manager = WalManager::new(opt.wal.clone());
        wal_manager.recover(version_set.clone(), summary.global_context(), flush_task_sender)
                   .await
                   .unwrap();

        version_set.clone()
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
                .await
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
            let mut version_set = self.version_set.write().await;
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
                           .await
                    }
                } else {
                    println!("[WARN] [tskv] ts_family for sid {} not found.", sid);
                }
            }
        }

        // let _ = self.kvctx.shard_write(0, write_batch).await;
        Ok(WritePointsRpcResponse { version: 1, points: vec![] })
    }

    pub async fn read_point(&self, sid: SeriesID, time_range: &TimeRange, field_id: FieldID) {
        let mut version_set = self.version_set.write().await;
        if let Some(tsf) = version_set.get_tsfamily(sid) {
            // get data from memcache
            if let Some(mem_entry) = tsf.cache().read().await.data_cache.get(&field_id) {
                println!("memcache::{}::{}", sid.clone(), field_id);
                mem_entry.read_cell(time_range)
            }

            // get data from im_memcache
            for mem_cache in tsf.im_cache().iter() {
                if let Some(mem_entry) = mem_cache.read().await.data_cache.get(&field_id) {
                    println!("im_memcache::{}::{}", sid.clone(), field_id);
                    mem_entry.read_cell(time_range)
                }
            }

            // get data from levelinfo
            for level_info in tsf.version().read().await.levels_info.iter() {
                println!("levelinfo::{}::{}", sid.clone(), field_id);
                level_info.read_columnfile(tsf.tf_id(), field_id, time_range);
            }
        } else {
            println!("ts_family with sid {} not found.", sid);
        }
    }

    pub async fn read(&self, sids: Vec<SeriesID>, time_range: &TimeRange, fields: Vec<FieldID>) {
        for sid in sids {
            for field_id in fields.iter() {
                self.read_point(sid, time_range, *field_id).await;
            }
        }
    }

    pub async fn insert_cache(&self, seq: u64, buf: &[u8]) -> Result<()> {
        let ps =
            flatbuffers::root::<fb_models::Points>(buf).context(error::InvalidFlatbufferSnafu)?;
        if let Some(points) = ps.points() {
            let mut version_set = self.version_set.write().await;
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
        println!("[WARN] [tskv] job 'WAL' starting.");
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
                                println!("[WARN] send WAL write result failed.")
                            },
                        }
                    },
                }
            }
        };
        tokio::spawn(f);
        println!("[WARN] [tskv] job 'WAL' started.");
    }

    fn run_flush_job(&self,
                     mut receiver: UnboundedReceiver<Arc<Mutex<Vec<FlushReq>>>>,
                     ctx: Arc<GlobalContext>,
                     version_set: Arc<RwLock<VersionSet>>) {
        let f = async move {
            while let Some(x) = receiver.recv().await {
                run_flush_memtable_job(x.clone(), ctx.clone(), HashMap::new(), version_set.clone()).await.unwrap();
            }
        };
        tokio::spawn(f);
        println!("[tskv] Flush task handler started");
    }

    pub fn start(tskv: TsKv, mut req_rx: UnboundedReceiver<Task>) {
        println!("[WARN] [tskv] job 'main' starting.");
        let f = async move {
            while let Some(command) = req_rx.recv().await {
                match command {
                    Task::WritePoints { req, tx } => {
                        println!("[WARN] [tskv] writing points.");
                        match tskv.write(req).await {
                            Ok(resp) => {
                                let _ret = tx.send(Ok(resp));
                            },
                            Err(err) => {
                                let _ret = tx.send(Err(err));
                            },
                        }
                        println!("[WARN] [tskv] write points completed.");
                    },
                    _ => panic!("unimplented."),
                }
            }
        };

        tokio::spawn(f);
        println!("[WARN] [tskv] job 'main' started.");
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
    use futures::{channel::oneshot, future::join_all, SinkExt};
    use models::{FieldInfo, SeriesInfo, Tag, ValueType};
    use protos::{
        kv_service, kv_service::WritePointsRpcResponse, models as fb_models, models_helper,
    };
    use snafu::ResultExt;
    use tokio::sync::{mpsc, oneshot::channel};

    use crate::{error, kv_option::WalConfig, tseries_family::TimeRange, Error, Task, TsKv};

    async fn get_tskv() -> TsKv {
        let opt = crate::kv_option::Options { wal: WalConfig { dir: String::from("/tmp/test/wal"),
                                                               ..Default::default() },
                                              ..Default::default() };

        TsKv::open(opt).await.unwrap()
    }

    #[tokio::test]
    async fn test_init() {
        let tskv = get_tskv().await;

        dbg!("Ok");
    }

    #[tokio::test]
    async fn test_write() {
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request).await.unwrap();
    }

    // tips : to test all read method, we can use a small MAX_MEMCACHE_SIZE
    #[tokio::test]
    async fn test_read() -> Result<(), Error> {
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points(&mut fbb, 25);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request.clone()).await.unwrap();

        let shared_write_batch = Arc::new(request.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&shared_write_batch).context(error::InvalidFlatbufferSnafu)?;
        let mut sids = vec![];
        let mut fields_id = vec![];
        for point in fb_points.points().unwrap() {
            let mut info = SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu)?;
            info.finish();
            sids.push(info.series_id());
            for field in info.field_infos().iter() {
                fields_id.push(field.field_id());
            }
        }
        // remove repeat sid and fields_id
        const MAX_APPEAR_TIMES: usize = 1;
        pub fn remove_duplicates(nums: &mut Vec<u64>) -> usize {
            if nums.len() <= MAX_APPEAR_TIMES {
                return nums.len() as usize;
            }
            let mut l = MAX_APPEAR_TIMES;
            for r in l..nums.len() {
                if nums[r] != nums[l - MAX_APPEAR_TIMES] {
                    nums[l] = nums[r];
                    l += 1;
                }
            }
            l as usize
        }
        sids.sort_unstable();
        fields_id.sort_unstable();
        let l = remove_duplicates(&mut sids);
        sids = sids[0..l].to_owned();
        let l = remove_duplicates(&mut fields_id);
        fields_id = fields_id[0..l].to_owned();
        tskv.read(sids, &TimeRange::new(Local::now().timestamp_millis() + 100, 0), fields_id).await;
        Ok(())
    }

    #[tokio::test]
    async fn test_big_write() {
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_big_random_points(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();

        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_insert_cache() {
        let tskv = get_tskv().await;

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data();

        tskv.insert_cache(1, points).await.unwrap();
    }

    #[tokio::test]
    async fn test_start() {
        let tskv = get_tskv().await;

        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (tx, rx) = channel();

        let database = "db".to_string();
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let points = models_helper::create_random_points(&mut fbb, 1);
        fbb.finish(points, None);
        let points = fbb.finished_data().to_vec();
        let req = kv_service::WritePointsRpcRequest { version: 1, database, points };

        wal_sender.send(Task::WritePoints { req, tx }).unwrap();

        TsKv::start(tskv, wal_receiver);

        match rx.await {
            Ok(Ok(resp)) => println!("successful"),
            _ => println!("wrong"),
        };
    }
}
