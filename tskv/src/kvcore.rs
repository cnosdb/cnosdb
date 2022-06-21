use std::{borrow::BorrowMut, cell::RefCell, ops::DerefMut, sync::Arc, thread::JoinHandle};

use ::models::{AbstractPoints, FieldInfo, FieldInfoFromParts, SeriesInfo, Tag, ValueType};
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
    context::GlobalContext,
    error::{self, Result},
    file_manager::{self, FileManager},
    file_utils,
    forward_index::ForwardIndex,
    kv_option::{DBOptions, Options, QueryOption, TseriesFamDesc, TseriesFamOpt, WalConfig},
    memcache::MemCache,
    record_file::Reader,
    runtime::WorkerQueue,
    summary::{Summary, VersionEdit},
    tseries_family::Version,
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
}

impl TsKv {
    pub async fn open(opt: Options) -> Result<Self> {
        let shared_options = Arc::new(opt);
        let kvctx = Arc::new(KvContext::new(shared_options.clone()));
        let version_set = Self::recover(shared_options.clone()).await;
        let mut fidx = ForwardIndex::new(&shared_options.forward_index_conf.path);
        fidx.load_cache_file().await.map_err(|err| Error::LogRecordErr { source: err })?;
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let core = Self { options: shared_options,
                          kvctx,
                          forward_index: Arc::new(RwLock::new(fidx)),
                          version_set,
                          wal_sender };
        core.run_wal_job(wal_receiver);

        Ok(core)
    }

    async fn recover(opt: Arc<Options>) -> Arc<RwLock<VersionSet>> {
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
        wal_manager.recover(version_set.clone(), summary.global_context()).await.unwrap();

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
            let mut info = SeriesInfo::new();
            for tag in point.tags().unwrap() {
                info.tags
                    .push(Tag::new(tag.key().unwrap().to_vec(), tag.value().unwrap().to_vec()));
            }
            for field in point.fields().unwrap() {
                info.field_infos.push(FieldInfo::from_parts(field.name().unwrap().to_vec(),
                                                            ValueType::from(field.type_())))
            }
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
            let version_set = self.version_set.read().await;
            for point in points.iter() {
                let p = AbstractPoints::from(point);
                let sid = p.series_id();
                if let Some(tsf) = version_set.get_tsfamily(sid) {
                    for f in p.fileds().iter() {
                        tsf.put_mutcache(f.filed_id(),
                                         &f.value,
                                         f.val_type,
                                         seq,
                                         point.timestamp() as i64)
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

    pub async fn insert_cache(&self, seq: u64, buf: &[u8]) -> Result<()> {
        let ps =
            flatbuffers::root::<fb_models::Points>(buf).context(error::InvalidFlatbufferSnafu)?;
        if let Some(points) = ps.points() {
            let version_set = self.version_set.read().await;
            for point in points.iter() {
                let p = AbstractPoints::from(point);
                // use sid to dispatch to tsfamily
                // so if you change the colume name
                // please keep the series id
                let sid = p.series_id();
                if let Some(tsf) = version_set.get_tsfamily(sid) {
                    for f in p.fileds().iter() {
                        tsf.put_mutcache(f.filed_id(),
                                         &f.value,
                                         f.val_type,
                                         seq,
                                         point.timestamp() as i64)
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

    use futures::{channel::oneshot, future::join_all, SinkExt};
    use tokio::sync::mpsc;
    use tokio::sync::oneshot::channel;
    use protos::{kv_service, models_helper};
    use protos::kv_service::WritePointsRpcResponse;

    use crate::{kv_option::WalConfig, TsKv, Task};

    async fn get_tskv() -> TsKv {
        let opt = crate::kv_option::Options {
            wal: WalConfig {
                dir: String::from("/tmp/test/wal"),
                ..Default::default()
            },
            ..Default::default()
        };

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
            _ => println!("wrong")
        };
    }
}
