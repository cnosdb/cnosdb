use std::{cell::RefCell, sync::Arc, thread::JoinHandle};

use ::models::{AbstractPoints, SeriesInfo, Tag, FieldInfo, FieldInfoFromParts, ValueType};
use crate::forward_index::ForwardIndex;

use once_cell::sync::OnceCell;
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models,
};
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot, Mutex, RwLock,
    },
};

use crate::{
    error::Result,
    kv_option::{DBOptions, Options, QueryOption, WalConfig},
    version_set,
    wal::{self, WalEntryType, WalManager, WalResult, WalTask},
    Error, FileManager, MemCache, Task, Version, VersionSet, WorkerQueue,
};

pub struct Entry {
    pub series_id: u64,
}

pub struct TsKv {
    options: Options,
    kvctx: Arc<KvContext>,
    version_set: Arc<RwLock<VersionSet>>,

    wal_sender: UnboundedSender<WalTask>,
    forward_index: Arc<RwLock<ForwardIndex>>,
}

impl TsKv {
    pub async fn open(opt: Options) -> Result<Self> {
        let kvctx = Arc::new(KvContext::new(opt.clone()));
        let vs = Self::recover(opt.clone()).await;
        let mut fidx = ForwardIndex::new(&opt.forward_index_conf.path);
        fidx.load_cache_file().await.map_err(|err| { Error::LogRecordErr { source: err } })?;

        let (sender, receiver) = mpsc::unbounded_channel();

        let core = Self {
            options: opt,
            kvctx,
            forward_index: Arc::new(RwLock::new(fidx)),
            version_set: Arc::new(RwLock::new(vs)),
            wal_sender: sender,
        };
        core.run_wal_job(receiver);

        Ok(core)
    }

    pub async fn recover(opt: Options) -> VersionSet {
        // todo! recover from manifest and build VersionSet
        let mut version_set = VersionSet::new_default();
        let wal_manager = WalManager::new(opt.wal);
        wal_manager.recover(&mut version_set).await.unwrap();

        version_set
    }

    pub async fn write(&self,
                       write_batch: WritePointsRpcRequest)
                       -> Result<WritePointsRpcResponse> {
        let entry = flatbuffers::root::<protos::models::Points>(&write_batch.points).map_err(|err| {
            Error::InvalidFlatbuffer { source: err }
        })?;
        for point in entry.points().unwrap() {
            let mut info = SeriesInfo::new();
            for tag in point.tags().unwrap() {
                info.tags.push(Tag::new(tag.key().unwrap().to_vec(),
                                        tag.value().unwrap().to_vec()));
            }
            for field in point.fields().unwrap() {
                info.field_infos.push(FieldInfo::from_parts(
                    field.name().unwrap().to_vec(),
                    ValueType::from(field.type_()),
                ))
            }
            self.forward_index.write().await.add_series_info_if_not_exists(info)
                .await.map_err(|err| {
                Error::ForwardIndexErr { source: err }
            })?;
        }


        let (cb, rx) = oneshot::channel();
        self.wal_sender
            .send(WalTask::Write { points: write_batch.points, cb })
            .map_err(|err| Error::Send)?;
        rx.await.unwrap().unwrap();

        // let _ = self.kvctx.shard_write(0, write_batch).await;
        Ok(WritePointsRpcResponse { version: 1, points: vec![] })
    }

    pub async fn insert_cache(&self, buf: &[u8]) {
        let ps = flatbuffers::root::<models::Points>(buf).unwrap();

        let version_set = self.version_set.read().await;

        for p in ps.points().unwrap().iter() {
            let s = AbstractPoints::from(p);
            // use sid to dispatch to tsfamily
            // so if you change the colume name
            // please keep the series id
            let sid = s.series_id();

            let tsf = version_set.get_tsfamily(sid).unwrap();
            for f in s.fileds().iter() {
                let fid = f.filed_id();
            }
        }
    }

    pub fn run_wal_job(&self, mut receiver: UnboundedReceiver<WalTask>) {
        let wal_opt = self.options.wal.clone();
        let mut wal_manager = WalManager::new(wal_opt);
        let f = async move {
            while let Some(x) = receiver.recv().await {
                match x {
                    WalTask::Write { points, cb } => {
                        let ret = wal_manager.write(wal::WalEntryType::Write, &points).await;
                        dbg!(points);
                        let _ = cb.send(ret);
                    }
                }
            }
        };
        tokio::spawn(f);
    }

    pub fn start(tskv: TsKv, mut req_rx: UnboundedReceiver<Task>) {
        let f = async move {
            while let Some(command) = req_rx.recv().await {
                match command {
                    Task::WritePoints { req, tx } => {
                        dbg!("TSKV writing points.");
                        match tskv.write(req).await {
                            Ok(resp) => {
                                let _ret = tx.send(Ok(resp));
                            }
                            Err(err) => {
                                let _ret = tx.send(Err(err));
                            }
                        }
                        dbg!("TSKV write points completed.");
                    }
                    _ => panic!("unimplented."),
                }
            }
        };

        tokio::spawn(f);
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
    pub fn new(opt: Options) -> Self {
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
            let ps = flatbuffers::root::<models::Points>(&entry.points).unwrap();
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
    use protos::kv_service;

    use crate::{kv_option::WalConfig, wal::WalTask, TsKv};

    async fn get_tskv() -> TsKv {
        let opt = crate::kv_option::Options {
            wal: WalConfig {
                dir: String::from("/tmp/test/"),
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
        let points = b"Hello world".to_vec();
        let request = kv_service::WritePointsRpcRequest { version: 1, database, points };

        tskv.write(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_write() {
        let tskv = get_tskv().await;

        for i in 0..2 {
            let database = "db".to_string();
            let points = b"Hello world".to_vec();
            let request = kv_service::WritePointsRpcRequest { version: 1, database, points };
            tskv.write(request).await.unwrap();
        }
    }
}
