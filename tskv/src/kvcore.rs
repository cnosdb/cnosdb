use std::sync::Arc;
use std::thread::JoinHandle;

use once_cell::sync::OnceCell;
use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest};
use protos::models;
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender},
    oneshot,
};

use crate::{
    error::Result,
    kv_option::{Options, QueryOption, WalConfig},
    version_set,
    wal::{self, WalFileManager, WalResult, WalTask},
    Error, FileManager, Version, VersionSet, WorkerQueue,
};

pub struct Entry {
    pub series_id: u64,
}

pub struct TsKv {
    options: Options,
    kvctx: Arc<KvContext>,
    version_set: Arc<VersionSet>,
    wal_sender: UnboundedSender<WalTask>,
}

impl TsKv {
    pub fn open(opt: Options) -> Result<Self> {
        let kvctx = Arc::new(KvContext::new(opt.clone()));
        let vs = Self::recover();

        let (sender, receiver) = mpsc::unbounded_channel();

        let core = Self {
            options: opt,
            kvctx,
            version_set: Arc::new(vs),
            wal_sender: sender,
        };

        core.run_wal_job(receiver);

        Ok(core)
    }

    pub fn recover() -> VersionSet {
        //todo! recover from manifest and build VersionSet
        VersionSet::new_default()
    }

    pub fn get_instance() -> &'static Self {
        static INSTANCE: OnceCell<TsKv> = OnceCell::new();
        INSTANCE.get_or_init(|| Self::open(Options::from_env()).unwrap())
    }

    pub async fn write(
        &self,
        write_batch: WritePointsRpcRequest,
    ) -> Result<WritePointsRpcResponse> {
        let (cb, rx) = oneshot::channel();
        self.wal_sender
            .send(WalTask::Write {
                points: write_batch.points,
                cb,
            })
            .map_err(|err| Error::Send)?;
        rx.await.unwrap().unwrap();

        // let _ = self.kvctx.shard_write(0, write_batch).await;
        Ok(WritePointsRpcResponse {
            version: 1,
            points: vec![],
        })
    }

    pub fn insert_cache(&self, buf: &[u8]) {
        let ps = flatbuffers::root::<models::Points>(buf).unwrap();
        for p in ps.points().unwrap().iter() {
            let s = AbstractPoints::from(p);
            //use sid to dispatch to tsfamily
            //so if you change the colume name
            //please keep the series id
            let sid = s.series_id();

            let tsf = self.version_set.get_tsfamily(sid).unwrap();
            for f in s.fileds().iter() {
                let fid = f.filed_id();
            }
        }
    }
    pub fn run_wal_job(&self, mut receiver: UnboundedReceiver<WalTask>) {
        let wal_opt = self.options.wal.clone();
        let f = async move {
            let mut wal_manager = WalFileManager::new(wal_opt);
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
        tokio::spawn(async move {
            f.await;
        });
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
        let front_work_queue = Arc::new(WorkerQueue::new(opt.front_cpu));
        let worker_handle = Vec::with_capacity(opt.back_cpu);
        Self {
            front_handler: front_work_queue,
            handler: worker_handle,
            status: KvStatus::Init,
        }
    }

    pub fn recover(&mut self) -> Result<()> {
        //todo: index wal summary shardinfo recover here.
        self.status = KvStatus::Recover;
        Ok(())
    }

    pub async fn shard_write(
        &self,
        partion_id: usize,
        mem: Arc<RwLock<MemCache>>,
        entry: WritePointsRpcRequest,
    ) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.front_handler.add_task(partion_id, async move {
            let ps = flatbuffers::root::<models::Points>(&entry.points).unwrap();
            let err = 0;
            //todo
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

    fn get_tskv() -> TsKv {
        let opt = crate::kv_option::Options {
            wal: WalConfig {
                dir: String::from("/tmp/test/"),
                ..Default::default()
            },
            ..Default::default()
        };

        TsKv::open(opt).unwrap()
    }

    #[tokio::test]
    async fn test_write() {
        let tskv = get_tskv();

        let database = "db".to_string();
        let points = b"Hello world".to_vec();
        let request = kv_service::WritePointsRpcRequest {
            version: 1,
            database,
            points,
        };

        tskv.write(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_write() {
        let tskv = get_tskv();

        for i in 0..2 {
            let database = "db".to_string();
            let points = b"Hello world".to_vec();
            let request = kv_service::WritePointsRpcRequest {
                version: 1,
                database,
                points,
            };
            tskv.write(request).await.unwrap();
        }
    }
}
