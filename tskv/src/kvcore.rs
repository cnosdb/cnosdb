use futures::channel::mpsc;
use futures::channel::mpsc::unbounded;
use futures::channel::mpsc::UnboundedReceiver;
use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot;
use futures::executor::ThreadPool;
use futures::task::Spawn;
use futures::task::SpawnExt;
use futures::SinkExt;
use futures::StreamExt;
use futures::TryFutureExt;
use protos::kv_service::WriteRowsRpcRequest;
use protos::models;

use crate::error::Result;
use crate::option::Options;
use crate::option::QueryOption;
use crate::option::WalConfig;
use crate::version_set;
use crate::wal::WalFileManager;
use crate::wal::WalResult;
use crate::wal::WalTask;
use crate::Error;
use crate::FileManager;
use crate::Version;
use crate::VersionSet;
use crate::WorkerQueue;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::thread::JoinHandle;

pub struct Entry {
    pub series_id: u64,
}

#[derive(Clone)]
pub struct TsKv {
    options: Options,
    kvctx: Arc<KvContext>,
    version_set: Arc<VersionSet>,
    file_manager: Arc<FileManager>,

    pool: Arc<ThreadPool>,
    wal_sender: UnboundedSender<WalTask>,
}

impl TsKv {
    pub fn open(opt: Options) -> Result<Self> {
        let kvctx = Arc::new(KvContext::new(opt.clone()));
        let vs = Self::recover();

        // let file_manager = FileManager::get_instance();
        let (sender, receiver) = unbounded();

        let core = Self {
            options: opt,
            kvctx,
            version_set: Arc::new(vs),
            file_manager: Arc::new(FileManager::new()),
            pool: Arc::new(ThreadPool::new().unwrap()),
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

    pub async fn write(&mut self, write_batch: WriteRowsRpcRequest) -> Result<()> {
        let (cb, rx) = oneshot::channel();
        self.wal_sender
            .send(WalTask::Write {
                rows: write_batch.rows,
                cb,
            })
            .await
            .map_err(|err| Error::Send)?;
        rx.await.unwrap().unwrap();

        // let _ = self.kvctx.shard_write(0, write_batch).await;
        Ok(())
    }

    pub fn run_wal_job(&self, mut receiver: UnboundedReceiver<WalTask>) {
        let fm = Arc::clone(&self.file_manager);
        let wal_opt = self.options.wal.clone();
        let f = async move {
            let mut wal_manager = WalFileManager::new(fm, wal_opt);
            while let Some(x) = receiver.next().await {
                match x {
                    WalTask::Write { rows, cb } => {
                        let ret = wal_manager.write(&rows).await;
                        let ret = cb.send(WalResult::Ok(()));
                    }
                }
            }
        };
        self.pool.spawn_ok(async move {
            f.await;
        });
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
    pub file_manager: &'static FileManager,
    front_handler: Arc<WorkerQueue>,
    handler: Vec<JoinHandle<()>>,
    status: KvStatus,
}

#[allow(dead_code)]
impl KvContext {
    pub fn new(opt: Options) -> Self {
        let file_manager = FileManager::get_instance();
        let front_work_queue = Arc::new(WorkerQueue::new(opt.front_cpu));
        let worker_handle = Vec::with_capacity(opt.back_cpu);
        Self {
            file_manager: file_manager,
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

    pub async fn shard_write(&self, partion_id: usize, _entry: WriteRowsRpcRequest) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.front_handler.work_queue.spawn(partion_id, async move {
            let err = 0;
            //memcache.insert();
            let _ = tx.send(err);
        });
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

    use crate::{option::WalConfig, wal::WalTask, TsKv};

    fn get_tskv() -> TsKv {
        let opt = crate::option::Options {
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
        let mut tskv = get_tskv();

        let rows = b"Hello world".to_vec();
        let request = kv_service::WriteRowsRpcRequest { version: 1, rows };

        tskv.write(request).await.unwrap();
    }

    #[tokio::test]
    async fn test_concurrent_write() {
        let mut tskv = get_tskv();

        for i in 0..2 {
            let rows = b"Hello world".to_vec();
            let request = kv_service::WriteRowsRpcRequest { version: 1, rows };
            tskv.write(request).await.unwrap();
        }
    }
}
