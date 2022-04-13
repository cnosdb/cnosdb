use tokio::sync::oneshot;

use crate::error::Result;
use crate::option::Options;
use crate::option::QueryOption;
use crate::version_set;
use crate::FileManager;
use crate::Version;
use crate::VersionSet;
use crate::WorkerQueue;
use once_cell::sync::OnceCell;
use std::sync::Arc;
use std::thread::JoinHandle;

#[derive(Clone)]
pub struct TsKv {
    kvctx: Arc<KvContext>,
    version_set: Arc<VersionSet>,
}

pub struct Entry {
    pub series_id: u64,
}

impl TsKv {
    pub fn open(opt: Options) -> Self {
        let kvctx = Arc::new(KvContext::new(opt));
        let vs = Self::recover();
        Self {
            kvctx,
            version_set: Arc::new(vs),
        }
    }
    pub fn recover() -> VersionSet {
        //todo! recover from manifest and build VersionSet
        VersionSet::new_default()
    }
    pub fn get_instance() -> &'static Self {
        static INSTANCE: OnceCell<TsKv> = OnceCell::new();
        INSTANCE.get_or_init(|| Self::open(Options::from_env()))
    }
    pub async fn write(&self, write_batch: Vec<Entry>) -> Result<()> {
        //wal
        let _ = self.kvctx.shard_write(0, write_batch).await;
        Ok(())
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

    pub async fn shard_write(&self, partion_id: usize, _entry: Vec<Entry>) -> Result<()> {
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
