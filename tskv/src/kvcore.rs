use std::rc::Rc;

use tokio::sync::oneshot;

use crate::error::Result;
use crate::option::Options;
use crate::option::QueryOption;
use crate::FileManager;
use crate::WorkerQueue;
use std::thread::JoinHandle;

pub struct TsKv {
    kvctx: Rc<KvContext>,
}

pub struct Entry {
    pub series_id: u64,
}

impl TsKv {
    pub fn open(opt: Options) -> Self {
        let kvctx = Rc::new(KvContext::new(opt));
        // let _err = kvctx.Recover();
        Self { kvctx }
    }
    pub async fn write(&self, write_batch: Vec<Entry>) -> Result<()> {
        self.kvctx.shard_write(0, write_batch).await;
        Ok(())
    }

    pub async fn query(&self, opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}

enum KvStatus {
    Init,
    Recover,
    Runing,
    Closed,
}
pub(crate) struct KvContext {
    pub file_manager: FileManager,
    front_handler: Rc<WorkerQueue>,
    handler: Vec<JoinHandle<()>>,
    status: KvStatus,
}

impl KvContext {
    pub fn new(opt: Options) -> Self {
        let file_manager = FileManager::new();
        let front_work_queue = Rc::new(WorkerQueue::new(opt.front_cpu));
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

    pub async fn shard_write(&self, partion_id: usize, entry: Vec<Entry>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.front_handler.work_queue.spawn(partion_id, async move {
            let err = 0;
            //wal.write_entry;
            //memcache.insert();
            tx.send(err);
        });
        rx.await.unwrap();
        Ok(())
    }
    pub async fn shard_query(&self, opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}
