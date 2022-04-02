use std::rc::Rc;

use tokio::sync::oneshot;

use crate::error::Result;
use crate::option::Options;
use crate::option::QueryOption;
use crate::{FileManager, wal};
use crate::WorkerQueue;
use std::thread::JoinHandle;

#[derive(Clone)]
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
    wal_manager: wal::WriteAheadLogManager,
    front_handler: Rc<WorkerQueue>,
    handler: Vec<JoinHandle<()>>,
    status: KvStatus,
}

#[allow(dead_code)]
impl KvContext {
    pub fn new(opt: Options) -> Self {
        let file_manager = FileManager::get_instance();
        let front_work_queue = Rc::new(WorkerQueue::new(opt.front_cpu));
        let worker_handle = Vec::with_capacity(opt.back_cpu);
        Self {
            file_manager,
            wal_manager: wal::WriteAheadLogManager::new(file_manager, opt.wal),
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
            //wal.write_entry;
            //memcache.insert();
            tx.send(err);
        });
        rx.await.unwrap();
        Ok(())
    }
    pub async fn shard_query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}
