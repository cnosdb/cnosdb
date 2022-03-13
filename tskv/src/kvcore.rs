use std::sync::Arc;

use crate::error::Result;
use crate::option::QueryOption;
use crate::{option::Options, FileManager};
use std::thread::JoinHandle;

pub struct TsKv {
    kvctx: Arc<KvContext>,
}

pub struct Entry {}

impl TsKv {
    pub fn open() {}
    pub async fn write(&self, write_batch: Vec<Entry>) -> Result<()> {
        self.kvctx.shard_write(write_batch).await;
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
    handler: Vec<JoinHandle<()>>,
    status: KvStatus,
}

impl KvContext {
    pub fn open(opt: Options) -> Self {
        let file_manager = FileManager::new();

        let mut worker_handle = Vec::with_capacity(opt.num_cpu);
        Self {
            handler: worker_handle,
            status: KvStatus::Init,
        }
    }

    pub async fn shard_write(&self, entry: Vec<Entry>) -> Result<()> {
        Ok(())
    }
    pub async fn shard_query(&self, opt: QueryOption) -> Result<Option<Entry>> {
        Ok(None)
    }
}
