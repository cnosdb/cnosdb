mod compact;
mod flush;
mod picker;

use std::sync::Arc;

pub use compact::*;
pub use flush::*;
use parking_lot::RwLock;
pub use picker::*;

use crate::{
    kv_option::TseriesFamOpt,
    memcache::MemCache,
    summary::VersionEdit,
    tseries_family::{ColumnFile, Version},
    LevelId, TseriesFamilyId,
};

#[async_trait::async_trait]
pub trait CompactionEngine: Clone + Sync + Send {
    async fn apply(&mut self, edits: Vec<VersionEdit>) -> crate::error::Result<()>;
}

pub struct CompactReq {
    ts_family_id: TseriesFamilyId,
    ts_family_opt: Arc<TseriesFamOpt>,

    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    out_level: LevelId,
}

#[derive(Debug)]
pub struct FlushReq {
    pub mems: Vec<(TseriesFamilyId, std::sync::Arc<RwLock<MemCache>>)>,
    pub wait_req: u64,
}

impl FlushReq {
    pub fn new(
        mems: Vec<(TseriesFamilyId, std::sync::Arc<RwLock<MemCache>>)>,
        wait_req: u64,
    ) -> Self {
        Self { mems, wait_req }
    }
}
