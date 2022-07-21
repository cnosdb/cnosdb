mod compact;
mod flush;
mod picker;

pub use compact::*;
pub use flush::*;
use parking_lot::RwLock;
pub use picker::*;

use crate::{
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
    files: (LevelId, Vec<std::sync::Arc<ColumnFile>>),
    version: std::sync::Arc<Version>,
    tsf_id: TseriesFamilyId,
    out_level: LevelId,
}

#[derive(Debug)]
pub struct FlushReq {
    //(tsf id,memcache)
    pub mems: Vec<(TseriesFamilyId, std::sync::Arc<RwLock<MemCache>>)>,
    pub wait_req: u64,
}

impl FlushReq {
    pub fn new(mems: Vec<(TseriesFamilyId, std::sync::Arc<RwLock<MemCache>>)>,
               wait_req: u64)
               -> Self {
        Self { mems, wait_req }
    }
}
