pub mod check;
mod compact;
mod flush;
pub mod job;
mod picker;

pub use compact::*;
pub use flush::*;
pub use picker::*;

use std::sync::Arc;

use parking_lot::RwLock;

use crate::{
    kv_option::StorageOptions,
    memcache::MemCache,
    summary::VersionEdit,
    tseries_family::{ColumnFile, Version},
    LevelId, TseriesFamilyId,
};

pub struct CompactReq {
    pub ts_family_id: TseriesFamilyId,
    pub database: Arc<String>,
    storage_opt: Arc<StorageOptions>,

    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    pub out_level: LevelId,
}

#[derive(Debug)]
pub struct FlushReq {
    pub mems: Vec<(TseriesFamilyId, Arc<RwLock<MemCache>>)>,
}

impl FlushReq {
    pub fn new(mems: Vec<(TseriesFamilyId, Arc<RwLock<MemCache>>)>) -> Self {
        Self { mems }
    }
}
