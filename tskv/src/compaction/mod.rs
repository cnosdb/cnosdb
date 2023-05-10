pub mod check;
mod compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::sync::Arc;

pub use compact::*;
pub use flush::*;
use parking_lot::RwLock;
pub use picker::*;

use crate::kv_option::StorageOptions;
use crate::memcache::MemCache;
use crate::tseries_family::{ColumnFile, Version};
use crate::{LevelId, TseriesFamilyId};

pub enum CompactTask {
    Vnode(TseriesFamilyId),
    ColdVnode(TseriesFamilyId),
}

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
    pub ts_family_id: TseriesFamilyId,
    pub mems: Vec<Arc<RwLock<MemCache>>>,
    pub force_flush: bool,
}
