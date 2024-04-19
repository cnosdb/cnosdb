pub mod check;
mod compact;
mod flush;
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

pub struct CompactTask {
    pub tsf_id: TseriesFamilyId,
}

pub struct CompactReq {
    pub ts_family_id: TseriesFamilyId,
    pub database: Arc<String>,
    storage_opt: Arc<StorageOptions>,

    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    pub out_level: LevelId,
}

pub struct FlushReq {
    pub owner: String,
    pub ts_family_id: TseriesFamilyId,
    pub mems: Vec<Arc<RwLock<MemCache>>>,
    pub low_seq_no: u64,
    pub high_seq_no: u64,
}

impl std::fmt::Display for FlushReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlushReq owner: {}, on vnode: {}, low_seq_no: {}, high_seq_no: {} caches_num: {}",
            self.owner,
            self.ts_family_id,
            self.low_seq_no,
            self.high_seq_no,
            self.mems.len(),
        )
    }
}
