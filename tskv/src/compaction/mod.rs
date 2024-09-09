pub mod check;
mod compact;
mod flush;
pub mod job;
pub mod metrics;
mod picker;

use std::sync::Arc;

pub use compact::*;
pub use picker::*;
use tokio::sync::RwLock;

use crate::index::ts_index::TSIndex;
use crate::kv_option::StorageOptions;
use crate::tsfamily::column_file::ColumnFile;
use crate::tsfamily::tseries_family::TseriesFamily;
use crate::tsfamily::version::Version;
use crate::{LevelId, VnodeId};

pub struct CompactTask {
    pub tsf_id: VnodeId,
}

pub struct CompactReq {
    pub ts_family_id: VnodeId,
    pub owner: Arc<String>,
    storage_opt: Arc<StorageOptions>,

    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    pub out_level: LevelId,
}

#[derive(Clone)]
pub struct FlushReq {
    pub tf_id: VnodeId,
    pub owner: String,
    pub ts_index: Arc<RwLock<TSIndex>>,
    pub ts_family: Arc<RwLock<TseriesFamily>>,
    pub trigger_compact: bool,
}

impl std::fmt::Display for FlushReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FlushReq owner: {}, on vnode: {}",
            self.owner, self.tf_id,
        )
    }
}
