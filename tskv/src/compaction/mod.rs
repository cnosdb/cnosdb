mod compact;
mod flush;
mod picker;

pub use compact::*;
pub use flush::*;
pub use picker::*;

use crate::{
    memcache::MemCache,
    summary::VersionEdit,
    tseries_family::{BlockFile, Version},
};

#[async_trait::async_trait]
pub trait CompactionEngine: Clone + Sync + Send {
    async fn apply(&mut self, edits: Vec<VersionEdit>) -> crate::error::Result<()>;
}
pub struct CompactionRequest {
    input: Vec<(u32, std::sync::Arc<BlockFile>)>,
    input_version: std::sync::Arc<Version>,
    cf: u32,
    output_level: u32,
}

pub struct FlushRequest {
    pub mems: Vec<(u32, std::sync::Arc<MemCache>)>,
    pub wait_commit_request: u64,
}

impl FlushRequest {
    pub fn new(mems: Vec<(u32, std::sync::Arc<MemCache>)>, wait_commit_request: u64) -> Self {
        Self { mems, wait_commit_request }
    }
}
