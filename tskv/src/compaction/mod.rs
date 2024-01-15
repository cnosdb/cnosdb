pub mod check;
mod compact;
mod delta_compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::collections::HashMap;
use std::sync::Arc;

pub use compact::*;
pub use flush::*;
use models::Timestamp;
use parking_lot::RwLock;
pub use picker::*;
use utils::BloomFilter;

use crate::context::GlobalContext;
use crate::memcache::MemCache;
use crate::tseries_family::{ColumnFile, Version};
use crate::{ColumnFileId, LevelId, TseriesFamilyId, VersionEdit};

#[cfg(test)]
pub mod test {
    pub use super::compact::test::{
        check_column_file, create_options, generate_data_block, prepare_compaction,
        read_data_blocks_from_column_file, write_data_block_desc, write_data_blocks_to_column_file,
        TsmSchema,
    };
    pub use super::flush::flush_tests::default_table_schema;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompactTask {
    /// Compact the files in the in_level into the out_level.
    Normal(TseriesFamilyId),
    /// Compact the files in level-0 to larger files.
    Delta(TseriesFamilyId),
    /// Flush memcaches and then compact the files in the in_level into the out_level.
    Cold(TseriesFamilyId),
}

impl CompactTask {
    pub fn ts_family_id(&self) -> TseriesFamilyId {
        match self {
            CompactTask::Normal(ts_family_id) => *ts_family_id,
            CompactTask::Cold(ts_family_id) => *ts_family_id,
            CompactTask::Delta(ts_family_id) => *ts_family_id,
        }
    }

    fn priority(&self) -> usize {
        match self {
            CompactTask::Delta(_) => 1,
            CompactTask::Normal(_) => 2,
            CompactTask::Cold(_) => 3,
        }
    }
}

impl Ord for CompactTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority().cmp(&other.priority())
    }
}

impl PartialOrd for CompactTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct CompactReq {
    compact_task: CompactTask,

    version: Arc<Version>,
    files: Vec<Arc<ColumnFile>>,
    lv0_files: Option<Vec<Arc<ColumnFile>>>,
    in_level: LevelId,
    out_level: LevelId,
}

impl std::fmt::Display for CompactReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_database: {}, ts_family: {}, files: [",
            self.version.borrowed_database(),
            self.version.tf_id(),
        )?;
        if !self.files.is_empty() {
            write!(
                f,
                "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                self.files[0].level(),
                self.files[0].file_id(),
                self.files[0].time_range().min_ts,
                self.files[0].time_range().max_ts
            )?;
            for file in self.files.iter().skip(1) {
                write!(
                    f,
                    ", {{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    file.level(),
                    file.file_id(),
                    file.time_range().min_ts,
                    file.time_range().max_ts
                )?;
            }
        }
        write!(f, "]")?;

        if let Some(lv0_files) = &self.lv0_files {
            write!(f, ", lv0_files: [")?;
            if !lv0_files.is_empty() {
                write!(
                    f,
                    "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    lv0_files[0].level(),
                    lv0_files[0].file_id(),
                    lv0_files[0].time_range().min_ts,
                    lv0_files[0].time_range().max_ts
                )?;
                for file in lv0_files.iter().skip(1) {
                    write!(
                        f,
                        ", {{ Level-{}, file_id: {}, time_range: {}-{} }}",
                        file.level(),
                        file.file_id(),
                        file.time_range().min_ts,
                        file.time_range().max_ts
                    )?;
                }
            }
            write!(f, "]")?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct FlushReq {
    pub ts_family_id: TseriesFamilyId,
    pub mems: Vec<Arc<RwLock<MemCache>>>,
    pub max_ts: Timestamp,
    pub force_flush: bool,
}

pub async fn run_compaction_job(
    request: CompactReq,
    ctx: Arc<GlobalContext>,
) -> crate::Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    if request.in_level == 0 {
        delta_compact::run_compaction_job(request, ctx).await
    } else {
        compact::run_compaction_job(request, ctx).await
    }
}
