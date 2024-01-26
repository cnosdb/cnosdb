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
use models::predicate::domain::TimeRange;
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

impl std::fmt::Display for CompactTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactTask::Normal(ts_family_id) => write!(f, "Normal({})", ts_family_id),
            CompactTask::Cold(ts_family_id) => write!(f, "Cold({})", ts_family_id),
            CompactTask::Delta(ts_family_id) => write!(f, "Delta({})", ts_family_id),
        }
    }
}

pub struct CompactReq {
    compact_task: CompactTask,

    version: Arc<Version>,
    files: Vec<Arc<ColumnFile>>,
    in_level: LevelId,
    out_level: LevelId,
}

impl CompactReq {
    /// Get the `out_time_range`, which is the time range of
    /// the part of `files` that joined the compaction.
    ///
    /// - If it's a delta compaction(`in_level` is 0), the `out_time_range`
    ///   is the time range of the level-1~4 file.
    /// - If it's a normal compaction, the `out_time_range` is the time range
    ///   of the out_level.
    /// - Otherwise, return `TimeRange::all()`.
    pub fn out_time_range(&self) -> TimeRange {
        if self.in_level == 0 {
            let mut out_time_range = TimeRange::none();
            for f in self.files.iter() {
                if !f.is_delta() {
                    out_time_range.merge(f.time_range());
                }
            }
            if !out_time_range.is_none() {
                return out_time_range;
            }
        }
        if (self.out_level as usize) < self.version.levels_info().len() {
            let level_time_range = self.version.levels_info()[self.out_level as usize].time_range;
            if level_time_range.is_none() {
                return TimeRange::all();
            } else {
                return level_time_range;
            }
        }
        TimeRange::none()
    }

    /// Split the `files` into delta files and an optional level-1~4 file.
    pub fn split_delta_and_level_files(&self) -> (Vec<Arc<ColumnFile>>, Option<Arc<ColumnFile>>) {
        let (mut delta_files, mut level_files) = (vec![], Option::None);
        for f in self.files.iter() {
            if f.is_delta() {
                delta_files.push(f.clone());
            } else if level_files.is_none() {
                level_files = Some(f.clone());
            }
        }
        (delta_files, level_files)
    }
}

impl std::fmt::Display for CompactReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_database: {}, ts_family: {}, in_level: {}, out_level: {:?}, out_time_range: {}, files: [",
            self.version.borrowed_database(),
            self.version.tf_id(),
            self.in_level,
            self.out_level,
            self.out_time_range(),
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
        write!(f, "]")
    }
}

#[derive(Debug)]
pub struct FlushReq {
    pub ts_family_id: TseriesFamilyId,
    pub mems: Vec<Arc<RwLock<MemCache>>>,
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
