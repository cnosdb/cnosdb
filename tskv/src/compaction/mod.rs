pub mod check;
mod compact;
mod delta_compact;
mod flush;
mod iterator;
pub mod job;
mod picker;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
pub use compact::*;
pub use flush::*;
use models::Timestamp;
use parking_lot::RwLock;
pub use picker::*;
use utils::BloomFilter;

use crate::context::GlobalContext;
use crate::kv_option::StorageOptions;
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

#[derive(Debug, Clone, Copy)]
pub enum CompactTask {
    /// Compact the files in the in_level into the out_level.
    Normal(TseriesFamilyId),
    /// Flush memcaches and then compact the files in the in_level into the out_level.
    Cold(TseriesFamilyId),
    /// Compact the files in level-0 into the out_level.
    Delta(TseriesFamilyId),
}

pub struct CompactReq {
    pub ts_family_id: TseriesFamilyId,
    database: Arc<String>,
    storage_opt: Arc<StorageOptions>,

    files: Vec<Arc<ColumnFile>>,
    version: Arc<Version>,
    in_level: LevelId,
    out_level: LevelId,
    /// The maximum timestamp of the data from the in_level to be compacted
    /// into the out_level, only used in delta compaction.
    max_ts: Timestamp,
}

impl std::fmt::Display for CompactReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "tenant_database: {}, ts_family: {}, max_ts: {}, files: [",
            self.database, self.ts_family_id, self.max_ts,
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
    pub max_ts: Timestamp,
    pub force_flush: bool,
}

const PICKER_CONTEXT_DATETIME_FORMAT: &str = "%d%m%Y_%H%M%S_%3f";

fn context_datetime() -> String {
    Utc::now()
        .format(PICKER_CONTEXT_DATETIME_FORMAT)
        .to_string()
}

pub async fn run_compaction_job(
    request: CompactReq,
    kernel: Arc<GlobalContext>,
) -> crate::Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    if request.in_level == 0 {
        delta_compact::run_compaction_job(request, kernel).await
    } else {
        compact::run_compaction_job(request, kernel).await
    }
}
