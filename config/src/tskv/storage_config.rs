use std::sync::Arc;
use std::time::Duration;

use macros::EnvKeys;
use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::codec::{bytes_num, duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
pub struct StorageConfig {
    #[serde(default = "StorageConfig::default_path")]
    pub path: String,

    #[serde(
        with = "bytes_num",
        default = "StorageConfig::default_max_summary_size"
    )]
    pub max_summary_size: u64,

    #[serde(with = "bytes_num", default = "StorageConfig::default_base_file_size")]
    pub base_file_size: u64,

    #[serde(default = "StorageConfig::default_flush_req_channel_cap")]
    pub flush_req_channel_cap: usize,

    #[serde(default = "StorageConfig::default_max_cached_readers")]
    pub max_cached_readers: usize,

    #[serde(default = "StorageConfig::default_max_level")]
    pub max_level: u16,

    #[serde(default = "StorageConfig::default_compact_trigger_file_num")]
    pub compact_trigger_file_num: u32,

    #[serde(
        with = "duration",
        default = "StorageConfig::default_compact_trigger_cold_duration"
    )]
    pub compact_trigger_cold_duration: Duration,

    #[serde(
        with = "bytes_num",
        default = "StorageConfig::default_max_compact_size"
    )]
    pub max_compact_size: u64,

    #[serde(default = "StorageConfig::default_max_concurrent_compaction")]
    pub max_concurrent_compaction: u16,

    #[serde(default = "StorageConfig::default_collect_compaction_metrics")]
    pub collect_compaction_metrics: bool,

    #[serde(default = "StorageConfig::default_strict_write")]
    pub strict_write: bool,

    #[serde(with = "bytes_num", default = "StorageConfig::default_reserve_space")]
    pub reserve_space: u64,

    #[serde(
        with = "bytes_num",
        default = "StorageConfig::default_copyinto_trigger_flush_size"
    )]
    pub copyinto_trigger_flush_size: u64,

    #[serde(
        with = "bytes_num",
        default = "StorageConfig::default_max_datablock_size"
    )]
    pub max_datablock_size: u64,

    #[serde(default = "StorageConfig::default_index_cache_capacity")]
    pub index_cache_capacity: u64,
}

impl StorageConfig {
    fn default_path() -> String {
        let path = std::path::Path::new("/tmp/cnosdb/cnosdb_data").join("data");
        path.to_string_lossy().to_string()
    }

    fn default_max_summary_size() -> u64 {
        128 * 1024 * 1024
    }

    fn default_base_file_size() -> u64 {
        16 * 1024 * 1024
    }

    fn default_flush_req_channel_cap() -> usize {
        16
    }

    fn default_max_cached_readers() -> usize {
        32
    }

    fn default_max_level() -> u16 {
        4
    }

    fn default_compact_trigger_file_num() -> u32 {
        4
    }

    fn default_compact_trigger_cold_duration() -> Duration {
        Duration::from_secs(60 * 60)
    }

    fn default_max_compact_size() -> u64 {
        2 * 1024 * 1024 * 1024
    }

    fn default_reserve_space() -> u64 {
        0
    }

    fn default_max_concurrent_compaction() -> u16 {
        4
    }

    fn default_collect_compaction_metrics() -> bool {
        false
    }

    fn default_strict_write() -> bool {
        false
    }

    fn default_copyinto_trigger_flush_size() -> u64 {
        128 * 1024 * 1024 // 128M
    }

    fn default_max_datablock_size() -> u64 {
        100 * 1024
    }

    fn default_index_cache_capacity() -> u64 {
        100_000
    }

    pub fn introspect(&mut self) {
        // Unit of storage.compact_trigger_cold_duration is seconds
        self.compact_trigger_cold_duration =
            Duration::from_secs(self.compact_trigger_cold_duration.as_secs());
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: Self::default_path(),
            max_summary_size: Self::default_max_summary_size(),
            base_file_size: Self::default_base_file_size(),
            flush_req_channel_cap: Self::default_flush_req_channel_cap(),
            max_cached_readers: Self::default_max_cached_readers(),
            max_level: Self::default_max_level(),
            compact_trigger_file_num: Self::default_compact_trigger_file_num(),
            compact_trigger_cold_duration: Self::default_compact_trigger_cold_duration(),
            max_compact_size: Self::default_max_compact_size(),
            max_concurrent_compaction: Self::default_max_concurrent_compaction(),
            collect_compaction_metrics: Self::default_collect_compaction_metrics(),
            strict_write: Self::default_strict_write(),
            reserve_space: Self::default_reserve_space(),
            copyinto_trigger_flush_size: Self::default_copyinto_trigger_flush_size(),
            max_datablock_size: Self::default_max_datablock_size(),
            index_cache_capacity: Self::default_index_cache_capacity(),
        }
    }
}

impl CheckConfig for StorageConfig {
    fn check(&self, _: &super::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("storage".to_string());
        let mut ret = CheckConfigResult::default();

        if self.path.is_empty() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "path".to_string(),
                message: "'path' is empty".to_string(),
            });
        }
        if self.max_summary_size < 1024 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "max_summary_size".to_string(),
                message: "'max_summary_size' maybe too small(less than 1K)".to_string(),
            });
        }
        if self.base_file_size < 1024 * 1024 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "base_file_size".to_string(),
                message: "'base_file_size' maybe too small(less than 1M)".to_string(),
            });
        }
        if self.flush_req_channel_cap < 16 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "flush_req_channel_cap".to_string(),
                message: "'flush_req_channel_cap' maybe too small(less than 16)".to_string(),
            });
        }
        if self.max_level != 4 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "max_level".to_string(),
                message: "'max_level' set to 4 is recommended".to_string(),
            });
        }
        if self.compact_trigger_cold_duration.as_nanos() < Duration::from_secs(1).as_nanos() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "compact_trigger_cold_duration".to_string(),
                message: "'compact_trigger_cold_duration' maybe too small(less than 1 second)"
                    .to_string(),
            });
        }
        if self.max_compact_size < 1024 * 1024 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "max_compact_size".to_string(),
                message: "'max_compact_size' maybe too small(less than 1M)".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
