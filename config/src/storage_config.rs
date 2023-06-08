use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::codec::{bytes_num, duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

    #[serde(default = "StorageConfig::default_strict_write")]
    pub strict_write: bool,
}

impl StorageConfig {
    fn default_path() -> String {
        "data/db".to_string()
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

    fn default_max_concurrent_compaction() -> u16 {
        4
    }

    fn default_strict_write() -> bool {
        false
    }

    pub fn override_by_env(&mut self) {
        if let Ok(path) = std::env::var("CNOSDB_APPLICATION_PATH") {
            self.path = path;
        }
        if let Ok(size) = std::env::var("CNOSDB_SUMMARY_MAX_SUMMARY_SIZE") {
            self.max_summary_size = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_BASE_FILE_SIZE") {
            self.base_file_size = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_FLUSH_REQ_CHANNEL_CAP") {
            self.flush_req_channel_cap = size.parse::<usize>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_MAX_LEVEL") {
            self.max_level = size.parse::<u16>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_COMPACT_TRIGGER_FILE_NUM") {
            self.compact_trigger_file_num = size.parse::<u32>().unwrap();
        }
        if let Ok(dur) = std::env::var("CNOSDB_STORAGE_compact_trigger_cold_duration") {
            self.compact_trigger_cold_duration = duration::parse_duration(&dur).unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_MAX_COMPACT_SIZE") {
            self.max_compact_size = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_MAX_CONCURRENT_COMPACTION") {
            self.max_concurrent_compaction = size.parse::<u16>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_STRICT_WRITE") {
            self.strict_write = size.parse::<bool>().unwrap();
        }

        self.introspect();
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
            strict_write: Self::default_strict_write(),
        }
    }
}

impl CheckConfig for StorageConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
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
