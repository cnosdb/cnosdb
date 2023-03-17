use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::codec::{bytes_num, duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalConfig {
    #[serde(default = "WalConfig::default_enabled")]
    pub enabled: bool,

    #[serde(default = "WalConfig::default_path")]
    pub path: String,

    #[serde(default = "WalConfig::default_wal_req_channel_cap")]
    pub wal_req_channel_cap: usize,

    #[serde(with = "bytes_num", default = "WalConfig::default_max_file_size")]
    pub max_file_size: u64,

    #[serde(
        with = "bytes_num",
        default = "WalConfig::default_flush_trigger_total_file_size"
    )]
    pub flush_trigger_total_file_size: u64,

    #[serde(default = "WalConfig::default_sync")]
    pub sync: bool,

    #[serde(with = "duration", default = "WalConfig::default_sync_interval")]
    pub sync_interval: Duration,
}

impl WalConfig {
    fn default_enabled() -> bool {
        true
    }

    fn default_path() -> String {
        "data/wal".to_string()
    }

    fn default_wal_req_channel_cap() -> usize {
        64
    }

    fn default_max_file_size() -> u64 {
        1024 * 1024 * 1024
    }

    fn default_flush_trigger_total_file_size() -> u64 {
        2 * 1024 * 1024 * 1024
    }

    fn default_sync() -> bool {
        false
    }

    fn default_sync_interval() -> Duration {
        Duration::from_secs(0)
    }

    pub fn override_by_env(&mut self) {
        if let Ok(enabled) = std::env::var("CNOSDB_WAL_ENABLED") {
            self.enabled = enabled.as_str() == "true";
        }
        if let Ok(path) = std::env::var("CNOSDB_WAL_PATH") {
            self.path = path;
        }
        if let Ok(cap) = std::env::var("CNOSDB_WAL_REQ_CHANNEL_CAP") {
            self.wal_req_channel_cap = cap.parse::<usize>().unwrap();
        }
        if let Ok(sync) = std::env::var("CNOSDB_WAL_SYNC") {
            self.sync = sync.as_str() == sync;
        }
    }

    pub fn introspect(&mut self) {
        // Unit of wal.sync_interval is seconds
        self.sync_interval = Duration::from_secs(self.sync_interval.as_secs());
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            path: Self::default_path(),
            wal_req_channel_cap: Self::default_wal_req_channel_cap(),
            max_file_size: Self::default_max_file_size(),
            flush_trigger_total_file_size: Self::default_flush_trigger_total_file_size(),
            sync: Self::default_sync(),
            sync_interval: Self::default_sync_interval(),
        }
    }
}

impl CheckConfig for WalConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("wal".to_string());
        let mut ret = CheckConfigResult::default();

        if self.path.is_empty() {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "path".to_string(),
                message: "'path' is empty".to_string(),
            });
        }
        if self.wal_req_channel_cap < 16 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "wal_req_channel_cap".to_string(),
                message: "'wal_req_channel_cap' maybe too small(less than 16)".to_string(),
            });
        }
        if self.sync_interval.as_nanos() < Duration::from_secs(1).as_nanos() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "sync_interval".to_string(),
                message: "'sync_interval' maybe too small(less than 1 second)".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
