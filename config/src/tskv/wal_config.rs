use std::sync::Arc;

use macros::EnvKeys;
use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::codec::bytes_num;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
pub struct WalConfig {
    #[serde(default = "WalConfig::default_path")]
    pub path: String,

    #[serde(default = "WalConfig::default_wal_req_channel_cap")]
    pub wal_req_channel_cap: usize,

    #[serde(with = "bytes_num", default = "WalConfig::default_max_file_size")]
    pub max_file_size: u64,

    #[serde(default = "WalConfig::default_sync")]
    pub sync: bool,

    #[serde(default = "WalConfig::default_compress")]
    pub compress: String,
}

impl WalConfig {
    fn default_path() -> String {
        let path = std::path::Path::new("/tmp/cnosdb/cnosdb_data").join("wal");
        path.to_string_lossy().to_string()
    }

    fn default_wal_req_channel_cap() -> usize {
        64
    }

    pub fn default_max_file_size() -> u64 {
        128 * 1024 * 1024
    }

    pub fn default_sync() -> bool {
        false
    }

    fn default_compress() -> String {
        "zstd".to_string()
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            path: Self::default_path(),
            wal_req_channel_cap: Self::default_wal_req_channel_cap(),
            max_file_size: Self::default_max_file_size(),
            sync: Self::default_sync(),
            compress: Self::default_compress(),
        }
    }
}

impl CheckConfig for WalConfig {
    fn check(&self, _: &super::Config) -> Option<CheckConfigResult> {
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

        if self.compress != "zstd"
            && self.compress != "snappy"
            && self.compress != "gzip"
            && self.compress != "bzip"
            && self.compress != "zlib"
        {
            ret.add_error(CheckConfigItemResult {
                config: config_name.clone(),
                item: "compress".to_string(),
                message: "'compress' must be 'zstd', 'snappy', 'gzip', 'bzip', 'zlib'".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
