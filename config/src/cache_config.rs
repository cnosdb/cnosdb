use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::codec::bytes_num;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheConfig {
    #[serde(with = "bytes_num", default = "CacheConfig::default_max_buffer_size")]
    pub max_buffer_size: u64,
    #[serde(default = "CacheConfig::default_max_immutable_number")]
    pub max_immutable_number: u16,
}

impl CacheConfig {
    fn default_max_buffer_size() -> u64 {
        128 * 1024 * 1024
    }

    fn default_max_immutable_number() -> u16 {
        4
    }

    pub fn override_by_env(&mut self) {
        if let Ok(size) = std::env::var("CNOSDB_CACHE_MAX_BUFFER_SIZE") {
            self.max_buffer_size = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_CACHE_MAX_IMMUTABLE_NUMBER") {
            self.max_immutable_number = size.parse::<u16>().unwrap();
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: Self::default_max_buffer_size(),
            max_immutable_number: Self::default_max_immutable_number(),
        }
    }
}

impl CheckConfig for CacheConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("cache".to_string());
        let mut ret = CheckConfigResult::default();
        if self.max_buffer_size < 1024 * 1024 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "max_buffer_size".to_string(),
                message: "'max_buffer_size' maybe too small(less than 1M)".to_string(),
            });
        }
        if self
            .max_buffer_size
            .checked_mul(self.max_immutable_number as u64 + 1)
            .is_none()
        {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: "max_immutable_number".to_string(),
                message: "'max_immutable_number' maybe too big( ('max_immutable_number' + 1) * 'max_buffer_size' caused an overflow)".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
