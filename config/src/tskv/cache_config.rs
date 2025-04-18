use std::sync::Arc;

use derive_traits::Keys;
use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::codec::bytes_num;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct CacheConfig {
    #[serde(with = "bytes_num", default = "CacheConfig::default_max_buffer_size")]
    pub max_buffer_size: u64,
    #[serde(default = "CacheConfig::default_partitions")]
    pub partition: usize,
}

impl CacheConfig {
    fn default_max_buffer_size() -> u64 {
        128 * 1024 * 1024
    }

    fn default_partitions() -> usize {
        num_cpus::get()
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: Self::default_max_buffer_size(),
            partition: Self::default_partitions(),
        }
    }
}

impl CheckConfig for CacheConfig {
    fn check(&self, _: &super::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("cache".to_string());
        let mut ret = CheckConfigResult::default();
        if self.max_buffer_size < 1024 * 1024 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "max_buffer_size".to_string(),
                message: "'max_buffer_size' maybe too small(less than 1M)".to_string(),
            });
        }

        if self.partition > 1024 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "partition".to_string(),
                message: "'partition' maybe too big(more than 1024)".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
