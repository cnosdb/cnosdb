use derive_traits::Keys;
use serde::{Deserialize, Serialize};

use crate::tskv;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct SysConfig {
    #[serde(default = "SysConfig::default_usage_schema_cache_size")]
    pub usage_schema_cache_size: u64,
    #[serde(default = "SysConfig::default_cluster_schema_cache_size")]
    pub cluster_schema_cache_size: u64,
    #[serde(default = "SysConfig::default_system_database_replica")]
    pub system_database_replica: u64,
}

impl SysConfig {
    fn default_usage_schema_cache_size() -> u64 {
        tskv::MetaConfig::default_cluster_schema_cache_size()
    }

    fn default_cluster_schema_cache_size() -> u64 {
        tskv::MetaConfig::default_usage_schema_cache_size()
    }

    fn default_system_database_replica() -> u64 {
        tskv::MetaConfig::default_system_database_replica()
    }
}

impl Default for SysConfig {
    fn default() -> Self {
        Self {
            usage_schema_cache_size: Self::default_usage_schema_cache_size(),
            cluster_schema_cache_size: Self::default_cluster_schema_cache_size(),
            system_database_replica: Self::default_system_database_replica(),
        }
    }
}
