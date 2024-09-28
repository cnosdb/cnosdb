use macros::EnvKeys;
use serde::{Deserialize, Serialize};

use crate::tskv;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
pub struct SysConfig {
    pub usage_schema_cache_size: u64,
    pub cluster_schema_cache_size: u64,
    pub system_database_replica: u64,
}

impl Default for SysConfig {
    fn default() -> Self {
        Self {
            usage_schema_cache_size: tskv::MetaConfig::default_cluster_schema_cache_size(),
            cluster_schema_cache_size: tskv::MetaConfig::default_usage_schema_cache_size(),
            system_database_replica: tskv::MetaConfig::default_system_database_replica(),
        }
    }
}
