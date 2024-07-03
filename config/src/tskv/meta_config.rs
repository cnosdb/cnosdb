use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::time::Duration;

use macros::EnvKeys;
use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::codec::{bytes_num, duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
pub struct MetaConfig {
    #[serde(default = "MetaConfig::default_service_addr")]
    pub service_addr: Vec<String>,
    #[serde(
        with = "duration",
        default = "MetaConfig::default_report_time_interval"
    )]
    pub report_time_interval: Duration,
    #[serde(
        with = "bytes_num",
        default = "MetaConfig::default_usage_schema_cache_size"
    )]
    pub usage_schema_cache_size: u64,
    #[serde(
        with = "bytes_num",
        default = "MetaConfig::default_cluster_schema_cache_size"
    )]
    pub cluster_schema_cache_size: u64,
}

impl MetaConfig {
    fn default_service_addr() -> Vec<String> {
        vec!["127.0.0.1:8901".to_string()]
    }

    fn default_report_time_interval() -> Duration {
        Duration::from_secs(30)
    }

    pub fn default_usage_schema_cache_size() -> u64 {
        2 * 1024 * 1024
    }

    pub fn default_cluster_schema_cache_size() -> u64 {
        2 * 1024 * 1024
    }
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            service_addr: MetaConfig::default_service_addr(),
            report_time_interval: MetaConfig::default_report_time_interval(),
            usage_schema_cache_size: MetaConfig::default_usage_schema_cache_size(),
            cluster_schema_cache_size: MetaConfig::default_cluster_schema_cache_size(),
        }
    }
}

impl CheckConfig for MetaConfig {
    fn check(&self, _: &super::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("meta".to_string());
        let mut ret = CheckConfigResult::default();

        for meta_addr in self.service_addr.iter() {
            if let Err(e) = meta_addr.to_socket_addrs() {
                ret.add_error(CheckConfigItemResult {
                    config: config_name.clone(),
                    item: meta_addr.clone(),
                    message: format!("Cannot resolve 'meta_service_addr': {}", e),
                });
            }
        }

        if self.report_time_interval.as_nanos() == Duration::from_secs(0).as_nanos() {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: "heartbeat".to_string(),
                message: "'report_time_interval' can not be zero".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
