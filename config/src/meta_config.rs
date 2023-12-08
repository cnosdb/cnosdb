use std::net::ToSocketAddrs;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::override_by_env::{entry_override, entry_override_to_vec_string, OverrideByEnv};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaConfig {
    #[serde(default = "MetaConfig::default_service_addr")]
    pub service_addr: Vec<String>,
    #[serde(default = "MetaConfig::default_report_time_interval_secs")]
    pub report_time_interval_secs: u64,
}

impl MetaConfig {
    fn default_service_addr() -> Vec<String> {
        vec!["127.0.0.1:8901".to_string()]
    }

    fn default_report_time_interval_secs() -> u64 {
        30
    }
}

impl OverrideByEnv for MetaConfig {
    fn override_by_env(&mut self) {
        entry_override_to_vec_string(&mut self.service_addr, "CNOSDB_META_SERVICE_ADDR");
        entry_override(
            &mut self.report_time_interval_secs,
            "CNOSDB_META_REPORT_TIME_INTERVAL_SECS",
        );
    }
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            service_addr: MetaConfig::default_service_addr(),
            report_time_interval_secs: MetaConfig::default_report_time_interval_secs(),
        }
    }
}

impl CheckConfig for MetaConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
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

        if self.report_time_interval_secs == 0 {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: "heartbeat".to_string(),
                message: "'report_time_interval_secs' can not be zero".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
