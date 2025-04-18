use std::sync::Arc;

use derive_traits::Keys;
use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct GlobalConfig {
    #[serde(default = "GlobalConfig::default_node_id")]
    pub node_id: u64,
    #[serde(default = "GlobalConfig::default_host")]
    pub host: String,
    #[serde(default = "GlobalConfig::default_cluster_name")]
    pub cluster_name: String,
    #[serde(default = "GlobalConfig::default_store_metrics")]
    pub store_metrics: bool,
    #[serde(default = "GlobalConfig::default_pre_create_bucket")]
    pub pre_create_bucket: bool,
}

impl GlobalConfig {
    fn default_node_id() -> u64 {
        1001
    }

    fn default_host() -> String {
        "127.0.0.1".to_string()
    }

    fn default_cluster_name() -> String {
        "cluster_xxx".to_string()
    }

    fn default_store_metrics() -> bool {
        true
    }

    fn default_pre_create_bucket() -> bool {
        false
    }
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            node_id: GlobalConfig::default_node_id(),
            host: GlobalConfig::default_host(),
            cluster_name: GlobalConfig::default_cluster_name(),
            store_metrics: GlobalConfig::default_store_metrics(),
            pre_create_bucket: GlobalConfig::default_pre_create_bucket(),
        }
    }
}

impl CheckConfig for GlobalConfig {
    fn check(&self, _: &super::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("global".to_string());
        let mut ret = CheckConfigResult::default();

        if self.cluster_name.is_empty() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "name".to_string(),
                message: "'name' is empty".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
