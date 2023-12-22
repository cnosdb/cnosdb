use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeBasicConfig {
    #[serde(default = "NodeBasicConfig::default_node_id")]
    pub node_id: u64,
    #[serde(default = "NodeBasicConfig::default_store_metrics")]
    pub store_metrics: bool,
}

impl NodeBasicConfig {
    pub fn default_node_id() -> u64 {
        1001
    }

    pub fn default_store_metrics() -> bool {
        true
    }

    pub fn override_by_env(&mut self) {
        if let Ok(val) = std::env::var("CNOSDB_STORE_METRICS") {
            self.store_metrics = val.parse::<bool>().unwrap();
        }
    }
}

impl Default for NodeBasicConfig {
    fn default() -> Self {
        Self {
            node_id: Self::default_node_id(),
            store_metrics: Self::default_store_metrics(),
        }
    }
}

impl CheckConfig for NodeBasicConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let _config_name = Arc::new("node".to_string());
        let ret = CheckConfigResult::default();

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
