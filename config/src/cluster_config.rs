use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigResult};
use crate::override_by_env::{entry_override, OverrideByEnv};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    #[serde(default = "ClusterConfig::default_raft_logs_to_keep")]
    pub raft_logs_to_keep: u64,

    #[serde(default = "ClusterConfig::default_using_raft_replication")]
    pub using_raft_replication: bool,
}

impl ClusterConfig {
    fn default_raft_logs_to_keep() -> u64 {
        5000
    }

    fn default_using_raft_replication() -> bool {
        false
    }
}

impl OverrideByEnv for ClusterConfig {
    fn override_by_env(&mut self) {
        entry_override(
            &mut self.raft_logs_to_keep,
            "CNOSDB_CLUSTER_RAFT_LOGS_TO_KEEP",
        );
        entry_override(
            &mut self.using_raft_replication,
            "CNOSDB_CLUSTER_USING_RAFT_REPLICATION",
        );
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            raft_logs_to_keep: ClusterConfig::default_raft_logs_to_keep(),
            using_raft_replication: ClusterConfig::default_using_raft_replication(),
        }
    }
}

impl CheckConfig for ClusterConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let _config_name = Arc::new("cluster".to_string());
        let ret = CheckConfigResult::default();

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
