use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigResult};
use crate::override_by_env::{entry_override, OverrideByEnv};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    #[serde(default = "ClusterConfig::default_raft_logs_to_keep")]
    pub raft_logs_to_keep: u64,

    #[serde(default = "ClusterConfig::default_lmdb_max_map_size")]
    pub lmdb_max_map_size: usize,

    #[serde(default = "ClusterConfig::default_heartbeat_interval")]
    pub heartbeat_interval: u64,

    #[serde(default = "ClusterConfig::default_send_append_entries_timeout")]
    pub send_append_entries_timeout: u64, //ms

    #[serde(default = "ClusterConfig::default_install_snapshot_timeout")]
    pub install_snapshot_timeout: u64, //ms
}

impl ClusterConfig {
    fn default_raft_logs_to_keep() -> u64 {
        5000
    }

    fn default_lmdb_max_map_size() -> usize {
        1024 * 1024 * 1024
    }

    fn default_heartbeat_interval() -> u64 {
        10 * 1000
    }

    fn default_send_append_entries_timeout() -> u64 {
        5 * 1000
    }

    fn default_install_snapshot_timeout() -> u64 {
        3600 * 1000
    }
}

impl OverrideByEnv for ClusterConfig {
    fn override_by_env(&mut self) {
        entry_override(
            &mut self.raft_logs_to_keep,
            "CNOSDB_CLUSTER_RAFT_LOGS_TO_KEEP",
        );

        entry_override(
            &mut self.lmdb_max_map_size,
            "CNOSDB_CLUSTER_LMDB_MAX_MAP_SIZE",
        );

        entry_override(
            &mut self.heartbeat_interval,
            "CNOSDB_CLUSTER_HEARTBEAT_INTERVAL",
        );

        entry_override(
            &mut self.send_append_entries_timeout,
            "CNOSDB_CLUSTER_SEND_APPEND_ENTRIES_TIMEOUT",
        );

        entry_override(
            &mut self.install_snapshot_timeout,
            "CNOSDB_CLUSTER_INSTALL_SNAPSHOT_TIMEOUT",
        );
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            raft_logs_to_keep: ClusterConfig::default_raft_logs_to_keep(),
            lmdb_max_map_size: ClusterConfig::default_lmdb_max_map_size(),
            heartbeat_interval: ClusterConfig::default_heartbeat_interval(),
            send_append_entries_timeout: ClusterConfig::default_send_append_entries_timeout(),
            install_snapshot_timeout: ClusterConfig::default_install_snapshot_timeout(),
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
