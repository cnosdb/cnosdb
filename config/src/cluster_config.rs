use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigResult};

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
