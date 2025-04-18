use derive_traits::Keys;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct MetaClusterConfig {
    #[serde(default = "MetaClusterConfig::default_lmdb_max_map_size")]
    pub lmdb_max_map_size: u64,
    #[serde(default = "MetaClusterConfig::default_heartbeat_interval")]
    pub heartbeat_interval: u64,
    #[serde(default = "MetaClusterConfig::default_raft_logs_to_keep")]
    pub raft_logs_to_keep: u64,
    #[serde(default = "MetaClusterConfig::default_install_snapshot_timeout")]
    pub install_snapshot_timeout: u64,
    #[serde(default = "MetaClusterConfig::default_send_append_entries_timeout")]
    pub send_append_entries_timeout: u64,
}

impl MetaClusterConfig {
    fn default_lmdb_max_map_size() -> u64 {
        1024 * 1024 * 1024
    }

    fn default_heartbeat_interval() -> u64 {
        3_000
    }

    fn default_raft_logs_to_keep() -> u64 {
        10_000
    }

    fn default_install_snapshot_timeout() -> u64 {
        3_600_000
    }

    fn default_send_append_entries_timeout() -> u64 {
        5_000
    }
}

impl Default for MetaClusterConfig {
    fn default() -> Self {
        Self {
            lmdb_max_map_size: Self::default_lmdb_max_map_size(),
            heartbeat_interval: Self::default_heartbeat_interval(),
            raft_logs_to_keep: Self::default_raft_logs_to_keep(),
            install_snapshot_timeout: Self::default_install_snapshot_timeout(),
            send_append_entries_timeout: Self::default_send_append_entries_timeout(),
        }
    }
}
