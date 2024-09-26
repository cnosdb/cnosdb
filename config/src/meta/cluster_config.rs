use macros::EnvKeys;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
pub struct MetaClusterConfig {
    pub lmdb_max_map_size: usize,
    pub heartbeat_interval: u64,
    pub raft_logs_to_keep: u64,
    pub install_snapshot_timeout: u64,
    pub send_append_entries_timeout: u64,
}

impl Default for MetaClusterConfig {
    fn default() -> Self {
        Self {
            lmdb_max_map_size: 1024 * 1024 * 1024,
            heartbeat_interval: 3 * 1000,
            raft_logs_to_keep: 10000,
            install_snapshot_timeout: 3600 * 1000,
            send_append_entries_timeout: 5 * 1000,
        }
    }
}
