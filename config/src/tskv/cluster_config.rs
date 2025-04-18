use std::sync::Arc;
use std::time::Duration;

use derive_traits::Keys;
use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigResult};
use crate::codec::{bytes_num, duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct ClusterConfig {
    #[serde(default = "ClusterConfig::default_raft_logs_to_keep")]
    pub raft_logs_to_keep: u64,

    #[serde(
        with = "duration",
        default = "ClusterConfig::default_snapshot_holding_time"
    )]
    pub snapshot_holding_time: Duration,

    #[serde(
        with = "duration",
        default = "ClusterConfig::default_trigger_snapshot_interval"
    )]
    pub trigger_snapshot_interval: Duration,

    #[serde(
        with = "bytes_num",
        default = "ClusterConfig::default_lmdb_max_map_size"
    )]
    pub lmdb_max_map_size: u64,

    #[serde(
        with = "duration",
        default = "ClusterConfig::default_heartbeat_interval"
    )]
    pub heartbeat_interval: Duration,

    #[serde(
        with = "duration",
        default = "ClusterConfig::default_send_append_entries_timeout"
    )]
    pub send_append_entries_timeout: Duration, //ms

    #[serde(
        with = "duration",
        default = "ClusterConfig::default_install_snapshot_timeout"
    )]
    pub install_snapshot_timeout: Duration, //ms
}

impl ClusterConfig {
    fn default_raft_logs_to_keep() -> u64 {
        5000
    }

    fn default_snapshot_holding_time() -> Duration {
        Duration::from_secs(3600)
    }

    fn default_trigger_snapshot_interval() -> Duration {
        Duration::from_secs(600)
    }

    fn default_lmdb_max_map_size() -> u64 {
        1024 * 1024 * 1024
    }

    fn default_heartbeat_interval() -> Duration {
        Duration::from_millis(300)
    }

    fn default_send_append_entries_timeout() -> Duration {
        Duration::from_millis(5_000)
    }

    fn default_install_snapshot_timeout() -> Duration {
        Duration::from_millis(3_600_000)
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            raft_logs_to_keep: ClusterConfig::default_raft_logs_to_keep(),
            snapshot_holding_time: ClusterConfig::default_snapshot_holding_time(),
            lmdb_max_map_size: ClusterConfig::default_lmdb_max_map_size(),
            heartbeat_interval: ClusterConfig::default_heartbeat_interval(),
            trigger_snapshot_interval: ClusterConfig::default_trigger_snapshot_interval(),
            send_append_entries_timeout: ClusterConfig::default_send_append_entries_timeout(),
            install_snapshot_timeout: ClusterConfig::default_install_snapshot_timeout(),
        }
    }
}

impl CheckConfig for ClusterConfig {
    fn check(&self, _: &super::Config) -> Option<CheckConfigResult> {
        let _config_name = Arc::new("cluster".to_string());
        let ret = CheckConfigResult::default();

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
