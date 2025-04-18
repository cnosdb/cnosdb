use derive_traits::Keys;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct HeartBeatConfig {
    #[serde(default = "HeartBeatConfig::default_heartbeat_recheck_interval")]
    pub heartbeat_recheck_interval: u64,
    #[serde(default = "HeartBeatConfig::default_heartbeat_expired_interval")]
    pub heartbeat_expired_interval: u64,
}

impl HeartBeatConfig {
    fn default_heartbeat_recheck_interval() -> u64 {
        30
    }

    fn default_heartbeat_expired_interval() -> u64 {
        180
    }
}

impl Default for HeartBeatConfig {
    fn default() -> Self {
        Self {
            heartbeat_recheck_interval: Self::default_heartbeat_recheck_interval(),
            heartbeat_expired_interval: Self::default_heartbeat_expired_interval(),
        }
    }
}
