use macros::EnvKeys;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
pub struct HeartBeatConfig {
    pub heartbeat_recheck_interval: u64,
    pub heartbeat_expired_interval: u64,
}

impl Default for HeartBeatConfig {
    fn default() -> Self {
        Self {
            heartbeat_recheck_interval: 30,
            heartbeat_expired_interval: 180,
        }
    }
}
