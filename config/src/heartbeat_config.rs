use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartBeatConfig {
    #[serde(default = "HeartBeatConfig::default_report_time_interval_secs")]
    pub report_time_interval_secs: u64,
}

impl HeartBeatConfig {
    pub fn default_report_time_interval_secs() -> u64 {
        30
    }
}

impl Default for HeartBeatConfig {
    fn default() -> Self {
        Self {
            report_time_interval_secs: Self::default_report_time_interval_secs(),
        }
    }
}

impl CheckConfig for HeartBeatConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("heartbeat".to_string());
        let mut ret = CheckConfigResult::default();

        if self.report_time_interval_secs == 0 {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: "heartbeat".to_string(),
                message: "'report_time_interval_secs' can not be zero".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
