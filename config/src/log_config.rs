use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::override_by_env::{entry_override, OverrideByEnv};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokioTrace {
    pub addr: String,
}

impl OverrideByEnv for Option<TokioTrace> {
    fn override_by_env(&mut self) {
        let is_some = self.is_some();
        let mut tokio_trace = self.take().unwrap_or(TokioTrace {
            addr: Default::default(),
        });
        *self = match (
            is_some,
            entry_override(&mut tokio_trace.addr, "CNOSDB_LOG_TOKIO_TRACE_ADDR"),
        ) {
            (_, true) | (true, false) => Some(tokio_trace),
            (false, false) => None,
        };
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogConfig {
    #[serde(default = "LogConfig::default_level")]
    pub level: String,
    #[serde(default = "LogConfig::default_path")]
    pub path: String,
    pub tokio_trace: Option<TokioTrace>,
}

impl LogConfig {
    fn default_level() -> String {
        "info".to_string()
    }

    fn default_path() -> String {
        "/var/log/cnosdb".to_string()
    }

    pub fn override_by_env(&mut self) {
        if let Ok(level) = std::env::var("CNOSDB_LOG_LEVEL") {
            self.level = level;
        }
        if let Ok(path) = std::env::var("CNOSDB_LOG_PATH") {
            self.path = path;
        }
    }
}

impl OverrideByEnv for LogConfig {
    fn override_by_env(&mut self) {
        entry_override(&mut self.level, "CNOSDB_LOG_LEVEL");
        entry_override(&mut self.path, "CNOSDB_LOG_PATH");
        self.tokio_trace.override_by_env();
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
            path: Self::default_path(),
            tokio_trace: None,
        }
    }
}

impl CheckConfig for LogConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("log".to_string());
        let mut ret = CheckConfigResult::default();

        if self.path.is_empty() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "path".to_string(),
                message: "'path' is empty".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
