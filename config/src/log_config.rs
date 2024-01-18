use std::env;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::override_by_env::{entry_override, entry_override_option, OverrideByEnv};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokioTrace {
    pub addr: String,
}

impl OverrideByEnv for Option<TokioTrace> {
    fn override_by_env(&mut self) {
        if env::var_os("CNOSDB_LOG_TOKIO_TRACE_ADDR").is_some() {
            let mut addr = String::new();
            entry_override(&mut addr, "CNOSDB_LOG_TOKIO_TRACE_ADDR");
            *self = Some(TokioTrace { addr });
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogConfig {
    #[serde(default = "LogConfig::default_level")]
    pub level: String,
    #[serde(default = "LogConfig::default_path")]
    pub path: String,
    #[serde(default = "LogConfig::default_max_file_count")]
    pub max_file_count: Option<usize>,
    #[serde(default = "LogConfig::default_file_rotation")]
    pub file_rotation: String,
    #[serde(default = "LogConfig::default_tokio_trace")]
    pub tokio_trace: Option<TokioTrace>,
}

impl LogConfig {
    fn default_level() -> String {
        "info".to_string()
    }

    fn default_path() -> String {
        let path = std::path::Path::new("cnosdb_data").join("logs");
        path.to_string_lossy().to_string()
    }

    fn default_max_file_count() -> Option<usize> {
        None
    }

    fn default_file_rotation() -> String {
        "daily".to_owned()
    }

    fn default_tokio_trace() -> Option<TokioTrace> {
        None
    }
}

impl OverrideByEnv for LogConfig {
    fn override_by_env(&mut self) {
        entry_override(&mut self.level, "CNOSDB_LOG_LEVEL");
        entry_override(&mut self.path, "CNOSDB_LOG_PATH");
        entry_override_option(&mut self.max_file_count, "CNOSDB_LOG_MAX_FILE_COUNT");
        entry_override(&mut self.file_rotation, "CNOSDB_LOG_FILE_ROTATION");
        self.tokio_trace.override_by_env();
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
            path: Self::default_path(),
            max_file_count: Self::default_max_file_count(),
            file_rotation: Self::default_file_rotation(),
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
