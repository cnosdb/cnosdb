use std::sync::Arc;

use derive_traits::Keys;
use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct TokioTrace {
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
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
        "/var/log/cnosdb".to_string()
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

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
            path: Self::default_path(),
            max_file_count: Self::default_max_file_count(),
            file_rotation: Self::default_file_rotation(),
            tokio_trace: Self::default_tokio_trace(),
        }
    }
}

impl CheckConfig for LogConfig {
    fn check(&self, _: &crate::tskv::Config) -> Option<CheckConfigResult> {
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
