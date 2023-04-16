use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubConfig {
    #[serde(default = "SubConfig::default_bufsize")]
    pub buf_size: usize,

    #[serde(default = "SubConfig::default_concurrency")]
    pub concurrency: usize,

    #[serde(default = "SubConfig::default_timeout")]
    pub timeout: usize,
}

impl SubConfig {
    fn default_bufsize() -> usize {
        1024
    }

    fn default_concurrency() -> usize {
        8
    }

    fn default_timeout() -> usize {
        1000
    }

    pub fn override_by_env(&mut self) {
        if let Ok(size) = std::env::var("CNOSDB_SUB_BUFSIZE") {
            self.buf_size = size.parse::<usize>().unwrap();
        }

        if let Ok(size) = std::env::var("CNOSDB_SUB_CONCURRENCY") {
            self.buf_size = size.parse::<usize>().unwrap();
        }

        if let Ok(size) = std::env::var("CNOSDB_SUB_TIMEOUT") {
            self.buf_size = size.parse::<usize>().unwrap();
        }
    }
}

impl Default for SubConfig {
    fn default() -> Self {
        Self {
            buf_size: Self::default_bufsize(),
            concurrency: Self::default_concurrency(),
            timeout: Self::default_timeout(),
        }
    }
}

impl CheckConfig for SubConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("subscriber".to_string());
        let mut ret = CheckConfigResult::default();

        if self.buf_size == 0 {
            ret.add_error(CheckConfigItemResult {
                config: config_name,
                item: "buf_size".to_string(),
                message: "'buf_size' too small".to_string(),
            });
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
