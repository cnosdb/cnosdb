use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct QueryConfig {
    #[serde(default = "QueryConfig::default_max_server_connections")]
    pub max_server_connections: u32,
    #[serde(default = "QueryConfig::default_query_sql_limit")]
    pub query_sql_limit: u64,
    #[serde(default = "QueryConfig::default_write_sql_limit")]
    pub write_sql_limit: u64,
    #[serde(default = "QueryConfig::default_auth_enabled")]
    pub auth_enabled: bool,
    #[serde(default = "QueryConfig::default_read_timeout_ms")]
    pub read_timeout_ms: u64,
    #[serde(default = "QueryConfig::default_write_timeout_ms")]
    pub write_timeout_ms: u64,
}

impl QueryConfig {
    fn default_max_server_connections() -> u32 {
        10240
    }

    fn default_query_sql_limit() -> u64 {
        16 * 1024 * 1024
    }

    fn default_write_sql_limit() -> u64 {
        160 * 1024 * 1024
    }

    fn default_auth_enabled() -> bool {
        false
    }

    fn default_read_timeout_ms() -> u64 {
        3 * 1000
    }

    fn default_write_timeout_ms() -> u64 {
        3 * 1000
    }

    pub fn override_by_env(&mut self) {
        if let Ok(size) = std::env::var("MAX_SERVER_CONNECTIONS") {
            self.max_server_connections = size.parse::<u32>().unwrap();
        }
        if let Ok(size) = std::env::var("QUERY_SQL_LIMIT") {
            self.query_sql_limit = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("WRITE_SQL_LIMIT") {
            self.write_sql_limit = size.parse::<u64>().unwrap();
        }
        if let Ok(val) = std::env::var("AUTH_ENABLED") {
            self.auth_enabled = val.parse::<bool>().unwrap();
        }
        if let Ok(size) = std::env::var("READ_TIMEOUT_MS") {
            self.read_timeout_ms = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("WRITE_TIMEOUT_MS") {
            self.write_timeout_ms = size.parse::<u64>().unwrap();
        }
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_server_connections: Self::default_max_server_connections(),
            query_sql_limit: Self::default_query_sql_limit(),
            write_sql_limit: Self::default_write_sql_limit(),
            auth_enabled: Self::default_auth_enabled(),
            read_timeout_ms: Self::default_read_timeout_ms(),
            write_timeout_ms: Self::default_write_timeout_ms(),
        }
    }
}

impl CheckConfig for QueryConfig {
    fn check(&self, _: &crate::Config) -> Option<CheckConfigResult> {
        let config_name = Arc::new("query".to_string());
        let mut ret = CheckConfigResult::default();

        if self.max_server_connections < 16 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "max_server_connections".to_string(),
                message: "'max_server_connections' maybe too small(less than 16)".to_string(),
            })
        }
        if self.query_sql_limit < 64 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "query_sql_limit".to_string(),
                message: "'query_sql_limit' maybe too small(less than 64)".to_string(),
            })
        }
        if self.write_sql_limit < 64 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "write_sql_limit".to_string(),
                message: "'write_sql_limit' maybe too small(less than 64)".to_string(),
            })
        }

        if self.read_timeout_ms < 10 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "read_timeout_ms".to_string(),
                message: "'read_timeout_ms' maybe too small(less than 10)".to_string(),
            })
        }

        if self.write_timeout_ms < 10 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "write_timeout_ms".to_string(),
                message: "'write_timeout_ms' maybe too small(less than 10)".to_string(),
            })
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
