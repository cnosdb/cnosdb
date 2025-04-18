use std::sync::Arc;
use std::time::Duration;

use derive_traits::Keys;
use serde::{Deserialize, Serialize};

use crate::check::{CheckConfig, CheckConfigItemResult, CheckConfigResult};
use crate::codec::{bytes_num, duration};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Keys)]
pub struct QueryConfig {
    #[serde(default = "QueryConfig::default_max_server_connections")]
    pub max_server_connections: u32,
    #[serde(with = "bytes_num", default = "QueryConfig::default_query_sql_limit")]
    pub query_sql_limit: u64,
    #[serde(with = "bytes_num", default = "QueryConfig::default_write_sql_limit")]
    pub write_sql_limit: u64,
    #[serde(default = "QueryConfig::default_auth_enabled")]
    pub auth_enabled: bool,
    #[serde(with = "duration", default = "QueryConfig::default_read_timeout")]
    pub read_timeout: Duration,
    #[serde(with = "duration", default = "QueryConfig::default_write_timeout")]
    pub write_timeout: Duration,
    #[serde(default = "QueryConfig::default_stream_trigger_cpu")]
    pub stream_trigger_cpu: usize,
    #[serde(default = "QueryConfig::default_stream_executor_cpu")]
    pub stream_executor_cpu: usize,
    #[serde(with = "duration", default = "QueryConfig::default_sql_record_timeout")]
    pub sql_record_timeout: Duration,
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

    fn default_read_timeout() -> Duration {
        Duration::from_millis(3_000)
    }

    fn default_write_timeout() -> Duration {
        Duration::from_millis(3_000)
    }

    fn default_stream_trigger_cpu() -> usize {
        1
    }

    fn default_stream_executor_cpu() -> usize {
        2
    }

    fn default_sql_record_timeout() -> Duration {
        Duration::from_secs(10)
    }
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_server_connections: Self::default_max_server_connections(),
            query_sql_limit: Self::default_query_sql_limit(),
            write_sql_limit: Self::default_write_sql_limit(),
            auth_enabled: Self::default_auth_enabled(),
            read_timeout: Self::default_read_timeout(),
            write_timeout: Self::default_write_timeout(),
            stream_trigger_cpu: Self::default_stream_trigger_cpu(),
            stream_executor_cpu: Self::default_stream_executor_cpu(),
            sql_record_timeout: Self::default_sql_record_timeout(),
        }
    }
}

impl CheckConfig for QueryConfig {
    fn check(&self, _: &super::Config) -> Option<CheckConfigResult> {
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

        if self.read_timeout.as_nanos() < Duration::from_millis(10).as_nanos() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "read_timeout".to_string(),
                message: "'read_timeout' maybe too small(less than 10)".to_string(),
            })
        }

        if self.write_timeout.as_nanos() < Duration::from_millis(10).as_nanos() {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "write_timeout".to_string(),
                message: "'write_timeout' maybe too small(less than 10)".to_string(),
            })
        }

        if self.stream_executor_cpu > 1024 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "stream_executor_cpu".to_string(),
                message: "'stream_executor_cpu' maybe too big(more than 1024)".to_string(),
            })
        }
        if self.stream_trigger_cpu > 1024 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name.clone(),
                item: "stream_trigger_cpu".to_string(),
                message: "'stream_trigger_cpu' maybe too big(more than 1024)".to_string(),
            })
        }

        if self.sql_record_timeout.as_secs() < 1 {
            ret.add_warn(CheckConfigItemResult {
                config: config_name,
                item: "sql_record_timeout".to_string(),
                message: "'sql_record_timeout' maybe too small(less than 1)".to_string(),
            })
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }
}
