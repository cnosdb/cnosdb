use std::fs::File;
use std::io;
use std::io::Read;
use std::path::{Path, PathBuf};

use check::{CheckConfig, CheckConfigResult};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

pub use crate::cache_config::*;
pub use crate::cluster_config::*;
pub use crate::deployment_config::*;
pub use crate::global_config::*;
pub use crate::limiter_config::*;
pub use crate::log_config::*;
pub use crate::meta_config::*;
pub use crate::override_by_env::OverrideByEnv;
pub use crate::query_config::*;
pub use crate::security_config::*;
pub use crate::service_config::*;
pub use crate::storage_config::*;
pub use crate::trace::*;
pub use crate::wal_config::*;

mod cache_config;
mod check;
mod cluster_config;
mod codec;
mod deployment_config;
mod global_config;
mod limiter_config;
mod log_config;
mod meta_config;
mod override_by_env;
mod query_config;
mod security_config;
mod service_config;
mod storage_config;
mod trace;
mod wal_config;

pub static VERSION: Lazy<String> = Lazy::new(|| {
    format!(
        "{}, revision {}",
        option_env!("CARGO_PKG_VERSION").unwrap_or("UNKNOWN"),
        option_env!("GIT_HASH").unwrap_or("UNKNOWN")
    )
});

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    ///
    #[serde(default = "Default::default")]
    pub global: GlobalConfig,

    ///
    #[serde(default = "Default::default")]
    pub deployment: DeploymentConfig,

    ///
    #[serde(default = "Default::default")]
    pub meta: MetaConfig,

    ///
    #[serde(default = "Default::default")]
    pub query: QueryConfig,

    ///
    #[serde(default = "Default::default")]
    pub storage: StorageConfig,

    ///
    #[serde(default = "Default::default")]
    pub wal: WalConfig,

    ///
    #[serde(default = "Default::default")]
    pub cache: CacheConfig,

    ///
    #[serde(default = "Default::default")]
    pub log: LogConfig,

    ///
    #[serde(default = "Default::default")]
    pub security: SecurityConfig,

    ///
    #[serde(default = "Default::default")]
    pub service: ServiceConfig,

    ///
    #[serde(default = "Default::default")]
    pub cluster: ClusterConfig,

    #[serde(default = "Default::default")]
    pub trace: TraceConfig,
}

impl Config {
    pub fn to_string_pretty(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_else(|_| "Failed to stringify Config".to_string())
    }
}

impl OverrideByEnv for Config {
    fn override_by_env(&mut self) {
        self.global.override_by_env();
        self.deployment.override_by_env();
        self.meta.override_by_env();
        self.query.override_by_env();
        self.storage.override_by_env();
        self.wal.override_by_env();
        self.cache.override_by_env();
        self.log.override_by_env();
        self.security.override_by_env();
        self.service.override_by_env();
        self.cluster.override_by_env();
        self.trace.override_by_env();
    }
}

pub fn get_config(path: impl AsRef<Path>) -> Result<Config, std::io::Error> {
    let path = path.as_ref();
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) => {
            return Err(io::Error::new(
                err.kind(),
                format!(
                    "Failed to open configurtion file '{}': {:?}",
                    path.display(),
                    err
                )
                .as_str(),
            ));
        }
    };
    let mut content = String::new();
    if let Err(err) = file.read_to_string(&mut content) {
        return Err(io::Error::new(
            err.kind(),
            format!(
                "Failed to read configurtion file '{}': {:?}",
                path.display(),
                err
            )
            .as_str(),
        ));
    }
    let mut config: Config = match toml::from_str(&content) {
        Ok(config) => config,
        Err(err) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "Failed to parse configurtion file '{}': {:?}",
                    path.display(),
                    err
                )
                .as_str(),
            ));
        }
    };
    config.wal.introspect();
    Ok(config)
}

pub fn get_config_for_test() -> Config {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path = path
        .parent()
        .unwrap()
        .join("config")
        .join("config_8902.toml");
    get_config(path).unwrap()
}

pub fn check_config(path: impl AsRef<Path>, show_warnings: bool) {
    match get_config(path) {
        Ok(cfg) => {
            let mut check_results = CheckConfigResult::default();

            if let Some(c) = cfg.global.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.deployment.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.meta.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.query.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.storage.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.wal.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.cache.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.log.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.security.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.service.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.cluster.check(&cfg) {
                check_results.add_all(c)
            }

            check_results.introspect();
            check_results.show_warnings = show_warnings;
            println!("{}", check_results);
        }
        Err(err) => {
            println!("{}", err);
        }
    };
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use crate::Config;

    #[test]
    fn test_write_read() {
        let cfg = Config::default();
        let dir = "/tmp/test/cnosdb/config/1/";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let cfg_path = "/tmp/test/cnosdb/config/1/config.toml";
        let mut cfg_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(false)
            .open(cfg_path)
            .unwrap();
        let _ = cfg_file.write(cfg.to_string_pretty().as_bytes()).unwrap();
        let cfg_2 = crate::get_config(cfg_path).unwrap();

        assert_eq!(cfg.to_string_pretty(), cfg_2.to_string_pretty());
    }

    #[test]
    fn test_get_test_config() {
        let _ = crate::get_config_for_test();
    }

    #[test]
    fn test_parse() {
        let config_str = r#"
[global]
# node_id = 100
host = "localhost"
cluster_name = 'cluster_xxx'
store_metrics = true

[deployment]
# mode = 'query_tskv'
# cpu = 8
# memory = 16

[meta]
service_addr = ["127.0.0.1:8901"]
report_time_interval_secs = 30

[query]
max_server_connections = 10240
query_sql_limit = 16777216     # 16 * 1024 * 1024
write_sql_limit = 167772160    # 160 * 1024 * 1024
auth_enabled = false
read_timeout_ms = 3000
write_timeout_ms = 3000
stream_trigger_cpu = 1
stream_executor_cpu = 2

[storage]

## The directory where database files stored.
# Directory for summary:    $path/summary
# Directory for index:      $path/$database/data/id/index
# Directory for tsm:        $path/$database/data/id/tsm
# Directory for delta:      $path/$database/data/id/delta
path = '/var/lib/cnosdb/data'

## The maximum file size of summary file.
# max_summary_size = "128M" # 134,217,728 bytes

## The maximum file size of a level is as follows:
## $base_file_size * level * $compact_trigger_file_num
# base_file_size = "16M" # 16,777,216 bytes

## The maxmimum amount of flush requests in memory
# flush_req_channel_cap = 16

## The maximum count of opened file handles (for query) in each vnode.
# max_cached_readers = 32

## The maxmimum level of a data file (from 0 to 4).
# max_level = 4

# Trigger of compaction using the number of level 0 files.
# compact_trigger_file_num = 4

## Duration since last write to trigger compaction.
# compact_trigger_cold_duration = "1h"

## The maximum size of all files in a compaction.
# max_compact_size = "2G" # 2,147,483,648 bytes

## The maximum concurrent compactions.
# max_concurrent_compaction = 4

## If true, write request will not be checked in detail.
strict_write = false

# The size of reserve space of the system.
reserve_space = "10G"

[wal]

## If true, write requets on disk before writing to memory.
enabled = true

## The directory where write ahead logs stored.
path = '/var/lib/cnosdb/wal'

## The maxmimum amount of write request in memory.
# wal_req_channel_cap = 64

## The maximum size of a WAL.
# max_file_size = '1G' # 1,073,741,824 bytes

## Trigger all vnode flushing if size of WALs exceeds this value.
# flush_trigger_total_file_size = '2G' # 2,147,483,648 bytes

## If true, fsync will be called after every WAL writes.
# sync = false

## Interval for automatic WAL fsync.
# sync_interval = '0' # h, m, s

[cache]

## The maximum size of a mutable cache.
# max_buffer_size = '128M' # 134,217,728 bytes

## The maximum amount of immutable caches.
# max_immutable_number = 4

## The partion number of memcache cache,default equal to cpu number
# partition = 8

[log]
level = 'info'
path = '/var/log/cnosdb'
## Tokio trace, default turn off tokio trace
# tokio_trace = { addr = "127.0.0.1:6669" }

[security]
# [security.tls_config]
# certificate = "./config/tls/server.crt"
# private_key = "./config/tls/server.key"

[service]
http_listen_port = 8902
grpc_listen_port = 8903
grpc_enable_gzip = false
flight_rpc_listen_port = 8904
tcp_listen_port = 8905
vector_listen_port = 8906
reporting_disabled = false


[cluster]
# raft_logs_to_keep = 5000

# [trace]
# auto_generate_span = false
# [trace.log]
# path = '/var/log/cnosdb'
# [trace.jaeger]
# jaeger_agent_endpoint = 'http://localhost:14268/api/traces'
# max_concurrent_exports = 2
# max_queue_size = 4096
"#;

        let config: Config = toml::from_str(config_str).unwrap();
        assert!(toml::to_string_pretty(&config).is_ok());
        dbg!(config);
    }

    #[test]
    fn test_parse_empty() {
        let config_str = "";

        let config: Config = toml::from_str(config_str).unwrap();
        assert!(toml::to_string_pretty(&config).is_ok());
        dbg!(config);
    }
}
