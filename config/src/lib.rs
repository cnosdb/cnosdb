use std::fs::File;
use std::io;
use std::io::Read;
use std::path::{Path, PathBuf};

use check::{CheckConfig, CheckConfigResult};
use serde::{Deserialize, Serialize};

pub use crate::cache_config::*;
pub use crate::cluster_config::*;
pub use crate::deployment_config::*;
pub use crate::heartbeat_config::*;
pub use crate::hinted_off_config::*;
pub use crate::limiter_config::*;
pub use crate::log_config::*;
pub use crate::node_config::*;
pub use crate::query_config::*;
pub use crate::security_config::*;
pub use crate::storage_config::*;
pub use crate::trace::*;
pub use crate::wal_config::*;

mod cache_config;
mod check;
mod cluster_config;
mod codec;
mod deployment_config;
mod heartbeat_config;
mod hinted_off_config;
mod limiter_config;
mod log_config;
mod node_config;
mod query_config;
mod security_config;
mod storage_config;
mod trace;
mod wal_config;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    ///
    #[serde(default = "Config::default_reporting_disabled")]
    pub reporting_disabled: bool,

    ///
    #[serde(default = "Config::default_host")]
    pub host: String,

    ///
    #[serde(default = "Default::default")]
    pub deployment: DeploymentConfig,

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
    pub cluster: ClusterConfig,

    ///
    #[serde(default = "Default::default")]
    pub hinted_off: HintedOffConfig,

    ///
    #[serde(default = "Default::default")]
    pub heartbeat: HeartBeatConfig,

    ///
    #[serde(default = "Default::default")]
    pub node_basic: NodeBasicConfig,

    #[serde(default = "Default::default")]
    pub trace: TraceConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            reporting_disabled: Self::default_reporting_disabled(),
            host: Self::default_host(),
            deployment: Default::default(),
            query: Default::default(),
            storage: Default::default(),
            wal: Default::default(),
            cache: Default::default(),
            log: Default::default(),
            security: Default::default(),
            cluster: Default::default(),
            hinted_off: Default::default(),
            heartbeat: Default::default(),
            node_basic: Default::default(),
            trace: Default::default(),
        }
    }
}

impl Config {
    fn default_reporting_disabled() -> bool {
        false
    }

    fn default_host() -> String {
        "localhost".to_string()
    }

    pub fn override_by_env(&mut self) {
        self.cluster.override_by_env();
        self.storage.override_by_env();
        self.wal.override_by_env();
        self.cache.override_by_env();
        self.query.override_by_env();
        self.node_basic.override_by_env();
    }

    pub fn to_string_pretty(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_else(|_| "Failed to stringify Config".to_string())
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

            if let Some(c) = cfg.deployment.check(&cfg) {
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
            if let Some(c) = cfg.cluster.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.hinted_off.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.heartbeat.check(&cfg) {
                check_results.add_all(c)
            }
            if let Some(c) = cfg.node_basic.check(&cfg) {
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
        std::fs::create_dir_all("/tmp/test/config/1/").unwrap();
        let cfg_path = "/tmp/test/config/1/config.toml";
        let mut cfg_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
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
#reporting_disabled = false
host = "localhost"

[deployment]
mode = 'singleton'
cpu = 4
memory = 16

[query]
max_server_connections = 10240
query_sql_limit = 16777216   # 16 * 1024 * 1024
write_sql_limit = 167772160  # 160 * 1024 * 1024
auth_enabled = false

[storage]

# The directory where database files stored.
# Directory for summary:    $path/summary
# Directory for index:      $path/$database/data/id/index
# Directory for tsm:        $path/$database/data/id/tsm
# Directory for delta:      $path/$database/data/id/delta
path = 'data/db'

# The maximum file size of summary file.
max_summary_size = "128M" # 134217728

# The maximum file size of a level is:
# $base_file_size * level * $compact_trigger_file_num
base_file_size = "16M" # 16777216

# The maxmimum data file level (from 0 to 4).
max_level = 4

# Trigger of compaction using the number of level 0 files.
compact_trigger_file_num = 4

# Duration since last write to trigger compaction.
compact_trigger_cold_duration = "1h"

# The maximum size of all files in a compaction.
max_compact_size = "2G" # 2147483648

# The maximum concurrent compactions.
max_concurrent_compaction = 4

# If true, write request will not be checked in detail.
strict_write = false

[wal]

# If true, write requets on disk before writing to memory.
enabled = true

# The directory where write ahead logs stored.
path = 'data/wal'

wal_req_channel_cap = 64

# The maximum size of a wal file.
max_file_size = "1G" # 1073741824
flush_trigger_total_file_size = "2G"

# If true, fsync will be called after every wal writes.
sync = false
sync_interval = "10s" # h, m, s

[cache]
max_buffer_size = "128M" # 134217728
max_immutable_number = 4

[log]
level = 'info'
path = 'data/log'
tokio_trace = { addr = "127.0.0.1:6669" }

[security]
# [security.tls_config]
# certificate = "./config/tls/server.crt"
# private_key = "./config/tls/server.key"

[cluster]
node_id = 100
name = 'cluster_xxx'
meta_service_addr = ["127.0.0.1:8901"]
tenant = ''

flight_rpc_listen_port = 31006
http_listen_port = 31007
grpc_listen_port = 31008

[node_basic]
node_id = 1001
cold_data_server = false
store_metrics = true

[heartbeat]
report_time_interval_secs = 30

[hinted_off]
enable = true
path = '/tmp/cnosdb/hh'
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
