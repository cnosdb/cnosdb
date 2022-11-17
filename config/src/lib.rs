mod bytes_num;
mod duration;
pub mod limiter_config;

use std::fs::File;
use std::io::prelude::Read;
use std::path::Path;
use std::time::Duration;

pub use limiter_config::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    #[serde(default = "Config::default_reporting_disabled")]
    pub reporting_disabled: bool,
    pub query: QueryConfig,
    pub storage: StorageConfig,
    pub wal: WalConfig,
    pub cache: CacheConfig,
    pub log: LogConfig,
    pub security: SecurityConfig,
    pub cluster: ClusterConfig,
    pub hintedoff: HintedOffConfig,
}

impl Config {
    fn default_reporting_disabled() -> bool {
        false
    }

    pub fn override_by_env(&mut self) {
        self.cluster.override_by_env();
        self.storage.override_by_env();
        self.wal.override_by_env();
        self.cache.override_by_env();
        self.query.override_by_env();
    }

    pub fn to_string_pretty(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_else(|_| "Failed to stringfy Config".to_string())
    }
}

pub fn get_config(path: impl AsRef<Path>) -> Config {
    let path = path.as_ref();
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) => panic!(
            "Failed to open configurtion file '{}': {}",
            path.display(),
            err
        ),
    };
    let mut content = String::new();
    if let Err(err) = file.read_to_string(&mut content) {
        panic!(
            "Failed to read configurtion file '{}': {}",
            path.display(),
            err
        );
    }
    let mut config: Config = match toml::from_str(&content) {
        Ok(config) => config,
        Err(err) => panic!(
            "Failed to parse configurtion file '{}': {}",
            path.display(),
            err
        ),
    };
    config.wal.introspect();
    config
}

pub fn default_config() -> Config {
    const DEFAULT_CONFIG: &str = r#"
    [query]
    [storage]
    [wal]
    [cache]
    [log]
    [security]
    [cluster]
    [hintedoff]"#;

    match toml::from_str(DEFAULT_CONFIG) {
        Ok(config) => config,
        Err(err) => panic!("Failed to get default configurtion : {}", err),
    }
}

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
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageConfig {
    #[serde(default = "StorageConfig::default_path")]
    pub path: String,
    #[serde(
        with = "bytes_num",
        default = "StorageConfig::default_max_summary_size"
    )]
    pub max_summary_size: u64,
    #[serde(with = "bytes_num", default = "StorageConfig::default_base_file_size")]
    pub base_file_size: u64,
    #[serde(default = "StorageConfig::default_flush_req_channel_cap")]
    pub flush_req_channel_cap: usize,
    #[serde(default = "StorageConfig::default_max_level")]
    pub max_level: u16,
    #[serde(default = "StorageConfig::default_compact_trigger_file_num")]
    pub compact_trigger_file_num: u32,
    #[serde(
        with = "duration",
        default = "StorageConfig::default_compact_trigger_cold_duration"
    )]
    pub compact_trigger_cold_duration: Duration,
    #[serde(
        with = "bytes_num",
        default = "StorageConfig::default_max_compact_size"
    )]
    pub max_compact_size: u64,
    #[serde(default = "StorageConfig::default_max_concurrent_compaction")]
    pub max_concurrent_compaction: u16,
    #[serde(default = "StorageConfig::default_strict_write")]
    pub strict_write: bool,
}

impl StorageConfig {
    fn default_path() -> String {
        "data/db".to_string()
    }

    fn default_max_summary_size() -> u64 {
        128 * 1024 * 1024
    }

    fn default_base_file_size() -> u64 {
        16 * 1024 * 1024
    }

    fn default_flush_req_channel_cap() -> usize {
        16
    }

    fn default_max_level() -> u16 {
        4
    }

    fn default_compact_trigger_file_num() -> u32 {
        4
    }

    fn default_compact_trigger_cold_duration() -> Duration {
        Duration::from_secs(0)
    }

    fn default_max_compact_size() -> u64 {
        2 * 1024 * 1024 * 1024
    }

    fn default_max_concurrent_compaction() -> u16 {
        4
    }

    fn default_strict_write() -> bool {
        false
    }

    pub fn override_by_env(&mut self) {
        if let Ok(path) = std::env::var("CNOSDB_APPLICATION_PATH") {
            self.path = path;
        }
        if let Ok(size) = std::env::var("CNOSDB_SUMMARY_MAX_SUMMARY_SIZE") {
            self.max_summary_size = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_BASE_FILE_SIZE") {
            self.base_file_size = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_FLUSH_REQ_CHANNEL_CAP") {
            self.flush_req_channel_cap = size.parse::<usize>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_MAX_LEVEL") {
            self.max_level = size.parse::<u16>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_COMPACT_TRIGGER_FILE_NUM") {
            self.compact_trigger_file_num = size.parse::<u32>().unwrap();
        }
        if let Ok(dur) = std::env::var("CNOSDB_STORAGE_compact_trigger_cold_duration") {
            self.compact_trigger_cold_duration = duration::parse_duration(&dur).unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_MAX_COMPACT_SIZE") {
            self.max_compact_size = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_MAX_CONCURRENT_COMPACTION") {
            self.max_concurrent_compaction = size.parse::<u16>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_STORAGE_STRICT_WRITE") {
            self.strict_write = size.parse::<bool>().unwrap();
        }

        self.introspect();
    }

    pub fn introspect(&mut self) {
        // Unit of storage.compact_trigger_cold_duration is seconds
        self.compact_trigger_cold_duration =
            Duration::from_secs(self.compact_trigger_cold_duration.as_secs());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalConfig {
    #[serde(default = "WalConfig::default_wal_req_channel_cap")]
    pub wal_req_channel_cap: usize,
    #[serde(default = "WalConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "WalConfig::default_path")]
    pub path: String,
    #[serde(with = "bytes_num", default = "WalConfig::default_max_file_size")]
    pub max_file_size: u64,
    #[serde(default = "WalConfig::default_sync")]
    pub sync: bool,
    #[serde(with = "duration", default = "WalConfig::default_sync_interval")]
    pub sync_interval: Duration,
}

impl WalConfig {
    fn default_wal_req_channel_cap() -> usize {
        64
    }

    fn default_enabled() -> bool {
        true
    }

    fn default_path() -> String {
        "data/wal".to_string()
    }

    fn default_max_file_size() -> u64 {
        1024 * 1024 * 1024
    }

    fn default_sync() -> bool {
        false
    }

    fn default_sync_interval() -> Duration {
        Duration::from_secs(0)
    }

    pub fn override_by_env(&mut self) {
        if let Ok(cap) = std::env::var("CNOSDB_WAL_REQ_CHANNEL_CAP") {
            self.wal_req_channel_cap = cap.parse::<usize>().unwrap();
        }
        if let Ok(enabled) = std::env::var("CNOSDB_WAL_ENABLED") {
            self.enabled = enabled.as_str() == "true";
        }
        if let Ok(path) = std::env::var("CNOSDB_WAL_PATH") {
            self.path = path;
        }
        if let Ok(sync) = std::env::var("CNOSDB_WAL_SYNC") {
            self.sync = sync.as_str() == sync;
        }
    }

    pub fn introspect(&mut self) {
        // Unit of wal.sync_interval is seconds
        self.sync_interval = Duration::from_secs(self.sync_interval.as_secs());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheConfig {
    #[serde(with = "bytes_num", default = "CacheConfig::default_max_buffer_size")]
    pub max_buffer_size: u64,
    #[serde(default = "CacheConfig::default_max_immutable_number")]
    pub max_immutable_number: u16,
}

impl CacheConfig {
    fn default_max_buffer_size() -> u64 {
        128 * 1024 * 1024
    }

    fn default_max_immutable_number() -> u16 {
        4
    }

    pub fn override_by_env(&mut self) {
        if let Ok(size) = std::env::var("CNOSDB_CACHE_MAX_BUFFER_SIZE") {
            self.max_buffer_size = size.parse::<u64>().unwrap();
        }
        if let Ok(size) = std::env::var("CNOSDB_CACHE_MAX_IMMUTABLE_NUMBER") {
            self.max_immutable_number = size.parse::<u16>().unwrap();
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogConfig {
    #[serde(default = "LogConfig::default_level")]
    pub level: String,
    #[serde(default = "LogConfig::default_path")]
    pub path: String,
}

impl LogConfig {
    fn default_level() -> String {
        "info".to_string()
    }

    fn default_path() -> String {
        "data/log".to_string()
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecurityConfig {
    pub tls_config: Option<TLSConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TLSConfig {
    #[serde(default = "TLSConfig::default_certificate")]
    pub certificate: String,
    #[serde(default = "TLSConfig::default_private_key")]
    pub private_key: String,
}

impl TLSConfig {
    fn default_certificate() -> String {
        "./config/tls/server.crt".to_string()
    }

    fn default_private_key() -> String {
        "./config/tls/server.key".to_string()
    }
}

#[derive(Debug, Clone, Serialize, Default, Deserialize, PartialEq, Eq)]
pub struct ClusterConfig {
    #[serde(default = "ClusterConfig::default_node_id")]
    pub node_id: u64,
    #[serde(default = "ClusterConfig::default_name")]
    pub name: String,
    #[serde(default = "ClusterConfig::default_meta_service_addr")]
    pub meta_service_addr: String,

    #[serde(default = "ClusterConfig::default_http_listen_addr")]
    pub http_listen_addr: String,
    #[serde(default = "ClusterConfig::default_grpc_listen_addr")]
    pub grpc_listen_addr: String,
    #[serde(default = "ClusterConfig::default_tcp_listen_addr")]
    pub tcp_listen_addr: String,
    #[serde(default = "ClusterConfig::default_flight_rpc_listen_addr")]
    pub flight_rpc_listen_addr: String,
}

impl ClusterConfig {
    fn default_node_id() -> u64 {
        100
    }

    fn default_name() -> String {
        "cluster_xxx".to_string()
    }

    fn default_meta_service_addr() -> String {
        "127.0.0.1:21001".to_string()
    }

    fn default_http_listen_addr() -> String {
        "127.0.0.1:31007".to_string()
    }

    fn default_grpc_listen_addr() -> String {
        "127.0.0.1:31008".to_string()
    }

    fn default_tcp_listen_addr() -> String {
        "127.0.0.1:31009".to_string()
    }

    fn default_flight_rpc_listen_addr() -> String {
        "127.0.0.1:31006".to_string()
    }

    pub fn override_by_env(&mut self) {
        if let Ok(name) = std::env::var("CNOSDB_CLUSTER_NAME") {
            self.name = name;
        }
        if let Ok(meta) = std::env::var("CNOSDB_CLUSTER_META") {
            self.meta_service_addr = meta;
        }
        if let Ok(id) = std::env::var("CNOSDB_NODE_ID") {
            self.node_id = id.parse::<u64>().unwrap();
        }

        if let Ok(val) = std::env::var("CNOSDB_http_listen_addr") {
            self.http_listen_addr = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_grpc_listen_addr") {
            self.grpc_listen_addr = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_tcp_listen_addr") {
            self.tcp_listen_addr = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_flight_rpc_listen_addr") {
            self.flight_rpc_listen_addr = val;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HintedOffConfig {
    #[serde(default = "HintedOffConfig::default_enable")]
    pub enable: bool,
    #[serde(default = "HintedOffConfig::default_path")]
    pub path: String,
}

impl HintedOffConfig {
    fn default_enable() -> bool {
        true
    }

    fn default_path() -> String {
        "/tmp/cnosdb/hh".to_string()
    }

    pub fn override_by_env(&mut self) {
        if let Ok(enable) = std::env::var("CNOSDB_HINTEDOFF_ENABLE") {
            self.enable = enable.parse::<bool>().unwrap();
        }
        if let Ok(path) = std::env::var("CNOSDB_HINTEDOFF_PATH") {
            self.path = path;
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use crate::Config;

    #[test]
    fn test_functions() {
        let cfg = crate::default_config();
        std::fs::create_dir_all("/tmp/test/config/1/").unwrap();
        let cfg_path = "/tmp/test/config/1/config.toml";
        let mut cfg_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(cfg_path)
            .unwrap();
        let _ = cfg_file.write(cfg.to_string_pretty().as_bytes()).unwrap();
        let cfg_2 = crate::get_config(cfg_path);

        assert_eq!(cfg, cfg_2);
    }

    #[test]
    fn test() {
        let config_str = r#"
#reporting_disabled = false

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

# The maximum size of a wal file.
max_file_size = "1G" # 1073741824

# If true, fsync will be called after every wal writes.
sync = false
sync_interval = "10s" # h, m, s

[cache]
max_buffer_size = "128M" # 134217728
max_immutable_number = 4

[log]
level = 'info'
path = 'data/log'

[security]
# [security.tls_config]
# certificate = "./config/tls/server.crt"
# private_key = "./config/tls/server.key"

[cluster]
node_id = 100
name = 'cluster_xxx'
meta_service_addr = '127.0.0.1:21001'
tenant = ''

flight_rpc_listen_addr = '127.0.0.1:31006'
http_listen_addr = '127.0.0.1:31007'
grpc_listen_addr = '127.0.0.1:31008'
tcp_listen_addr = '127.0.0.1:31009'

[hintedoff]
enable = true
path = '/tmp/cnosdb/hh'
"#;

        let config: Config = toml::from_str(config_str).unwrap();
        assert!(toml::to_string_pretty(&config).is_ok());
        dbg!(config);
    }
}
