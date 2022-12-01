mod duration;

use std::{fs::File, io::prelude::Read, time::Duration};

use serde::{Deserialize, Serialize};
use trace::info;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub cluster: ClusterConfig,
    pub query: QueryConfig,
    pub storage: StorageConfig,
    pub wal: WalConfig,
    pub cache: CacheConfig,
    pub log: LogConfig,
    pub security: SecurityConfig,
    pub hintedoff: HintedOffConfig,
    pub reporting_disabled: Option<bool>,
}

impl Config {
    pub fn override_by_env(&mut self) {
        self.cluster.override_by_env();
        self.storage.override_by_env();
        self.wal.override_by_env();
        self.cache.override_by_env();
        self.query.override_by_env();
    }
}

pub fn get_config(path: &str) -> Config {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) => panic!("Failed to open configurtion file '{}': {}", path, err),
    };
    let mut content = String::new();
    if let Err(err) = file.read_to_string(&mut content) {
        panic!("Failed to read configurtion file '{}': {}", path, err);
    }
    let config: Config = match toml::from_str(&content) {
        Ok(config) => config,
        Err(err) => panic!("Failed to parse configurtion file '{}': {}", path, err),
    };
    info!("Start with configuration: {:#?}", config);
    config
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "StorageConfig::default_path")]
    pub path: String,
    #[serde(default = "StorageConfig::default_max_summary_size")]
    pub max_summary_size: u64,
    #[serde(default = "StorageConfig::default_base_file_size")]
    pub base_file_size: u64,
    #[serde(default = "StorageConfig::default_max_level")]
    pub max_level: u16,
    #[serde(default = "StorageConfig::default_compact_trigger_file_num")]
    pub compact_trigger_file_num: u32,
    #[serde(
        with = "duration",
        default = "StorageConfig::default_compact_trigger_cold_duration"
    )]
    pub compact_trigger_cold_duration: Duration,
    #[serde(default = "StorageConfig::default_max_compact_size")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    #[serde(default = "WalConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "WalConfig::default_path")]
    pub path: String,
    #[serde(default = "WalConfig::default_max_file_size")]
    pub max_file_size: u64,
    #[serde(default = "WalConfig::default_sync")]
    pub sync: bool,
}

impl WalConfig {
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

    pub fn override_by_env(&mut self) {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "CacheConfig::default_max_buffer_size")]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub tls_config: Option<TLSConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
pub struct ClusterConfig {
    #[serde(default = "ClusterConfig::default_node_id")]
    pub node_id: u64,
    #[serde(default = "ClusterConfig::default_name")]
    pub name: String,
    #[serde(default = "ClusterConfig::default_meta")]
    pub meta: String,
    #[serde(default = "ClusterConfig::default_tenant")]
    pub tenant: String,

    #[serde(default = "ClusterConfig::default_http_server")]
    pub http_server: String,
    #[serde(default = "ClusterConfig::default_grpc_server")]
    pub grpc_server: String,
    #[serde(default = "ClusterConfig::default_tcp_server")]
    pub tcp_server: String,
    #[serde(default = "ClusterConfig::default_flight_rpc_server")]
    pub flight_rpc_server: String,
}

impl ClusterConfig {
    fn default_node_id() -> u64 {
        100
    }

    fn default_name() -> String {
        "cluster_xxx".to_string()
    }

    fn default_meta() -> String {
        "127.0.0.1:21001".to_string()
    }

    fn default_tenant() -> String {
        "".to_string()
    }

    fn default_http_server() -> String {
        "127.0.0.1:31007".to_string()
    }

    fn default_grpc_server() -> String {
        "127.0.0.1:31008".to_string()
    }

    fn default_tcp_server() -> String {
        "127.0.0.1:31009".to_string()
    }

    fn default_flight_rpc_server() -> String {
        "127.0.0.1:31006".to_string()
    }

    pub fn override_by_env(&mut self) {
        if let Ok(name) = std::env::var("CNOSDB_CLUSTER_NAME") {
            self.name = name;
        }
        if let Ok(meta) = std::env::var("CNOSDB_CLUSTER_META") {
            self.meta = meta;
        }
        if let Ok(tenant) = std::env::var("CNOSDB_TENANT_NAME") {
            self.tenant = tenant;
        }
        if let Ok(id) = std::env::var("CNOSDB_NODE_ID") {
            self.node_id = id.parse::<u64>().unwrap();
        }

        if let Ok(val) = std::env::var("CNOSDB_HTTP_SERVER") {
            self.http_server = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_GRPC_SERVER") {
            self.grpc_server = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_TCP_SERVER") {
            self.tcp_server = val;
        }

        if let Ok(val) = std::env::var("CNOSDB_FLIGHT_RPC_SERVER") {
            self.flight_rpc_server = val;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[test]
fn test() {
    let config_str = r#"


#reporting_disabled = false

[query]
max_server_connections = 10240
query_sql_limit = 16777216
write_sql_limit = 167772160
auth_enabled = false

[storage]
# Directory for summary: $path/summary/
# Directory for index: $path/index/$database/
# Directory for tsm: $path/data/$database/tsm/
# Directory for delta: $path/data/$database/delta/
path = 'data/db'
max_summary_size = 134217728 # 128 * 1024 * 1024
base_file_size = 16777216 # 16 * 1024 * 1024
max_level = 4
compact_trigger_file_num = 4
compact_trigger_cold_duration = "1h"
max_compact_size = 2147483648 # 2 * 1024 * 1024 * 1024
max_concurrent_compaction = 4
strict_write = false

[wal]
enabled = true
path = 'data/wal'
sync = false

[cache]
max_buffer_size = 134217728 # 128 * 1024 * 1024
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
meta = '127.0.0.1,22001'

flight_rpc_server = '127.0.0.1:31006'
http_server = '127.0.0.1:31007'
grpc_server = '127.0.0.1:31008'
tcp_server = '127.0.0.1:31009'

[hintedoff]
enable = true
path = '/tmp/cnosdb/hh'

"#;

    let config: Config = toml::from_str(config_str).unwrap();
    dbg!(config);
}
