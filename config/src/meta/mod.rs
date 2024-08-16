mod heart_beat_config;

use std::collections::HashMap;
use std::path::Path;

use figment::providers::{Env, Format, Toml};
use figment::value::Uncased;
use figment::Figment;
pub use heart_beat_config::*;
use macros::EnvKeys;
use serde::{Deserialize, Serialize};

use crate::common::LogConfig;
use crate::{tskv, EnvKeys as _};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, EnvKeys)]
#[serde(default = "Default::default")]
pub struct Opt {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub data_path: String,
    pub grpc_enable_gzip: bool,
    pub lmdb_max_map_size: usize,
    pub heartbeat_interval: u64,
    pub raft_logs_to_keep: u64,
    pub install_snapshot_timeout: u64,    //ms
    pub send_append_entries_timeout: u64, //ms
    pub cluster_name: String,
    pub usage_schema_cache_size: u64,
    pub cluster_schema_cache_size: u64,
    pub system_database_replica: u64,

    pub log: LogConfig,
    pub heartbeat: HeartBeatConfig,
}

impl Opt {
    pub fn to_string_pretty(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_else(|_| "Failed to stringify Config".to_string())
    }
}

impl Default for Opt {
    fn default() -> Self {
        Self {
            id: 1,
            host: String::from("127.0.0.1"),
            port: 8901,
            data_path: String::from("/var/lib/cnosdb/meta"),
            grpc_enable_gzip: false,
            log: Default::default(),
            cluster_name: String::from("cluster_xxx"),
            heartbeat: Default::default(),

            lmdb_max_map_size: 1024 * 1024 * 1024,
            heartbeat_interval: 3 * 1000,
            raft_logs_to_keep: 10000,
            install_snapshot_timeout: 3600 * 1000,
            send_append_entries_timeout: 5 * 1000,
            usage_schema_cache_size: tskv::MetaConfig::default_cluster_schema_cache_size(),
            cluster_schema_cache_size: tskv::MetaConfig::default_usage_schema_cache_size(),
            system_database_replica: tskv::MetaConfig::default_system_database_replica(),
        }
    }
}

pub fn get_opt(path: Option<impl AsRef<Path>>) -> Opt {
    let env_keys = Opt::env_keys();
    let env_key_map = env_keys
        .into_iter()
        .map(|key| (key.replace('.', "_"), key))
        .collect::<HashMap<String, String>>();
    let mut figment = Figment::new();
    if let Some(path) = path.as_ref() {
        figment = figment.merge(Toml::file(path.as_ref()));
    }
    figment = figment.merge(Env::prefixed("CNOSDB_META_").filter_map(move |env| {
        env_key_map
            .get(env.as_str())
            .map(|key| Uncased::from_owned(key.clone()))
    }));
    figment.extract().unwrap()
}

#[cfg(test)]
mod test {
    use crate::meta::Opt;

    #[test]
    fn test() {
        let config_str = r#"
id = 1
host = "127.0.0.1"
port = 8901
data_path = "/tmp/cnosdb/meta"
cluster_name = "cluster_xxx"
grpc_enable_gzip = false

[log]
level = "warn"
path = "/tmp/cnosdb/logs"


[heartbeat]
heartbeat_recheck_interval = 300
heartbeat_expired_interval = 600
"#;

        let config: Opt = toml::from_str(config_str).unwrap();
        assert!(toml::to_string_pretty(&config).is_ok());
        dbg!(config);
    }
}
