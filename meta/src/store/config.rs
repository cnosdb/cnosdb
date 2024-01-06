use std::fs::File;
use std::io::prelude::Read;
use std::path::Path;

use config::LogConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaInit {
    pub cluster_name: String,
    pub admin_user: String,
    pub system_tenant: String,
    pub default_database: Vec<String>,
}

impl Default for MetaInit {
    fn default() -> Self {
        Self {
            cluster_name: String::from("cluster_xxx"),
            admin_user: String::from("root"),
            system_tenant: String::from("cnosdb"),
            default_database: vec![String::from("public"), String::from("usage_schema")],
        }
    }
}

impl MetaInit {
    pub fn default_db_config(tenant: &str, db: &str) -> String {
        format!(
            "{{\"tenant\":\"{}\",\"database\":\"{}\",\"config\":{{\"ttl\":null,\"shard_num\":null,\"vnode_duration\":null,\"replica\":null,\"precision\":null,\"db_is_hidden\":false}}}}",
            tenant, db
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartBeatConfig {
    pub heartbeat_recheck_interval: u64,
    pub heartbeat_expired_interval: u64,
}

impl Default for HeartBeatConfig {
    fn default() -> Self {
        Self {
            heartbeat_recheck_interval: 300,
            heartbeat_expired_interval: 600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

    pub log: LogConfig,
    pub meta_init: MetaInit,
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
            meta_init: Default::default(),
            heartbeat: Default::default(),

            lmdb_max_map_size: 1024 * 1024 * 1024,
            heartbeat_interval: 3 * 1000,
            raft_logs_to_keep: 10000,
            install_snapshot_timeout: 3600 * 1000,
            send_append_entries_timeout: 5 * 1000,
        }
    }
}

pub fn get_opt(path: Option<impl AsRef<Path>>) -> Opt {
    if path.is_none() {
        return Default::default();
    }
    let path = path.unwrap();
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
    let config: Opt = match toml::from_str(&content) {
        Ok(config) => config,
        Err(err) => panic!(
            "Failed to parse configurtion file '{}': {}",
            path.display(),
            err
        ),
    };
    config
}

#[cfg(test)]
mod test {
    use crate::store::config::Opt;

    #[test]
    fn test() {
        let config_str = r#"
id = 1
host = "127.0.0.1"
port = 8901
data_path = "/tmp/cnosdb/meta"
grpc_enable_gzip = false

[log]
level = "warn"
path = "/tmp/cnosdb/logs"

[meta_init]
cluster_name = "cluster_xxx"
admin_user = "root"
system_tenant = "cnosdb"
default_database = ["public", "usage_schema"]

[heartbeat]
heartbeat_recheck_interval = 300
heartbeat_expired_interval = 600
"#;

        let config: Opt = toml::from_str(config_str).unwrap();
        assert!(toml::to_string_pretty(&config).is_ok());
        dbg!(config);
    }
}
