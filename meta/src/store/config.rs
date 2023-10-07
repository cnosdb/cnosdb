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

impl MetaInit {
    pub fn default_db_config(tenant: &str, db: &str) -> String {
        format!(
            "{{\"tenant\":\"{}\",\"database\":\"{}\",\"config\":{{\"ttl\":null,\"shard_num\":null,\"vnode_duration\":null,\"replica\":null,\"precision\":null}}}}",
            tenant, db
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HeartBeatConfig {
    pub heartbeat_recheck_interval: u64,
    pub heartbeat_expired_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Opt {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub data_path: String,
    pub log: LogConfig,
    pub meta_init: MetaInit,
    pub heartbeat: HeartBeatConfig,
}

pub fn get_opt(path: impl AsRef<Path>) -> Opt {
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
