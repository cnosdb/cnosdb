mod cache_config;
mod cluster_config;
mod deployment_config;
mod global_config;
mod meta_config;
mod query_config;
mod security_config;
mod service_config;
mod storage_config;
mod trace;
mod wal_config;

use std::collections::HashMap;
use std::path::{Path, PathBuf};

pub use cache_config::*;
pub use cluster_config::*;
pub use deployment_config::*;
use figment::providers::{Env, Format, Toml};
use figment::value::Uncased;
use figment::Figment;
pub use global_config::*;
use macros::EnvKeys;
pub use meta_config::*;
pub use query_config::*;
pub use security_config::*;
use serde::{Deserialize, Serialize};
pub use service_config::*;
pub use storage_config::*;
pub use trace::*;
pub use wal_config::*;

use crate::check::{CheckConfig, CheckConfigResult};
use crate::common::LogConfig;
use crate::EnvKeys as _;

#[derive(Debug, Clone, Serialize, Deserialize, Default, EnvKeys)]
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

pub fn get_config(path: impl AsRef<Path>) -> Result<Config, figment::Error> {
    let env_keys = Config::env_keys();
    let env_key_map = env_keys
        .into_iter()
        .map(|key| (key.replace('.', "_"), key))
        .collect::<HashMap<String, String>>();
    let figment =
        Figment::new()
            .merge(Toml::file(path.as_ref()))
            .merge(Env::prefixed("CNOSDB_").filter_map(move |env| {
                env_key_map
                    .get(env.as_str())
                    .map(|key| Uncased::from_owned(key.clone()))
            }));
    let config: Config = figment.extract()?;
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

    use super::get_config_for_test;
    use crate::tskv::{get_config, Config};
    use crate::EnvKeys;

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
        let cfg_2 = get_config(cfg_path).unwrap();

        assert_eq!(cfg.to_string_pretty(), cfg_2.to_string_pretty());
    }

    #[test]
    fn test_get_test_config() {
        let _ = get_config_for_test();
    }

    #[test]
    fn test_parse() {
        let config_str = std::fs::read_to_string("./config.toml").unwrap();

        let config: Config = toml::from_str(&config_str).unwrap();
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

    #[test]
    fn test_env_key() {
        let keys = Config::env_keys();
        dbg!(keys);
    }
}
