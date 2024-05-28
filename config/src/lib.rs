use std::path::{Path, PathBuf};

use check::{CheckConfig, CheckConfigResult};
use figment::providers::{Env, Format, Toml};
use figment::Figment;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

pub use crate::cache_config::*;
pub use crate::cluster_config::*;
pub use crate::deployment_config::*;
pub use crate::global_config::*;
pub use crate::limiter_config::*;
pub use crate::log_config::*;
pub use crate::meta_config::*;
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

pub fn get_config(path: impl AsRef<Path>) -> Result<Config, figment::Error> {
    let figment = Figment::new()
        .merge(Toml::file(path.as_ref()))
        .merge(Env::prefixed("CNOSDB__").split("__"));
    let mut config: Config = figment.extract()?;
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
}
