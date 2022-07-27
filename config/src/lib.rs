use std::{fs::File, io::prelude::Read};

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use trace::debug;

#[derive(Debug, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub max_memcache_size: u64,
    pub max_summary_size: u64,
    pub max_immemcache_num: usize,
    // DBOption
    pub front_cpu: usize,
    pub back_cpu: usize,
    pub create_if_missing: bool,
    pub db_path: String,
    pub db_name: String,
    // WalConfig
    pub enabled: bool,
    pub wal_config_dir: String,
    pub sync: bool,
    // TseriesFamOpt
    pub max_level: u32,
    // pub base_file_size: u64,
    pub level_ratio: f64,
    pub base_file_size: u64,
    pub compact_trigger: u32,
    pub max_compact_size: u64,
    pub tsm_dir: String,
    pub delta_dir: String,
    // MemCacheOpt
    pub tf_id: u32,
    pub seq_no: u64,
    // SchemaStoreConfig
    pub schema_store_config_dir: String,
    // tsfamily num
    pub tsfamily_num: u32,
    pub forward_index_path: String,
}

impl GlobalConfig {
    pub const fn max_memcache_size(&self) -> &u64 {
        &self.max_memcache_size
    }
}

lazy_static! {
    pub static ref GLOBAL_CONFIG: GlobalConfig = {
        let mut file = File::open("../config/config.toml").unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        let config: GlobalConfig = toml::from_str(&content).unwrap();
        debug!("{:#?}", config);
        config
    };
}
