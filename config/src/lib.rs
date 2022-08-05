use std::{fs::File, io::prelude::Read};

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use trace::debug;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub tsfamily_num: u32,
    // DBOption
    pub front_cpu: usize,
    pub back_cpu: usize,
    pub max_summary_size: u64,
    pub create_if_missing: bool,
    pub db_path: String,
    pub db_name: String,
    // WalConfig
    pub enabled: bool,
    pub wal_config_dir: String,
    pub sync: bool,
    // TseriesFamOpt
    pub max_level: u32,
    pub level_ratio: f64,
    pub base_file_size: u64,
    pub compact_trigger: u32,
    pub max_compact_size: u64,
    pub tsm_dir: String,
    pub delta_dir: String,
    pub max_memcache_size: u64,
    pub max_immemcache_num: u16,
    // SchemaStoreConfig
    pub schema_store_config_dir: String,
    // ForwardIndex
    pub forward_index_path: String,
}

pub fn get_config(path: &str) -> &'static GlobalConfig {
    static INSTANCE: OnceCell<GlobalConfig> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let mut file = File::open(path).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        let config: GlobalConfig = toml::from_str(&content).unwrap();
        debug!("{:#?}", config);
        config
    })
}
