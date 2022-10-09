#![allow(dead_code)]

use std::{path::PathBuf, sync::Arc};

use config::Config;
use serde::{Deserialize, Serialize};

use crate::{direct_io, index::IndexConfig, summary};

const SUMMARY_PATH: &str = "summary";
const INDEX_PATH: &str = "index";
const DATA_PATH: &str = "data";
const TSM_PATH: &str = "tsm";
const DELTA_PATH: &str = "delta";

#[derive(Debug, Clone)]
pub struct Options {
    pub storage: Arc<StorageOptions>,
    pub wal: Arc<WalOptions>,
    pub cache: Arc<CacheOptions>,
}

impl From<&Config> for Options {
    fn from(config: &Config) -> Self {
        Self {
            storage: Arc::new(StorageOptions::from(config)),
            wal: Arc::new(WalOptions::from(config)),
            cache: Arc::new(CacheOptions::from(config)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageOptions {
    pub path: PathBuf,
    pub max_summary_size: u64,
    pub max_level: u32,
    pub base_file_size: u64,
    pub compact_trigger: u32,
    pub max_compact_size: u64,
    pub dio_max_resident: u64,
    pub dio_max_non_resident: u64,
    pub dio_page_len_scale: u64,
    pub strict_write: bool,
}

impl StorageOptions {
    pub fn level_file_size(&self, lvl: u32) -> u64 {
        self.base_file_size * lvl as u64 * self.compact_trigger as u64
    }

    pub fn summary_dir(&self) -> PathBuf {
        self.path.join(SUMMARY_PATH)
    }

    pub fn index_base_dir(&self) -> PathBuf {
        self.path.join(INDEX_PATH)
    }

    pub fn index_dir(&self, database: &str) -> PathBuf {
        self.path.join(INDEX_PATH).join(database)
    }

    pub fn database_dir(&self, database: &str) -> PathBuf {
        self.path.join(DATA_PATH).join(database)
    }

    pub fn tsm_dir(&self, database: &str, ts_family_id: u32) -> PathBuf {
        self.database_dir(database)
            .join(TSM_PATH)
            .join(ts_family_id.to_string())
    }

    pub fn delta_dir(&self, database: &str, ts_family_id: u32) -> PathBuf {
        self.database_dir(database)
            .join(DELTA_PATH)
            .join(ts_family_id.to_string())
    }

    pub fn direct_io_options(&self) -> direct_io::Options {
        let mut opt = direct_io::Options::default();
        opt.max_resident(self.dio_max_resident as usize)
            .max_non_resident(self.dio_max_non_resident as usize)
            .page_len_scale(self.dio_page_len_scale as usize);
        opt
    }
}

impl From<&Config> for StorageOptions {
    fn from(config: &Config) -> Self {
        Self {
            path: PathBuf::from(config.storage.path.clone()),
            max_summary_size: config.storage.max_summary_size,
            max_level: config.storage.max_level,
            base_file_size: config.storage.base_file_size,
            compact_trigger: config.storage.compact_trigger,
            max_compact_size: config.storage.max_compact_size,
            dio_max_resident: config.storage.dio_max_resident,
            dio_max_non_resident: config.storage.dio_max_non_resident,
            dio_page_len_scale: config.storage.dio_page_len_scale,
            strict_write: config.storage.strict_write,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub enabled: bool,
    pub path: PathBuf,
    pub sync: bool,
}

impl From<&Config> for WalOptions {
    fn from(config: &Config) -> Self {
        Self {
            enabled: config.wal.enabled,
            path: PathBuf::from(config.wal.path.clone()),
            sync: config.wal.sync,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheOptions {
    pub max_buffer_size: u64,
    pub max_immutable_number: u16,
}

impl From<&Config> for CacheOptions {
    fn from(config: &Config) -> Self {
        Self {
            max_buffer_size: config.cache.max_buffer_size,
            max_immutable_number: config.cache.max_immutable_number,
        }
    }
}
