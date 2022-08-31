#![allow(dead_code)]

use std::{path::PathBuf, sync::Arc};

use config::Config;
use serde::{Deserialize, Serialize};

use crate::{index::IndexConfig, summary};

#[derive(Debug, Clone)]
pub struct Options {
    pub application: Arc<ApplicationOptions>,
    pub wal: Arc<WalOptions>,
    pub summary: Arc<SummaryOptions>,
    pub storage: Arc<StorageOptions>,
    pub cache: Arc<CacheOptions>,
    pub schema_store: Arc<SchemaStoreOptions>,
}

impl From<&Config> for Options {
    fn from(config: &Config) -> Self {
        let application = ApplicationOptions::from(config);
        let mut summary = SummaryOptions::from(config);
        summary.path = application.path.clone();
        let mut storage = StorageOptions::from(config);
        storage.path = application.path.clone();
        Self {
            application: Arc::new(application),
            wal: Arc::new(WalOptions::from(config)),
            summary: Arc::new(summary),
            storage: Arc::new(storage),
            cache: Arc::new(CacheOptions::from(config)),
            schema_store: Arc::new(SchemaStoreOptions::from(config)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ApplicationOptions {
    pub path: PathBuf,
}

const SUMMARY_PATH: &str = "summary";
const TSM_PATH: &str = "tsm";
const DELTA_PATH: &str = "delta";
const INDEX_PATH: &str = "index";

impl From<&Config> for ApplicationOptions {
    fn from(config: &Config) -> Self {
        Self {
            path: PathBuf::from(config.application.path.clone()),
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
pub struct SummaryOptions {
    pub max_summary_size: u64,
    pub path: PathBuf,
}

impl SummaryOptions {
    pub fn summary_dir(&self) -> PathBuf {
        self.path.join(SUMMARY_PATH)
    }
}

impl From<&Config> for SummaryOptions {
    fn from(config: &Config) -> Self {
        Self {
            max_summary_size: config.summary.max_summary_size,
            path: PathBuf::new(),
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

#[derive(Debug, Clone, PartialEq)]
pub struct StorageOptions {
    pub max_level: u32,
    pub base_file_size: u64,
    pub compact_trigger: u32,
    pub max_compact_size: u64,
    pub path: PathBuf,
}

impl StorageOptions {
    pub fn level_file_size(&self, lvl: u32) -> u64 {
        self.base_file_size * lvl as u64 * self.compact_trigger as u64
    }

    pub fn index_dir(&self) -> PathBuf {
        self.path.join(DELTA_PATH)
    }

    pub fn tsm_dir(&self, database: &str, ts_family_id: u32) -> PathBuf {
        self.path
            .join(database)
            .join(TSM_PATH)
            .join(ts_family_id.to_string())
    }

    pub fn delta_dir(&self, database: &str, ts_family_id: u32) -> PathBuf {
        self.path
            .join(database)
            .join(DELTA_PATH)
            .join(ts_family_id.to_string())
    }
}

impl From<&Config> for StorageOptions {
    fn from(config: &Config) -> Self {
        Self {
            max_level: config.storage.max_level,
            base_file_size: config.storage.base_file_size,
            compact_trigger: config.storage.compact_trigger,
            max_compact_size: config.storage.max_compact_size,
            path: PathBuf::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchemaStoreOptions {
    pub config_dir: PathBuf,
}

impl From<&Config> for SchemaStoreOptions {
    fn from(config: &Config) -> Self {
        Self {
            config_dir: PathBuf::from(config.schema_store.config_dir.clone()),
        }
    }
}
