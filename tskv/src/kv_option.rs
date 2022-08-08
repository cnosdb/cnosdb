#![allow(dead_code)]
use std::{path::PathBuf, sync::Arc};

use config::GlobalConfig;
use serde::{Deserialize, Serialize};

use crate::index::IndexConfig;

#[derive(Clone)]
pub struct DBOptions {
    pub front_cpu: usize,
    pub back_cpu: usize,
    pub max_summary_size: u64,
    pub create_if_missing: bool,
    pub db_path: String,
    pub db_name: String,
}

impl From<&GlobalConfig> for DBOptions {
    fn from(config: &GlobalConfig) -> Self {
        Self {
            front_cpu: config.front_cpu,
            back_cpu: config.back_cpu,
            max_summary_size: config.max_memcache_size, // 128MB
            create_if_missing: config.create_if_missing,
            db_path: config.db_path.clone(),
            db_name: config.db_name.clone(),
        }
    }
}

#[derive(Clone)]
pub struct Options {
    pub db: Arc<DBOptions>,
    pub ts_family: Arc<TseriesFamOpt>,
    pub lrucache: Arc<CacheConfig>,
    pub wal: Arc<WalConfig>,
    // pub(crate) write_batch: WriteBatchConfig,
    pub compact_conf: Arc<CompactConfig>,
    pub index_conf: Arc<IndexConfig>,
    pub schema_store: Arc<SchemaStoreConfig>,
}

impl Options {
    // TODO
    pub fn override_by_env(&mut self) -> Self {
        Self {
            db: self.db.clone(),
            ts_family: self.ts_family.clone(),
            lrucache: self.lrucache.clone(),
            wal: self.wal.clone(),
            compact_conf: self.compact_conf.clone(),
            index_conf: self.index_conf.clone(),
            schema_store: self.schema_store.clone(),
        }
    }
}

impl From<&GlobalConfig> for Options {
    fn from(config: &GlobalConfig) -> Self {
        Self {
            db: Arc::new(DBOptions::from(config)),
            ts_family: Arc::new(TseriesFamOpt::from(config)),
            lrucache: Arc::new(CacheConfig::from(config)),
            wal: Arc::new(WalConfig::from(config)),
            compact_conf: Arc::new(CompactConfig::from(config)),
            index_conf: Arc::new(IndexConfig::from(config)),
            schema_store: Arc::new(SchemaStoreConfig::from(config)),
        }
    }
}

#[derive(Default, Clone)]
pub struct CacheConfig {}

impl From<&GlobalConfig> for CacheConfig {
    fn from(_: &GlobalConfig) -> Self {
        Self {}
    }
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct WalConfig {
    pub enabled: bool,
    pub dir: String,
    pub sync: bool,
}

impl From<&GlobalConfig> for WalConfig {
    fn from(config: &GlobalConfig) -> Self {
        Self {
            enabled: config.enabled,
            dir: config.wal_config_dir.clone(),
            sync: config.sync,
        }
    }
}

#[allow(dead_code)]
pub struct WriteBatchConfig {}

impl From<&GlobalConfig> for WriteBatchConfig {
    fn from(_: &GlobalConfig) -> Self {
        Self {}
    }
}

#[derive(Default, Clone)]
pub struct CompactConfig {}

impl From<&GlobalConfig> for CompactConfig {
    fn from(_: &GlobalConfig) -> Self {
        Self {}
    }
}

pub struct TimeRange {}

pub struct QueryOption {
    timerange: TimeRange,
    db: String,
    table: String,
    series_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TseriesFamOpt {
    pub max_level: u32,
    pub level_ratio: f64,
    pub base_file_size: u64,
    pub compact_trigger: u32,
    pub max_compact_size: u64,
    pub tsm_dir: String,
    pub delta_dir: String,
    pub max_memcache_size: u64,
    pub max_immemcache_num: u16,
}

impl TseriesFamOpt {
    pub fn level_file_size(&self, lvl: u32) -> u64 {
        self.base_file_size * lvl as u64 * self.compact_trigger as u64
    }

    pub fn tsm_dir(&self, tsf_id: u32) -> PathBuf {
        PathBuf::from(self.tsm_dir.clone()).join(tsf_id.to_string())
    }

    pub fn delta_dir(&self, tsf_id: u32) -> PathBuf {
        PathBuf::from(self.delta_dir.clone()).join(tsf_id.to_string())
    }
}

impl From<&GlobalConfig> for TseriesFamOpt {
    fn from(config: &GlobalConfig) -> Self {
        Self {
            max_level: config.max_level,
            level_ratio: config.level_ratio,
            base_file_size: config.base_file_size,
            compact_trigger: config.compact_trigger,
            max_compact_size: config.max_compact_size,
            tsm_dir: config.tsm_dir.clone(),
            delta_dir: config.delta_dir.clone(),
            max_memcache_size: config.max_memcache_size,
            max_immemcache_num: config.max_immemcache_num,
        }
    }
}

pub struct TseriesFamDesc {
    pub name: String,
    pub tsf_opt: Arc<TseriesFamOpt>,
}

#[derive(Clone)]
pub struct SchemaStoreConfig {
    pub dir: String,
}

impl From<&GlobalConfig> for SchemaStoreConfig {
    fn from(config: &GlobalConfig) -> Self {
        Self {
            dir: config.schema_store_config_dir.clone(),
        }
    }
}
