#![allow(dead_code)]
use std::{path::PathBuf, sync::Arc};

use config::GLOBAL_CONFIG;
use serde::{Deserialize, Serialize};

use crate::forward_index::ForwardIndexConfig;

#[derive(Clone)]
pub struct DBOptions {
    pub front_cpu: usize,
    pub back_cpu: usize,
    pub max_summary_size: u64,
    pub create_if_missing: bool,
    pub db_path: String,
    pub db_name: String,
}

impl Default for DBOptions {
    fn default() -> Self {
        Self { front_cpu: GLOBAL_CONFIG.front_cpu,
               back_cpu: GLOBAL_CONFIG.back_cpu,
               max_summary_size: GLOBAL_CONFIG.max_memcache_size, // 128MB
               create_if_missing: GLOBAL_CONFIG.create_if_missing,
               db_path: GLOBAL_CONFIG.db_path.clone(),
               db_name: GLOBAL_CONFIG.db_name.clone() }
    }
}

#[derive(Default, Clone)]
pub struct Options {
    pub db: Arc<DBOptions>,
    pub lrucache: Arc<CacheConfig>,
    pub wal: Arc<WalConfig>,
    // pub(crate) write_batch: WriteBatchConfig,
    pub compact_conf: Arc<CompactConfig>,
    pub forward_index_conf: Arc<ForwardIndexConfig>,
    pub schema_store: Arc<SchemaStoreConfig>,
}

impl Options {
    // todo:
    pub fn from_env() -> Self {
        Self { ..Default::default() }
    }
}

#[derive(Default, Clone)]
pub struct CacheConfig {}

#[allow(dead_code)]
#[derive(Clone)]
pub struct WalConfig {
    pub enabled: bool,
    pub dir: String,
    pub sync: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self { enabled: GLOBAL_CONFIG.enabled,
               dir: GLOBAL_CONFIG.wal_config_dir.clone(),
               sync: GLOBAL_CONFIG.sync }
    }
}

#[allow(dead_code)]
pub struct WriteBatchConfig {}

#[derive(Default, Clone)]
pub struct CompactConfig {}

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
    // pub base_file_size: u64,
    pub level_ratio: f64,
    pub base_file_size: u64,
    pub compact_trigger: u32,
    pub max_compact_size: u64,
    pub tsm_dir: String,
    pub delta_dir: String,
}

impl TseriesFamOpt {
    pub fn level_file_size(&self, lvl: u32) -> u64 {
        self.base_file_size * lvl as u64 * self.compact_trigger as u64
    }
}

impl Default for TseriesFamOpt {
    fn default() -> Self {
        Self { max_level: GLOBAL_CONFIG.max_level,
               // base_file_size: 256 * 1024 * 1024,
               level_ratio: GLOBAL_CONFIG.level_ratio,
               base_file_size: GLOBAL_CONFIG.base_file_size,
               compact_trigger: GLOBAL_CONFIG.compact_trigger,
               max_compact_size: GLOBAL_CONFIG.max_compact_size,
               tsm_dir: GLOBAL_CONFIG.tsm_dir.clone(),
               delta_dir: GLOBAL_CONFIG.delta_dir.clone() }
    }
}

pub struct TseriesFamDesc {
    pub name: String,
    pub opt: Arc<TseriesFamOpt>,
}

pub struct MemCacheOpt {
    tf_id: u32,
    max_size: u64,
    seq_no: u64,
}

impl Default for MemCacheOpt {
    fn default() -> Self {
        Self { tf_id: GLOBAL_CONFIG.tf_id,
               max_size: GLOBAL_CONFIG.max_memcache_size,
               seq_no: GLOBAL_CONFIG.seq_no }
    }
}

#[derive(Clone)]
pub struct SchemaStoreConfig {
    pub dir: String,
}

impl Default for SchemaStoreConfig {
    fn default() -> Self {
        Self { dir: GLOBAL_CONFIG.schema_store_config_dir.clone() }
    }
}
