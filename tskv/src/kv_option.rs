#![allow(dead_code)]

use std::path::PathBuf;
use crate::forward_index::ForwardIndexConfig;

pub const MAX_MEMCACHE_SIZE: u64 = 128 * 1024 * 1024;
//128M
pub const MAX_SUMMARY_SIZE: u64 = 128 * 1024 * 1024; //128M

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
        Self {
            front_cpu: 2,
            back_cpu: 2,
            max_summary_size: MAX_SUMMARY_SIZE, // 128MB
            create_if_missing: false,
            db_path: "db".to_string(),
            db_name: "db".to_string(),
        }
    }
}

#[derive(Clone)]
pub struct Options {
    pub db: DBOptions,
    pub lrucache: CacheConfig,
    pub wal: WalConfig,
    // pub(crate) write_batch: WriteBatchConfig,
    pub compact_conf: CompactConfig,
    pub forward_index_conf: ForwardIndexConfig,
}

impl Options {
    // todo:
    pub fn from_env() -> Self {
        Self { ..Default::default() }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            db: Default::default(),
            lrucache: Default::default(),
            wal: Default::default(),
            compact_conf: Default::default(),
            forward_index_conf: Default::default(),
        }
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
        Self { enabled: true, dir: "/tmp/test".to_string(), sync: true }
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

#[derive(Clone, PartialEq)]
pub struct TseriesFamOpt {
    max_level: u32,
    max_bytes_for_level_base: u64,
    level_ratio: f64,
    target_file_size_base: u64,
    file_number_compact_trigger: u32,
    max_compact_size: u64,
}

impl Default for TseriesFamOpt {
    fn default() -> Self {
        Self {
            max_level: 6,
            max_bytes_for_level_base: 256 * 1024 * 1024,
            level_ratio: 16f64,
            target_file_size_base: 64 * 1024 * 1024,
            file_number_compact_trigger: 4,
            max_compact_size: 2 * 1024 * 1024 * 1024,
        }
    }
}

pub struct TseriesFamDesc {
    pub name: String,
    pub opt: TseriesFamOpt,
}

pub struct MemCacheOpt {
    tf_id: u32,
    max_size: u64,
    seq_no: u64,
}

impl Default for MemCacheOpt {
    fn default() -> Self {
        Self { tf_id: 0, max_size: MAX_MEMCACHE_SIZE, seq_no: 0 }
    }
}
