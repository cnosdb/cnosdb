#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use config::Config;
use serde::{Deserialize, Serialize};

use crate::{file_system, summary, TseriesFamilyId};

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
    pub query: Arc<QueryOptions>,
}

impl From<&Config> for Options {
    fn from(config: &Config) -> Self {
        Self {
            storage: Arc::new(StorageOptions::from(config)),
            wal: Arc::new(WalOptions::from(config)),
            cache: Arc::new(CacheOptions::from(config)),
            query: Arc::new(QueryOptions::from(config)),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct StorageOptions {
    pub path: PathBuf,
    pub max_summary_size: u64,
    pub base_file_size: u64,
    pub flush_req_channel_cap: usize,
    pub max_level: u16,
    pub compact_trigger_file_num: u32,
    pub compact_trigger_cold_duration: Duration,
    pub max_compact_size: u64,
    pub max_concurrent_compaction: u16,
    pub strict_write: bool,
}

// database/data/ts_family_id/tsm
// database/data/ts_family_id/delta
// database/data/ts_family_id/index
impl StorageOptions {
    pub fn level_file_size(&self, lvl: u32) -> u64 {
        self.base_file_size * lvl as u64 * self.compact_trigger_file_num as u64
    }

    pub fn summary_dir(&self) -> PathBuf {
        self.path.join(SUMMARY_PATH)
    }

    pub fn database_dir(&self, database: &str) -> PathBuf {
        self.path.join(DATA_PATH).join(database)
    }

    pub fn ts_family_dir(&self, database: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.database_dir(database).join(ts_family_id.to_string())
    }

    pub fn index_dir(&self, database: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.database_dir(database)
            .join(ts_family_id.to_string())
            .join(INDEX_PATH)
    }

    pub fn tsm_dir(&self, database: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.database_dir(database)
            .join(ts_family_id.to_string())
            .join(TSM_PATH)
    }

    pub fn delta_dir(&self, database: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.database_dir(database)
            .join(ts_family_id.to_string())
            .join(DELTA_PATH)
    }

    pub fn tsfamily_dir(&self, database: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.database_dir(database).join(ts_family_id.to_string())
    }
}

impl From<&Config> for StorageOptions {
    fn from(config: &Config) -> Self {
        Self {
            path: PathBuf::from(config.storage.path.clone()),
            max_summary_size: config.storage.max_summary_size,
            base_file_size: config.storage.base_file_size,
            flush_req_channel_cap: config.storage.flush_req_channel_cap,
            max_level: config.storage.max_level,
            compact_trigger_file_num: config.storage.compact_trigger_file_num,
            compact_trigger_cold_duration: config.storage.compact_trigger_cold_duration,
            max_compact_size: config.storage.max_compact_size,
            max_concurrent_compaction: config.storage.max_concurrent_compaction,
            strict_write: config.storage.strict_write,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOptions {
    pub max_server_connections: u32,
    pub auth_enabled: bool,
}

impl From<&Config> for QueryOptions {
    fn from(config: &Config) -> Self {
        Self {
            max_server_connections: config.query.max_server_connections,
            auth_enabled: config.query.auth_enabled,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub wal_req_channel_cap: usize,
    pub enabled: bool,
    pub path: PathBuf,
    pub max_file_size: u64,
    pub sync: bool,
    pub sync_interval: Duration,
}

impl From<&Config> for WalOptions {
    fn from(config: &Config) -> Self {
        Self {
            wal_req_channel_cap: config.wal.wal_req_channel_cap,
            enabled: config.wal.enabled,
            path: PathBuf::from(config.wal.path.clone()),
            max_file_size: config.wal.max_file_size,
            sync: config.wal.sync,
            sync_interval: config.wal.sync_interval,
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
