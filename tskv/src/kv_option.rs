#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use config::tskv::Config;
use models::meta_data::{NodeId, VnodeId};

use crate::TseriesFamilyId;

const SUMMARY_PATH: &str = "summary";
pub const INDEX_PATH: &str = "index";
pub const DATA_PATH: &str = "data";
pub const TSM_PATH: &str = "tsm";
pub const DELTA_PATH: &str = "delta";

#[derive(Debug, Clone)]
pub struct Options {
    pub storage: Arc<StorageOptions>,
    pub wal: Arc<WalOptions>,
    pub query: Arc<QueryOptions>,
}

impl From<&Config> for Options {
    fn from(config: &Config) -> Self {
        Self {
            storage: Arc::new(StorageOptions::from(config)),
            wal: Arc::new(WalOptions::from(config)),
            query: Arc::new(QueryOptions::from(config)),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct StorageOptions {
    pub path: PathBuf,
    pub node_id: NodeId,
    pub max_summary_size: u64,
    pub base_file_size: u64,
    pub flush_req_channel_cap: usize,
    pub max_level: u16,
    pub compact_trigger_file_num: u32,
    pub compact_trigger_cold_duration: Duration,
    pub max_compact_size: u64,
    pub max_concurrent_compaction: u16,
    pub collect_compaction_metrics: bool,
    pub snapshot_holding_time: i64,
    pub max_datablock_size: u64,
    pub index_cache_capacity: u64,
}

// database/data/ts_family_id/tsm
// database/data/ts_family_id/delta
// database/data/ts_family_id/index
impl StorageOptions {
    pub fn level_max_file_size(&self, lvl: u32) -> u64 {
        // TODO(zipper): size of lvl-0 is zero?
        self.base_file_size * lvl as u64 * self.compact_trigger_file_num as u64
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn summary_dir(&self) -> PathBuf {
        self.path.join(SUMMARY_PATH)
    }

    pub fn owner_dir(&self, owner: &str) -> PathBuf {
        self.path.join(DATA_PATH).join(owner)
    }

    pub fn ts_family_dir(&self, owner: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.owner_dir(owner).join(ts_family_id.to_string())
    }

    pub fn index_dir(&self, owner: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.ts_family_dir(owner, ts_family_id).join(INDEX_PATH)
    }

    pub fn tsm_dir(&self, owner: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.ts_family_dir(owner, ts_family_id).join(TSM_PATH)
    }

    pub fn delta_dir(&self, owner: &str, ts_family_id: TseriesFamilyId) -> PathBuf {
        self.ts_family_dir(owner, ts_family_id).join(DELTA_PATH)
    }
}

impl From<&Config> for StorageOptions {
    fn from(config: &Config) -> Self {
        Self {
            node_id: config.global.node_id,
            path: PathBuf::from(config.storage.path.clone()),
            max_summary_size: config.storage.max_summary_size,
            base_file_size: config.storage.base_file_size,
            flush_req_channel_cap: config.storage.flush_req_channel_cap,
            max_level: config.storage.max_level,
            compact_trigger_file_num: config.storage.compact_trigger_file_num,
            compact_trigger_cold_duration: config.storage.compact_trigger_cold_duration,
            max_compact_size: config.storage.max_compact_size,
            max_concurrent_compaction: config.storage.max_concurrent_compaction,
            collect_compaction_metrics: config.storage.collect_compaction_metrics,
            snapshot_holding_time: config.cluster.snapshot_holding_time.as_secs() as i64,
            max_datablock_size: config.storage.max_datablock_size,
            index_cache_capacity: config.storage.index_cache_capacity,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOptions {
    pub max_server_connections: u32,
    pub auth_enabled: bool,
    pub query_sql_limit: u64,
    pub write_sql_limit: u64,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub stream_trigger_cpu: usize,
    pub stream_executor_cpu: usize,
}

impl From<&Config> for QueryOptions {
    fn from(config: &Config) -> Self {
        Self {
            max_server_connections: config.query.max_server_connections,
            auth_enabled: config.query.auth_enabled,
            query_sql_limit: config.query.query_sql_limit,
            write_sql_limit: config.query.write_sql_limit,
            read_timeout: config.query.read_timeout,
            write_timeout: config.query.write_timeout,
            stream_trigger_cpu: config.query.stream_trigger_cpu,
            stream_executor_cpu: config.query.stream_executor_cpu,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalOptions {
    pub path: PathBuf,
    pub wal_max_file_size: u64,
    pub compress: String,
    pub wal_sync: bool,
}

impl From<&Config> for WalOptions {
    fn from(config: &Config) -> Self {
        Self {
            path: PathBuf::from(config.wal.path.clone()),
            wal_max_file_size: config.wal.max_file_size,
            compress: config.wal.compress.clone(),
            wal_sync: config.wal.sync,
        }
    }
}

/// database/data/ts_family_id/
impl WalOptions {
    pub fn wal_dir(&self, owner: &str, vnode_id: VnodeId) -> PathBuf {
        self.path.join(owner).join(vnode_id.to_string())
    }
}
