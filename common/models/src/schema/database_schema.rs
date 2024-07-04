use std::sync::Arc;
use std::time::Duration;

use config::tskv::Config;
use serde::{Deserialize, Serialize};
use utils::duration::{CnosDuration, YEAR_SECOND};
use utils::precision::Precision;

#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct DatabaseSchema {
    tenant: String,
    database: String,
    is_hidden: bool,
    // modifiable options
    pub options: DatabaseOptions,
    // unmodifiable config
    pub config: Arc<DatabaseConfig>,
}

impl DatabaseSchema {
    pub fn new(
        tenant_name: &str,
        database_name: &str,
        options: DatabaseOptions,
        config: Arc<DatabaseConfig>,
    ) -> Self {
        DatabaseSchema {
            tenant: tenant_name.to_string(),
            database: database_name.to_string(),
            is_hidden: false,
            options,
            config,
        }
    }

    pub fn database_name(&self) -> &str {
        &self.database
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant
    }

    pub fn is_hidden(&self) -> bool {
        self.is_hidden
    }

    pub fn owner(&self) -> (String, String) {
        (self.tenant.clone(), self.database.clone())
    }

    pub fn is_empty(&self) -> bool {
        if self.tenant.is_empty() && self.database.is_empty() {
            return true;
        }

        false
    }

    pub fn options(&self) -> &DatabaseOptions {
        &self.options
    }

    pub fn config(&self) -> Arc<DatabaseConfig> {
        self.config.clone()
    }

    // return the min timestamp value database allowed to store
    pub fn time_to_expired(&self) -> i64 {
        let (ttl, now) = match self.config().precision() {
            Precision::MS => (
                self.options.ttl().to_millisecond(),
                crate::utils::now_timestamp_millis(),
            ),
            Precision::US => (
                self.options.ttl().to_microseconds(),
                crate::utils::now_timestamp_micros(),
            ),
            Precision::NS => (
                self.options.ttl().to_nanoseconds(),
                crate::utils::now_timestamp_nanos(),
            ),
        };
        now - ttl
    }

    pub fn set_db_is_hidden(&mut self, is_hidden: bool) {
        self.is_hidden = is_hidden;
    }
}

pub fn make_owner(tenant_name: &str, database_name: &str) -> String {
    format!("{}.{}", tenant_name, database_name)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseOptionsBuilder {
    ttl: Option<CnosDuration>,
    shard_num: Option<u64>,
    vnode_duration: Option<CnosDuration>,
    replica: Option<u64>,
}

impl Default for DatabaseOptionsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseOptionsBuilder {
    pub fn new() -> Self {
        DatabaseOptionsBuilder {
            ttl: None,
            shard_num: None,
            vnode_duration: None,
            replica: None,
        }
    }

    pub fn with_ttl(&mut self, ttl: CnosDuration) -> &mut Self {
        self.ttl = Some(ttl);
        self
    }

    pub fn with_shard_num(&mut self, shard_num: u64) -> &mut Self {
        self.shard_num = Some(shard_num);
        self
    }

    pub fn with_vnode_duration(&mut self, vnode_duration: CnosDuration) -> &mut Self {
        self.vnode_duration = Some(vnode_duration);
        self
    }

    pub fn with_replica(&mut self, replica: u64) -> &mut Self {
        self.replica = Some(replica);
        self
    }

    pub fn build(self) -> DatabaseOptions {
        let ttl = self.ttl.unwrap_or(DatabaseOptions::DEFAULT_TTL);
        let shard_num = self.shard_num.unwrap_or(DatabaseOptions::DEFAULT_SHARD_NUM);
        let vnode_duration = self
            .vnode_duration
            .unwrap_or(DatabaseOptions::DEFAULT_VNODE_DURATION);
        let replica = self.replica.unwrap_or(DatabaseOptions::DEFAULT_REPLICA);
        DatabaseOptions::new(ttl, shard_num, vnode_duration, replica)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptions {
    ttl: CnosDuration,
    shard_num: u64,
    vnode_duration: CnosDuration,
    replica: u64,
}

impl DatabaseOptions {
    pub const DEFAULT_TTL: CnosDuration = CnosDuration::new_inf();
    pub const DEFAULT_SHARD_NUM: u64 = 1;
    pub const DEFAULT_REPLICA: u64 = 1;
    pub const DEFAULT_VNODE_DURATION: CnosDuration =
        CnosDuration::new_with_duration(Duration::from_secs(YEAR_SECOND));
    pub fn new(
        ttl: CnosDuration,
        shard_num: u64,
        vnode_duration: CnosDuration,
        replica: u64,
    ) -> Self {
        DatabaseOptions {
            ttl,
            shard_num,
            vnode_duration,
            replica,
        }
    }

    pub fn ttl(&self) -> &CnosDuration {
        &self.ttl
    }

    pub fn set_ttl(&mut self, ttl: CnosDuration) {
        self.ttl = ttl;
    }

    pub fn shard_num(&self) -> u64 {
        self.shard_num
    }

    pub fn set_shard_num(&mut self, shard_num: u64) {
        self.shard_num = shard_num;
    }

    pub fn vnode_duration(&self) -> &CnosDuration {
        &self.vnode_duration
    }

    pub fn set_vnode_duration(&mut self, vnode_duration: CnosDuration) {
        self.vnode_duration = vnode_duration;
    }

    pub fn replica(&self) -> u64 {
        self.replica
    }

    pub fn set_replica(&mut self, replica: u64) {
        self.replica = replica;
    }

    pub fn apply_builder(&mut self, builder: &DatabaseOptionsBuilder) {
        if let Some(ref ttl) = builder.ttl {
            self.ttl = ttl.clone();
        }
        if let Some(shard_num) = builder.shard_num {
            self.shard_num = shard_num;
        }
        if let Some(ref vnode_duration) = builder.vnode_duration {
            self.vnode_duration = vnode_duration.clone();
        }
        if let Some(replica) = builder.replica {
            self.replica = replica;
        }
    }
}

impl Default for DatabaseOptions {
    fn default() -> Self {
        DatabaseOptions {
            ttl: DatabaseOptions::DEFAULT_TTL,
            shard_num: DatabaseOptions::DEFAULT_SHARD_NUM,
            vnode_duration: DatabaseOptions::DEFAULT_VNODE_DURATION,
            replica: DatabaseOptions::DEFAULT_REPLICA,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseConfigBuilder {
    precision: Option<Precision>,
    max_memcache_size: Option<u64>,
    memcache_partitions: Option<u64>,
    wal_max_file_size: Option<u64>,
    wal_sync: Option<bool>,
    strict_write: Option<bool>,
    max_cache_readers: Option<u64>,
}

impl Default for DatabaseConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseConfigBuilder {
    pub fn new() -> Self {
        DatabaseConfigBuilder {
            precision: None,
            max_memcache_size: None,
            memcache_partitions: None,
            wal_max_file_size: None,
            wal_sync: None,
            strict_write: None,
            max_cache_readers: None,
        }
    }

    pub fn with_precision(&mut self, precision: Precision) -> &mut Self {
        self.precision = Some(precision);
        self
    }

    pub fn with_max_memcache_size(&mut self, max_memcache_size: u64) -> &mut Self {
        self.max_memcache_size = Some(max_memcache_size);
        self
    }

    pub fn with_memcache_partitions(&mut self, memcache_partitions: u64) -> &mut Self {
        self.memcache_partitions = Some(memcache_partitions);
        self
    }

    pub fn with_wal_max_file_size(&mut self, wal_max_file_size: u64) -> &mut Self {
        self.wal_max_file_size = Some(wal_max_file_size);
        self
    }

    pub fn with_wal_sync(&mut self, wal_sync: bool) -> &mut Self {
        self.wal_sync = Some(wal_sync);
        self
    }

    pub fn with_strict_write(&mut self, strict_write: bool) -> &mut Self {
        self.strict_write = Some(strict_write);
        self
    }

    pub fn with_max_cache_readers(&mut self, max_cache_readers: u64) -> &mut Self {
        self.max_cache_readers = Some(max_cache_readers);
        self
    }

    pub fn build(self, config: Config) -> DatabaseConfig {
        let precision = self.precision.unwrap_or(DatabaseConfig::DEFAULT_PRECISION);
        let max_memcache_size = self
            .max_memcache_size
            .unwrap_or(config.cache.max_buffer_size);
        let memcache_partitions = self
            .memcache_partitions
            .unwrap_or(config.cache.partition as u64);
        let wal_max_file_size = self.wal_max_file_size.unwrap_or(config.wal.max_file_size);
        let wal_sync = self.wal_sync.unwrap_or(config.wal.sync);
        let strict_write = self.strict_write.unwrap_or(config.storage.strict_write);
        let max_cache_readers = self
            .max_cache_readers
            .unwrap_or(config.storage.max_cached_readers as u64);
        DatabaseConfig::new(
            precision,
            max_memcache_size,
            memcache_partitions,
            wal_max_file_size,
            wal_sync,
            strict_write,
            max_cache_readers,
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseConfig {
    precision: Precision,
    max_memcache_size: u64,
    memcache_partitions: u64,
    wal_max_file_size: u64,
    wal_sync: bool,
    strict_write: bool,
    max_cache_readers: u64,
}

impl DatabaseConfig {
    pub const DEFAULT_PRECISION: Precision = Precision::NS;
    pub const DEFAULT_MAX_MEMCACHE_SIZE: u64 = 512 * 1024 * 1024;
    pub const DEFAULT_MEMCACHE_PARTITIONS: u64 = 16;
    pub const DEFAULT_WAL_MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024;
    pub const DEFAULT_WAL_SYNC: bool = false;
    pub const DEFAULT_STRICT_WRITE: bool = false;
    pub const DEFAULT_MAX_CACHE_READERS: u64 = 32;

    pub fn new(
        precision: Precision,
        max_memcache_size: u64,
        memcache_partitions: u64,
        wal_max_file_size: u64,
        wal_sync: bool,
        strict_write: bool,
        max_cache_readers: u64,
    ) -> Self {
        DatabaseConfig {
            precision,
            max_memcache_size,
            memcache_partitions,
            wal_max_file_size,
            wal_sync,
            strict_write,
            max_cache_readers,
        }
    }

    pub fn precision(&self) -> &Precision {
        &self.precision
    }

    pub fn max_memcache_size(&self) -> u64 {
        self.max_memcache_size
    }

    pub fn memcache_partitions(&self) -> u64 {
        self.memcache_partitions
    }

    pub fn wal_max_file_size(&self) -> u64 {
        self.wal_max_file_size
    }

    pub fn wal_sync(&self) -> bool {
        self.wal_sync
    }

    pub fn strict_write(&self) -> bool {
        self.strict_write
    }

    pub fn max_cache_readers(&self) -> u64 {
        self.max_cache_readers
    }

    pub fn set_max_memcache_size(&mut self, max_memcache_size: u64) {
        self.max_memcache_size = max_memcache_size;
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        DatabaseConfig {
            precision: DatabaseConfig::DEFAULT_PRECISION,
            max_memcache_size: DatabaseConfig::DEFAULT_MAX_MEMCACHE_SIZE,
            memcache_partitions: DatabaseConfig::DEFAULT_MEMCACHE_PARTITIONS,
            wal_max_file_size: DatabaseConfig::DEFAULT_WAL_MAX_FILE_SIZE,
            wal_sync: DatabaseConfig::DEFAULT_WAL_SYNC,
            strict_write: DatabaseConfig::DEFAULT_STRICT_WRITE,
            max_cache_readers: DatabaseConfig::DEFAULT_MAX_CACHE_READERS,
        }
    }
}
