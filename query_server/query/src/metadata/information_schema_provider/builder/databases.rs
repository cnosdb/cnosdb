use std::sync::Arc;

use datafusion::arrow::array::{BooleanBuilder, StringBuilder, UInt64Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

pub const DATABASES_TENANT_NAME: &str = "tenant_name";
pub const DATABASES_DATABASE_NAME: &str = "database_name";
pub const DATABASES_TTL: &str = "ttl";
pub const DATABASES_SHARD: &str = "shard";
pub const DATABASES_VNODE_DURATION: &str = "vnode_duration";
pub const DATABASES_REPLICA: &str = "replica";
pub const DATABASES_PRECISION: &str = "precision";
pub const DATABASES_MAX_MEMCACHE_SIZE: &str = "max_memcache_size";
pub const DATABASES_MEMCACHE_PARTITIONS: &str = "memcache_partitions";
pub const DATABASES_WAL_MAX_FILE_SIZE: &str = "wal_max_file_size";
pub const DATABASES_WAL_SYNC: &str = "wal_sync";
pub const DATABASES_STRICT_WRITE: &str = "strict_write";
pub const DATABASES_MAX_CACHE_READERS: &str = "max_cache_readers";

lazy_static! {
    pub static ref DATABASE_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new(DATABASES_TENANT_NAME, DataType::Utf8, false),
        Field::new(DATABASES_DATABASE_NAME, DataType::Utf8, false),
        Field::new(DATABASES_TTL, DataType::Utf8, false),
        Field::new(DATABASES_SHARD, DataType::UInt64, false),
        Field::new(DATABASES_VNODE_DURATION, DataType::Utf8, false),
        Field::new(DATABASES_REPLICA, DataType::UInt64, false),
        Field::new(DATABASES_PRECISION, DataType::Utf8, false),
        Field::new(DATABASES_MAX_MEMCACHE_SIZE, DataType::Utf8, false),
        Field::new(DATABASES_MEMCACHE_PARTITIONS, DataType::UInt64, false),
        Field::new(DATABASES_WAL_MAX_FILE_SIZE, DataType::Utf8, false),
        Field::new(DATABASES_WAL_SYNC, DataType::Boolean, false),
        Field::new(DATABASES_STRICT_WRITE, DataType::Boolean, false),
        Field::new(DATABASES_MAX_CACHE_READERS, DataType::UInt64, false),
    ]));
}

/// Builds the `information_schema.DATABASES` table row by row
#[derive(Default)]
pub struct InformationSchemaDatabasesBuilder {
    tenant_names: StringBuilder,
    database_names: StringBuilder,
    option_ttls: StringBuilder,
    option_shards: UInt64Builder,
    option_vnode_durations: StringBuilder,
    option_replicas: UInt64Builder,
    config_percisions: StringBuilder,
    config_max_memcache_sizes: StringBuilder,
    config_memcache_partitions: UInt64Builder,
    config_wal_max_file_sizes: StringBuilder,
    config_wal_syncs: BooleanBuilder,
    config_strict_writes: BooleanBuilder,
    config_max_cache_readers: UInt64Builder,
}

impl InformationSchemaDatabasesBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn append_row(
        &mut self,
        tenant_name: impl AsRef<str>,
        database_name: impl AsRef<str>,
        option_ttl: impl AsRef<str>,
        option_shard: u64,
        option_vnode_duration: impl AsRef<str>,
        option_replica: u64,
        option_percision: impl AsRef<str>,
        config_max_memcache_size: impl AsRef<str>,
        config_memcache_partition: u64,
        config_wal_max_file_size: impl AsRef<str>,
        config_wal_sync: bool,
        config_strict_write: bool,
        config_max_cache_reader: u64,
    ) {
        // Note: append_value is actually infallable.
        self.tenant_names.append_value(tenant_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.option_ttls.append_value(option_ttl.as_ref());
        self.option_shards.append_value(option_shard);
        self.option_vnode_durations
            .append_value(option_vnode_duration.as_ref());
        self.option_replicas.append_value(option_replica);
        self.config_percisions.append_value(option_percision);
        self.config_max_memcache_sizes
            .append_value(config_max_memcache_size.as_ref());
        self.config_memcache_partitions
            .append_value(config_memcache_partition);
        self.config_wal_max_file_sizes
            .append_value(config_wal_max_file_size.as_ref());
        self.config_wal_syncs.append_value(config_wal_sync);
        self.config_strict_writes.append_value(config_strict_write);
        self.config_max_cache_readers
            .append_value(config_max_cache_reader);
    }
}

impl TryFrom<InformationSchemaDatabasesBuilder> for RecordBatch {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaDatabasesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaDatabasesBuilder {
            mut tenant_names,
            mut database_names,
            mut option_ttls,
            mut option_shards,
            mut option_vnode_durations,
            mut option_replicas,
            mut config_percisions,
            mut config_max_memcache_sizes,
            mut config_memcache_partitions,
            mut config_wal_max_file_sizes,
            mut config_wal_syncs,
            mut config_strict_writes,
            mut config_max_cache_readers,
        } = value;

        let batch = RecordBatch::try_new(
            DATABASE_SCHEMA.clone(),
            vec![
                Arc::new(tenant_names.finish()),
                Arc::new(database_names.finish()),
                Arc::new(option_ttls.finish()),
                Arc::new(option_shards.finish()),
                Arc::new(option_vnode_durations.finish()),
                Arc::new(option_replicas.finish()),
                Arc::new(config_percisions.finish()),
                Arc::new(config_max_memcache_sizes.finish()),
                Arc::new(config_memcache_partitions.finish()),
                Arc::new(config_wal_max_file_sizes.finish()),
                Arc::new(config_wal_syncs.finish()),
                Arc::new(config_strict_writes.finish()),
                Arc::new(config_max_cache_readers.finish()),
            ],
        )?;

        Ok(batch)
    }
}
