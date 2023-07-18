use std::sync::Arc;

use datafusion::arrow::array::{StringBuilder, UInt64Builder};
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
pub const DATABASES_PERCISION: &str = "percision";

lazy_static! {
    pub static ref DATABASE_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new(DATABASES_TENANT_NAME, DataType::Utf8, false),
        Field::new(DATABASES_DATABASE_NAME, DataType::Utf8, false),
        Field::new(DATABASES_TTL, DataType::Utf8, false),
        Field::new(DATABASES_SHARD, DataType::UInt64, false),
        Field::new(DATABASES_VNODE_DURATION, DataType::Utf8, false),
        Field::new(DATABASES_REPLICA, DataType::UInt64, false),
        Field::new(DATABASES_PERCISION, DataType::Utf8, false),
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
    option_percisions: StringBuilder,
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
    ) {
        // Note: append_value is actually infallable.
        self.tenant_names.append_value(tenant_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.option_ttls.append_value(option_ttl.as_ref());
        self.option_shards.append_value(option_shard);
        self.option_vnode_durations
            .append_value(option_vnode_duration.as_ref());
        self.option_replicas.append_value(option_replica);
        self.option_percisions.append_value(option_percision);
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
            mut option_percisions,
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
                Arc::new(option_percisions.finish()),
            ],
        )?;

        Ok(batch)
    }
}
