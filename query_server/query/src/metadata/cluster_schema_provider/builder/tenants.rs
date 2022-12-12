use std::sync::Arc;

use datafusion::{
    arrow::{
        array::StringBuilder,
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    error::DataFusionError,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("tenant_name", DataType::Utf8, false),
        Field::new("tenant_option", DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.TENANTS` table row by row
pub struct ClusterSchemaTenantsBuilder {
    tenant_ids: StringBuilder,
    tenant_names: StringBuilder,
    tenant_options: StringBuilder,
}

impl Default for ClusterSchemaTenantsBuilder {
    fn default() -> Self {
        Self {
            tenant_ids: StringBuilder::new(),
            tenant_names: StringBuilder::new(),
            tenant_options: StringBuilder::new(),
        }
    }
}

impl ClusterSchemaTenantsBuilder {
    pub fn _append_row(
        &mut self,
        tenant_id: impl AsRef<str>,
        tenant_name: impl AsRef<str>,
        tenant_option: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.tenant_ids.append_value(tenant_id.as_ref());
        self.tenant_names.append_value(tenant_name.as_ref());
        self.tenant_options.append_value(tenant_option.as_ref());
    }
}

impl TryFrom<ClusterSchemaTenantsBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: ClusterSchemaTenantsBuilder) -> Result<Self, Self::Error> {
        let ClusterSchemaTenantsBuilder {
            mut tenant_ids,
            mut tenant_names,
            mut tenant_options,
        } = value;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(tenant_ids.finish()),
                Arc::new(tenant_names.finish()),
                Arc::new(tenant_options.finish()),
            ],
        )?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
