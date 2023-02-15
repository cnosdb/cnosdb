use std::sync::Arc;

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("tenant_name", DataType::Utf8, false),
        Field::new("tenant_options", DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.TENANTS` table row by row
pub struct ClusterSchemaTenantsBuilder {
    tenant_names: StringBuilder,
    tenant_options: StringBuilder,
}

impl Default for ClusterSchemaTenantsBuilder {
    fn default() -> Self {
        Self {
            tenant_names: StringBuilder::new(),
            tenant_options: StringBuilder::new(),
        }
    }
}

impl ClusterSchemaTenantsBuilder {
    pub fn append_row(&mut self, tenant_name: impl AsRef<str>, tenant_options: impl AsRef<str>) {
        // Note: append_value is actually infallable.
        self.tenant_names.append_value(tenant_name.as_ref());
        self.tenant_options.append_value(tenant_options.as_ref());
    }
}

impl TryFrom<ClusterSchemaTenantsBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: ClusterSchemaTenantsBuilder) -> Result<Self, Self::Error> {
        let ClusterSchemaTenantsBuilder {
            mut tenant_names,
            mut tenant_options,
        } = value;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(tenant_names.finish()),
                Arc::new(tenant_options.finish()),
            ],
        )?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
