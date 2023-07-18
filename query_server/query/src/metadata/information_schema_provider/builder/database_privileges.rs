use std::sync::Arc;

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref DATABASE_PRIVILEGE_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("tenant_name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("privilege_type", DataType::Utf8, false),
        Field::new("role_name", DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.DatabasePrivileges` table row by row
#[derive(Default)]
pub struct InformationSchemaDatabasePrivilegesBuilder {
    tenant_names: StringBuilder,
    database_names: StringBuilder,
    privilege_types: StringBuilder,
    role_names: StringBuilder,
}

impl InformationSchemaDatabasePrivilegesBuilder {
    pub fn append_row(
        &mut self,
        tenant_name: impl AsRef<str>,
        database_name: impl AsRef<str>,
        privilege_type: impl AsRef<str>,
        role_name: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.tenant_names.append_value(tenant_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.privilege_types.append_value(privilege_type.as_ref());
        self.role_names.append_value(role_name);
    }
}

impl TryFrom<InformationSchemaDatabasePrivilegesBuilder> for RecordBatch {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaDatabasePrivilegesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaDatabasePrivilegesBuilder {
            mut tenant_names,
            mut database_names,
            mut privilege_types,
            mut role_names,
        } = value;

        let batch = RecordBatch::try_new(
            DATABASE_PRIVILEGE_SCHEMA.clone(),
            vec![
                Arc::new(tenant_names.finish()),
                Arc::new(database_names.finish()),
                Arc::new(privilege_types.finish()),
                Arc::new(role_names.finish()),
            ],
        )?;

        Ok(batch)
    }
}
