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
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "role_name",
        DataType::Utf8,
        false
    ),]));
}

/// Builds the `information_schema.EnabledRoles` table row by row
pub struct InformationSchemaEnabledRolesBuilder {
    role_names: StringBuilder,
}

impl Default for InformationSchemaEnabledRolesBuilder {
    fn default() -> Self {
        Self {
            role_names: StringBuilder::new(),
        }
    }
}

impl InformationSchemaEnabledRolesBuilder {
    pub fn append_row(&mut self, role_name: impl AsRef<str>) {
        // Note: append_value is actually infallable.
        self.role_names.append_value(role_name.as_ref());
    }
}

impl TryFrom<InformationSchemaEnabledRolesBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaEnabledRolesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaEnabledRolesBuilder { mut role_names } = value;

        let batch = RecordBatch::try_new(SCHEMA.clone(), vec![Arc::new(role_names.finish())])?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
