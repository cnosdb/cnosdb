use std::sync::Arc;

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ENABLED_ROLE_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "role_name",
        DataType::Utf8,
        false
    ),]));
}

/// Builds the `information_schema.EnabledRoles` table row by row
#[derive(Default)]
pub struct InformationSchemaEnabledRolesBuilder {
    role_names: StringBuilder,
}

impl InformationSchemaEnabledRolesBuilder {
    pub fn append_row(&mut self, role_name: impl AsRef<str>) {
        // Note: append_value is actually infallable.
        self.role_names.append_value(role_name.as_ref());
    }
}

impl TryFrom<InformationSchemaEnabledRolesBuilder> for RecordBatch {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaEnabledRolesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaEnabledRolesBuilder { mut role_names } = value;

        let batch = RecordBatch::try_new(
            ENABLED_ROLE_SCHEMA.clone(),
            vec![Arc::new(role_names.finish())],
        )?;

        Ok(batch)
    }
}
