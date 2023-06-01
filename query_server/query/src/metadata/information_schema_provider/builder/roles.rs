use std::sync::Arc;

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref ROLE_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("role_name", DataType::Utf8, false),
        Field::new("role_type", DataType::Utf8, false),
        Field::new("inherit_role", DataType::Utf8, true),
    ]));
}

/// Builds the `information_schema.Roles` table row by row
#[derive(Default)]
pub struct InformationSchemaRolesBuilder {
    role_names: StringBuilder,
    role_types: StringBuilder,
    inherit_roles: StringBuilder,
}

impl InformationSchemaRolesBuilder {
    pub fn append_row(
        &mut self,
        role_name: impl AsRef<str>,
        role_type: impl AsRef<str>,
        inherit_role: Option<impl AsRef<str>>,
    ) {
        // Note: append_value is actually infallable.
        self.role_names.append_value(role_name.as_ref());
        self.role_types.append_value(role_type.as_ref());
        self.inherit_roles.append_option(inherit_role.as_ref());
    }
}

impl TryFrom<InformationSchemaRolesBuilder> for RecordBatch {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaRolesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaRolesBuilder {
            mut role_names,
            mut role_types,
            mut inherit_roles,
        } = value;

        let batch = RecordBatch::try_new(
            ROLE_SCHEMA.clone(),
            vec![
                Arc::new(role_names.finish()),
                Arc::new(role_types.finish()),
                Arc::new(inherit_roles.finish()),
            ],
        )?;

        Ok(batch)
    }
}
