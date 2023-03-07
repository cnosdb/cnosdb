use std::sync::Arc;

use datafusion::arrow::array::{BooleanBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("user_name", DataType::Utf8, false),
        Field::new("is_admin", DataType::Boolean, false),
        Field::new("user_options", DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.USERS` table row by row
pub struct ClusterSchemaUsersBuilder {
    user_names: StringBuilder,
    is_admins: BooleanBuilder,
    options: StringBuilder,
}

impl Default for ClusterSchemaUsersBuilder {
    fn default() -> Self {
        Self {
            user_names: StringBuilder::new(),
            is_admins: BooleanBuilder::new(),
            options: StringBuilder::new(),
        }
    }
}

impl ClusterSchemaUsersBuilder {
    pub fn append_row(
        &mut self,
        user_name: impl AsRef<str>,
        is_admin: bool,
        options: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.user_names.append_value(user_name.as_ref());
        self.is_admins.append_value(is_admin);
        self.options.append_value(options.as_ref());
    }
}

impl TryFrom<ClusterSchemaUsersBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: ClusterSchemaUsersBuilder) -> Result<Self, Self::Error> {
        let ClusterSchemaUsersBuilder {
            mut user_names,
            mut is_admins,
            mut options,
        } = value;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(user_names.finish()),
                Arc::new(is_admins.finish()),
                Arc::new(options.finish()),
            ],
        )?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
