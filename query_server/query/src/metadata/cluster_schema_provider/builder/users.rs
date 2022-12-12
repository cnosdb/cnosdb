use std::sync::Arc;

use datafusion::{
    arrow::{
        array::{BooleanBuilder, StringBuilder},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    },
    datasource::MemTable,
    error::DataFusionError,
};

use lazy_static::lazy_static;

lazy_static! {
    static ref SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("user_name", DataType::Utf8, false),
        Field::new("is_admin", DataType::Boolean, false),
        Field::new("comment", DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.USERS` table row by row
pub struct ClusterSchemaUsersBuilder {
    user_ids: StringBuilder,
    user_names: StringBuilder,
    is_admins: BooleanBuilder,
    comments: StringBuilder,
}

impl Default for ClusterSchemaUsersBuilder {
    fn default() -> Self {
        Self {
            user_ids: StringBuilder::new(),
            user_names: StringBuilder::new(),
            is_admins: BooleanBuilder::new(),
            comments: StringBuilder::new(),
        }
    }
}

impl ClusterSchemaUsersBuilder {
    pub fn _append_row(
        &mut self,
        user_id: impl AsRef<str>,
        user_name: impl AsRef<str>,
        is_admin: bool,
        comment: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.user_ids.append_value(user_id.as_ref());
        self.user_names.append_value(user_name.as_ref());
        self.is_admins.append_value(is_admin);
        self.comments.append_value(comment.as_ref());
    }
}

impl TryFrom<ClusterSchemaUsersBuilder> for MemTable {
    type Error = DataFusionError;

    fn try_from(value: ClusterSchemaUsersBuilder) -> Result<Self, Self::Error> {
        let ClusterSchemaUsersBuilder {
            mut user_ids,
            mut user_names,
            mut is_admins,
            mut comments,
        } = value;

        let batch = RecordBatch::try_new(
            SCHEMA.clone(),
            vec![
                Arc::new(user_ids.finish()),
                Arc::new(user_names.finish()),
                Arc::new(is_admins.finish()),
                Arc::new(comments.finish()),
            ],
        )?;

        MemTable::try_new(SCHEMA.clone(), vec![vec![batch]])
    }
}
