use std::sync::Arc;

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

pub const RESOURCE_STATUS_TIME: &str = "time";
pub const RESOURCE_STATUS_NAME: &str = "name";
pub const RESOURCE_STATUS_OPERATOR: &str = "action";
pub const RESOURCE_STATUS_TRY_COUNT: &str = "try_count";
pub const RESOURCE_STATUS_STATUS: &str = "status";
pub const RESOURCE_STATUS_COMMENT: &str = "comment";

lazy_static! {
    pub static ref RESOURCE_STATUS_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new(RESOURCE_STATUS_TIME, DataType::Utf8, false),
        Field::new(RESOURCE_STATUS_NAME, DataType::Utf8, false),
        Field::new(RESOURCE_STATUS_OPERATOR, DataType::Utf8, false),
        Field::new(RESOURCE_STATUS_TRY_COUNT, DataType::Utf8, false),
        Field::new(RESOURCE_STATUS_STATUS, DataType::Utf8, false),
        Field::new(RESOURCE_STATUS_COMMENT, DataType::Utf8, true),
    ]));
}

#[derive(Default)]
pub struct InformationSchemaResourceStatusBuilder {
    time: StringBuilder,
    name: StringBuilder,
    operator: StringBuilder,
    try_count: StringBuilder,
    status: StringBuilder,
    comment: StringBuilder,
}

impl InformationSchemaResourceStatusBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn append_row(
        &mut self,
        time: impl AsRef<str>,
        name: impl AsRef<str>,
        operator: impl AsRef<str>,
        try_count: impl AsRef<str>,
        status: impl AsRef<str>,
        comment: impl AsRef<str>,
    ) {
        self.time.append_value(time);
        self.name.append_value(name.as_ref());
        self.operator.append_value(operator.as_ref());
        self.try_count.append_value(try_count.as_ref());
        self.status.append_value(status.as_ref());
        self.comment.append_value(comment.as_ref());
    }
}

impl TryFrom<InformationSchemaResourceStatusBuilder> for RecordBatch {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaResourceStatusBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaResourceStatusBuilder {
            mut time,
            mut name,
            mut operator,
            mut try_count,
            mut status,
            mut comment,
        } = value;

        let batch = RecordBatch::try_new(
            RESOURCE_STATUS_SCHEMA.clone(),
            vec![
                Arc::new(time.finish()),
                Arc::new(name.finish()),
                Arc::new(operator.finish()),
                Arc::new(try_count.finish()),
                Arc::new(status.finish()),
                Arc::new(comment.finish()),
            ],
        )?;
        Ok(batch)
    }
}
