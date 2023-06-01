use std::sync::Arc;

use datafusion::arrow::array::{Float64Builder, StringBuilder, UInt64Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref QUERY_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("query_id", DataType::Utf8, false),
        Field::new("query_type", DataType::Utf8, false),
        Field::new("query_text", DataType::Utf8, false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("user_name", DataType::Utf8, false),
        Field::new("tenant_id", DataType::Utf8, false),
        Field::new("tenant_name", DataType::Utf8, false),
        Field::new("state", DataType::Utf8, false),
        Field::new("duration", DataType::Float64, false),
        Field::new("processed_count", DataType::UInt64, false),
        Field::new("error_count", DataType::UInt64, false),
    ]));
}

/// Builds the `information_schema.Queries` table row by row
#[derive(Default)]
pub struct InformationSchemaQueriesBuilder {
    query_ids: StringBuilder,
    query_types: StringBuilder,
    query_texts: StringBuilder,
    user_ids: StringBuilder,
    user_names: StringBuilder,
    tenant_ids: StringBuilder,
    tenant_names: StringBuilder,
    states: StringBuilder,
    durations: Float64Builder,
    processed_counts: UInt64Builder,
    error_counts: UInt64Builder,
}

impl InformationSchemaQueriesBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn append_row(
        &mut self,
        query_id: impl AsRef<str>,
        query_type: impl AsRef<str>,
        query_text: impl AsRef<str>,
        user_id: impl AsRef<str>,
        user_name: impl AsRef<str>,
        tenant_id: impl AsRef<str>,
        tenant_name: impl AsRef<str>,
        state: impl AsRef<str>,
        duration: f64,
        processed_count: u64,
        error_count: u64,
    ) {
        // Note: append_value is actually infallable.
        self.query_ids.append_value(query_id.as_ref());
        self.query_types.append_value(query_type.as_ref());
        self.query_texts.append_value(query_text.as_ref());
        self.user_ids.append_value(user_id.as_ref());
        self.user_names.append_value(user_name.as_ref());
        self.tenant_ids.append_value(tenant_id.as_ref());
        self.tenant_names.append_value(tenant_name.as_ref());
        self.states.append_value(state.as_ref());
        self.durations.append_value(duration);
        self.processed_counts.append_value(processed_count);
        self.error_counts.append_value(error_count);
    }
}

impl TryFrom<InformationSchemaQueriesBuilder> for RecordBatch {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaQueriesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaQueriesBuilder {
            mut query_ids,
            mut query_types,
            mut query_texts,
            mut user_ids,
            mut user_names,
            mut tenant_ids,
            mut tenant_names,
            mut states,
            mut durations,
            mut processed_counts,
            mut error_counts,
        } = value;

        let batch = RecordBatch::try_new(
            QUERY_SCHEMA.clone(),
            vec![
                Arc::new(query_ids.finish()),
                Arc::new(query_types.finish()),
                Arc::new(query_texts.finish()),
                Arc::new(user_ids.finish()),
                Arc::new(user_names.finish()),
                Arc::new(tenant_ids.finish()),
                Arc::new(tenant_names.finish()),
                Arc::new(states.finish()),
                Arc::new(durations.finish()),
                Arc::new(processed_counts.finish()),
                Arc::new(error_counts.finish()),
            ],
        )?;

        Ok(batch)
    }
}
