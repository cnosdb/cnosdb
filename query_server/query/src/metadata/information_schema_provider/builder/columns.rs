use std::sync::Arc;

use datafusion::arrow::array::{BooleanBuilder, StringBuilder, UInt64Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref COLUMN_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("tenant_name", DataType::Utf8, false),
        Field::new("database_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("column_type", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::UInt64, false),
        Field::new("column_default", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Boolean, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("compression_codec", DataType::Utf8, true),
    ]));
}

/// Builds the `information_schema.Columns` table row by row
#[derive(Default)]
pub struct InformationSchemaColumnsBuilder {
    tenant_names: StringBuilder,
    database_names: StringBuilder,
    table_names: StringBuilder,
    column_names: StringBuilder,
    column_types: StringBuilder,
    ordinal_positions: UInt64Builder,
    column_defaults: StringBuilder,
    is_nullables: BooleanBuilder,
    data_types: StringBuilder,
    compression_codecs: StringBuilder,
}

impl InformationSchemaColumnsBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn append_row(
        &mut self,
        tenant_name: impl AsRef<str>,
        database_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        column_name: impl AsRef<str>,
        column_type: impl AsRef<str>,
        ordinal_position: u64,
        column_default: impl AsRef<str>,
        is_nullable: bool,
        data_type: impl AsRef<str>,
        compression_codec: Option<impl AsRef<str>>,
    ) {
        // Note: append_value is actually infallable.
        self.tenant_names.append_value(tenant_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.column_names.append_value(column_name);
        self.column_types.append_value(column_type.as_ref());
        self.ordinal_positions.append_value(ordinal_position);
        self.column_defaults.append_value(column_default.as_ref());
        self.is_nullables.append_value(is_nullable);
        self.data_types.append_value(data_type.as_ref());
        self.compression_codecs
            .append_option(compression_codec.as_ref());
    }
}

impl TryFrom<InformationSchemaColumnsBuilder> for RecordBatch {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaColumnsBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaColumnsBuilder {
            mut tenant_names,
            mut database_names,
            mut table_names,
            mut column_names,
            mut column_types,
            mut ordinal_positions,
            mut column_defaults,
            mut is_nullables,
            mut data_types,
            mut compression_codecs,
        } = value;

        let batch = RecordBatch::try_new(
            COLUMN_SCHEMA.clone(),
            vec![
                Arc::new(tenant_names.finish()),
                Arc::new(database_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(column_names.finish()),
                Arc::new(column_types.finish()),
                Arc::new(ordinal_positions.finish()),
                Arc::new(column_defaults.finish()),
                Arc::new(is_nullables.finish()),
                Arc::new(data_types.finish()),
                Arc::new(compression_codecs.finish()),
            ],
        )?;

        Ok(batch)
    }
}
