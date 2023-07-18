use std::sync::Arc;

use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::TableType;
use lazy_static::lazy_static;

pub const TABLES_TABLE_TENANT: &str = "table_tenant";
pub const TABLES_TABLE_DATABASE: &str = "table_database";
pub const TABLES_TABLE_NAME: &str = "table_name";
pub const TABLES_TABLE_TYPE: &str = "table_type";
pub const TABLES_TABLE_ENGINE: &str = "table_engine";
pub const TABLES_TABLE_OPTIONS: &str = "table_options";

lazy_static! {
    pub static ref TABLES_SCHEMA: SchemaRef = Arc::new(Schema::new(vec![
        Field::new(TABLES_TABLE_TENANT, DataType::Utf8, false),
        Field::new(TABLES_TABLE_DATABASE, DataType::Utf8, false),
        Field::new(TABLES_TABLE_NAME, DataType::Utf8, false),
        Field::new(TABLES_TABLE_TYPE, DataType::Utf8, false),
        Field::new(TABLES_TABLE_ENGINE, DataType::Utf8, false),
        Field::new(TABLES_TABLE_OPTIONS, DataType::Utf8, false),
    ]));
}

/// Builds the `information_schema.TABLES` table row by row
#[derive(Default)]
pub struct InformationSchemaTablesBuilder {
    tenant_names: StringBuilder,
    database_names: StringBuilder,
    table_names: StringBuilder,
    table_types: StringBuilder,
    table_engines: StringBuilder,
    table_options: StringBuilder,
}

impl InformationSchemaTablesBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn append_row(
        &mut self,
        tenant_name: impl AsRef<str>,
        database_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        table_type: TableType,
        table_engine: impl AsRef<str>,
        table_option: impl AsRef<str>,
    ) {
        // Note: append_value is actually infallable.
        self.tenant_names.append_value(tenant_name.as_ref());
        self.database_names.append_value(database_name.as_ref());
        self.table_names.append_value(table_name.as_ref());
        self.table_types.append_value(match table_type {
            TableType::Base => "TABLE",
            TableType::View => "VIEW",
            TableType::Temporary => "LOCAL TEMPORARY",
        });
        self.table_engines.append_value(table_engine.as_ref());
        self.table_options.append_value(table_option.as_ref());
    }
}

impl TryFrom<InformationSchemaTablesBuilder> for RecordBatch {
    type Error = DataFusionError;

    fn try_from(value: InformationSchemaTablesBuilder) -> Result<Self, Self::Error> {
        let InformationSchemaTablesBuilder {
            mut tenant_names,
            mut database_names,
            mut table_names,
            mut table_types,
            mut table_engines,
            mut table_options,
        } = value;

        let batch = RecordBatch::try_new(
            TABLES_SCHEMA.clone(),
            vec![
                Arc::new(tenant_names.finish()),
                Arc::new(database_names.finish()),
                Arc::new(table_names.finish()),
                Arc::new(table_types.finish()),
                Arc::new(table_engines.finish()),
                Arc::new(table_options.finish()),
            ],
        )?;
        Ok(batch)
    }
}
