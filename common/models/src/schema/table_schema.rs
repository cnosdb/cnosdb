use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use serde::{Deserialize, Serialize};

use crate::schema::external_table_schema::ExternalTableSchema;
use crate::schema::stream_table_schema::StreamTable;
use crate::schema::tskv_table_schema::TskvTableSchemaRef;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum TableSchema {
    TsKvTableSchema(TskvTableSchemaRef),
    ExternalTableSchema(Arc<ExternalTableSchema>),
    StreamTableSchema(Arc<StreamTable>),
}

impl TableSchema {
    pub fn name(&self) -> &str {
        match self {
            TableSchema::TsKvTableSchema(schema) => schema.name.as_ref(),
            TableSchema::ExternalTableSchema(schema) => schema.name.as_ref(),
            TableSchema::StreamTableSchema(schema) => schema.name(),
        }
    }

    pub fn db(&self) -> &str {
        match self {
            TableSchema::TsKvTableSchema(schema) => schema.db.as_ref(),
            TableSchema::ExternalTableSchema(schema) => schema.db.as_ref(),
            TableSchema::StreamTableSchema(schema) => schema.db(),
        }
    }

    pub fn engine_name(&self) -> &str {
        match self {
            TableSchema::TsKvTableSchema(_) => "TSKV",
            TableSchema::ExternalTableSchema(_) => "EXTERNAL",
            TableSchema::StreamTableSchema(_) => "STREAM",
        }
    }

    pub fn to_arrow_schema(&self) -> SchemaRef {
        match self {
            Self::ExternalTableSchema(e) => Arc::new(e.schema.clone()),
            Self::TsKvTableSchema(e) => e.build_arrow_schema(),
            Self::StreamTableSchema(e) => e.schema(),
        }
    }
}
