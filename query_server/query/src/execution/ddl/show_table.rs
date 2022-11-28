use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use snafu::ResultExt;
use spi::catalog::MetaDataRef;
use spi::query::execution::ExternalSnafu;
use spi::query::execution::MetadataSnafu;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use std::sync::Arc;

pub struct ShowTablesTask {
    database_name: Option<String>,
}

impl ShowTablesTask {
    pub fn new(database_name: Option<String>) -> Self {
        Self { database_name }
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowTablesTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        show_tables(&self.database_name, query_state_machine.catalog.clone())
    }
}

fn show_tables(
    database_name: &Option<String>,
    catalog: MetaDataRef,
) -> Result<Output, ExecutionError> {
    let tables = catalog.show_tables(database_name).context(MetadataSnafu)?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "Table",
        DataType::Utf8,
        false,
    )]));

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(tables))])
        .map_err(datafusion::error::DataFusionError::ArrowError)
        .context(ExternalSnafu)?;

    let batches = vec![batch];

    Ok(Output::StreamData(schema, batches))
}
