use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use spi::Result;

use meta::error::MetaError;
// use spi::query::execution::spi::DatafusionSnafu;
// use spi::query::spi::MetaSnafu;
use spi::query::execution::{
    // ExecutionError,
    Output,
    QueryStateMachineRef,
};
use std::sync::Arc;

pub struct ShowDatabasesTask {}

impl ShowDatabasesTask {
    pub fn new() -> Self {
        ShowDatabasesTask {}
    }
}

#[async_trait]
impl DDLDefinitionTask for ShowDatabasesTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        show_databases(query_state_machine)
    }
}

fn show_databases(machine: QueryStateMachineRef) -> Result<Output> {
    let tenant = machine.session.tenant();
    let client =
        machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })?;
    let databases = client.list_databases()?;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "Database",
        DataType::Utf8,
        false,
    )]));

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(databases))])?;
    let batches = vec![batch];

    Ok(Output::StreamData(schema, batches))
}
