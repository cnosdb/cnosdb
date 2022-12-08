use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use meta::meta_client::MetaError;
use snafu::ResultExt;

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
        show_tables(&self.database_name, query_state_machine)
    }
}

fn show_tables(
    database_name: &Option<String>,
    machine: QueryStateMachineRef,
) -> Result<Output, ExecutionError> {
    let tenant = machine.session.tenant();
    let client = machine
        .meta
        .tenant_manager()
        .tenant_meta(tenant)
        .ok_or(MetaError::TenantNotFound {
            tenant: tenant.to_string(),
        })
        .context(MetadataSnafu)?;
    let database_name = match database_name {
        None => machine.session.default_database(),
        Some(v) => v.as_str(),
    };
    let tables = client.list_tables(database_name).context(MetadataSnafu)?;
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
