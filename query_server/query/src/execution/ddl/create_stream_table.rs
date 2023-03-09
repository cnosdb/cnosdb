use std::sync::Arc;

use async_trait::async_trait;
use meta::error::MetaError;
use models::schema::{StreamTable, TableSchema};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateStreamTable;
use spi::Result;

use crate::execution::ddl::DDLDefinitionTask;

pub struct CreateStreamTableTask {
    stmt: CreateStreamTable,
}

impl CreateStreamTableTask {
    pub fn new(stmt: CreateStreamTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateStreamTableTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let CreateStreamTable {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;

        let tenant = query_state_machine.session.tenant();
        let client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .await
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })?;
        let table = client.get_table_schema(name.database(), name.table())?;

        match (if_not_exists, table) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetaError::TableAlreadyExists {
                table_name: name.to_string(),
            })?,
            // does not exist, create
            (_, None) => {
                create_table(&self.stmt, query_state_machine).await?;
                Ok(Output::Nil(()))
            }
        }
    }
}

async fn create_table(stmt: &CreateStreamTable, machine: QueryStateMachineRef) -> Result<()> {
    let table = build_table(stmt);
    machine
        .meta
        .tenant_manager()
        .tenant_meta(table.tenant())
        .await
        .ok_or(MetaError::TenantNotFound {
            tenant: table.tenant().into(),
        })?
        .create_table(&TableSchema::StreamTableSchema(Arc::new(table)))
        .await
        .context(spi::MetaSnafu)
}

fn build_table(stmt: &CreateStreamTable) -> StreamTable {
    let CreateStreamTable {
        schema,
        name,
        stream_type,
        watermark,
        extra_options,
        ..
    } = stmt;

    StreamTable::new(
        name.tenant(),
        name.database(),
        name.table(),
        Arc::new(schema.to_owned()),
        stream_type.to_owned(),
        watermark.to_owned(),
        extra_options.to_owned(),
    )
}
