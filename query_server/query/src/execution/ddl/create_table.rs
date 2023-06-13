use std::sync::Arc;

use async_trait::async_trait;
use meta::error::MetaError;
use models::schema::{TableSchema, TskvTableSchema};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateTable;
use spi::Result;

use crate::execution::ddl::DDLDefinitionTask;

pub struct CreateTableTask {
    stmt: CreateTable,
}

impl CreateTableTask {
    pub fn new(stmt: CreateTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateTableTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let CreateTable {
            ref name,
            ref if_not_exists,
            ..
        } = self.stmt;

        let tenant = query_state_machine.session.tenant();
        let client = query_state_machine.meta.tenant_meta(tenant).await.ok_or(
            MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            },
        )?;
        let table = client.get_tskv_table_schema(name.database(), name.table())?;

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

async fn create_table(stmt: &CreateTable, machine: QueryStateMachineRef) -> Result<()> {
    let CreateTable { name, .. } = stmt;

    let client =
        machine
            .meta
            .tenant_meta(name.tenant())
            .await
            .ok_or(MetaError::TenantNotFound {
                tenant: name.tenant().to_string(),
            })?;

    let table_schema = build_schema(stmt);
    client
        .create_table(&TableSchema::TsKvTableSchema(Arc::new(table_schema)))
        .await
        .context(spi::MetaSnafu)
}

fn build_schema(stmt: &CreateTable) -> TskvTableSchema {
    let CreateTable { schema, name, .. } = stmt;

    TskvTableSchema::new(
        name.tenant().to_string(),
        name.database().to_string(),
        name.table().to_string(),
        schema.to_owned(),
    )
}
