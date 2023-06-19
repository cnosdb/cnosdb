use std::sync::Arc;

use async_trait::async_trait;
use meta::error::MetaError;
use models::schema::{StreamTable, TableSchema};
use snafu::ResultExt;
use spi::query::datasource::stream::checker::StreamTableCheckerRef;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateStreamTable;
use spi::{QueryError, Result};

use crate::execution::ddl::DDLDefinitionTask;

pub struct CreateStreamTableTask {
    checker: Option<StreamTableCheckerRef>,
    stmt: CreateStreamTable,
}

impl CreateStreamTableTask {
    pub fn new(checker: Option<StreamTableCheckerRef>, stmt: CreateStreamTable) -> Self {
        Self { checker, stmt }
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
        let client = query_state_machine.meta.tenant_meta(tenant).await.ok_or(
            MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            },
        )?;
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
                let table = build_table(&self.stmt);
                self.checker
                    .as_ref()
                    .ok_or_else(|| QueryError::UnsupportedStreamType {
                        stream_type: table.stream_type().to_string(),
                    })?
                    .check(&client, &table)?;

                create_table(table, query_state_machine).await?;
                Ok(Output::Nil(()))
            }
        }
    }
}

async fn create_table(table: StreamTable, machine: QueryStateMachineRef) -> Result<()> {
    machine
        .meta
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
