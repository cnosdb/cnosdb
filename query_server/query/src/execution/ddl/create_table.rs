use std::sync::Arc;

use async_trait::async_trait;
use meta::error::MetaError;
use models::schema::table_schema::TableSchema;
use models::schema::tskv_table_schema::TskvTableSchema;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateTable;
use spi::{MetaSnafu, QueryError, QueryResult};

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let res = create_table(&self.stmt, query_state_machine).await;
        if self.stmt.if_not_exists
            && matches!(
                res,
                Err(QueryError::Meta {
                    source: MetaError::TableAlreadyExists { .. }
                })
            )
        {
            return Ok(Output::Nil(()));
        }
        res.map(|_| Output::Nil(()))
    }
}

async fn create_table(stmt: &CreateTable, machine: QueryStateMachineRef) -> QueryResult<()> {
    let CreateTable { name, .. } = stmt;

    let client = machine
        .meta
        .tenant_meta(name.tenant())
        .await
        .ok_or_else(|| MetaError::TenantNotFound {
            tenant: name.tenant().to_string(),
        })
        .context(MetaSnafu)?;

    let table_schema = build_schema(stmt);
    client
        .create_table(&TableSchema::TsKvTableSchema(Arc::new(table_schema)))
        .await
        .context(MetaSnafu)
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
