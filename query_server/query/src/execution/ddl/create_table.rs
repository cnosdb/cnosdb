use async_trait::async_trait;
use datafusion::sql::TableReference;
use meta::error::MetaError;
use models::schema::{TableSchema, TskvTableSchema};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateTable;
use spi::query::session::IsiphoSessionCtx;
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
        let client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })?;
        let table_ref = TableReference::from(name.as_str())
            .resolve(tenant, query_state_machine.session.default_database());
        let table = client.get_tskv_table_schema(table_ref.schema, table_ref.table)?;

        match (if_not_exists, table) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetaError::TableAlreadyExists {
                table_name: name.clone(),
            })?,
            // does not exist, create
            (_, None) => {
                create_table(&self.stmt, query_state_machine)?;
                Ok(Output::Nil(()))
            }
        }
    }
}

fn create_table(stmt: &CreateTable, machine: QueryStateMachineRef) -> Result<()> {
    let CreateTable { .. } = stmt;
    let table_schema = build_schema(stmt, &machine.session);
    let tenant = machine.session.tenant();
    let client =
        machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })?;
    // .context(MetaSnafu)?;
    client
        .create_table(&TableSchema::TsKvTableSchema(table_schema))
        .context(spi::MetaSnafu)
}

fn build_schema(stmt: &CreateTable, session: &IsiphoSessionCtx) -> TskvTableSchema {
    let CreateTable { schema, name, .. } = stmt;

    let table: TableReference = name.as_str().into();
    let catalog_name = session.tenant();
    let schema_name = session.default_database();
    let table_ref = table.resolve(catalog_name, schema_name);

    TskvTableSchema::new(
        catalog_name.to_string(),
        table_ref.schema.to_string(),
        table.table().to_string(),
        schema.to_owned(),
    )
}
