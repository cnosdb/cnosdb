use crate::execution::ddl::query::execution::MetadataSnafu;
use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use datafusion::common::TableReference;
use models::schema::TableSchema;
use snafu::ResultExt;
use meta::error::MetaError;

use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::{AlterTable, AlterTableAction};

pub struct AlterTableTask {
    stmt: AlterTable,
}

impl AlterTableTask {
    pub fn new(stmt: AlterTable) -> AlterTableTask {
        Self { stmt }
    }
}
#[async_trait]
impl DDLDefinitionTask for AlterTableTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let tenant = query_state_machine.session.tenant();
        let table_name = TableReference::from(self.stmt.table_name.as_str())
            .resolve(tenant, query_state_machine.session.default_database());
        let client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetadataSnafu)?;

        let mut schema = client
            .get_tskv_table_schema(table_name.schema, table_name.table)
            .context(MetadataSnafu)?
            .ok_or(MetaError::TableNotFound {
                table: table_name.table.to_string(),
            })
            .context(MetadataSnafu)?;

        match &self.stmt.alter_action {
            AlterTableAction::AddColumn { table_column } => schema.add_column(table_column.clone()),
            AlterTableAction::DropColumn { column_name } => schema.drop_column(column_name),
            AlterTableAction::AlterColumn {
                column_name,
                new_column,
            } => schema.change_column(column_name, new_column.clone()),
        }
        schema.schema_id += 1;

        client
            .update_table(&TableSchema::TsKvTableSchema(schema))
            .context(MetadataSnafu)?;

        return Ok(Output::Nil(()));
    }
}
