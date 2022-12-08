use crate::execution::ddl::query::execution::MetadataSnafu;
use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use meta::meta_client::MetaError;
use snafu::ResultExt;


use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::{AlterTable};

pub struct AlterTableTask {
    _stmt: AlterTable,
}

impl AlterTableTask {
    pub fn new(stmt: AlterTable) -> AlterTableTask {
        Self { _stmt: stmt }
    }
}
#[async_trait]
impl DDLDefinitionTask for AlterTableTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let tenant = query_state_machine.session.tenant();
        let _table_name = &self._stmt.table_name;
        let _client = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .ok_or(MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            })
            .context(MetadataSnafu)?;

        todo!("alter method for meta")

        // match &self._stmt.alter_action {
        //     AlterTableAction::AddColumn { table_column } => catalog
        //         .alter_table_add_column(table_name, table_column.clone())
        //         .context(MetadataSnafu)?,
        //     AlterTableAction::DropColumn { column_name } => catalog
        //         .alter_table_drop_column(table_name, column_name)
        //         .context(MetadataSnafu)?,
        //     AlterTableAction::AlterColumn {
        //         column_name,
        //         new_column,
        //     } => catalog
        //         .alter_table_alter_column(table_name, column_name, new_column.clone())
        //         .context(MetadataSnafu)?,
        // }
        // return Ok(Output::Nil(()));
    }
}
