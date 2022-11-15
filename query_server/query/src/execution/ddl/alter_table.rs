use crate::execution::ddl::query::execution::MetadataSnafu;
use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use snafu::ResultExt;

use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::{AlterTable, AlterTableAction};

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
        let table_name = &self._stmt.table_name;
        let catalog = query_state_machine.catalog.clone();
        match &self._stmt.alter_action {
            AlterTableAction::AddColumn { table_column } => catalog
                .alter_table_add_column(table_name, table_column.clone())
                .context(MetadataSnafu)?,
            AlterTableAction::DropColumn { column_name } => catalog
                .alter_table_drop_column(table_name, column_name)
                .context(MetadataSnafu)?,
            AlterTableAction::AlterColumn {
                column_name,
                new_column,
            } => catalog
                .alter_table_alter_column(table_name, column_name, new_column.clone())
                .context(MetadataSnafu)?,
        }
        return Ok(Output::Nil(()));
    }
}
