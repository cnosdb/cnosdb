use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;

use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::AlterTable;

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
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        //TODO
        return Ok(Output::Nil(()));
    }
}
