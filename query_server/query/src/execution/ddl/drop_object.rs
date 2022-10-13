use async_trait::async_trait;
use spi::query::{
    ast::ObjectType,
    execution::{Output, QueryStateMachineRef},
    logical_planner::DropPlan,
};

use spi::query::execution;
use spi::query::execution::ExecutionError;

use super::DDLDefinitionTask;

use snafu::ResultExt;

pub struct DropObjectTask {
    stmt: DropPlan,
}

impl DropObjectTask {
    #[inline(always)]
    pub fn new(stmt: DropPlan) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropObjectTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let DropPlan {
            ref object_name,
            ref if_exist,
            ref obj_type,
        } = self.stmt;

        let res = match obj_type {
            ObjectType::Table => query_state_machine.catalog.drop_table(object_name),
            ObjectType::Database => query_state_machine.catalog.drop_database(object_name),
        };

        if *if_exist {
            return Ok(Output::Nil(()));
        }

        res.map(|_| Output::Nil(()))
            .context(execution::MetadataSnafu)
    }
}
