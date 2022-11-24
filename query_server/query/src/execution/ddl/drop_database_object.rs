use async_trait::async_trait;
use spi::query::{
    execution::{Output, QueryStateMachineRef},
    logical_planner::{DatabaseObjectType, DropDatabaseObject},
};

use spi::query::execution;
use spi::query::execution::ExecutionError;
use trace::debug;

use super::DDLDefinitionTask;

use snafu::ResultExt;

pub struct DropDatabaseObjectTask {
    stmt: DropDatabaseObject,
}

impl DropDatabaseObjectTask {
    #[inline(always)]
    pub fn new(stmt: DropDatabaseObject) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropDatabaseObjectTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let DropDatabaseObject {
            tenant_id: _,
            ref object_name,
            ref if_exist,
            ref obj_type,
        } = self.stmt;

        let res = match obj_type {
            DatabaseObjectType::Table => {
                // TODO 删除指定租户下的表
                debug!("Drop table {}", object_name);
                query_state_machine.catalog.drop_table(object_name)
            }
        };

        if *if_exist {
            return Ok(Output::Nil(()));
        }

        res.map(|_| Output::Nil(()))
            .context(execution::MetadataSnafu)
    }
}
