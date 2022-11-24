use async_trait::async_trait;
use spi::query::{
    execution::{Output, QueryStateMachineRef},
    logical_planner::{DropGlobalObject, GlobalObjectType},
};

use spi::query::execution;
use spi::query::execution::ExecutionError;
use trace::debug;

use super::DDLDefinitionTask;

use snafu::ResultExt;

pub struct DropGlobalObjectTask {
    stmt: DropGlobalObject,
}

impl DropGlobalObjectTask {
    #[inline(always)]
    pub fn new(stmt: DropGlobalObject) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropGlobalObjectTask {
    async fn execute(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let DropGlobalObject {
            ref name,
            ref if_exist,
            ref obj_type,
        } = self.stmt;

        let res = match obj_type {
            GlobalObjectType::User => {
                // TODO 删除用户
                // fn drop_user(
                //     &mut self,
                //     name: &str
                // ) -> Result<bool>;
                debug!("Drop user {}", name);
                Ok(())
            }
            GlobalObjectType::Tenant => {
                // TODO 删除租户
                // fn drop_tenant(
                //     &self,
                //     name: &str
                // ) -> Result<bool>;
                debug!("Drop tenant {}", name);
                Ok(())
            }
        };

        if *if_exist {
            return Ok(Output::Nil(()));
        }

        res.map(|_| Output::Nil(()))
            .context(execution::MetadataSnafu)
    }
}
