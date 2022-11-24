use async_trait::async_trait;
use spi::query::{
    execution::{Output, QueryStateMachineRef},
    logical_planner::{DropTenantObject, TenantObjectType},
};

use spi::query::execution;
use spi::query::execution::{ExecutionError, MetadataSnafu};
use trace::debug;

use super::DDLDefinitionTask;

use snafu::ResultExt;

pub struct DropTenantObjectTask {
    stmt: DropTenantObject,
}

impl DropTenantObjectTask {
    #[inline(always)]
    pub fn new(stmt: DropTenantObject) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropTenantObjectTask {
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let DropTenantObject {
            ref tenant_id,
            ref name,
            ref if_exist,
            ref obj_type,
        } = self.stmt;

        let res = match obj_type {
            TenantObjectType::Role => {
                // TODO 删除租户下的自定义角色
                // tenant_id
                // role_name
                // fn drop_custom_role_of_tenant(
                //     &mut self,
                //     role_name: &str,
                //     tenant_id: &Oid
                // ) -> Result<bool>;
                debug!("Drop role {} of tenant {}", name, tenant_id);
                Ok(())
            }
            TenantObjectType::Database => {
                // TODO 删除租户下的database
                // tenant_id
                // database_name
                debug!("Drop database {} of tenant {}", name, tenant_id);
                query_state_machine
                    .catalog
                    .drop_database(name)
                    .context(MetadataSnafu)?;
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
