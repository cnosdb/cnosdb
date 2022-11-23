use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::GrantRevoke;
use trace::debug;

pub struct GrantRevokeTask {
    stmt: GrantRevoke,
}

impl GrantRevokeTask {
    pub fn new(stmt: GrantRevoke) -> GrantRevokeTask {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for GrantRevokeTask {
    async fn execute(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let GrantRevoke {
            is_grant,
            database_privileges: _,
            tenant_id,
            ref role_name,
        } = self.stmt;

        // database_privileges: Vec<(DatabasePrivilege, Oid)>,
        // role_name: &str,
        // tenant_id: &Oid,
        if is_grant {
            // TODO 给租户下的自定义角色赋予若干权限
            // fn grant_privilege_to_custom_role_of_tenant(
            //     &mut self,
            //     database_name: String,
            //     database_privileges: Vec<(DatabasePrivilege, Oid)>,
            //     role_name: &str,
            //     tenant_id: &Oid,
            // ) -> Result<()>;
            debug!(
                "Grant privileges to role {} of tenant {}",
                role_name, tenant_id
            )
        } else {
            // TODO 给租户下的自定义角色撤销若干权限
            // fn revoke_privilege_from_custom_role_of_tenant(
            //     &mut self,
            //     database_name: &str,
            //     database_privileges: Vec<(DatabasePrivilege, Oid)>,
            //     role_name: &str,
            //     tenant_id: &Oid,
            // ) -> Result<bool>;
            debug!(
                "Revoke privileges from role {} of tenant {}",
                role_name, tenant_id
            )
        }

        return Ok(Output::Nil(()));
    }
}
