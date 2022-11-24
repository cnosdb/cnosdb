use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::{
    AlterTenant, AlterTenantAction, AlterTenantAddUser, AlterTenantSetUser,
};
use trace::debug;

pub struct AlterTenantTask {
    stmt: AlterTenant,
}

impl AlterTenantTask {
    pub fn new(stmt: AlterTenant) -> AlterTenantTask {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for AlterTenantTask {
    async fn execute(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let AlterTenant {
            ref tenant_id,
            ref alter_tenant_action,
        } = self.stmt;

        match alter_tenant_action {
            AlterTenantAction::AddUser(AlterTenantAddUser {
                user_id,
                role,
                tenant_id,
            }) => {
                // TODO 向租户中添加指定角色的成员
                // user_id: Oid,
                // role: TenantRoleIdentifier,
                // tenant_id: Oid,
                // fn add_member_with_role_to_tenant(
                //     &mut self,
                //     user_id: Oid,
                //     role: TenantRoleIdentifier,
                //     tenant_id: Oid,
                // ) -> Result<()>
                debug!(
                    "Add user {} to tenant {} with role {:?}",
                    user_id, tenant_id, role
                );
            }
            AlterTenantAction::SetUser(AlterTenantSetUser {
                user_id,
                role,
                tenant_id,
            }) => {
                // TODO 重设租户中指定成员的角色
                // user_id: Oid,
                // role: TenantRoleIdentifier,
                // tenant_id: Oid,
                // fn reasign_member_role_in_tenant(
                //     &mut self,
                //     user_id: Oid,
                //     role: TenantRoleIdentifier,
                //     tenant_id: Oid,
                // ) -> Result<()>;
                debug!(
                    "Reasign role {:?} of user {} in tenant {}",
                    role, user_id, tenant_id
                );
            }
            AlterTenantAction::RemoveUser(user_id) => {
                // TODO 从租户中移除指定成员
                // user_id: Oid,
                // tenant_id: Oid
                // fn remove_member_from_tenant(
                //     &mut self,
                //     user_id: Oid,
                //     tenant_id: Oid
                // ) -> Result<()>;
                debug!("Remove user {} from tenant {}", user_id, tenant_id);
            }
            AlterTenantAction::Set(options) => {
                // TODO 修改租户的信息
                // tenant_id: Oid,
                // options: TenantOptions
                // fn alter_tenant(
                //     &self,
                //     tenant_id: Oid,
                //     options: TenantOptions
                // ) -> Result<()>;
                debug!("Alter tenant {} with options [{}]", tenant_id, options);
            }
        }

        return Ok(Output::Nil(()));
    }
}
