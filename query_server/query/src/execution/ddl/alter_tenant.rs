use async_trait::async_trait;
use meta::error::MetaError;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{
    AlterTenant, AlterTenantAction, AlterTenantAddUser, AlterTenantSetUser,
};
use spi::QueryError;
use trace::debug;

use crate::execution::ddl::DDLDefinitionTask;

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
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, QueryError> {
        let AlterTenant {
            ref tenant_name,
            ref alter_tenant_action,
        } = self.stmt;

        let tenant_manager = query_state_machine.meta.tenant_manager();

        let meta = tenant_manager
            .tenant_meta(tenant_name)
            .await
            .ok_or_else(|| QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                },
            })?;

        match alter_tenant_action {
            AlterTenantAction::AddUser(AlterTenantAddUser { user_id, role }) => {
                // 向租户中添加指定角色的成员
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
                    user_id, tenant_name, role
                );
                meta.add_member_with_role(*user_id, role.clone()).await?;
                // .context(MetaSnafu)?;
            }
            AlterTenantAction::SetUser(AlterTenantSetUser { user_id, role }) => {
                // 重设租户中指定成员的角色
                // user_id: Oid,
                // role: TenantRoleIdentifier,
                // tenant_id: Oid,
                // fn reassign_member_role_in_tenant(
                //     &mut self,
                //     user_id: Oid,
                //     role: TenantRoleIdentifier,
                //     tenant_id: Oid,
                // ) -> Result<()>;
                debug!(
                    "Reassign role {:?} of user {} in tenant {}",
                    role, user_id, tenant_name
                );
                meta.reassign_member_role(*user_id, role.clone()).await?;
                // .context(MetaSnafu)?;
            }
            AlterTenantAction::RemoveUser(user_id) => {
                // 从租户中移除指定成员
                // user_id: Oid,
                // tenant_id: Oid
                // fn remove_member_from_tenant(
                //     &mut self,
                //     user_id: Oid,
                //     tenant_id: Oid
                // ) -> Result<()>;
                debug!("Remove user {} from tenant {}", user_id, tenant_name);
                meta.remove_member(*user_id).await?;
                // .context(MetaSnafu)?;
            }
            AlterTenantAction::SetOption(tenant_option) => {
                tenant_manager
                    .alter_tenant(tenant_name, *tenant_option.clone())
                    .await?;
            }
        }

        return Ok(Output::Nil(()));
    }
}
