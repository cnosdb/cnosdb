use async_trait::async_trait;
use meta::error::MetaError;
use snafu::ResultExt;
use spi::query::execution::{
    // ExecutionError,
    Output,
    QueryStateMachineRef,
};
use spi::query::logical_planner::CreateRole;
use spi::{QueryError, Result};
use trace::debug;

use crate::execution::ddl::DDLDefinitionTask;

pub struct CreateRoleTask {
    stmt: CreateRole,
}

impl CreateRoleTask {
    pub fn new(stmt: CreateRole) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CreateRoleTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let CreateRole {
            ref tenant_name,
            ref name,
            ref if_not_exists,
            ref inherit_tenant_role,
        } = self.stmt;

        // 元数据接口查询tenant_id下自定义角色是否存在
        // fn custom_role_of_tenant(
        //     &self,
        //     role_name: &str,
        //     tenant_id: &Oid,
        // ) -> Result<Option<CustomTenantRole<Oid>>>;
        let meta = query_state_machine
            .meta
            .tenant_meta(tenant_name)
            .await
            .ok_or_else(|| QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                },
            })?;

        let role = meta.custom_role(name).await?;
        // .context(MetaSnafu)?;

        match (if_not_exists, role) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => {
                Err(MetaError::RoleAlreadyExists { role: name.clone() }).context(spi::MetaSnafu)
            }
            // .context(spi::MetaSnafu),
            // does not exist, create
            (_, None) => {
                // 创建自定义角色
                // tenant_id: Oid
                // name: String
                // inherit_tenant_role: SystemTenantRole
                // fn create_custom_role_of_tenant(
                //     &mut self,
                //     tenant_id: &Oid,
                //     role_name: String,
                //     system_role: SystemTenantRole,
                //     additiona_privileges: HashMap<String, DatabasePrivilege>,
                // ) -> Result<()>;
                debug!(
                    "Create role {} of tenant {} inherit {:?}",
                    name, tenant_name, inherit_tenant_role
                );

                meta.create_custom_role(
                    name.to_string(),
                    inherit_tenant_role.clone(),
                    Default::default(),
                )
                .await?;
                // .context(MetaSnafu)?;

                Ok(Output::Nil(()))
            }
        }
    }
}
