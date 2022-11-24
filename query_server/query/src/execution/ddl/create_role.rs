use crate::execution::ddl::DDLDefinitionTask;
use async_trait::async_trait;
use models::auth::role::CustomTenantRole;
use models::oid::Oid;
use snafu::ResultExt;
use spi::catalog::MetadataError;
use spi::query::execution;
use spi::query::execution::{ExecutionError, Output, QueryStateMachineRef};
use spi::query::logical_planner::CreateRole;
use trace::debug;

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
    async fn execute(
        &self,
        _query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let CreateRole {
            ref tenant_id,
            ref name,
            ref if_not_exists,
            ref inherit_tenant_role,
        } = self.stmt;

        // TODO 元数据接口查询tenant_id下自定义角色是否存在
        // fn custom_role_of_tenant(
        //     &self,
        //     role_name: &str,
        //     tenant_id: &Oid,
        // ) -> Result<Option<CustomTenantRole<Oid>>>;

        let role: Option<CustomTenantRole<Oid>> = None;

        match (if_not_exists, role) {
            // do not create if exists
            (true, Some(_)) => Ok(Output::Nil(())),
            // Report an error if it exists
            (false, Some(_)) => Err(MetadataError::RoleAlreadyExists {
                role_name: name.clone(),
            })
            .context(execution::MetadataSnafu),
            // does not exist, create
            (_, None) => {
                // TODO 创建自定义角色
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
                    name, tenant_id, inherit_tenant_role
                );

                Ok(Output::Nil(()))
            }
        }
    }
}
