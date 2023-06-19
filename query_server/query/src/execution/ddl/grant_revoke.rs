use async_trait::async_trait;
use meta::error::MetaError;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::GrantRevoke;
use spi::{QueryError, Result};
use trace::debug;

use crate::execution::ddl::DDLDefinitionTask;

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let GrantRevoke {
            is_grant,
            ref database_privileges,
            ref tenant_name,
            ref role_name,
        } = self.stmt;

        let meta = query_state_machine
            .meta
            .tenant_meta(tenant_name)
            .await
            .ok_or_else(|| QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                },
            })?;

        // database_privileges: Vec<(DatabasePrivilege, Oid)>,
        // role_name: &str,
        // tenant_id: &Oid,
        if is_grant {
            // 给租户下的自定义角色赋予若干权限
            // fn grant_privilege_to_custom_role_of_tenant(
            //     &mut self,
            //     database_name: String,
            //     database_privileges: Vec<(DatabasePrivilege, Oid)>,
            //     role_name: &str,
            //     tenant_id: &Oid,
            // ) -> Result<()>;
            debug!(
                "Grant privileges to role {} of tenant {}",
                role_name, tenant_name
            );

            meta.grant_privilege_to_custom_role(database_privileges.clone(), role_name)
                .await?;
        } else {
            // 给租户下的自定义角色撤销若干权限
            // fn revoke_privilege_from_custom_role_of_tenant(
            //     &mut self,
            //     database_name: &str,
            //     database_privileges: Vec<(DatabasePrivilege, Oid)>,
            //     role_name: &str,
            //     tenant_id: &Oid,
            // ) -> Result<bool>;
            debug!(
                "Revoke privileges from role {} of tenant {}",
                role_name, tenant_name
            );

            meta.revoke_privilege_from_custom_role(database_privileges.clone(), role_name)
                .await?;
        }

        return Ok(Output::Nil(()));
    }
}
