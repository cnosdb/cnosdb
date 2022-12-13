use async_trait::async_trait;
use coordinator::command;
use spi::query::{
    execution::{CoordinatorErrSnafu, Output, QueryStateMachineRef},
    logical_planner::{DropTenantObject, TenantObjectType},
};

use spi::query::execution::{ExecutionError, MetadataSnafu};
use trace::debug;

use super::DDLDefinitionTask;

use meta::meta_client::MetaError;
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
            ref tenant_name,
            ref name,
            ref if_exist,
            ref obj_type,
        } = self.stmt;

        let meta = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant_name)
            .ok_or_else(|| ExecutionError::Metadata {
                source: MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                },
            })?;

        match obj_type {
            TenantObjectType::Role => {
                // 删除租户下的自定义角色
                // tenant_id
                // role_name
                // fn drop_custom_role_of_tenant(
                //     &mut self,
                //     role_name: &str,
                //     tenant_id: &Oid
                // ) -> Result<bool>;
                debug!("Drop role {} of tenant {}", name, tenant_name);
                let success = meta.drop_custom_role(name).context(MetadataSnafu)?;

                if let (false, false) = (if_exist, success) {
                    return Err(ExecutionError::Metadata {
                        source: MetaError::RoleNotFound {
                            role: name.to_string(),
                        },
                    });
                }

                Ok(Output::Nil(()))
            }

            TenantObjectType::Database => {
                // 删除租户下的database
                // tenant_id
                // database_name

                let req = command::AdminStatementRequest {
                    tenant: tenant_name.to_string(),
                    stmt: command::AdminStatementType::DropDB(name.clone()),
                };

                query_state_machine
                    .coord
                    .exec_admin_stat_on_all_node(req)
                    .await
                    .context(CoordinatorErrSnafu)?;

                debug!("Drop database {} of tenant {}", name, tenant_name);
                let success = meta.drop_db(name).context(MetadataSnafu)?;

                if let (false, false) = (if_exist, success) {
                    return Err(ExecutionError::Metadata {
                        source: MetaError::DatabaseNotFound {
                            database: name.to_string(),
                        },
                    });
                }

                Ok(Output::Nil(()))
            }
        }
    }
}
