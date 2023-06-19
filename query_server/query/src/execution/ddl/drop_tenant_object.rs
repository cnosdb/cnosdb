use async_trait::async_trait;
use meta::error::MetaError;
use protos::kv_service::admin_command_request::Command::DropDb;
use protos::kv_service::{AdminCommandRequest, DropDbRequest};
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DropTenantObject, TenantObjectType};
use spi::{QueryError, Result};
use trace::debug;

use super::DDLDefinitionTask;

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let DropTenantObject {
            ref tenant_name,
            ref name,
            ref if_exist,
            ref obj_type,
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
                let success = meta.drop_custom_role(name).await?;

                if let (false, false) = (if_exist, success) {
                    return Err(QueryError::Meta {
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

                let req = AdminCommandRequest {
                    tenant: tenant_name.to_string(),
                    command: Some(DropDb(DropDbRequest { db: name.clone() })),
                };

                query_state_machine.coord.broadcast_command(req).await?;

                debug!("Drop database {} of tenant {}", name, tenant_name);
                let success = meta.drop_db(name).await?;

                if let (false, false) = (if_exist, success) {
                    return Err(QueryError::Meta {
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
