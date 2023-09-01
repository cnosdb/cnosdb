use async_trait::async_trait;
use coordinator::resource_manager::ResourceManager;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::{ResourceInfo, ResourceOperator, ResourceType};
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DropGlobalObject, GlobalObjectType};
use spi::{QueryError, Result};
use trace::debug;

use super::DDLDefinitionTask;

pub struct DropGlobalObjectTask {
    stmt: DropGlobalObject,
}

impl DropGlobalObjectTask {
    pub fn new(stmt: DropGlobalObject) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropGlobalObjectTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let DropGlobalObject {
            ref name,
            ref if_exist,
            ref obj_type,
            ref after,
        } = self.stmt;

        let meta = query_state_machine.meta.clone();

        match obj_type {
            GlobalObjectType::User => {
                // 删除用户
                // fn drop_user(
                //     &mut self,
                //     name: &str
                // ) -> Result<bool>;
                debug!("Drop user {}", name);
                let success = meta.drop_user(name).await?;

                if let (false, false) = (if_exist, success) {
                    return Err(QueryError::Meta {
                        source: MetaError::UserNotFound {
                            user: name.to_string(),
                        },
                    });
                }

                Ok(Output::Nil(()))
            }
            GlobalObjectType::Tenant => {
                // 删除租户
                // fn drop_tenant(
                //     &self,
                //     name: &str
                // ) -> Result<bool>;
                debug!("Drop tenant {}", name);

                match meta.tenant_meta(name).await {
                    Some(tenant) => {
                        let resourceinfo = ResourceInfo::new(
                            *tenant.tenant().id(),
                            vec![name.clone()],
                            ResourceType::Tenant,
                            ResourceOperator::Drop,
                            after,
                        )
                        .unwrap();
                        ResourceManager::add_resource_task(
                            query_state_machine.coord.clone(),
                            resourceinfo,
                        )
                        .await?;
                        Ok(Output::Nil(()))
                    }
                    None => {
                        if !if_exist {
                            return Err(QueryError::Meta {
                                source: MetaError::TenantNotFound {
                                    tenant: name.to_string(),
                                },
                            });
                        } else {
                            Ok(Output::Nil(()))
                        }
                    }
                }
            }
        }
    }
}
