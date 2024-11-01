use async_trait::async_trait;
use meta::error::MetaError;
use protos::kv_service::admin_command_request::Command::DropDb;
use protos::kv_service::{AdminCommandRequest, DropDbRequest};
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DropGlobalObject, GlobalObjectType};
use spi::{QueryError, Result};
use trace::{debug, warn};

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

                query_state_machine.remove_user_from_cache(name);

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
                    Some(tm) => {
                        // drop database in the tenant
                        let all_dbs = tm.list_databases()?;
                        for db_name in all_dbs {
                            let req = AdminCommandRequest {
                                tenant: name.to_string(),
                                command: Some(DropDb(DropDbRequest {
                                    db: db_name.clone(),
                                })),
                            };

                            query_state_machine.coord.broadcast_command(req).await?;

                            debug!("Drop database {} of tenant {}", db_name, name);
                            if !tm.drop_db(&db_name).await? {
                                warn!("drop database {} failed.", db_name);
                            }
                        }

                        // drop tenant metadata
                        meta.drop_tenant(name).await?;
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
