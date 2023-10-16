use async_trait::async_trait;
use meta::error::MetaError;
use models::auth::user::ROOT;
use models::oid::Identifier;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DropGlobalObject, GlobalObjectType};
use spi::{QueryError, Result};
use trace::{debug, warn};

use super::DDLDefinitionTask;

static FORBIDDEN_DROP_USERS: [&str; 1] = [ROOT];

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

                if FORBIDDEN_DROP_USERS.contains(&name.as_str()) {
                    return Err(QueryError::ForbiddenDropUser {
                        user: name.to_string(),
                    });
                }

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
                    Some(tm) => {
                        // drop role in the tenant
                        let all_roles = tm.custom_roles().await?;
                        for role in all_roles {
                            if !tm.drop_custom_role(role.name()).await? {
                                warn!("drop role {} failed.", role.name());
                            }
                        }

                        // drop database in the tenant
                        let all_dbs = tm.list_databases()?;
                        for db_name in all_dbs {
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
