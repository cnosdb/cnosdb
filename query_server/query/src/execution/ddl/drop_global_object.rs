use async_trait::async_trait;
use coordinator::resource_manager::ResourceManager;
use meta::error::MetaError;
use models::auth::user::ROOT;
use models::oid::Identifier;
use models::schema::resource_info::{ResourceInfo, ResourceOperator};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DropGlobalObject, GlobalObjectType};
use spi::{CoordinatorSnafu, MetaSnafu, QueryError, QueryResult};
use trace::debug;

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let DropGlobalObject {
            ref name,
            ref if_exist,
            ref obj_type,
            mut after,
        } = self.stmt.clone();

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

                let success = meta.drop_user(name).await.context(MetaSnafu)?;

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

                match meta.tenant(name).await {
                    Ok(Some(tenant_schema)) => {
                        // first, set hidden to TRUE
                        meta.set_tenant_is_hidden(name, true)
                            .await
                            .context(MetaSnafu)?;

                        if after.is_none() {
                            after = tenant_schema.options().get_drop_after();
                        }

                        // second, add drop task
                        let resourceinfo = ResourceInfo::new(
                            (*tenant_schema.id(), "".to_string()),
                            name.clone(),
                            ResourceOperator::DropTenant(name.clone()),
                            &after,
                            query_state_machine.coord.node_id(),
                        );
                        ResourceManager::add_resource_task(
                            query_state_machine.coord.clone(),
                            resourceinfo,
                        )
                        .await
                        .context(CoordinatorSnafu)?;
                        Ok(Output::Nil(()))
                    }
                    Ok(None) => {
                        if *if_exist {
                            Ok(Output::Nil(()))
                        } else {
                            Err(QueryError::Meta {
                                source: MetaError::TenantNotFound {
                                    tenant: name.to_string(),
                                },
                            })
                        }
                    }
                    Err(err) => match err {
                        MetaError::TenantNotFound { .. } => {
                            if *if_exist {
                                Ok(Output::Nil(()))
                            } else {
                                Err(QueryError::Meta { source: err })
                            }
                        }
                        _ => Err(QueryError::Meta { source: err }),
                    },
                }
            }
        }
    }
}
