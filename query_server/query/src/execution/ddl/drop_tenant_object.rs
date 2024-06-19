use async_trait::async_trait;
use coordinator::resource_manager::ResourceManager;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::resource_info::{ResourceInfo, ResourceOperator};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DropTenantObject, TenantObjectType};
use spi::{CoordinatorSnafu, MetaSnafu, QueryError, QueryResult};
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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let DropTenantObject {
            ref tenant_name,
            ref name,
            ref if_exist,
            ref obj_type,
            ref after,
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
                let success = meta.drop_custom_role(name).await.context(MetaSnafu)?;

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

                if meta.get_db_schema(name).is_ok_and(|opt| opt.is_none()) {
                    if *if_exist {
                        return Ok(Output::Nil(()));
                    } else {
                        return Err(QueryError::Meta {
                            source: MetaError::DatabaseNotFound {
                                database: name.to_string(),
                            },
                        });
                    }
                }

                // first, set hidden to TRUE
                meta.set_db_is_hidden(tenant_name, name, true)
                    .await
                    .map_err(|err| QueryError::Meta { source: err })?;

                // second, add drop task
                let resourceinfo = ResourceInfo::new(
                    (*meta.tenant().id(), name.to_string()),
                    tenant_name.clone() + "-" + name,
                    ResourceOperator::DropDatabase(tenant_name.clone(), name.clone()),
                    after,
                    query_state_machine.coord.node_id(),
                );
                ResourceManager::add_resource_task(query_state_machine.coord.clone(), resourceinfo)
                    .await
                    .context(CoordinatorSnafu)?;

                Ok(Output::Nil(()))
            }
        }
    }
}
