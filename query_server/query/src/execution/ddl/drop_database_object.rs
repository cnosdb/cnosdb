use async_trait::async_trait;
use coordinator::errors::CoordinatorError;
use coordinator::resource_manager::ResourceManager;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::{ResourceInfo, ResourceOperator, ResourceType};
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DatabaseObjectType, DropDatabaseObject};
use spi::{QueryError, Result};
use trace::info;

use super::DDLDefinitionTask;

pub struct DropDatabaseObjectTask {
    stmt: DropDatabaseObject,
}

impl DropDatabaseObjectTask {
    #[inline(always)]
    pub fn new(stmt: DropDatabaseObject) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropDatabaseObjectTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let DropDatabaseObject {
            ref object_name,
            ref if_exist,
            ref obj_type,
        } = self.stmt;

        match obj_type {
            DatabaseObjectType::Table => {
                // TODO 删除指定租户下的表
                info!("Drop table {}", object_name);
                let tenant = object_name.tenant();
                let client = query_state_machine.meta.tenant_meta(tenant).await.ok_or(
                    MetaError::TenantNotFound {
                        tenant: tenant.to_string(),
                    },
                )?;

                let resourceinfo = ResourceInfo::new(
                    *client.tenant().id(),
                    vec![
                        object_name.tenant().to_string(),
                        object_name.database().to_string(),
                        object_name.table().to_string(),
                    ],
                    ResourceType::Table,
                    ResourceOperator::Drop,
                    &None,
                );
                let res = ResourceManager::add_resource_task(
                    query_state_machine.coord.clone(),
                    resourceinfo,
                )
                .await;

                if let Err(err) = res {
                    if let CoordinatorError::Meta {
                        source: MetaError::TableNotFound { .. },
                    } = &err
                    {
                        if *if_exist {
                            return Ok(Output::Nil(()));
                        }
                    }
                    return Err(QueryError::Coordinator { source: err });
                }
            }
        };

        Ok(Output::Nil(()))
    }
}
