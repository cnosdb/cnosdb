use async_trait::async_trait;
use coordinator::resource_manager::ResourceManager;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::resource_info::{ResourceInfo, ResourceOperator};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DatabaseObjectType, DropDatabaseObject};
use spi::{CoordinatorSnafu, MetaSnafu, QueryError, QueryResult};
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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
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
                let client = query_state_machine
                    .meta
                    .tenant_meta(tenant)
                    .await
                    .ok_or_else(|| MetaError::TenantNotFound {
                        tenant: tenant.to_string(),
                    })
                    .context(MetaSnafu)?;

                if client
                    .get_table_schema(object_name.database(), object_name.table())
                    .is_ok_and(|opt| opt.is_none())
                {
                    if *if_exist {
                        return Ok(Output::Nil(()));
                    } else {
                        return Err(QueryError::Meta {
                            source: MetaError::TableNotFound {
                                table: object_name.table().to_string(),
                            },
                        });
                    }
                }

                let resourceinfo = ResourceInfo::new(
                    (*client.tenant().id(), object_name.database().to_string()),
                    object_name.tenant().to_string()
                        + "-"
                        + object_name.database()
                        + "-"
                        + object_name.table(),
                    ResourceOperator::DropTable(
                        object_name.tenant().to_string(),
                        object_name.database().to_string(),
                        object_name.table().to_string(),
                    ),
                    &None,
                    query_state_machine.coord.node_id(),
                );
                ResourceManager::add_resource_task(query_state_machine.coord.clone(), resourceinfo)
                    .await
                    .context(CoordinatorSnafu)?;
            }
        };

        Ok(Output::Nil(()))
    }
}
