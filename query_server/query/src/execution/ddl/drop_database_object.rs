use async_trait::async_trait;
use coordinator::command;
use meta::error::MetaError;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::{DatabaseObjectType, DropDatabaseObject};
use spi::{MetaSnafu, Result};
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

        let res = match obj_type {
            DatabaseObjectType::Table => {
                // TODO 删除指定租户下的表
                info!("Drop table {}", object_name);
                let tenant = object_name.tenant();
                let client = query_state_machine
                    .meta
                    .tenant_manager()
                    .tenant_meta(tenant)
                    .await
                    .ok_or(MetaError::TenantNotFound {
                        tenant: tenant.to_string(),
                    })?;

                let req = command::AdminStatementRequest {
                    tenant: tenant.to_string(),
                    stmt: command::AdminStatementType::DropTable {
                        db: object_name.database().to_string(),
                        table: object_name.table().to_string(),
                    },
                };

                query_state_machine
                    .coord
                    .exec_admin_stat_on_all_node(req)
                    .await?;

                client
                    .drop_table(object_name.database(), object_name.table())
                    .await
            }
        };

        if *if_exist {
            return Ok(Output::Nil(()));
        }

        res.map(|_| Output::Nil(())).context(MetaSnafu)
    }
}
