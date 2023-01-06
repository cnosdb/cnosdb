use async_trait::async_trait;
use coordinator::command;
use datafusion::sql::TableReference;
use snafu::ResultExt;
use spi::query::{
    execution::{Output, QueryStateMachineRef},
    logical_planner::{DatabaseObjectType, DropDatabaseObject},
};
use spi::MetaSnafu;
use spi::Result;

use trace::info;

use super::DDLDefinitionTask;
use meta::error::MetaError;

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
                let tenant = query_state_machine.session.tenant();
                let client = query_state_machine
                    .meta
                    .tenant_manager()
                    .tenant_meta(tenant)
                    .ok_or(MetaError::TenantNotFound {
                        tenant: tenant.to_string(),
                    })?;
                let table = TableReference::from(object_name.as_str()).resolve(
                    query_state_machine.session.tenant(),
                    query_state_machine.session.default_database(),
                );

                let req = command::AdminStatementRequest {
                    tenant: tenant.to_string(),
                    stmt: command::AdminStatementType::DropTable(
                        table.schema.to_string(),
                        table.table.to_string(),
                    ),
                };

                query_state_machine
                    .coord
                    .exec_admin_stat_on_all_node(req)
                    .await?;

                client.drop_table(table.schema, table.table)
            }
        };

        if *if_exist {
            return Ok(Output::Nil(()));
        }

        res.map(|_| Output::Nil(())).context(MetaSnafu)
    }
}
