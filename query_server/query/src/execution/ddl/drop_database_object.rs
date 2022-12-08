use async_trait::async_trait;
use datafusion::sql::TableReference;
use spi::query::{
    execution::{MetadataSnafu, Output, QueryStateMachineRef},
    logical_planner::{DatabaseObjectType, DropDatabaseObject},
};


use spi::query::execution::ExecutionError;
use trace::debug;

use super::DDLDefinitionTask;

use meta::meta_client::MetaError;
use snafu::ResultExt;

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
    async fn execute(
        &self,
        query_state_machine: QueryStateMachineRef,
    ) -> Result<Output, ExecutionError> {
        let DropDatabaseObject {
            tenant_id: _,
            ref object_name,
            ref if_exist,
            ref obj_type,
        } = self.stmt;

        let res = match obj_type {
            DatabaseObjectType::Table => {
                // TODO 删除指定租户下的表
                debug!("Drop table {}", object_name);
                let tenant = query_state_machine.session.tenant();
                let client = query_state_machine
                    .meta
                    .tenant_manager()
                    .tenant_meta(tenant)
                    .ok_or(MetaError::TenantNotFound {
                        tenant: tenant.to_string(),
                    })
                    .context(MetadataSnafu)?;
                let table = TableReference::from(object_name.as_str()).resolve(
                    query_state_machine.session.tenant(),
                    query_state_machine.session.default_database(),
                );
                client
                    .drop_table(table.schema, table.table)
            }
        };

        if *if_exist {
            return Ok(Output::Nil(()));
        }

        res.map(|_| Output::Nil(()))
            .context(MetadataSnafu)
    }
}
