use async_trait::async_trait;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::{ResourceInfo, ResourceOperator, ResourceStatus, ResourceType};
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::RecoverTable;
use spi::{QueryError, Result};
use trace::info;

use super::DDLDefinitionTask;

pub struct RecoverTableTask {
    stmt: RecoverTable,
}

impl RecoverTableTask {
    #[inline(always)]
    pub fn new(stmt: RecoverTable) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for RecoverTableTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let RecoverTable {
            ref table,
            ref if_exist,
        } = self.stmt;

        // TODO 删除指定租户下的表
        info!("recover table {}", table);
        let tenant = table.tenant();
        let client = query_state_machine.meta.tenant_meta(tenant).await.ok_or(
            MetaError::TenantNotFound {
                tenant: tenant.to_string(),
            },
        )?;

        // first, cancel drop task
        let mut resourceinfo = ResourceInfo::new(
            *client.tenant().id(),
            vec![
                table.tenant().to_string(),
                table.database().to_string(),
                table.table().to_string(),
            ],
            ResourceType::Table,
            ResourceOperator::Drop,
            &None,
        );
        resourceinfo.set_status(ResourceStatus::Cancel);
        resourceinfo.set_comment("");

        query_state_machine
            .meta
            .write_resourceinfo(resourceinfo.get_names(), resourceinfo.clone())
            .await?;

        // second, set hidden to FALSE
        if (client
            .set_table_is_hidden(tenant, table.database(), table.table(), false)
            .await)
            .is_err()
            && !if_exist
        {
            return Err(QueryError::Meta {
                source: MetaError::TableNotFound {
                    table: table.table().to_string(),
                },
            });
        }

        Ok(Output::Nil(()))
    }
}
