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

        match client.get_table_schema(table.database(), table.table()) {
            Ok(_) => {
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
                )
                .unwrap();
                resourceinfo.status = ResourceStatus::Cancel;
                resourceinfo.comment.clear();

                query_state_machine
                    .meta
                    .write_resourceinfo(resourceinfo.names(), resourceinfo.clone())
                    .await?;
                Ok(Output::Nil(()))
            }
            Err(_) => {
                if !if_exist {
                    return Err(QueryError::Meta {
                        source: MetaError::TableNotFound {
                            table: table.table().to_string(),
                        },
                    });
                } else {
                    Ok(Output::Nil(()))
                }
            }
        }
    }
}
