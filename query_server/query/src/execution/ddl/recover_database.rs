use async_trait::async_trait;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::{ResourceInfo, ResourceOperator, ResourceStatus, ResourceType};
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::RecoverDatabase;
use spi::{QueryError, Result};

use super::DDLDefinitionTask;

pub struct RecoverDatabaseTask {
    stmt: RecoverDatabase,
}

impl RecoverDatabaseTask {
    #[inline(always)]
    pub fn new(stmt: RecoverDatabase) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for RecoverDatabaseTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let RecoverDatabase {
            ref tenant_name,
            ref db_name,
            ref if_exist,
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

        match meta.get_db_info(db_name) {
            Ok(_) => {
                let mut resourceinfo = ResourceInfo::new(
                    *meta.tenant().id(),
                    vec![tenant_name.clone(), db_name.clone()],
                    ResourceType::Database,
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
                        source: MetaError::DatabaseNotFound {
                            database: db_name.to_string(),
                        },
                    });
                } else {
                    Ok(Output::Nil(()))
                }
            }
        }
    }
}
