use async_trait::async_trait;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::{ResourceInfo, ResourceOperator, ResourceStatus, ResourceType};
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::RecoverTenant;
use spi::{QueryError, Result};
use trace::debug;

use super::DDLDefinitionTask;

pub struct RecoverTenantTask {
    stmt: RecoverTenant,
}

impl RecoverTenantTask {
    pub fn new(stmt: RecoverTenant) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for RecoverTenantTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let RecoverTenant {
            ref tenant_name,
            ref if_exist,
        } = self.stmt;

        let meta = query_state_machine.meta.clone();

        debug!("Recover tenant {}", tenant_name);

        match meta.tenant_meta(tenant_name).await {
            Some(tenant) => {
                let mut resourceinfo = ResourceInfo::new(
                    *tenant.tenant().id(),
                    vec![tenant_name.clone()],
                    ResourceType::Tenant,
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
            None => {
                if !if_exist {
                    return Err(QueryError::Meta {
                        source: MetaError::TenantNotFound {
                            tenant: tenant_name.to_string(),
                        },
                    });
                } else {
                    Ok(Output::Nil(()))
                }
            }
        }
    }
}
