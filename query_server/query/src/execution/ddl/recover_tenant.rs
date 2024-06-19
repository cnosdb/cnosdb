use async_trait::async_trait;
use meta::error::MetaError;
use models::oid::Identifier;
use models::schema::resource_info::{ResourceInfo, ResourceOperator, ResourceStatus};
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::RecoverTenant;
use spi::{MetaSnafu, QueryError, QueryResult};
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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let RecoverTenant {
            ref tenant_name,
            ref if_exist,
        } = self.stmt;

        debug!("Recover tenant {}", tenant_name);

        let meta = query_state_machine.meta.clone();
        let tenant_meta = meta
            .tenant_meta(tenant_name)
            .await
            .ok_or_else(|| QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                },
            })?;

        // first, cancel drop task
        let mut resourceinfo = ResourceInfo::new(
            (*tenant_meta.tenant().id(), "".to_string()),
            tenant_name.clone(),
            ResourceOperator::DropTenant(tenant_name.clone()),
            &None,
            query_state_machine.coord.node_id(),
        );
        resourceinfo.set_status(ResourceStatus::Cancel);
        resourceinfo.set_comment("");
        query_state_machine
            .meta
            .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
            .await
            .context(MetaSnafu)?;

        // second, set hidden to FALSE
        if (meta.set_tenant_is_hidden(tenant_name, false).await).is_err() && !if_exist {
            return Err(QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                },
            });
        }

        Ok(Output::Nil(()))
    }
}
