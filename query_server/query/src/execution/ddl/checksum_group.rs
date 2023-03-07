use async_trait::async_trait;
use meta::error::MetaError;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ChecksumGroup;
use spi::{QueryError, Result};

use super::DDLDefinitionTask;

pub struct ChecksumGroupTask {
    stmt: ChecksumGroup,
}

impl ChecksumGroupTask {
    #[inline(always)]
    pub fn new(stmt: ChecksumGroup) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ChecksumGroupTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let ChecksumGroup {
            replication_set_id: _,
        } = self.stmt;
        let tenant = query_state_machine.session.tenant();
        let _meta = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .await
            .ok_or_else(|| QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant.to_string(),
                },
            })?;
        todo!()
    }
}
