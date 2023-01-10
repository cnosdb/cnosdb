use async_trait::async_trait;

use spi::query::{
    execution::{Output, QueryStateMachineRef},
    logical_planner::DropVnode,
};

use super::DDLDefinitionTask;
use meta::error::MetaError;

use spi::QueryError;
use spi::Result;

pub struct DropVnodeTask {
    stmt: DropVnode,
}

impl DropVnodeTask {
    #[inline(always)]
    pub fn new(stmt: DropVnode) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for DropVnodeTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let DropVnode { vnode_id: _ } = self.stmt;
        let tenant = query_state_machine.session.tenant();
        let _meta = query_state_machine
            .meta
            .tenant_manager()
            .tenant_meta(tenant)
            .ok_or_else(|| QueryError::Meta {
                source: MetaError::TenantNotFound {
                    tenant: tenant.to_string(),
                },
            })?;
        todo!()
    }
}
