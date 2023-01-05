use async_trait::async_trait;

use spi::query::{
    execution::{Output, QueryStateMachineRef},
    logical_planner::CopyVnode,
};

use super::DDLDefinitionTask;
use meta::error::MetaError;

use spi::QueryError;
use spi::Result;

pub struct CopyVnodeTask {
    stmt: CopyVnode,
}

impl CopyVnodeTask {
    #[inline(always)]
    pub fn new(stmt: CopyVnode) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CopyVnodeTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let CopyVnode {
            vnode_id: _,
            node_id: _,
        } = self.stmt;
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
