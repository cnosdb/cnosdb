use async_trait::async_trait;
use coordinator::VnodeManagerCmdType;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CompactVnode;
use spi::Result;

use super::DDLDefinitionTask;

pub struct CompactVnodeTask {
    stmt: CompactVnode,
}

impl CompactVnodeTask {
    #[inline(always)]
    pub fn new(stmt: CompactVnode) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for CompactVnodeTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let vnode_ids = self.stmt.vnode_ids.clone();
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();
        let cmd_type = VnodeManagerCmdType::Compact(vnode_ids);
        coord.vnode_manager(tenant, cmd_type).await?;

        Ok(Output::Nil(()))
    }
}
