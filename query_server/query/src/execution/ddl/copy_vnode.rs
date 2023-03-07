use async_trait::async_trait;
use coordinator::VnodeManagerCmdType;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::CopyVnode;
use spi::Result;

use super::DDLDefinitionTask;

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
        let (vnode_id, node_id) = (self.stmt.vnode_id, self.stmt.node_id);
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();
        let cmd_type = VnodeManagerCmdType::Copy(vnode_id, node_id);
        coord.vnode_manager(tenant, cmd_type).await?;

        Ok(Output::Nil(()))
    }
}
