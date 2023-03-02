use async_trait::async_trait;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::MoveVnode;
use spi::Result;

use super::DDLDefinitionTask;

pub struct MoveVnodeTask {
    stmt: MoveVnode,
}

impl MoveVnodeTask {
    #[inline(always)]
    pub fn new(stmt: MoveVnode) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for MoveVnodeTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let (vnode_id, node_id) = (self.stmt.vnode_id, self.stmt.node_id);
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();
        let cmd_type = coordinator::VnodeManagerCmdType::Move(vnode_id, node_id);
        coord.vnode_manager(tenant, cmd_type).await?;

        Ok(Output::Nil(()))
    }
}
