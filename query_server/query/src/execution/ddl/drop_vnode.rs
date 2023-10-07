use async_trait::async_trait;
use coordinator::VnodeManagerCmdType;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::DropVnode;
use spi::Result;

use super::DDLDefinitionTask;

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
        let vnode_id = self.stmt.vnode_id;
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();
        if coord.using_raft_replication() {
            let cmd_type = VnodeManagerCmdType::RemoveRaftNode(vnode_id);
            coord.vnode_manager(tenant, cmd_type).await?;
        } else {
            let cmd_type = VnodeManagerCmdType::Drop(vnode_id);
            coord.vnode_manager(tenant, cmd_type).await?;
        }

        Ok(Output::Nil(()))
    }
}
