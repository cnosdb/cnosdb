use async_trait::async_trait;
use coordinator::NodeManagerCmdType;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ChangeNodeState;
use spi::Result;

use super::DDLDefinitionTask;

pub struct ChangeNodeStateTask {
    stmt: ChangeNodeState,
}

impl ChangeNodeStateTask {
    #[inline(always)]
    pub fn new(stmt: ChangeNodeState) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ChangeNodeStateTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let (node_id, node_state) = (self.stmt.node_id, self.stmt.node_state.clone());
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();
        let cmd_type = NodeManagerCmdType::ChangeNodeState(node_id, node_state);
        coord.execute_node_command(tenant, cmd_type).await?;

        Ok(Output::Nil(()))
    }
}
