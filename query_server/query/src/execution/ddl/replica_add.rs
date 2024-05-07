use async_trait::async_trait;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ReplicaAdd;
use spi::Result;

use super::DDLDefinitionTask;

pub struct ReplicaAddTask {
    stmt: ReplicaAdd,
}

impl ReplicaAddTask {
    #[inline(always)]
    pub fn new(stmt: ReplicaAdd) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ReplicaAddTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let (replica_id, node_id) = (self.stmt.replica_id, self.stmt.node_id);
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();

        let cmd_type = coordinator::ReplicationCmdType::AddRaftFollower(replica_id, node_id);
        coord.replication_manager(tenant, cmd_type).await?;

        Ok(Output::Nil(()))
    }
}
