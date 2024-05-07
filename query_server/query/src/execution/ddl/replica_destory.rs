use async_trait::async_trait;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ReplicaDestory;
use spi::Result;

use super::DDLDefinitionTask;

pub struct ReplicaDestoryTask {
    stmt: ReplicaDestory,
}

impl ReplicaDestoryTask {
    #[inline(always)]
    pub fn new(stmt: ReplicaDestory) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ReplicaDestoryTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> Result<Output> {
        let replica_id = self.stmt.replica_id;
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();

        let cmd_type = coordinator::ReplicationCmdType::DestoryRaftGroup(replica_id);
        coord.replication_manager(tenant, cmd_type).await?;

        Ok(Output::Nil(()))
    }
}
