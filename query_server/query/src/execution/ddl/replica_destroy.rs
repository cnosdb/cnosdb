use async_trait::async_trait;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ReplicaDestroy;
use spi::{CoordinatorSnafu, QueryResult};

use super::DDLDefinitionTask;

pub struct ReplicaDestroyTask {
    stmt: ReplicaDestroy,
}

impl ReplicaDestroyTask {
    #[inline(always)]
    pub fn new(stmt: ReplicaDestroy) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ReplicaDestroyTask {
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let replica_id = self.stmt.replica_id;
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();

        let cmd_type = coordinator::ReplicationCmdType::DestroyRaftGroup(replica_id);
        coord
            .replication_manager(tenant, cmd_type)
            .await
            .context(CoordinatorSnafu)?;

        Ok(Output::Nil(()))
    }
}
