use async_trait::async_trait;
use coordinator::ReplicationCmdType;
use snafu::ResultExt;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::DropVnode;
use spi::{CoordinatorSnafu, QueryResult};

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
    async fn execute(&self, query_state_machine: QueryStateMachineRef) -> QueryResult<Output> {
        let vnode_id = self.stmt.vnode_id;
        let tenant = query_state_machine.session.tenant();

        let coord = query_state_machine.coord.clone();

        let cmd_type = ReplicationCmdType::RemoveRaftNode(vnode_id);
        coord
            .replication_manager(tenant, cmd_type)
            .await
            .context(CoordinatorSnafu)?;

        Ok(Output::Nil(()))
    }
}
