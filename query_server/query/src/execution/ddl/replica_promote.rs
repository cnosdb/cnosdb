use async_trait::async_trait;
use spi::query::execution::{Output, QueryStateMachineRef};
use spi::query::logical_planner::ReplicaPromote;
use spi::Result;

use super::DDLDefinitionTask;

pub struct ReplicaPromoteTask {
    stmt: ReplicaPromote,
}

impl ReplicaPromoteTask {
    #[inline(always)]
    pub fn new(stmt: ReplicaPromote) -> Self {
        Self { stmt }
    }
}

#[async_trait]
impl DDLDefinitionTask for ReplicaPromoteTask {
    async fn execute(&self, _query_state_machine: QueryStateMachineRef) -> Result<Output> {
        println!("-----------promote: {:?}", self.stmt);
        todo!()
    }
}
