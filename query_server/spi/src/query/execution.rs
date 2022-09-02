use std::sync::Arc;

use async_trait::async_trait;
use datafusion::physical_plan::SendableRecordBatchStream;

use crate::service::protocol::Query;

use super::{logical_planner::Plan, session::IsiphoSessionCtx, Result};

#[async_trait]
pub trait QueryExecution: Send + Sync {
    // 开始
    async fn start(&self) -> Result<Output>;
    // 停止
    // query状态
    // 查询计划
    // 静态信息
    // 运行时信息
    // sql
    // 资源占用（cpu时间/内存/吞吐量等）
    // ......
}
// pub trait Output {
//     fn as_any(&self) -> &dyn Any;
// }
pub enum Output {
    StreamData(SendableRecordBatchStream),
    Nil(()),
}

pub trait QueryExecutionFactory {
    fn create_query_execution(
        &self,
        plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> Box<dyn QueryExecution>;
}

pub type QueryStateMachineRef = Arc<QueryStateMachine>;

pub struct QueryStateMachine {
    pub session: IsiphoSessionCtx,
    pub query: Query,
}

impl QueryStateMachine {
    pub fn begin(query: Query, session: IsiphoSessionCtx) -> Self {
        Self { session, query }
    }

    pub fn begin_analyze(&self) {
        // TODO
    }

    pub fn end_analyze(&self) {
        // TODO
    }

    pub fn begin_optimize(&self) {
        // TODO
    }

    pub fn end_optimize(&self) {
        // TODO
    }

    pub fn begin_schedule(&self) {
        // TODO
    }

    pub fn end_schedule(&self) {
        // TODO
    }
}
