use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use snafu::Snafu;

use crate::catalog::{MetaData, MetaDataRef};
use crate::service::protocol::QueryId;
use crate::{catalog::MetadataError, service::protocol::Query};

use super::dispatcher::{QueryInfo, QueryStatus};
use super::{logical_planner::Plan, session::IsiphoSessionCtx, Result};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ExecutionError {
    #[snafu(display("External err: {}", source))]
    External { source: DataFusionError },

    #[snafu(display("Arrow err: {}", source))]
    Arrow { source: ArrowError },

    #[snafu(display("Metadata operator err: {}", source))]
    Metadata { source: MetadataError },

    #[snafu(display("Query not found: {:?}", query_id))]
    QueryNotFound { query_id: QueryId },
}

#[async_trait]
pub trait QueryExecution: Send + Sync {
    // 开始
    async fn start(&self) -> Result<Output>;
    // 停止
    fn cancel(&self) -> Result<()>;
    // query状态
    // 查询计划
    // 静态信息
    fn info(&self) -> QueryInfo;
    // 运行时信息
    fn status(&self) -> QueryStatus;
    // sql
    // 资源占用（cpu时间/内存/吞吐量等）
    // ......
}
// pub trait Output {
//     fn as_any(&self) -> &dyn Any;
// }
#[derive(Clone)]
pub enum Output {
    StreamData(SchemaRef, Vec<RecordBatch>),
    Nil(()),
}

impl Output {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::StreamData(schema, _) => schema.clone(),
            Self::Nil(_) => Arc::new(Schema::empty()),
        }
    }

    pub fn chunk_result(&self) -> &[RecordBatch] {
        match self {
            Self::StreamData(_, result) => result,
            Self::Nil(_) => &[],
        }
    }

    pub fn num_rows(&self) -> usize {
        self.chunk_result()
            .iter()
            .map(|e| e.num_rows())
            .reduce(|p, c| p + c)
            .unwrap_or(0)
    }
}

pub trait QueryExecutionFactory {
    fn create_query_execution(
        &self,
        plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> Arc<dyn QueryExecution>;
}

pub type QueryStateMachineRef = Arc<QueryStateMachine>;

pub struct QueryStateMachine {
    pub session: IsiphoSessionCtx,
    pub query_id: QueryId,
    pub query: Query,
    pub catalog: MetaDataRef,

    state: AtomicPtr<QueryState>,
    start: Instant,
}

impl QueryStateMachine {
    pub fn begin(
        query_id: QueryId,
        query: Query,
        session: IsiphoSessionCtx,
        catalog: Arc<dyn MetaData>,
    ) -> Self {
        Self {
            query_id,
            session,
            query,
            catalog,
            state: AtomicPtr::new(Box::into_raw(Box::new(QueryState::ACCEPTING))),
            start: Instant::now(),
        }
    }

    pub fn begin_analyze(&self) {
        // TODO record time
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::ANALYZING)));
    }

    pub fn end_analyze(&self) {
        // TODO record time
    }

    pub fn begin_optimize(&self) {
        // TODO record time
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::OPTMIZING)));
    }

    pub fn end_optimize(&self) {
        // TODO
    }

    pub fn begin_schedule(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::RUNNING(RUNNING::SCHEDULING)));
    }

    pub fn end_schedule(&self) {
        // TODO
    }

    pub fn finish(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::DONE(DONE::FINISHED)));
    }

    pub fn cancel(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::DONE(DONE::CANCELLED)));
    }

    pub fn fail(&self) {
        // TODO
        self.translate_to(Box::new(QueryState::DONE(DONE::FAILED)));
    }

    pub fn state(&self) -> &QueryState {
        unsafe { &*self.state.load(Ordering::Relaxed) }
    }

    pub fn duration(&self) -> Duration {
        self.start.elapsed()
    }

    fn translate_to(&self, state: Box<QueryState>) {
        self.state.store(Box::into_raw(state), Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
pub enum QueryState {
    ACCEPTING,
    RUNNING(RUNNING),
    DONE(DONE),
}

impl ToString for QueryState {
    fn to_string(&self) -> String {
        match self {
            QueryState::ACCEPTING => format!("{:?}", self),
            QueryState::RUNNING(e) => e.to_string(),
            QueryState::DONE(e) => e.to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RUNNING {
    DISPATCHING,
    ANALYZING,
    OPTMIZING,
    SCHEDULING,
}

impl ToString for RUNNING {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}

#[derive(Debug, Clone)]
pub enum DONE {
    FINISHED,
    FAILED,
    CANCELLED,
}

impl ToString for DONE {
    fn to_string(&self) -> String {
        format!("{:?}", self)
    }
}
