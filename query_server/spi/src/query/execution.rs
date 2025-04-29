use std::fmt::Display;
use std::pin::Pin;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use config::tskv::Config;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt, TryStreamExt};
use meta::model::MetaRef;
use models::arrow::Field;
use models::auth::auth_cache::{AuthCache, AuthCacheKey};
use models::auth::user::User;
use models::oid::{Identifier, Oid};
use models::schema::query_info::{QueryId, QueryInfo};
use trace::{debug, warn, SpanContext};

use super::dispatcher::QueryStatus;
use super::logical_planner::Plan;
use super::session::SessionCtx;
use crate::query::DataType;
use crate::service::protocol::Query;
use crate::{DataFusionError, QueryError, QueryResult};

pub type QueryExecutionRef = Arc<dyn QueryExecution>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryType {
    Batch,
    Stream,
}

impl Display for QueryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Batch => write!(f, "batch"),
            Self::Stream => write!(f, "stream"),
        }
    }
}

#[async_trait]
pub trait QueryExecution: Send + Sync {
    fn query_type(&self) -> QueryType {
        QueryType::Batch
    }
    // 开始
    async fn start(&self) -> QueryResult<Output>;
    // 停止
    fn cancel(&self) -> QueryResult<()>;
    // query状态
    // 查询计划
    // 静态信息
    fn info(&self) -> QueryInfo;
    // 运行时信息
    fn status(&self) -> QueryStatus;
    // sql
    // 资源占用（cpu时间/内存/吞吐量等）
    // 是否需要持久化query信息
    fn need_persist(&self) -> bool {
        false
    }
}

pub enum Output {
    StreamData(SendableRecordBatchStream),
    Nil(()),
}

impl Output {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::StreamData(stream) => stream.schema(),
            Self::Nil(_) => Arc::new(Schema::empty()),
        }
    }

    pub async fn chunk_result(self) -> QueryResult<Vec<RecordBatch>> {
        match self {
            Self::Nil(_) => Ok(vec![]),
            Self::StreamData(stream) => {
                let schema = stream.schema();
                let mut res: Vec<RecordBatch> = stream.try_collect::<Vec<RecordBatch>>().await?;
                if res.is_empty() {
                    res.push(RecordBatch::new_empty(schema));
                }
                Ok(res)
            }
        }
    }

    pub async fn num_rows(self) -> usize {
        match self.chunk_result().await {
            Ok(rb) => rb.iter().map(|e| e.num_rows()).sum(),
            Err(_) => 0,
        }
    }

    /// Returns the number of records affected by the query operation
    ///
    /// If it is a select statement, returns the number of rows in the result set
    ///
    /// -1 means unknown
    ///
    /// panic! when StreamData's number of records greater than i64::Max
    pub async fn affected_rows(self) -> i64 {
        self.num_rows().await as i64
    }
}

impl Stream for Output {
    type Item = std::result::Result<RecordBatch, QueryError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this {
            Output::StreamData(stream) => stream.poll_next_unpin(cx).map_err(|e| e.into()),
            Output::Nil(_) => Poll::Ready(None),
        }
    }
}

// pub struct FlightDataEncoderWrapper {
//     inner: FlightDataEncoder,
//     done: bool,
// }

// impl FlightDataEncoderWrapper {
//     fn new(inner: FlightDataEncoder) -> Self {
//         Self { inner, done: false }
//     }
// }

// pub struct FlightDataEncoderBuilderWrapper {
//     inner: FlightDataEncoderBuilder,
// }

// impl FlightDataEncoderBuilderWrapper {
//     pub fn new(schema: SchemaRef) -> Self {
//         Self {
//             inner: FlightDataEncoderBuilder::new().with_schema(Arc::clone(&schema)),
//         }
//     }

//     pub fn build<S>(self, input: S) -> FlightDataEncoderWrapper
//         where
//             S: Stream<Item=datafusion::common::Result<RecordBatch>> + Send + 'static,
//     {
//         FlightDataEncoderWrapper::new(
//             self.inner
//                 .build(input.map_err(|e| FlightError::ExternalError(e.into()))),
//         )
//     }
// }

// impl Stream for FlightDataEncoderWrapper {
//     type Item = arrow_flight::error::Result<FlightData>;

//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         if self.done {
//             return Poll::Ready(None);
//         }

//         let res = ready!(self.inner.poll_next_unpin(cx));
//         match res {
//             None => {
//                 self.done = true;
//                 Poll::Ready(None)
//             }
//             Some(Ok(data)) => Poll::Ready(Some(Ok(data))),
//             Some(Err(e)) => {
//                 self.done = true;
//                 Poll::Ready(Some(Err(e)))
//             }
//         }
//     }
// }

#[async_trait]
pub trait QueryExecutionFactory {
    async fn create_query_execution(
        &self,
        plan: Plan,
        query_state_machine: QueryStateMachineRef,
    ) -> QueryResult<QueryExecutionRef>;
}

pub type QueryStateMachineRef = Arc<QueryStateMachine>;

pub struct QueryStateMachine {
    pub session: SessionCtx,
    pub query_id: QueryId,
    pub query: Query,
    pub meta: MetaRef,
    pub coord: CoordinatorRef,
    pub auth_cache: Arc<AuthCache<AuthCacheKey, User>>,

    state: AtomicPtr<QueryState>,
    start: Instant,
}

impl QueryStateMachine {
    /// only for test
    pub fn test(query: Query, span_context: Option<SpanContext>) -> Self {
        use coordinator::service_mock::MockCoordinator;
        use datafusion::execution::memory_pool::UnboundedMemoryPool;

        use super::session::SessionCtxFactory;

        let factory = SessionCtxFactory::new(None, "/tmp".into(), None);
        let ctx = query.context().clone();
        QueryStateMachine::begin(
            QueryId::next_id(),
            query,
            factory
                .create_session_ctx(
                    "session_id",
                    &ctx,
                    0,
                    Arc::new(UnboundedMemoryPool::default()),
                    span_context,
                    Arc::new(MockCoordinator {}),
                )
                .expect("create test session ctx"),
            Arc::new(MockCoordinator {}),
            Arc::new(AuthCache::new(1024, Some(Duration::from_secs(60)))),
        )
    }

    pub fn begin(
        query_id: QueryId,
        query: Query,
        session: SessionCtx,
        coord: CoordinatorRef,
        auth_cache: Arc<AuthCache<AuthCacheKey, User>>,
    ) -> Self {
        let meta = coord.meta_manager();

        Self {
            query_id,
            session,
            query,
            meta,
            coord,
            auth_cache,
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

    pub fn with_span_ctx(&self, span_ctx: Option<SpanContext>) -> Self {
        let state = AtomicPtr::new(Box::into_raw(Box::new(self.state().clone())));
        Self {
            session: self.session.with_span_ctx(span_ctx),
            query_id: self.query_id,
            query: self.query.clone(),
            meta: self.meta.clone(),
            coord: self.coord.clone(),
            auth_cache: self.auth_cache.clone(),
            state,
            start: self.start,
        }
    }

    pub fn remove_user_from_cache_by_user_name(&self, username: &str) {
        let auths: Vec<AuthCacheKey> = self
            .auth_cache
            .iter()
            .filter(|(_, user)| user.desc().name() == username)
            .map(|(auth, _)| auth)
            .collect();

        for auth in auths {
            match self.auth_cache.remove(&auth) {
                true => debug!("Successfully removed auth cache for user {}", username),
                false => warn!("Failed to remove auth cache for user {}", username),
            }
        }
    }

    pub fn remove_user_from_cache_by_user_id(&self, user_id: &Oid) {
        let auths: Vec<AuthCacheKey> = self
            .auth_cache
            .iter()
            .filter(|(_, user)| user.desc().id() == user_id)
            .map(|(auth, _)| auth)
            .collect();

        for auth in auths {
            match self.auth_cache.remove(&auth) {
                true => debug!("Successfully removed auth cache for user {}", user_id),
                false => warn!("Failed to remove auth cache for user {}", user_id),
            }
        }
    }

    pub fn clear_auth_cache(&self) {
        self.auth_cache.clear();
    }

    pub fn config(&self) -> Config {
        self.coord.get_config()
    }
}

#[derive(Debug, Clone)]
pub enum QueryState {
    ACCEPTING,
    RUNNING(RUNNING),
    DONE(DONE),
}

impl AsRef<str> for QueryState {
    fn as_ref(&self) -> &str {
        match self {
            QueryState::ACCEPTING => "ACCEPTING",
            QueryState::RUNNING(e) => e.as_ref(),
            QueryState::DONE(e) => e.as_ref(),
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

impl AsRef<str> for RUNNING {
    fn as_ref(&self) -> &str {
        match self {
            Self::DISPATCHING => "DISPATCHING",
            Self::ANALYZING => "ANALYZING",
            Self::OPTMIZING => "OPTMIZING",
            Self::SCHEDULING => "SCHEDULING",
        }
    }
}

#[derive(Debug, Clone)]
pub enum DONE {
    FINISHED,
    FAILED,
    CANCELLED,
}

impl AsRef<str> for DONE {
    fn as_ref(&self) -> &str {
        match self {
            Self::FINISHED => "FINISHED",
            Self::FAILED => "FAILED",
            Self::CANCELLED => "CANCELLED",
        }
    }
}

pub struct SingleMessageStream {
    schema: SchemaRef,
    message: String,
    sent: bool,
}

impl SingleMessageStream {
    pub fn new(message: String) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("tips", DataType::Utf8, false)]));
        Self {
            schema,
            message,
            sent: false,
        }
    }
}

impl Stream for SingleMessageStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.sent {
            Poll::Ready(None)
        } else {
            let batch = RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(StringArray::from(vec![self.message.clone()])) as ArrayRef],
            )
            .map_err(|e| {
                DataFusionError::ArrowError(
                    e,
                    Some("pull next RecordBatch from SingleMessageStream".to_string()),
                )
            })?;
            self.sent = true;
            Poll::Ready(Some(Ok(batch)))
        }
    }
}

impl RecordBatchStream for SingleMessageStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub fn create_single_message_stream(message: String) -> SendableRecordBatchStream {
    Box::pin(SingleMessageStream::new(message))
}
