use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use memory_pool::MemoryPoolRef;
use meta::error::MetaError;
use models::oid::Oid;
use models::schema::DEFAULT_CATALOG;
use spi::query::ast::ExtStatement;
use spi::query::datasource::stream::checker::StreamCheckerManagerRef;
use spi::query::datasource::stream::StreamProviderManagerRef;
use spi::query::dispatcher::{QueryDispatcher, QueryInfo, QueryStatus};
use spi::query::execution::{Output, QueryExecutionFactory, QueryStateMachine};
use spi::query::function::FuncMetaManagerRef;
use spi::query::logical_planner::LogicalPlanner;
use spi::query::optimizer::Optimizer;
use spi::query::parser::Parser;
use spi::query::scheduler::SchedulerRef;
use spi::query::session::SessionCtxFactory;
use spi::service::protocol::{Query, QueryId};
use spi::{QueryError, Result};

use super::query_tracker::QueryTracker;
use crate::data_source::split::SplitManagerRef;
use crate::execution::factory::SqlQueryExecutionFactory;
use crate::metadata::{ContextProviderExtension, MetadataProvider};
use crate::sql::logical::planner::DefaultLogicalPlanner;

#[derive(Clone)]
pub struct SimpleQueryDispatcher {
    coord: CoordinatorRef,
    split_manager: SplitManagerRef,
    session_factory: Arc<SessionCtxFactory>,
    // memory pool
    memory_pool: MemoryPoolRef,
    // query tracker
    query_tracker: Arc<QueryTracker>,
    // parser
    parser: Arc<dyn Parser + Send + Sync>,
    // get query execution factory
    query_execution_factory: Arc<dyn QueryExecutionFactory + Send + Sync>,
    func_manager: FuncMetaManagerRef,
    stream_provider_manager: StreamProviderManagerRef,
}

#[async_trait]
impl QueryDispatcher for SimpleQueryDispatcher {
    fn start(&self) {
        // TODO
    }

    fn stop(&self) {
        // TODO
    }

    fn create_query_id(&self) -> QueryId {
        QueryId::next_id()
    }

    fn query_info(&self, _id: &QueryId) {
        // TODO
    }

    async fn execute_query(
        &self,
        tenant_id: Oid,
        query_id: QueryId,
        query: &Query,
    ) -> Result<Output> {
        let session = self.session_factory.create_session_ctx(
            query_id.to_string(),
            query.context().clone(),
            tenant_id,
            self.memory_pool.clone(),
        )?;
        let meta_client = self
            .coord
            .tenant_meta(query.context().tenant())
            .await
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: query.context().tenant().to_string(),
            })?;

        let default_catalog_meta_client = self
            .coord
            .tenant_meta(DEFAULT_CATALOG)
            .await
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: DEFAULT_CATALOG.to_string(),
            })?;

        let scheme_provider = MetadataProvider::new(
            self.coord.clone(),
            self.split_manager.clone(),
            meta_client,
            self.func_manager.clone(),
            self.stream_provider_manager.clone(),
            self.query_tracker.clone(),
            session.clone(),
            default_catalog_meta_client,
        );

        let logical_planner = DefaultLogicalPlanner::new(&scheme_provider);

        let statements = self.parser.parse(query.content())?;

        // not allow multi statement
        if statements.len() > 1 {
            return Err(QueryError::MultiStatement {
                num: statements.len(),
                sql: query.content().to_string(),
            });
        }

        let stmt = match statements.front() {
            Some(stmt) => stmt.clone(),
            None => return Ok(Output::Nil(())),
        };

        let query_state_machine = Arc::new(QueryStateMachine::begin(
            query_id,
            query.clone(),
            session.clone(),
            self.coord.clone(),
        ));

        let result = self
            .execute_statement(stmt, &logical_planner, query_state_machine)
            .await?;

        Ok(result)
    }

    fn running_query_infos(&self) -> Vec<QueryInfo> {
        self.query_tracker
            .running_queries()
            .iter()
            .map(|e| e.info())
            .collect()
    }

    fn running_query_status(&self) -> Vec<QueryStatus> {
        self.query_tracker
            .running_queries()
            .iter()
            .map(|e| e.status())
            .collect()
    }

    fn cancel_query(&self, id: &QueryId) {
        self.query_tracker.query(id).map(|e| e.cancel());
    }
}

impl SimpleQueryDispatcher {
    async fn execute_statement<S: ContextProviderExtension + Send + Sync>(
        &self,
        stmt: ExtStatement,
        logical_planner: &DefaultLogicalPlanner<'_, S>,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> Result<Output> {
        // begin analyze
        query_state_machine.begin_analyze();
        let logical_plan = logical_planner
            .create_logical_plan(stmt, &query_state_machine.session)
            .await?;
        query_state_machine.end_analyze();

        let execution = self
            .query_execution_factory
            .create_query_execution(logical_plan, query_state_machine.clone());

        // TrackedQuery.drop() is called implicitly when the value goes out of scope,
        self.query_tracker
            .try_track_query(query_state_machine.query_id, execution)?
            .start()
            .await
    }
}

#[derive(Default, Clone)]
pub struct SimpleQueryDispatcherBuilder {
    coord: Option<CoordinatorRef>,
    split_manager: Option<SplitManagerRef>,
    session_factory: Option<Arc<SessionCtxFactory>>,
    parser: Option<Arc<dyn Parser + Send + Sync>>,
    // cnosdb optimizer
    optimizer: Option<Arc<dyn Optimizer + Send + Sync>>,
    scheduler: Option<SchedulerRef>,

    queries_limit: usize,
    memory_pool: Option<MemoryPoolRef>, // memory

    func_manager: Option<FuncMetaManagerRef>,
    stream_provider_manager: Option<StreamProviderManagerRef>,
    stream_checker_manager: Option<StreamCheckerManagerRef>,
}

impl SimpleQueryDispatcherBuilder {
    pub fn with_coord(mut self, coord: CoordinatorRef) -> Self {
        self.coord = Some(coord);
        self
    }

    pub fn with_session_factory(mut self, session_factory: Arc<SessionCtxFactory>) -> Self {
        self.session_factory = Some(session_factory);
        self
    }

    pub fn with_split_manager(mut self, split_manager: SplitManagerRef) -> Self {
        self.split_manager = Some(split_manager);
        self
    }

    pub fn with_parser(mut self, parser: Arc<dyn Parser + Send + Sync>) -> Self {
        self.parser = Some(parser);
        self
    }

    pub fn with_optimizer(mut self, optimizer: Arc<dyn Optimizer + Send + Sync>) -> Self {
        self.optimizer = Some(optimizer);
        self
    }

    pub fn with_scheduler(mut self, scheduler: SchedulerRef) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn with_queries_limit(mut self, limit: u32) -> Self {
        self.queries_limit = limit as usize;
        self
    }

    pub fn with_memory_pool(mut self, memory_pool: MemoryPoolRef) -> Self {
        self.memory_pool = Some(memory_pool);
        self
    }

    pub fn with_func_manager(mut self, func_manager: FuncMetaManagerRef) -> Self {
        self.func_manager = Some(func_manager);
        self
    }

    pub fn with_stream_provider_manager(
        mut self,
        stream_provider_manager: StreamProviderManagerRef,
    ) -> Self {
        self.stream_provider_manager = Some(stream_provider_manager);
        self
    }

    pub fn with_stream_checker_manager(
        mut self,
        stream_checker_manager: StreamCheckerManagerRef,
    ) -> Self {
        self.stream_checker_manager = Some(stream_checker_manager);
        self
    }

    pub fn build(self) -> Result<SimpleQueryDispatcher> {
        let coord = self.coord.ok_or_else(|| QueryError::BuildQueryDispatcher {
            err: "lost of coord".to_string(),
        })?;

        let split_manager = self
            .split_manager
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of split manager".to_string(),
            })?;

        let session_factory =
            self.session_factory
                .ok_or_else(|| QueryError::BuildQueryDispatcher {
                    err: "lost of session_factory".to_string(),
                })?;

        let parser = self
            .parser
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of parser".to_string(),
            })?;

        let optimizer = self
            .optimizer
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of optimizer".to_string(),
            })?;

        let scheduler = self
            .scheduler
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of scheduler".to_string(),
            })?;

        let func_manager = self
            .func_manager
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of func_manager".to_string(),
            })?;

        let stream_provider_manager =
            self.stream_provider_manager
                .ok_or_else(|| QueryError::BuildQueryDispatcher {
                    err: "lost of stream_provider_manager".to_string(),
                })?;

        let stream_checker_manager =
            self.stream_checker_manager
                .ok_or_else(|| QueryError::BuildQueryDispatcher {
                    err: "lost of stream_checker_manager".to_string(),
                })?;

        let query_tracker = Arc::new(QueryTracker::new(self.queries_limit));
        // TODO restore from metastore

        let query_execution_factory = Arc::new(SqlQueryExecutionFactory::new(
            optimizer,
            scheduler,
            query_tracker.clone(),
            stream_checker_manager,
        ));
        let memory_pool = self
            .memory_pool
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of memory pool".to_string(),
            })?;
        Ok(SimpleQueryDispatcher {
            coord,
            split_manager,
            session_factory,
            memory_pool,
            parser,
            query_execution_factory,
            query_tracker,
            func_manager,
            stream_provider_manager,
        })
    }
}
