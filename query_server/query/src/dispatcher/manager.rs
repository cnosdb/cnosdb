use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{scheduler::Scheduler, sql::planner::ContextProvider};
use snafu::ResultExt;
use spi::query::execution::Output;
use spi::{
    query::{
        ast::ExtStatement,
        dispatcher::QueryDispatcher,
        execution::{QueryExecutionFactory, QueryStateMachine},
        logical_planner::LogicalPlanner,
        optimizer::Optimizer,
        parser::Parser,
        session::{IsiphoSessionCtx, IsiphoSessionCtxFactory},
    },
    service::protocol::{Query, QueryId},
};

use spi::query::QueryError::BuildQueryDispatcher;
use spi::query::{LogicalPlannerSnafu, Result};

use crate::metadata::{MetaDataRef, MetadataProvider};
use crate::{
    execution::factory::SqlQueryExecutionFactory, sql::logical::planner::DefaultLogicalPlanner,
};

pub struct SimpleQueryDispatcher {
    metadata: MetaDataRef,
    session_factory: Arc<IsiphoSessionCtxFactory>,
    // TODO resource manager
    // TODO query tracker
    // parser
    parser: Arc<dyn Parser + Send + Sync>,
    // get query execution factory
    query_execution_factory: Arc<dyn QueryExecutionFactory + Send + Sync>,
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

    fn get_query_info(&self, _id: &QueryId) {
        // TODO
    }

    async fn execute_query(&self, _id: QueryId, query: &Query) -> Result<Vec<Output>> {
        let mut results = vec![];

        let session = self.session_factory.default_isipho_session_ctx();
        let metadata = self
            .metadata
            .with_catalog(&query.context().catalog)
            .with_database(&query.context().database);
        let scheme_provider = MetadataProvider::new(metadata);

        let logical_planner = DefaultLogicalPlanner::new(scheme_provider);

        let statements = self.parser.parse(query.content())?;

        for stmt in statements.iter() {
            // TODO save query_state_machine，track query state
            let query_state_machine =
                Arc::new(QueryStateMachine::begin(query.clone(), session.clone()));

            let result = self
                .execute_statement(
                    stmt.clone(),
                    &session,
                    &logical_planner,
                    query_state_machine,
                )
                .await?;

            results.push(result);
        }

        Ok(results)
    }

    fn cancel_query(&self, _id: &QueryId) {
        // TODO
    }
}

impl SimpleQueryDispatcher {
    async fn execute_statement<S: ContextProvider>(
        &self,
        stmt: ExtStatement,
        session: &IsiphoSessionCtx,
        logical_planner: &DefaultLogicalPlanner<S>,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> Result<Output> {
        // begin analyze
        query_state_machine.begin_analyze();
        let logical_plan = logical_planner
            .create_logical_plan(stmt.clone(), session)
            .context(LogicalPlannerSnafu)?;
        query_state_machine.end_analyze();

        // begin execute
        self.query_execution_factory
            .create_query_execution(logical_plan, query_state_machine.clone())
            .start()
            .await
    }
}

#[derive(Default)]
pub struct SimpleQueryDispatcherBuilder {
    metadata: Option<MetaDataRef>,
    session_factory: Option<Arc<IsiphoSessionCtxFactory>>,
    parser: Option<Arc<dyn Parser + Send + Sync>>,
    // cnosdb optimizer
    optimizer: Option<Arc<dyn Optimizer + Send + Sync>>,
    // TODO 需要封装 scheduler
    scheduler: Option<Arc<Scheduler>>,
}

impl SimpleQueryDispatcherBuilder {
    pub fn with_metadata(mut self, meta: MetaDataRef) -> Self {
        self.metadata = Some(meta);
        self
    }

    pub fn with_session_factory(mut self, session_factory: Arc<IsiphoSessionCtxFactory>) -> Self {
        self.session_factory = Some(session_factory);
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

    pub fn with_scheduler(mut self, scheduler: Arc<Scheduler>) -> Self {
        self.scheduler = Some(scheduler);
        self
    }

    pub fn build(self) -> Result<SimpleQueryDispatcher> {
        let metadata = self.metadata.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of metadata".to_string(),
        })?;
        let session_factory = self.session_factory.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of session_factory".to_string(),
        })?;

        let parser = self.parser.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of parser".to_string(),
        })?;

        let optimizer = self.optimizer.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of optimizer".to_string(),
        })?;

        let scheduler = self.scheduler.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of scheduler".to_string(),
        })?;

        let query_execution_factory = Arc::new(SqlQueryExecutionFactory::new(
            metadata.clone(),
            optimizer,
            scheduler,
        ));

        Ok(SimpleQueryDispatcher {
            metadata,
            session_factory,
            parser,
            query_execution_factory,
        })
    }
}
