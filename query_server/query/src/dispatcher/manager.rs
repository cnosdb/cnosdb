use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    physical_plan::SendableRecordBatchStream, scheduler::Scheduler, sql::planner::ContextProvider,
};
use spi::{
    catalog::factory::CatalogManager,
    query::{
        ast::ExtStatement,
        dispatcher::QueryDispatcher,
        execution::{QueryExecutionFactory, QueryStateMachine},
        function::FunctionMetadataManager,
        logical_planner::LogicalPlanner,
        optimizer::Optimizer,
        parser::Parser,
        session::{IsiphoSessionCtx, IsiphoSessionCtxFactory},
    },
    service::protocol::{Query, QueryId},
};

use spi::query::QueryError::BuildQueryDispatcher;
use spi::query::Result;

use crate::metadata::MetadataProvider;
use crate::{
    execution::factory::SqlQueryExecutionFactory, sql::logical::planner::DefaultLogicalPlanner,
};

pub struct SimpleQueryDispatcher {
    catalog_manager: Arc<dyn CatalogManager + Send + Sync>,
    function_manager: Arc<dyn FunctionMetadataManager + Send + Sync>,
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

    async fn execute_query(
        &self,
        _id: QueryId,
        query: &Query,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let mut results = vec![];

        let session = self.session_factory.default_isipho_session_ctx();

        // TODO Construct the corresponding catalog according to the user information carried by the client request
        let schema_provider = MetadataProvider::new(
            self.catalog_manager.clone(),
            self.function_manager.clone(),
            query.context().catalog.clone(),
            query.context().schema.clone(),
        );
        let logical_planner = DefaultLogicalPlanner::new(schema_provider);

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
    ) -> Result<SendableRecordBatchStream> {
        // begin analyze
        query_state_machine.begin_analyze();
        let logical_plan = logical_planner.create_logical_plan(stmt.clone(), session)?;
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
    catalog_manager: Option<Arc<dyn CatalogManager + Send + Sync>>,
    function_manager: Option<Arc<dyn FunctionMetadataManager + Send + Sync>>,
    session_factory: Option<Arc<IsiphoSessionCtxFactory>>,
    parser: Option<Arc<dyn Parser + Send + Sync>>,
    // cnosdb optimizer
    optimizer: Option<Arc<dyn Optimizer + Send + Sync>>,
    // TODO 需要封装 scheduler
    scheduler: Option<Arc<Scheduler>>,
}

impl SimpleQueryDispatcherBuilder {
    pub fn with_catalog_manager(
        mut self,
        catalog_manager: Arc<dyn CatalogManager + Send + Sync>,
    ) -> Self {
        self.catalog_manager = Some(catalog_manager);
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

    pub fn with_function_manager(
        mut self,
        function_manager: Arc<dyn FunctionMetadataManager + Send + Sync>,
    ) -> Self {
        self.function_manager = Some(function_manager);
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
        let catalog_manager = self.catalog_manager.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of catalog_manager".to_string(),
        })?;

        let function_manager = self.function_manager.ok_or_else(|| BuildQueryDispatcher {
            err: "lost of function_manager".to_string(),
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
            optimizer,
            scheduler,
            catalog_manager.clone(),
        ));

        Ok(SimpleQueryDispatcher {
            catalog_manager,
            function_manager,
            session_factory,
            parser,
            query_execution_factory,
        })
    }
}
