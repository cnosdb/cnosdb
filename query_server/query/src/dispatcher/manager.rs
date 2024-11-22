use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use coordinator::resource_manager::ResourceManager;
use coordinator::service::CoordinatorRef;
use memory_pool::MemoryPoolRef;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::auth_cache::{AuthCache, AuthCacheKey};
use models::auth::user::User;
use models::meta_data::{MetaModifyType, NodeId};
use models::oid::Oid;
use models::schema::query_info::{QueryId, QueryInfo};
use models::schema::resource_info::{ResourceInfo, ResourceStatus};
use models::utils::now_timestamp_nanos;
use snafu::ResultExt;
use spi::query::ast::ExtStatement;
use spi::query::datasource::stream::StreamProviderManagerRef;
use spi::query::dispatcher::{QueryDispatcher, QueryStatus};
use spi::query::execution::{Output, QueryStateMachine};
use spi::query::function::FuncMetaManagerRef;
use spi::query::logical_planner::{LogicalPlanner, Plan};
use spi::query::parser::Parser;
use spi::query::session::{SessionCtx, SessionCtxFactory};
use spi::service::protocol::{ContextBuilder, Query};
use spi::{MetaSnafu, QueryError, QueryResult};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Retry;
use trace::span_ext::SpanExt;
use trace::{error, info, Span, SpanContext};

use super::query_tracker::QueryTracker;
use crate::data_source::split::SplitManagerRef;
use crate::execution::factory::QueryExecutionFactoryRef;
use crate::metadata::{
    BaseTableProvider, ContextProviderExtension, MetadataProvider, TableHandleProviderRef,
};
use crate::sql::logical::planner::DefaultLogicalPlanner;

#[derive(Clone)]
pub struct SimpleQueryDispatcher {
    coord: CoordinatorRef,
    // client for default tenant
    default_table_provider: TableHandleProviderRef,
    split_manager: SplitManagerRef,
    session_factory: Arc<SessionCtxFactory>,
    // memory pool
    memory_pool: MemoryPoolRef,
    // query tracker
    query_tracker: Arc<QueryTracker>,
    // parser
    parser: Arc<dyn Parser + Send + Sync>,
    // get query execution factory
    query_execution_factory: QueryExecutionFactoryRef,
    func_manager: FuncMetaManagerRef,
    stream_provider_manager: StreamProviderManagerRef,
    span_ctx: Option<SpanContext>,

    async_task_joinhandle: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    failed_task_joinhandle: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
    auth_cache: Arc<AuthCache<AuthCacheKey, User>>,
}

#[async_trait]
impl QueryDispatcher for SimpleQueryDispatcher {
    async fn start(&self) -> QueryResult<()> {
        self.execute_persister_query(self.coord.node_id()).await
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
        span_ctx: Option<&SpanContext>,
    ) -> QueryResult<Output> {
        let query_state_machine = {
            let _span = Span::from_context("init session ctx", span_ctx);
            self.build_query_state_machine(
                tenant_id,
                query_id,
                query.clone(),
                span_ctx,
                self.auth_cache.clone(),
            )
            .await?
        };

        let logical_plan = self.build_logical_plan(query_state_machine.clone()).await?;
        let logical_plan = match logical_plan {
            Some(plan) => plan,
            None => return Ok(Output::Nil(())),
        };
        let result = self
            .execute_logical_plan(logical_plan, query_state_machine)
            .await?;
        Ok(result)
    }

    async fn build_logical_plan(
        &self,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> QueryResult<Option<Plan>> {
        let session = &query_state_machine.session;
        let query = &query_state_machine.query;

        let scheme_provider = self.build_scheme_provider(session).await?;

        let logical_planner = DefaultLogicalPlanner::new(&scheme_provider);

        let span_recorder = session.get_child_span("parse sql");
        let statements = self.parser.parse(query.content())?;

        // not allow multi statement
        if statements.len() > 1 {
            return Err(QueryError::MultiStatement {
                num: statements.len(),
                sql: query_state_machine.query.content().to_string(),
            });
        }

        let stmt = match statements.front() {
            Some(stmt) => stmt.clone(),
            None => return Ok(None),
        };

        drop(span_recorder);

        let logical_plan = self
            .statement_to_logical_plan(stmt, &logical_planner, query_state_machine)
            .await?;
        Ok(Some(logical_plan))
    }

    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> QueryResult<Output> {
        self.execute_logical_plan(logical_plan, query_state_machine)
            .await
    }

    async fn build_query_state_machine(
        &self,
        tenant_id: Oid,
        query_id: QueryId,
        query: Query,
        span_ctx: Option<&SpanContext>,
        auth_cache: Arc<AuthCache<AuthCacheKey, User>>,
    ) -> QueryResult<Arc<QueryStateMachine>> {
        let session = self.session_factory.create_session_ctx(
            query_id.to_string(),
            query.context(),
            tenant_id,
            self.memory_pool.clone(),
            span_ctx.cloned(),
            self.coord.clone(),
        )?;

        let query_state_machine = Arc::new(QueryStateMachine::begin(
            query_id,
            query,
            session,
            self.coord.clone(),
            auth_cache,
        ));
        Ok(query_state_machine)
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
    async fn execute_persister_query(&self, node_id: NodeId) -> QueryResult<()> {
        // 执行被持久化的任务
        let queries = self.query_tracker.persistent_queries(node_id).await?;

        for query in queries {
            let query_id = query.query_id();
            let sql = query.query();
            let database_name = query.database_name();
            let tenant_name = query.tenant_name();
            let tenant_id = query.tenant_id();
            let user = query.user().clone();
            let ctx = ContextBuilder::new(user)
                .with_tenant(Some(tenant_name.to_owned()))
                .with_database(Some(database_name.to_owned()))
                .with_is_old(Some(true))
                .build();
            let query = Query::new(ctx, sql.to_owned());
            match self
                .execute_query(tenant_id, query_id, &query, self.span_ctx.as_ref())
                .await
            {
                Ok(_) => {
                    info!("Re-execute persistent query: {}", query.content());
                }
                Err(err) => {
                    trace::warn!("Ignore, failed to re-execute persistent query: {}", err)
                }
            }
        }

        Ok(())
    }
    async fn recv_meta_modify(
        dispatcher: Arc<SimpleQueryDispatcher>,
        mut receiver: Receiver<MetaModifyType>,
    ) {
        while let Some(modify_data) = receiver.recv().await {
            // if error, max retry count 10
            let _ = Retry::spawn(
                ExponentialBackoff::from_millis(10).map(jitter).take(10),
                || async {
                    let res = SimpleQueryDispatcher::handle_meta_modify(
                        dispatcher.clone(),
                        modify_data.clone(),
                    )
                    .await;
                    if let Err(e) = &res {
                        error!("handle meta modify error: {}, retry later", e.to_string());
                    }
                    res
                },
            )
            .await;
        }
    }

    async fn handle_meta_modify(
        dispatcher: Arc<SimpleQueryDispatcher>,
        modify_data: MetaModifyType,
    ) -> QueryResult<()> {
        match modify_data {
            MetaModifyType::ResourceInfo(resourceinfo) => {
                if !resourceinfo.get_is_new_add() {
                    return Ok(()); // ignore the old task
                }

                // if current node get the lock, handle meta modify
                let (id, lock) = dispatcher
                    .coord
                    .meta_manager()
                    .read_resourceinfos_mark()
                    .await
                    .map_err(|meta_err| QueryError::Meta { source: meta_err })?;
                if id == dispatcher.coord.node_id() && lock {
                    match *resourceinfo.get_status() {
                        ResourceStatus::Schedule => {
                            if let Ok(mut joinhandle_map) = dispatcher.async_task_joinhandle.lock()
                            {
                                if let Some(handle) = joinhandle_map.get(resourceinfo.get_name()) {
                                    handle.abort(); // same resource name, abort the old task
                                }
                                joinhandle_map.insert(
                                    resourceinfo.get_name().to_string(),
                                    tokio::spawn(SimpleQueryDispatcher::exec_async_task(
                                        dispatcher.coord.clone(),
                                        *resourceinfo,
                                    )),
                                );
                            }
                        }
                        ResourceStatus::Failed => {
                            if let Ok(mut joinhandle_map) = dispatcher.failed_task_joinhandle.lock()
                            {
                                if joinhandle_map.contains_key(resourceinfo.get_name()) {
                                    return Ok(()); // ignore repetition failed task
                                }
                                joinhandle_map.insert(
                                    resourceinfo.get_name().to_string(),
                                    tokio::spawn(ResourceManager::retry_failed_task(
                                        dispatcher.coord.clone(),
                                        *resourceinfo,
                                    )),
                                );
                            }
                        }
                        ResourceStatus::Cancel => {
                            if let Ok(mut joinhandle_map) = dispatcher.async_task_joinhandle.lock()
                            {
                                if let Some(handle) = joinhandle_map.get(resourceinfo.get_name()) {
                                    handle.abort(); // abort task
                                }
                                joinhandle_map.remove(resourceinfo.get_name()); // remove task
                            }
                        }
                        _ => {}
                    }
                }
                Ok(())
            }
            MetaModifyType::NodeMetrics(node_metrics) => {
                // if lock node dead, grap lock again
                let (id, lock) = dispatcher
                    .coord
                    .meta_manager()
                    .read_resourceinfos_mark()
                    .await
                    .map_err(|meta_err| QueryError::Meta { source: meta_err })?;
                if node_metrics.id == id && lock {
                    // unlock the dead node
                    if let Err(e) = dispatcher
                        .coord
                        .meta_manager()
                        .write_resourceinfos_mark(node_metrics.id, false)
                        .await
                    {
                        match e {
                            MetaError::ResourceInfosMarkIsLock { .. } => {
                                return Ok(());
                            }
                            _ => {
                                return Err(QueryError::Meta { source: e });
                            }
                        }
                    }

                    // grap lock again
                    if let Err(e) = dispatcher
                        .coord
                        .meta_manager()
                        .write_resourceinfos_mark(dispatcher.coord.node_id(), true)
                        .await
                    {
                        match e {
                            MetaError::ResourceInfosMarkIsLock { .. } => {
                                return Ok(());
                            }
                            _ => {
                                return Err(QueryError::Meta { source: e });
                            }
                        }
                    }
                }

                // if current node get the lock, get the dead node task
                let (id, lock) = dispatcher
                    .coord
                    .meta_manager()
                    .read_resourceinfos_mark()
                    .await
                    .map_err(|meta_err| QueryError::Meta { source: meta_err })?;
                if dispatcher.coord.node_id() == id && lock {
                    let resourceinfos = dispatcher
                        .coord
                        .meta_manager()
                        .read_resourceinfos_by_nodeid(node_metrics.id)
                        .await
                        .map_err(|meta_err| QueryError::Meta { source: meta_err })?;
                    for mut resourceinfo in resourceinfos {
                        resourceinfo.set_execute_node_id(dispatcher.coord.node_id());
                        resourceinfo.set_is_new_add(false);
                        dispatcher
                            .coord
                            .meta_manager()
                            .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
                            .await
                            .context(MetaSnafu)?;
                        match *resourceinfo.get_status() {
                            ResourceStatus::Schedule => {
                                if let Ok(mut joinhandle_map) =
                                    dispatcher.async_task_joinhandle.lock()
                                {
                                    if joinhandle_map.contains_key(resourceinfo.get_name()) {
                                        return Ok(()); // ignore the dead node task
                                    }

                                    joinhandle_map.insert(
                                        resourceinfo.get_name().to_string(),
                                        tokio::spawn(SimpleQueryDispatcher::exec_async_task(
                                            dispatcher.coord.clone(),
                                            resourceinfo,
                                        )),
                                    );
                                }
                            }
                            ResourceStatus::Executing => {
                                let _ = ResourceManager::add_resource_task(
                                    dispatcher.coord.clone(),
                                    resourceinfo,
                                )
                                .await;
                            }
                            ResourceStatus::Failed => {
                                if let Ok(mut joinhandle_map) =
                                    dispatcher.failed_task_joinhandle.lock()
                                {
                                    if joinhandle_map.contains_key(resourceinfo.get_name()) {
                                        return Ok(()); // ignore repetition failed task
                                    }
                                    joinhandle_map.insert(
                                        resourceinfo.get_name().to_string(),
                                        tokio::spawn(ResourceManager::retry_failed_task(
                                            dispatcher.coord.clone(),
                                            resourceinfo,
                                        )),
                                    );
                                }
                            }
                            _ => {}
                        }
                    }

                    // handle dead node persistent query
                    // 1.execute the persistent query
                    dispatcher.execute_persister_query(node_metrics.id).await?;

                    // 2.move the dead node persistent query to current node
                    dispatcher
                        .coord
                        .meta_manager()
                        .move_queryinfo(node_metrics.id, dispatcher.coord.node_id())
                        .await
                        .context(MetaSnafu)?;
                }
                Ok(())
            }
        }
    }

    async fn exec_async_task(coord: CoordinatorRef, mut resourceinfo: ResourceInfo) {
        let future_interval = resourceinfo.get_time() - now_timestamp_nanos();
        let future_time = Instant::now() + Duration::from_nanos(future_interval as u64);
        tokio::time::sleep_until(future_time).await;
        resourceinfo.set_status(ResourceStatus::Executing);
        resourceinfo.set_is_new_add(false);
        if let Err(meta_err) = coord
            .meta_manager()
            .write_resourceinfo(resourceinfo.get_name(), resourceinfo.clone())
            .await
        {
            error!("failed to execute the async task: {}", meta_err.to_string());
        }
        // execute, if failed, retry later
        let _ = ResourceManager::do_operator(coord.clone(), resourceinfo.clone()).await;
    }

    async fn statement_to_logical_plan<S: ContextProviderExtension + Send + Sync>(
        &self,
        stmt: ExtStatement,
        logical_planner: &DefaultLogicalPlanner<'_, S>,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> QueryResult<Plan> {
        // begin analyze
        query_state_machine.begin_analyze();
        let logical_plan = logical_planner
            .create_logical_plan(
                stmt,
                &query_state_machine.session,
                self.coord.get_config().query.auth_enabled,
            )
            .await?;
        query_state_machine.end_analyze();

        Ok(logical_plan)
    }

    async fn execute_logical_plan(
        &self,
        logical_plan: Plan,
        query_state_machine: Arc<QueryStateMachine>,
    ) -> QueryResult<Output> {
        let execution = self
            .query_execution_factory
            .create_query_execution(logical_plan, query_state_machine.clone())
            .await?;

        // TrackedQuery.drop() is called implicitly when the value goes out of scope,
        self.query_tracker
            .try_track_query(query_state_machine.query_id, execution)
            .await?
            .start()
            .await
    }

    async fn build_scheme_provider(&self, session: &SessionCtx) -> QueryResult<MetadataProvider> {
        let meta_client = self.build_current_session_meta_client(session).await?;
        let current_session_table_provider =
            self.build_table_handle_provider(meta_client.clone())?;
        let metadata_provider = MetadataProvider::new(
            self.coord.clone(),
            meta_client,
            current_session_table_provider,
            self.default_table_provider.clone(),
            self.func_manager.clone(),
            self.query_tracker.clone(),
            session.clone(),
        );

        Ok(metadata_provider)
    }

    async fn build_current_session_meta_client(
        &self,
        session: &SessionCtx,
    ) -> QueryResult<MetaClientRef> {
        let meta_client = self
            .coord
            .tenant_meta(session.tenant())
            .await
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: session.tenant().to_string(),
            })
            .context(MetaSnafu)?;

        Ok(meta_client)
    }

    fn build_table_handle_provider(
        &self,
        meta_client: MetaClientRef,
    ) -> QueryResult<TableHandleProviderRef> {
        let current_session_table_provider: Arc<BaseTableProvider> =
            Arc::new(BaseTableProvider::new(
                self.coord.clone(),
                self.split_manager.clone(),
                meta_client,
                self.stream_provider_manager.clone(),
            ));

        Ok(current_session_table_provider)
    }
}

#[derive(Default, Clone)]
pub struct SimpleQueryDispatcherBuilder {
    coord: Option<CoordinatorRef>,
    default_table_provider: Option<TableHandleProviderRef>,
    split_manager: Option<SplitManagerRef>,
    session_factory: Option<Arc<SessionCtxFactory>>,
    parser: Option<Arc<dyn Parser + Send + Sync>>,

    query_execution_factory: Option<QueryExecutionFactoryRef>,
    query_tracker: Option<Arc<QueryTracker>>,
    memory_pool: Option<MemoryPoolRef>, // memory

    func_manager: Option<FuncMetaManagerRef>,
    stream_provider_manager: Option<StreamProviderManagerRef>,
    span_ctx: Option<SpanContext>,
    auth_cache: Option<Arc<AuthCache<AuthCacheKey, User>>>,
}

impl SimpleQueryDispatcherBuilder {
    pub fn with_coord(mut self, coord: CoordinatorRef) -> Self {
        self.coord = Some(coord);
        self
    }

    pub fn with_default_table_provider(
        mut self,
        default_table_provider: TableHandleProviderRef,
    ) -> Self {
        self.default_table_provider = Some(default_table_provider);
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

    pub fn with_query_execution_factory(
        mut self,
        query_execution_factory: QueryExecutionFactoryRef,
    ) -> Self {
        self.query_execution_factory = Some(query_execution_factory);
        self
    }

    pub fn with_query_tracker(mut self, query_tracker: Arc<QueryTracker>) -> Self {
        self.query_tracker = Some(query_tracker);
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

    pub fn with_span_ctx(mut self, span_ctx: Option<SpanContext>) -> Self {
        self.span_ctx = span_ctx;
        self
    }

    pub fn with_auth_cache(mut self, auth_cache: Arc<AuthCache<AuthCacheKey, User>>) -> Self {
        self.auth_cache = Some(auth_cache);
        self
    }

    pub fn build(self) -> QueryResult<Arc<SimpleQueryDispatcher>> {
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

        let query_execution_factory =
            self.query_execution_factory
                .ok_or_else(|| QueryError::BuildQueryDispatcher {
                    err: "lost of query_execution_factory".to_string(),
                })?;

        let query_tracker = self
            .query_tracker
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of query_tracker".to_string(),
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

        let memory_pool = self
            .memory_pool
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of memory pool".to_string(),
            })?;

        let default_table_provider =
            self.default_table_provider
                .ok_or_else(|| QueryError::BuildQueryDispatcher {
                    err: "lost of default_table_provider".to_string(),
                })?;

        let span_ctx = self.span_ctx;

        let auth_cache = self
            .auth_cache
            .ok_or_else(|| QueryError::BuildQueryDispatcher {
                err: "lost of auth_cache".to_string(),
            })?;

        let dispatcher = Arc::new(SimpleQueryDispatcher {
            coord,
            default_table_provider,
            split_manager,
            session_factory,
            memory_pool,
            parser,
            query_execution_factory,
            query_tracker,
            func_manager,
            stream_provider_manager,
            span_ctx,
            async_task_joinhandle: Arc::new(Mutex::new(HashMap::new())),
            failed_task_joinhandle: Arc::new(Mutex::new(HashMap::new())),
            auth_cache,
        });

        let meta_task_receiver = dispatcher
            .coord
            .meta_manager()
            .take_resourceinfo_rx()
            .expect("meta resource channel only has one consumer");
        tokio::spawn(SimpleQueryDispatcher::recv_meta_modify(
            dispatcher.clone(),
            meta_task_receiver,
        ));

        Ok(dispatcher)
    }
}
