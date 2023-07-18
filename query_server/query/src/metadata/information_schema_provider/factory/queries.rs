use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use meta::model::MetaClientRef;
use models::auth::user::User;
use models::oid::Identifier;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::queries::{
    InformationSchemaQueriesBuilder, QUERY_SCHEMA,
};
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_QUERIES: &str = "QUERIES";

/// This view shows real-time snapshots of SQL statements for real-time monitoring of SQL jobs
///
/// All records of this view are visible to the Owner of the current tenant.
///
/// For non-Owner members, only the SQL submitted by the current member is displayed.
pub struct QueriesFactory {}

impl InformationSchemaTableFactory for QueriesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_QUERIES
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationQueriesTable::new(
            query_tracker,
            metadata,
            user.clone(),
        ))
    }
}

pub struct InformationQueriesTable {
    user: User,
    query_tracker: Arc<QueryTracker>,
    metadata: MetaClientRef,
}

impl InformationQueriesTable {
    pub fn new(query_tracker: Arc<QueryTracker>, metadata: MetaClientRef, user: User) -> Self {
        Self {
            user,
            query_tracker,
            metadata,
        }
    }
}

#[async_trait]
impl TableProvider for InformationQueriesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        QUERY_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _agg_with_grouping: Option<&AggWithGrouping>,
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut builder = InformationSchemaQueriesBuilder::default();

        let user_id = self.user.desc().id();
        let tenant_id = *self.metadata.tenant().id();

        let queries_of_tenant = self
            .query_tracker
            .running_queries()
            .into_iter()
            .filter(|e| e.info().tenant_id() == tenant_id);

        let running_queries = if !self.user.can_access_system(tenant_id) {
            queries_of_tenant
                .filter(|e| e.info().user_id() == *user_id)
                .collect::<Vec<_>>()
        } else {
            queries_of_tenant.collect::<Vec<_>>()
        };

        for query in running_queries {
            let info = query.info();
            let status = query.status();

            let query_id = info.query_id().to_string();
            let query_text = info.query();
            let user_id = info.user_id().to_string();
            let user_name = info.user_name();
            let tenant_id = info.tenant_id().to_string();
            let tenant_name = info.tenant_name();

            let state = status.query_state();
            let duration = status.duration().as_secs_f64();
            let processed_count = status.processed_count();
            let error_count = status.error_count();

            builder.append_row(
                query_id,
                query.query_type().to_string(),
                query_text,
                user_id,
                user_name,
                tenant_id,
                tenant_name,
                state,
                duration,
                processed_count,
                error_count,
            );
        }
        let rb: RecordBatch = builder.try_into()?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![rb]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
