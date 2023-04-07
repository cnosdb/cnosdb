use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::user::User;
use models::oid::Identifier;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::queries::InformationSchemaQueriesBuilder;
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_QUERIES: &str = "QUERIES";

/// This view shows real-time snapshots of SQL statements for real-time monitoring of SQL jobs
///
/// All records of this view are visible to the Owner of the current tenant.
///
/// For non-Owner members, only the SQL submitted by the current member is displayed.
pub struct QueriesFactory {}

#[async_trait::async_trait]
impl InformationSchemaTableFactory for QueriesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_QUERIES
    }

    async fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaQueriesBuilder::default();

        let user_id = user.desc().id();
        let tenant_id = *metadata.tenant().id();

        let queries_of_tenant = query_tracker
            .running_queries()
            .into_iter()
            .filter(|e| e.info().tenant_id() == tenant_id);

        let running_queries = if !user.can_access_system(tenant_id) {
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

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
