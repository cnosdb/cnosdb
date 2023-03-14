use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::user::User;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::members::InformationSchemaMembersBuilder;
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_MEMBERS: &str = "MEMBERS";

/// This view displays member information under the tenant.
///
/// All records for this view are visible to all members of the current tenant.
pub struct MembersFactory {}

#[async_trait::async_trait]
impl InformationSchemaTableFactory for MembersFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_MEMBERS
    }

    async fn create(
        &self,
        _user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaMembersBuilder::default();

        for (user_name, role) in metadata.members().await? {
            builder.append_row(user_name, role.name());
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
