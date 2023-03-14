use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::error::MetaError;
use meta::model::MetaRef;
use models::auth::user::User;
use models::oid::Identifier;

use crate::metadata::cluster_schema_provider::builder::users::ClusterSchemaUsersBuilder;
use crate::metadata::cluster_schema_provider::ClusterSchemaTableFactory;

const INFORMATION_SCHEMA_USERS: &str = "USERS";

pub struct ClusterSchemaUsersFactory {}

#[async_trait::async_trait]
impl ClusterSchemaTableFactory for ClusterSchemaUsersFactory {
    fn table_name(&self) -> &str {
        INFORMATION_SCHEMA_USERS
    }

    async fn create(
        &self,
        user: &User,
        metadata: MetaRef,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = ClusterSchemaUsersBuilder::default();

        // Only visible to admin
        if user.desc().is_admin() {
            for user in metadata.user_manager().users().await? {
                let mut options = user.options().clone();
                options.hidden_password();
                let options_str = serde_json::to_string(&options)
                    .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;

                builder.append_row(user.name(), user.is_admin(), options_str);
            }
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
