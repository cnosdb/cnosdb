use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::MemorySourceConfig;
use datafusion::catalog::Session;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::logical_plan::TableScanAggregate;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use meta::model::MetaRef;
use models::auth::user::User;

use crate::metadata::cluster_schema_provider::builder::users::{
    ClusterSchemaUsersBuilder, USER_SCHEMA,
};
use crate::metadata::cluster_schema_provider::{ClusterSchemaTableFactory, CLUSTER_SCHEMA_USERS};

pub struct ClusterSchemaUsersFactory {}

impl ClusterSchemaTableFactory for ClusterSchemaUsersFactory {
    fn table_name(&self) -> &str {
        CLUSTER_SCHEMA_USERS
    }

    fn create(&self, user: &User, metadata: MetaRef) -> Arc<dyn TableProvider> {
        Arc::new(ClusterSchemaUsersTable::new(metadata, user.clone()))
    }
}

#[derive(Debug)]
pub struct ClusterSchemaUsersTable {
    user: User,
    metadata: MetaRef,
}

impl ClusterSchemaUsersTable {
    pub fn new(metadata: MetaRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait::async_trait]
impl TableProvider for ClusterSchemaUsersTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        USER_SCHEMA.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _aggregate: Option<&TableScanAggregate>,
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut builder = ClusterSchemaUsersBuilder::default();

        // Only visible to admin
        if self.user.desc().is_admin() {
            let users =
                self.metadata.users().await.map_err(|e| {
                    DataFusionError::Internal(format!("Failed to get users: {:?}", e))
                })?;
            for user in users {
                let mut options = user.options().clone();
                options.hash_password_hidden();
                let options_str = serde_json::to_string(&options).map_err(|e| {
                    DataFusionError::Internal(format!("failed to serialize options: {}", e))
                })?;

                builder.append_row(user.name(), user.is_admin(), options_str);
            }
        }

        let rb: RecordBatch = builder.try_into()?;

        let mem_exec =
            MemorySourceConfig::try_new_exec(&[vec![rb]], self.schema(), projection.cloned())?;
        Ok(mem_exec)
    }
}
