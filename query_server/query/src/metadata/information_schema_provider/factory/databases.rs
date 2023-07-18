use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use meta::model::MetaClientRef;
use models::auth::user::User;
use models::oid::Identifier;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::databases::{
    InformationSchemaDatabasesBuilder, DATABASE_SCHEMA,
};
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

pub const INFORMATION_SCHEMA_DATABASES: &str = "DATABASES";

/// This view only displays database information for which the current user has Read permission or higher.
pub struct DatabasesFactory {}

impl InformationSchemaTableFactory for DatabasesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_DATABASES
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationDatabasesTable::new(metadata, user.clone()))
    }
}

pub struct InformationDatabasesTable {
    user: User,
    metadata: MetaClientRef,
}

impl InformationDatabasesTable {
    pub fn new(metadata: MetaClientRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait]
impl TableProvider for InformationDatabasesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        DATABASE_SCHEMA.clone()
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
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut builder = InformationSchemaDatabasesBuilder::default();

        let dbs = self
            .metadata
            .list_databases()
            .map_err(|e| DataFusionError::Internal(format!("Failed to list databases: {}", e)))?;
        let tenant = self.metadata.tenant();
        let tenant_id = tenant.id();
        let tenant_name = tenant.name();

        for db in dbs {
            // Check if the current user has at least read permission on this db, skip if not
            if !self.user.can_read_database(*tenant_id, &db) {
                continue;
            }

            if let Some(db_schema) = self
                .metadata
                .get_db_schema(&db)
                .map_err(|e| DataFusionError::Internal(format!("Failed to get db schema: {}", e)))?
            {
                let options = db_schema.options();
                builder.append_row(
                    tenant_name,
                    db_schema.database_name(),
                    options.ttl_or_default().to_string(),
                    options.shard_num_or_default(),
                    options.vnode_duration_or_default().to_string(),
                    options.replica_or_default(),
                    options.precision_or_default().to_string(),
                );
            }
        }
        let rb: RecordBatch = builder.try_into()?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![rb]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
