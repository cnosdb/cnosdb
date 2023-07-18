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
use crate::metadata::information_schema_provider::builder::tables;
use crate::metadata::information_schema_provider::builder::tables::InformationSchemaTablesBuilder;
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

pub const INFORMATION_SCHEMA_TABLES: &str = "TABLES";

/// This view only displays table information under the database for which the current user has Read permission or higher.
pub struct TablesFactory {}

impl InformationSchemaTableFactory for TablesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_TABLES
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationTable::new(metadata, user.clone()))
    }
}

pub struct InformationTable {
    user: User,
    metadata: MetaClientRef,
}

impl InformationTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn new(metadata: MetaClientRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait]
impl TableProvider for InformationTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        tables::TABLES_SCHEMA.clone()
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
        let mut builder = InformationSchemaTablesBuilder::default();

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

            let tables = self
                .metadata
                .list_tables(&db)
                .map_err(|e| DataFusionError::Internal(format!("failed to list tables {}", e)))?;
            for table in tables {
                if let Some(table) = self.metadata.get_table_schema(&db, &table).map_err(|e| {
                    DataFusionError::Internal(format!("failed to get table schema {}", e))
                })? {
                    builder.append_row(
                        tenant_name,
                        &db,
                        table.name(),
                        TableType::Base,
                        table.engine_name(),
                        "TODO",
                    );
                }
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
