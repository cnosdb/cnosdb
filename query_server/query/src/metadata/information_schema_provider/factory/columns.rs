use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use meta::model::MetaClientRef;
use models::auth::user::User;
use models::oid::Identifier;
use models::schema::{
    ColumnType, ExternalTableSchema, StreamTable, TableSchema, TskvTableSchemaRef,
};
use models::ValueType;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::columns::{
    InformationSchemaColumnsBuilder, COLUMN_SCHEMA,
};
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

pub const INFORMATION_SCHEMA_COLUMNS: &str = "COLUMNS";

/// This view only displays the column information of tables under the database that the current user has Read permission or higher.
pub struct ColumnsFactory {}

impl InformationSchemaTableFactory for ColumnsFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_COLUMNS
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationColumnsTable::new(metadata, user.clone()))
    }
}

pub struct InformationColumnsTable {
    user: User,
    metadata: MetaClientRef,
}

impl InformationColumnsTable {
    pub fn new(metadata: MetaClientRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait]
impl TableProvider for InformationColumnsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        COLUMN_SCHEMA.clone()
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
        let mut builder = InformationSchemaColumnsBuilder::default();

        let tenant = self.metadata.tenant();

        let dbs = self
            .metadata
            .list_databases()
            .map_err(|e| DataFusionError::Internal(format!("Failed to list databases: {}", e)))?;
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
                .map_err(|e| DataFusionError::Internal(format!("Failed to list tables: {}", e)))?;
            for table in tables {
                if let Some(table) = self.metadata.get_table_schema(&db, &table).map_err(|e| {
                    DataFusionError::Internal(format!("Failed to get table schema: {}", e))
                })? {
                    match table {
                        TableSchema::TsKvTableSchema(t) => {
                            append_tskv_table(tenant_name, &db, t.clone(), &mut builder);
                        }
                        TableSchema::ExternalTableSchema(t) => {
                            append_external_table(tenant_name, &db, t.clone(), &mut builder);
                        }
                        TableSchema::StreamTableSchema(t) => {
                            append_stream_table(tenant_name, &db, t.clone(), &mut builder);
                        }
                    }
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

fn append_tskv_table(
    tenant_name: &str,
    database_name: &str,
    table: TskvTableSchemaRef,
    builder: &mut InformationSchemaColumnsBuilder,
) {
    for (idx, col) in table.columns().iter().enumerate() {
        builder.append_row(
            tenant_name,
            database_name,
            &table.name,
            &col.name,
            col.column_type.as_column_type_str(),
            idx as u64,
            "NULL",
            col.nullable(),
            col.column_type.to_sql_type_str(),
            Some(col.encoding.as_str()),
        );
    }
}

fn append_external_table(
    tenant_name: &str,
    database_name: &str,
    table: Arc<ExternalTableSchema>,
    builder: &mut InformationSchemaColumnsBuilder,
) {
    for (idx, col) in table.schema.all_fields().iter().enumerate() {
        builder.append_row(
            tenant_name,
            database_name,
            &table.name,
            col.name(),
            // The fields of the external table are all type FIELD
            ColumnType::Field(ValueType::Unknown).as_column_type_str(),
            idx as u64,
            "NULL",
            col.is_nullable(),
            col.data_type().to_string(),
            None::<String>,
        );
    }
}

fn append_stream_table(
    tenant_name: &str,
    database_name: &str,
    table: Arc<StreamTable>,
    builder: &mut InformationSchemaColumnsBuilder,
) {
    for (idx, col) in table.schema().all_fields().iter().enumerate() {
        builder.append_row(
            tenant_name,
            database_name,
            table.name(),
            col.name(),
            // The fields of the external table are all type FIELD
            ColumnType::Field(ValueType::Unknown).as_column_type_str(),
            idx as u64,
            "UNKNOWN",
            col.is_nullable(),
            col.data_type().to_string(),
            None::<String>,
        );
    }
}
