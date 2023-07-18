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

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::members::{
    InformationSchemaMembersBuilder, MEMBER_SCHEMA,
};
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_MEMBERS: &str = "MEMBERS";

/// This view displays member information under the tenant.
///
/// All records for this view are visible to all members of the current tenant.
pub struct MembersFactory {}

impl InformationSchemaTableFactory for MembersFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_MEMBERS
    }

    fn create(
        &self,
        _user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationMembersTable::new(metadata))
    }
}

pub struct InformationMembersTable {
    metadata: MetaClientRef,
}

impl InformationMembersTable {
    pub fn new(metadata: MetaClientRef) -> Self {
        Self { metadata }
    }
}

#[async_trait]
impl TableProvider for InformationMembersTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        MEMBER_SCHEMA.clone()
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
        let mut builder = InformationSchemaMembersBuilder::default();

        for (user_name, role) in
            self.metadata.members().await.map_err(|e| {
                DataFusionError::Internal(format!("Failed to list databases: {}", e))
            })?
        {
            builder.append_row(user_name, role.name());
        }
        let rb: RecordBatch = builder.try_into()?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![rb]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
