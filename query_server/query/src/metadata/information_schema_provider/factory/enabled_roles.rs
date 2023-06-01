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

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::enabled_roles::{
    InformationSchemaEnabledRolesBuilder, ENABLED_ROLE_SCHEMA,
};
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_ENABLED_ROLES: &str = "ENABLED_ROLES";

/// This view displays the role information of the current user under the current tenant.
pub struct EnabledRolesFactory {}

impl InformationSchemaTableFactory for EnabledRolesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_ENABLED_ROLES
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationEnabledRolesTable::new(metadata, user.clone()))
    }
}

pub struct InformationEnabledRolesTable {
    user: User,
    metadata: MetaClientRef,
}

impl InformationEnabledRolesTable {
    pub fn new(metadata: MetaClientRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait]
impl TableProvider for InformationEnabledRolesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        ENABLED_ROLE_SCHEMA.clone()
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
        let mut builder = InformationSchemaEnabledRolesBuilder::default();

        if let Some(role) = self
            .metadata
            .member_role(self.user.desc().id())
            .await
            .map_err(|e| DataFusionError::Internal(format!("Failed to list databases: {}", e)))?
        {
            builder.append_row(role.name());
        }
        let rb: RecordBatch = builder.try_into()?;

        Ok(Arc::new(MemoryExec::try_new(
            &[vec![rb]],
            self.schema(),
            projection.cloned(),
        )?))
    }
}
