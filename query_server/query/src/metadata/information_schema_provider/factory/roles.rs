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
use models::auth::role::SystemTenantRole;
use models::auth::user::User;
use models::oid::Identifier;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::roles::{
    InformationSchemaRolesBuilder, ROLE_SCHEMA,
};
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_ROLES: &str = "ROLES";

/// This view displays all available roles (including system roles and custom roles) under the current tenant.
///
/// All records of this view are visible to the Owner of the current tenant.
pub struct RolesFactory {}

impl InformationSchemaTableFactory for RolesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_ROLES
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationRolesTable::new(metadata, user.clone()))
    }
}

pub struct InformationRolesTable {
    user: User,
    metadata: MetaClientRef,
}

impl InformationRolesTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn new(metadata: MetaClientRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait]
impl TableProvider for InformationRolesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        ROLE_SCHEMA.clone()
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
        let mut builder = InformationSchemaRolesBuilder::default();

        let tenant_id = *self.metadata.tenant().id();

        if self.user.can_access_role(tenant_id) {
            // All records of this view are visible to the Owner of the current tenant.
            let sys_roles = &[SystemTenantRole::Owner, SystemTenantRole::Member];
            for role in sys_roles {
                builder.append_row(role.name(), "system", None::<String>)
            }

            for role in self.metadata.custom_roles().await.map_err(|e| {
                DataFusionError::Internal(format!("Failed to list databases: {}", e))
            })? {
                let inherit_role = role.inherit_role();
                builder.append_row(role.name(), "custom", Some(inherit_role.name()))
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
