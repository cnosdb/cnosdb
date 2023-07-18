use std::any::Any;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::logical_plan::AggWithGrouping;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use meta::model::MetaRef;
use models::auth::user::User;
use models::oid::Identifier;

use crate::metadata::cluster_schema_provider::builder::tenants::{
    ClusterSchemaTenantsBuilder, TENANT_SCHEMA,
};
use crate::metadata::cluster_schema_provider::ClusterSchemaTableFactory;

const INFORMATION_SCHEMA_TENANTS: &str = "TENANTS";

pub struct ClusterSchemaTenantsFactory {}

impl ClusterSchemaTableFactory for ClusterSchemaTenantsFactory {
    fn table_name(&self) -> &str {
        INFORMATION_SCHEMA_TENANTS
    }

    fn create(&self, user: &User, metadata: MetaRef) -> Arc<dyn TableProvider> {
        Arc::new(ClusterSchemaTenantsTable::new(metadata, user.clone()))
    }
}

pub struct ClusterSchemaTenantsTable {
    user: User,
    metadata: MetaRef,
}

impl ClusterSchemaTenantsTable {
    pub fn new(metadata: MetaRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait::async_trait]
impl TableProvider for ClusterSchemaTenantsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        TENANT_SCHEMA.clone()
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
        let mut builder = ClusterSchemaTenantsBuilder::default();

        // Only visible to admin
        if self.user.desc().is_admin() {
            let tenants =
                self.metadata.tenants().await.map_err(|e| {
                    DataFusionError::Internal(format!("failed to list tenant {}", e))
                })?;
            for tenant in tenants.iter() {
                let options_str = serde_json::to_string(tenant.options()).map_err(|e| {
                    DataFusionError::Internal(format!("failed to serialize options: {}", e))
                })?;

                builder.append_row(tenant.name(), options_str);
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
