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
use models::auth::role::TenantRoleIdentifier;
use models::auth::user::User;
use models::oid::Identifier;
use trace::error;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::database_privileges::{
    InformationSchemaDatabasePrivilegesBuilder, DATABASE_PRIVILEGE_SCHEMA,
};
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_DATABASE_PRIVILEGES: &str = "DATABASE_PRIVILEGES";

/// This view displays all permissions on db that have been granted to the specified role under the tenant.
///
/// All records of this view are visible to the Owner of the current tenant.
///
/// For non-Owner members, only the records corresponding to the role are displayed.
pub struct DatabasePrivilegesFactory {}

impl InformationSchemaTableFactory for DatabasePrivilegesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_DATABASE_PRIVILEGES
    }

    fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> Arc<dyn TableProvider> {
        Arc::new(InformationDatabasePrivilegesTable::new(
            metadata,
            user.clone(),
        ))
    }
}

pub struct InformationDatabasePrivilegesTable {
    user: User,
    metadata: MetaClientRef,
}

impl InformationDatabasePrivilegesTable {
    pub fn new(metadata: MetaClientRef, user: User) -> Self {
        Self { user, metadata }
    }
}

#[async_trait]
impl TableProvider for InformationDatabasePrivilegesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        DATABASE_PRIVILEGE_SCHEMA.clone()
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
        let mut builder = InformationSchemaDatabasePrivilegesBuilder::default();

        let user_id = self.user.desc().id();
        let user_name = self.user.desc().name();
        let tenant = self.metadata.tenant();
        let tenant_id = tenant.id();
        let tenant_name = tenant.name();

        if self.user.can_access_role(*tenant_id) {
            // All records of this view are visible to the Owner of the current tenant.
            for role in self.metadata.custom_roles().await.map_err(|e| {
                DataFusionError::Internal(format!("Failed to get custom roles, cause: {:?}", e))
            })? {
                for (database_name, privilege) in role.additiona_privileges() {
                    builder.append_row(tenant_name, database_name, privilege.as_str(), role.name())
                }
            }
        } else {
            // For non-Owner members, only records corresponding to own role are accessed
            if let Some(role) = self.metadata.member_role(user_id).await.map_err(|e| {
                DataFusionError::Internal(format!("Failed to get member role, cause: {:?}", e))
            })? {
                match role {
                    TenantRoleIdentifier::System(_) => {
                        // not show system roles
                    }
                    TenantRoleIdentifier::Custom(ref role_name) => {
                        if let Some(role) =
                            self.metadata.custom_role(role_name).await.map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Failed to get custom role {}, cause: {:?}",
                                    role_name, e
                                ))
                            })?
                        {
                            for (database_name, privilege) in role.additiona_privileges() {
                                builder.append_row(
                                    tenant_name,
                                    database_name,
                                    privilege.as_str(),
                                    role.name(),
                                )
                            }
                        } else {
                            error!("The metadata is inconsistent, member {} of the tenant {} have the role {}, but this role does not exist",
                        user_name, tenant_name, role_name);
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
