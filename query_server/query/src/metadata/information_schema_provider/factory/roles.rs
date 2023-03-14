use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::role::SystemTenantRole;
use models::auth::user::User;
use models::oid::Identifier;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::information_schema_provider::builder::roles::InformationSchemaRolesBuilder;
use crate::metadata::information_schema_provider::InformationSchemaTableFactory;

const INFORMATION_SCHEMA_ROLES: &str = "ROLES";

/// This view displays all available roles (including system roles and custom roles) under the current tenant.
///
/// All records of this view are visible to the Owner of the current tenant.
pub struct RolesFactory {}

#[async_trait::async_trait]
impl InformationSchemaTableFactory for RolesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_ROLES
    }

    async fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaRolesBuilder::default();

        let tenant_id = *metadata.tenant().id();

        if user.can_access_role(tenant_id) {
            // All records of this view are visible to the Owner of the current tenant.
            let sys_roles = &[SystemTenantRole::Owner, SystemTenantRole::Member];
            for role in sys_roles {
                builder.append_row(role.name(), "system", None::<String>)
            }

            for role in metadata.custom_roles().await? {
                let inherit_role = role.inherit_role();
                builder.append_row(role.name(), "custom", Some(inherit_role.name()))
            }
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
