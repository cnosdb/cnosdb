use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::meta_client::{MetaClientRef, MetaError};
use models::{
    auth::{role::TenantRoleIdentifier, user::User},
    oid::Identifier,
};
use trace::error;

use crate::{
    dispatcher::query_tracker::QueryTracker,
    metadata::information_schema_provider::{
        builder::database_privileges::InformationSchemaDatabasePrivilegesBuilder,
        InformationSchemaTableFactory,
    },
};

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
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaDatabasePrivilegesBuilder::default();

        let user_id = user.desc().id();
        let user_name = user.desc().name();
        let tenant_id = metadata.tenant().id();
        let tenant_name = metadata.tenant().name();

        if user.can_access_role(*tenant_id) {
            // All records of this view are visible to the Owner of the current tenant.
            for role in metadata.custom_roles()? {
                for (database_name, privilege) in role.additiona_privileges() {
                    builder.append_row(tenant_name, database_name, privilege.as_str(), role.name())
                }
            }
        } else {
            // For non-Owner members, only records corresponding to own role are accessed
            let role = metadata.member_role(user_id)?;
            match role {
                TenantRoleIdentifier::System(_) => {
                    // not show system roles
                }
                TenantRoleIdentifier::Custom(ref role_name) => {
                    if let Some(role) = metadata.custom_role(role_name)? {
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

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
