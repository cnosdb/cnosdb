use std::{
    cell::{Ref, RefCell},
    collections::{HashMap, HashSet},
};

use models::oid::{Id, Identifier, MemoryOidGenerator, Oid, OidGenerator};
use parking_lot::RwLock;

use super::{
    privilege::DatabasePrivilege,
    role::{CustomTenantRole, CustomTenantRoleRef, SystemTenantRole, TenantRole},
    AuthError, Result,
};

#[derive(Default)]
pub struct TenantRoleManager {
    // userId -> tenantIds
    user_tenant_map: HashMap<Oid, HashSet<Oid>>,
    // tenantId -> tenantAcl
    tenant_acls: HashMap<Oid, TenantAcl<Oid>>,

    lock: RwLock<()>,
    oid_generator: MemoryOidGenerator,
}

impl TenantRoleManager {
    pub async fn create_tenant(&mut self, tenant_id: Oid) -> Result<()> {
        let _lock = self.lock.write();

        if self.tenant_acls.contains_key(&tenant_id) {
            return Err(AuthError::Internal {
                err: format!(
                    "The tenant {} already exists. This was likely caused by a bug.",
                    tenant_id
                ),
            });
        }

        self.tenant_acls.insert(tenant_id, TenantAcl::default());

        Ok(())
    }

    pub async fn drop_tenant(&mut self, tenant_id: &Oid) -> Result<bool> {
        let _lock = self.lock.write();

        Ok(self.tenant_acls.remove(tenant_id).is_some())
    }

    pub async fn tenants_of_member(&mut self, user_id: &Oid) -> Result<Option<&HashSet<Oid>>> {
        let _lock = self.lock.write();

        Ok(self.user_tenant_map.get(user_id))
    }

    pub async fn tenants(&mut self) -> Result<HashSet<&Oid>> {
        let _lock = self.lock.write();

        Ok(self.tenant_acls.keys().collect())
    }

    pub async fn create_custom_role_of_tenant(
        &mut self,
        tenant_id: &Oid,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<Oid, DatabasePrivilege>,
    ) -> Result<()> {
        let id = self
            .oid_generator
            .next_oid()
            .await
            .map_err(|error| AuthError::IdGenerate { error })?;

        let _lock = self.lock.read();

        self.tenant_acls
            .get_mut(tenant_id)
            .ok_or(AuthError::TenantNotFound)?
            .create_custom_role(CustomTenantRole::new(
                id,
                role_name,
                system_role,
                additiona_privileges,
            ))
    }

    pub async fn grant_privilege_to_custom_role_of_tenant(
        &mut self,
        database_id: Oid,
        privilege: DatabasePrivilege,
        role_name: &str,
        tenant_id: &Oid,
    ) -> Result<()> {
        let _lock = self.lock.read();

        self.tenant_acls
            .get_mut(tenant_id)
            .ok_or(AuthError::TenantNotFound)?
            .grant_privilege_to_custom_role(database_id, privilege, role_name)
    }

    pub async fn revoke_privilege_from_custom_role_of_tenant(
        &mut self,
        database_id: &Oid,
        privilege: &DatabasePrivilege,
        role_name: &str,
        tenant_id: &Oid,
    ) -> Result<bool> {
        let _lock = self.lock.read();

        self.tenant_acls
            .get_mut(tenant_id)
            .ok_or(AuthError::TenantNotFound)?
            .revoke_privilege_from_custom_role(database_id, privilege, role_name)
    }

    pub async fn drop_custom_role_of_tenant(
        &mut self,
        role_name: &str,
        tenant_id: &Oid,
    ) -> Result<bool> {
        let _lock = self.lock.read();

        self.tenant_acls
            .get_mut(tenant_id)
            .ok_or(AuthError::TenantNotFound)?
            .drop_custom_role(role_name)
    }

    pub async fn custom_roles_of_tenant(
        &self,
        tenant_id: &Oid,
    ) -> Result<Vec<Ref<CustomTenantRole<Oid>>>> {
        let _lock = self.lock.read();

        self.tenant_acls
            .get(tenant_id)
            .ok_or(AuthError::TenantNotFound)?
            .custom_roles()
    }

    pub async fn custom_role_of_tenant(
        &self,
        role_name: &str,
        tenant_id: &Oid,
    ) -> Result<Option<Ref<CustomTenantRole<Oid>>>> {
        let _lock = self.lock.read();

        self.tenant_acls
            .get(tenant_id)
            .ok_or(AuthError::TenantNotFound)?
            .custom_role(role_name)
    }

    pub async fn add_member_with_role_to_tenant(
        &mut self,
        user_id: Oid,
        role: TenantRole<Oid>,
        tenant_id: Oid,
    ) -> Result<()> {
        let _lock = self.lock.read();

        if let Some(acl) = self.tenant_acls.get_mut(&tenant_id) {
            acl.add_member(user_id, role)?;
            self.user_tenant_map
                .entry(user_id)
                .or_default()
                .insert(tenant_id);
        } else {
            return Err(AuthError::TenantNotFound);
        }

        Ok(())
    }

    pub async fn remove_member_from_tenant(&mut self, user_id: Oid, tenant_id: Oid) -> Result<()> {
        let _lock = self.lock.read();

        if let Some(acl) = self.tenant_acls.get_mut(&tenant_id) {
            acl.remove_member(user_id)?;
            self.user_tenant_map
                .entry(user_id)
                .or_default()
                .remove(&tenant_id);
        } else {
            return Err(AuthError::TenantNotFound);
        }

        Ok(())
    }

    pub async fn remove_member_from_all_tenants(&mut self, user_id: &Oid) -> Result<bool> {
        let _lock = self.lock.write();

        Ok(self.user_tenant_map.remove(user_id).is_some())
    }

    pub async fn reasign_member_role_in_tenant(
        &mut self,
        user_id: Oid,
        role: TenantRole<Oid>,
        tenant_id: Oid,
    ) -> Result<()> {
        let _lock = self.lock.read();

        if let Some(acl) = self.tenant_acls.get_mut(&tenant_id) {
            acl.reasign_member_role(user_id, role)?;
        } else {
            return Err(AuthError::TenantNotFound);
        }

        Ok(())
    }

    pub async fn member_role_of_tenant(
        &self,
        user_id: &Oid,
        tenant_id: &Oid,
    ) -> Result<TenantRole<Oid>> {
        let _lock = self.lock.read();

        let tenant_id = self
            .user_tenant_map
            .get(user_id)
            .ok_or(AuthError::MemberNotFound)?
            .get(tenant_id)
            .ok_or(AuthError::MemberNotFound)?;

        let role = self
            .tenant_acls
            .get(tenant_id)
            .ok_or_else(|| AuthError::Internal {
                err: "Query the user's tenant permissions".to_string(),
            })?
            .member_role(user_id)?
            .clone();

        Ok(role)
    }

    pub async fn members_of_tenant(&self, tenant_id: &Oid) -> Result<Option<HashSet<&Oid>>> {
        let _lock = self.lock.read();

        if let Some(acl) = self.tenant_acls.get(tenant_id) {
            return Ok(Some(acl.members()?));
        }

        Ok(None)
    }
}

#[derive(Debug, Default)]
struct TenantAcl<T> {
    // all custom roles in this tenant
    // CustomTenantRoleName -> CustomTenantRole
    // Store in meta(tenantId -> CustomTenantRole)
    custom_roles: HashMap<String, CustomTenantRoleRef<T>>,
    // userId -> tenant role
    // Store in meta(tenantId -> userId -> tenant role)
    user_role_map: HashMap<T, TenantRole<T>>,

    lock: RwLock<()>,
}

impl<T> TenantAcl<T>
where
    T: Id,
{
    /// Err:
    ///
    fn add_member(&mut self, user_id: T, role: TenantRole<T>) -> Result<()> {
        let _lock = self.lock.write();

        if self.user_role_map.contains_key(&user_id) {
            return Err(AuthError::MemberAlreadyExists);
        }

        self.user_role_map.insert(user_id, role);

        Ok(())
    }
    /// Err:
    ///
    fn remove_member(&mut self, user_id: T) -> Result<()> {
        let _lock = self.lock.write();

        if self.user_role_map.remove(&user_id).is_none() {
            return Err(AuthError::MemberNotFound);
        }

        Ok(())
    }
    fn member_role(&self, user_id: &T) -> Result<&TenantRole<T>> {
        let _lock = self.lock.read();

        if let Some(tenant_role) = self.user_role_map.get(user_id) {
            return Ok(tenant_role);
        }

        Err(AuthError::MemberNotFound)
    }
    fn members(&self) -> Result<HashSet<&T>> {
        let _lock = self.lock.read();

        Ok(self.user_role_map.keys().collect())
    }
    /// Err:
    ///
    fn reasign_member_role(&mut self, user_id: T, role: TenantRole<T>) -> Result<()> {
        let _lock = self.lock.write();

        if !self.user_role_map.contains_key(&user_id) {
            return Err(AuthError::MemberNotFound);
        }

        self.user_role_map.insert(user_id, role);

        Ok(())
    }

    /// Err:
    ///
    fn create_custom_role(&mut self, custom_role: CustomTenantRole<T>) -> Result<()> {
        let _lock = self.lock.write();

        let custom_role_name = custom_role.name();
        // 检查自定义角色是否存在
        if self.custom_roles.contains_key(custom_role_name) {
            return Err(AuthError::RoleAlreadyExists);
        }
        // 通过后，将自定义权限添加到custom_roles中
        self.custom_roles.insert(
            custom_role_name.to_string(),
            Box::new(RefCell::new(custom_role)),
        );

        Ok(())
    }
    /// Err:
    ///
    fn grant_privilege_to_custom_role(
        &mut self,
        database_ident: T,
        privilege: DatabasePrivilege,
        custom_role_name: &str,
    ) -> Result<()> {
        let _lock = self.lock.write();

        self.custom_roles
            .get_mut(custom_role_name)
            .ok_or(AuthError::RoleNotFound)?
            .borrow_mut()
            .grant_privilege(database_ident, privilege)
    }
    /// Err:
    ///
    fn revoke_privilege_from_custom_role(
        &mut self,
        database_ident: &T,
        privilege: &DatabasePrivilege,
        custom_role_name: &str,
    ) -> Result<bool> {
        let _lock = self.lock.write();

        self.custom_roles
            .get_mut(custom_role_name)
            .ok_or(AuthError::RoleNotFound)?
            .borrow_mut()
            .revoke_privilege(database_ident, privilege)
    }
    /// Err:
    ///
    fn drop_custom_role(&mut self, custom_role_name: &str) -> Result<bool> {
        let _lock = self.lock.write();

        Ok(self.custom_roles.remove(custom_role_name).is_some())
    }
    /// Err:
    ///
    fn custom_role(&self, custom_role_name: &str) -> Result<Option<Ref<CustomTenantRole<T>>>> {
        let _lock = self.lock.write();

        Ok(self.custom_roles.get(custom_role_name).map(|e| e.borrow()))
    }
    /// Err:
    ///
    fn custom_roles(&self) -> Result<Vec<Ref<CustomTenantRole<T>>>> {
        let _lock = self.lock.write();

        let result: Vec<Ref<CustomTenantRole<T>>> =
            self.custom_roles.values().map(|e| e.borrow()).collect();

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::auth::privilege::{Privilege, TenantObjectPrivilege};

    use super::*;

    #[tokio::test]
    async fn test_add_member_to_tenant() {
        let mut manager = TenantRoleManager::default();

        let id_generator = MemoryOidGenerator::default();

        let tenant1_id = id_generator.next_oid().await.expect("generate id");
        manager
            .create_tenant(tenant1_id)
            .await
            .expect("create tenant");

        let user1_id = id_generator.next_oid().await.expect("generate id");
        let tenant_owner: TenantRole<Oid> = TenantRole::System(SystemTenantRole::Owner);
        manager
            .add_member_with_role_to_tenant(user1_id, tenant_owner.clone(), tenant1_id)
            .await
            .expect("add member to tenant");

        let role = manager
            .member_role_of_tenant(&user1_id, &tenant1_id)
            .await
            .expect("query member role of tenant");
        assert_eq!(&tenant_owner, &role);

        let members = manager
            .members_of_tenant(&tenant1_id)
            .await
            .expect("members_of_tenant")
            .expect("tenant exists");
        assert_eq!(members.len(), 1);
        assert!(members.contains(&user1_id));

        let tenant2_id = id_generator.next_oid().await.expect("generate id");
        manager
            .create_tenant(tenant2_id)
            .await
            .expect("create tenant");

        let tenants = manager.tenants().await.expect("query tenants");
        assert_eq!(tenants.len(), 2);

        let members = manager
            .members_of_tenant(&tenant2_id)
            .await
            .expect("members_of_tenant")
            .expect("tenant exists");
        assert_eq!(members.len(), 0);

        let tenant_not_exists_id = id_generator.next_oid().await.expect("generate id");
        let members = manager
            .members_of_tenant(&tenant_not_exists_id)
            .await
            .expect("members_of_tenant");
        assert!(members.is_none());

        let user2_id = id_generator.next_oid().await.expect("generate id");
        let tenant_member: TenantRole<Oid> = TenantRole::System(SystemTenantRole::Member);
        manager
            .add_member_with_role_to_tenant(user2_id, tenant_member.clone(), tenant2_id)
            .await
            .expect("add member to tenant");

        let members = manager
            .members_of_tenant(&tenant2_id)
            .await
            .expect("members_of_tenant")
            .expect("tenant exists");
        assert_eq!(members.len(), 1);

        let tenant_member: TenantRole<Oid> = TenantRole::System(SystemTenantRole::Member);
        manager
            .add_member_with_role_to_tenant(user1_id, tenant_member.clone(), tenant2_id)
            .await
            .expect("add member to tenant");

        let members = manager
            .members_of_tenant(&tenant2_id)
            .await
            .expect("members_of_tenant")
            .expect("tenant exists");
        assert_eq!(members.len(), 2);

        let success = manager
            .remove_member_from_all_tenants(&user1_id)
            .await
            .expect("remove_member_from_all_tenants");
        assert!(success);

        let error = manager
            .member_role_of_tenant(&user1_id, &tenant1_id)
            .await
            .expect_err("member_role_of_tenant error");
        matches!(error, AuthError::MemberNotFound);

        let error = manager
            .member_role_of_tenant(&user1_id, &tenant2_id)
            .await
            .expect_err("member_role_of_tenant error");
        matches!(error, AuthError::MemberNotFound);

        let success = manager.drop_tenant(&tenant1_id).await.expect("drop tenant");
        assert!(success);

        let members = manager
            .members_of_tenant(&tenant1_id)
            .await
            .expect("members_of_tenant");
        assert!(members.is_none());

        let tenants = manager.tenants().await.expect("query tenants");
        assert_eq!(tenants.len(), 1);

        let success = manager.drop_tenant(&tenant2_id).await.expect("drop tenant");
        assert!(success);

        let members = manager
            .members_of_tenant(&tenant2_id)
            .await
            .expect("members_of_tenant");
        assert!(members.is_none());

        let tenants = manager.tenants().await.expect("query tenants");
        assert_eq!(tenants.len(), 0);
    }

    #[tokio::test]
    async fn test_create_custom_role_with_not_exists_tenant() {
        let mut manager = TenantRoleManager::default();
        let id_generator = MemoryOidGenerator::default();

        let role_name = "test_role".to_string();
        let system_role = SystemTenantRole::Member;
        let additiona_privileges = HashMap::default();
        let tenant_not_exists_id = id_generator.next_oid().await.expect("id_generator");
        let error = manager
            .create_custom_role_of_tenant(
                &tenant_not_exists_id,
                role_name,
                system_role,
                additiona_privileges,
            )
            .await
            .expect_err("create_custom_role_of_tenant error");
        matches!(error, AuthError::TenantNotFound);
    }

    #[tokio::test]
    async fn test_create_custom_role() {
        let mut manager = TenantRoleManager::default();
        let id_generator = MemoryOidGenerator::default();

        let tenant_id = id_generator.next_oid().await.expect("id_generator");
        manager
            .create_tenant(tenant_id)
            .await
            .expect("create_tenant");

        let role_name = "test_role".to_string();
        let system_role = SystemTenantRole::Member;
        let additiona_privileges = HashMap::default();
        manager
            .create_custom_role_of_tenant(
                &tenant_id,
                role_name.clone(),
                system_role,
                additiona_privileges,
            )
            .await
            .expect("create_custom_role_of_tenant");

        let custom_roles = manager
            .custom_roles_of_tenant(&tenant_id)
            .await
            .expect("custom_roles_of_tenant");
        let exists = custom_roles.iter().any(|e| e.name() == role_name);
        assert!(exists);
    }

    #[tokio::test]
    async fn test_grant_privilege_to_custom_role() {
        let mut manager = TenantRoleManager::default();
        let id_generator = MemoryOidGenerator::default();

        let tenant_id = id_generator.next_oid().await.expect("id_generator");
        manager
            .create_tenant(tenant_id)
            .await
            .expect("create_tenant");

        let role_name = "test_role".to_string();
        let system_role = SystemTenantRole::Member;
        let additiona_privileges = HashMap::default();
        manager
            .create_custom_role_of_tenant(
                &tenant_id,
                role_name.clone(),
                system_role,
                additiona_privileges,
            )
            .await
            .expect("create_custom_role_of_tenant");

        let database_id = id_generator.next_oid().await.expect("id_generator");
        let privilege = DatabasePrivilege::Write;
        manager
            .grant_privilege_to_custom_role_of_tenant(
                database_id,
                privilege.clone(),
                &role_name,
                &tenant_id,
            )
            .await
            .expect("grant_privilege_to_custom_role_of_tenant");

        {
            let role = manager
                .custom_role_of_tenant(&role_name, &tenant_id)
                .await
                .expect("custom_role_of_tenant")
                .expect("role exists");
            let privileges = role.to_privileges(&tenant_id);
            let expect_privilege = Privilege::TenantObject(
                TenantObjectPrivilege::Database(privilege, Some(database_id)),
                Some(tenant_id),
            );
            assert!(privileges.contains(&expect_privilege));
        }

        let success = manager
            .drop_custom_role_of_tenant(&role_name, &tenant_id)
            .await
            .expect("drop_custom_role_of_tenant");
        assert!(success);
        let role = manager
            .custom_role_of_tenant(&role_name, &tenant_id)
            .await
            .expect("custom_role_of_tenant");
        assert!(role.is_none());
    }

    #[tokio::test]
    async fn test_revoke_privilege_from_custom_role() {
        let mut manager = TenantRoleManager::default();
        let id_generator = MemoryOidGenerator::default();

        let tenant_id = id_generator.next_oid().await.expect("id_generator");
        manager
            .create_tenant(tenant_id)
            .await
            .expect("create_tenant");

        let role_name = "test_role".to_string();
        let system_role = SystemTenantRole::Member;
        let additiona_privileges = HashMap::default();
        manager
            .create_custom_role_of_tenant(
                &tenant_id,
                role_name.clone(),
                system_role,
                additiona_privileges,
            )
            .await
            .expect("create_custom_role_of_tenant");

        let database_id = id_generator.next_oid().await.expect("id_generator");
        let privilege = DatabasePrivilege::Write;
        manager
            .grant_privilege_to_custom_role_of_tenant(
                database_id,
                privilege.clone(),
                &role_name,
                &tenant_id,
            )
            .await
            .expect("grant_privilege_to_custom_role_of_tenant");

        {
            let role = manager
                .custom_role_of_tenant(&role_name, &tenant_id)
                .await
                .expect("custom_role_of_tenant")
                .expect("role exists");
            let privileges = role.to_privileges(&tenant_id);
            let expect_privilege = Privilege::TenantObject(
                TenantObjectPrivilege::Database(privilege.clone(), Some(database_id)),
                Some(tenant_id),
            );
            assert!(privileges.contains(&expect_privilege));
        }

        let success = manager
            .revoke_privilege_from_custom_role_of_tenant(
                &database_id,
                &privilege,
                &role_name,
                &tenant_id,
            )
            .await
            .expect("revoke_privilege_from_custom_role_of_tenant");
        assert!(success);

        {
            let role = manager
                .custom_role_of_tenant(&role_name, &tenant_id)
                .await
                .expect("custom_role_of_tenant")
                .expect("role exists");
            let privileges = role.to_privileges(&tenant_id);
            let expect_privilege = Privilege::TenantObject(
                TenantObjectPrivilege::Database(privilege.clone(), Some(database_id)),
                Some(tenant_id),
            );
            assert!(!privileges.contains(&expect_privilege));
        }

        let success = manager
            .revoke_privilege_from_custom_role_of_tenant(
                &database_id,
                &privilege,
                &role_name,
                &tenant_id,
            )
            .await
            .expect("revoke_privilege_from_custom_role_of_tenant");
        assert!(!success);
    }
}
