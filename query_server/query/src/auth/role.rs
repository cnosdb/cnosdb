use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    hash::Hash,
};

use models::oid::{Id, Identifier};

use super::{
    privilege::{DatabasePrivilege, GlobalPrivilege, Privilege, TenantObjectPrivilege},
    Result,
};

pub enum UserRole<T> {
    // 拥有对整个数据库实例的最高级权限
    // 支持用户管理和组织管理，但是不能管理组织中的内容
    Dba,
    // tenantId -> rogRole
    Public(HashMap<T, TenantRole<T>>),
}

impl<T: Id> UserRole<T> {
    pub fn to_privileges(&self) -> HashSet<Privilege<T>> {
        match self {
            Self::Dba => vec![
                Privilege::Global(GlobalPrivilege::Tenant),
                Privilege::Global(GlobalPrivilege::User),
                Privilege::TenantObject(TenantObjectPrivilege::MemberFull, None),
                Privilege::TenantObject(TenantObjectPrivilege::RoleFull, None),
                Privilege::TenantObject(
                    TenantObjectPrivilege::Database(DatabasePrivilege::Full, None),
                    None,
                ),
            ]
            .into_iter()
            .collect(),
            Self::Public(roles) => roles
                .iter()
                .map(|(tenant_id, tenant_role)| tenant_role.to_privileges(tenant_id))
                .reduce(|a, b| a.union(&b).cloned().collect())
                .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TenantRole<T> {
    System(SystemTenantRole),
    Custom(CustomTenantRoleRef<T>),
}

impl<T: Id> TenantRole<T> {
    pub fn to_privileges(&self, tenant_id: &T) -> HashSet<Privilege<T>> {
        match self {
            Self::System(e) => e.to_privileges(tenant_id),
            Self::Custom(e) => e.borrow().clone().to_privileges(tenant_id),
        }
    }
}

impl<T> Eq for TenantRole<T> where T: Eq {}

impl<T> PartialEq for TenantRole<T>
where
    T: Eq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::System(l0), Self::System(r0)) => l0 == r0,
            (Self::Custom(l0), Self::Custom(r0)) => l0.borrow().id() == r0.borrow().id(),
            (_, _) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SystemTenantRole {
    Owner,
    Member,
}

impl SystemTenantRole {
    pub fn to_privileges<T>(&self, tenant_id: &T) -> HashSet<Privilege<T>>
    where
        T: Id,
    {
        match self {
            Self::Owner => vec![
                Privilege::TenantObject(TenantObjectPrivilege::MemberFull, Some(tenant_id.clone())),
                Privilege::TenantObject(TenantObjectPrivilege::RoleFull, Some(tenant_id.clone())),
                Privilege::TenantObject(
                    TenantObjectPrivilege::Database(DatabasePrivilege::Full, None),
                    Some(tenant_id.clone()),
                ),
            ]
            .into_iter()
            .collect(),
            Self::Member => vec![Privilege::TenantObject(
                TenantObjectPrivilege::Database(DatabasePrivilege::Read, None),
                Some(tenant_id.clone()),
            )]
            .into_iter()
            .collect(),
        }
    }
}

pub type CustomTenantRoleRef<T> = Box<RefCell<CustomTenantRole<T>>>;

#[derive(Debug, Clone)]
pub struct CustomTenantRole<T> {
    id: T,
    name: String,
    system_role: SystemTenantRole,
    // databaseId -> privileges
    // only add database privilege
    additiona_privileges: HashMap<T, DatabasePrivilege>,
}

impl<T> CustomTenantRole<T> {
    pub fn new(
        id: T,
        name: String,
        system_role: SystemTenantRole,
        // databaseId -> privileges
        // only add database privilege
        additiona_privileges: HashMap<T, DatabasePrivilege>,
    ) -> Self {
        Self {
            id,
            name,
            system_role,
            additiona_privileges,
        }
    }
}

impl<T: Id> CustomTenantRole<T> {
    pub fn to_privileges(&self, tenant_id: &T) -> HashSet<Privilege<T>> {
        let privileges = self.system_role.to_privileges(tenant_id);

        let additiona_privileges = self
            .additiona_privileges
            .iter()
            .map(|(db_id, privilege)| {
                Privilege::TenantObject(
                    TenantObjectPrivilege::Database(privilege.clone(), Some(db_id.clone())),
                    Some(tenant_id.clone()),
                )
            })
            .collect::<HashSet<Privilege<T>>>();

        privileges.union(&additiona_privileges).cloned().collect()
    }

    pub fn grant_privilege(
        &mut self,
        database_ident: T,
        privilege: DatabasePrivilege,
    ) -> Result<()> {
        self.additiona_privileges.insert(database_ident, privilege);

        Ok(())
    }

    pub fn revoke_privilege(
        &mut self,
        database_ident: &T,
        privilege: &DatabasePrivilege,
    ) -> Result<bool> {
        if let Some(p) = self.additiona_privileges.get(database_ident) {
            if p == privilege {
                Ok(self.additiona_privileges.remove(database_ident).is_some())
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }
}

impl<T> Identifier<T> for CustomTenantRole<T> {
    fn id(&self) -> &T {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }
}
