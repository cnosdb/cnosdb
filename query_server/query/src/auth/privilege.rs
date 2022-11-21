use std::hash::Hash;

use models::oid::Id;

pub trait PrivilegeChecker {
    fn check_privilege(&self, other: &Self) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Privilege<T> {
    Global(GlobalPrivilege),
    // T: tenantId
    // None: all tenants
    TenantObject(TenantObjectPrivilege<T>, Option<T>),
}

impl<T: Id> PrivilegeChecker for Privilege<T> {
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Global(s), Self::Global(o)) => s.check_privilege(o),
            (Self::TenantObject(s, None), Self::TenantObject(o, _)) => s.check_privilege(o),
            (Self::TenantObject(s, Some(s_t)), Self::TenantObject(o, Some(o_t))) => {
                s_t == o_t && s.check_privilege(o)
            }
            (_, _) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GlobalPrivilege {
    User,
    Tenant,
}

impl PrivilegeChecker for GlobalPrivilege {
    fn check_privilege(&self, other: &Self) -> bool {
        self == other
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TenantObjectPrivilege<T> {
    MemberFull,
    RoleFull,
    // T: databaseId
    // None: all databases in this tenant
    Database(DatabasePrivilege, Option<T>),
}

impl<T: Id> PrivilegeChecker for TenantObjectPrivilege<T> {
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::MemberFull, Self::MemberFull) => true,
            (Self::RoleFull, Self::RoleFull) => true,
            (Self::Database(s, None), Self::Database(o, _)) => s.check_privilege(o),
            (Self::Database(s, Some(s_t)), Self::Database(o, Some(o_t))) => {
                s_t == o_t && s.check_privilege(o)
            }
            (_, _) => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatabasePrivilege {
    Read,
    Write,
    Full,
}

impl PrivilegeChecker for DatabasePrivilege {
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Full, _) => true,
            (Self::Write, Self::Write | Self::Read) => true,
            (Self::Read, Self::Read) => true,
            (_, _) => false,
        }
    }
}
