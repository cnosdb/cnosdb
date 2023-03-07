use std::fmt::Display;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::oid::Id;

pub trait PrivilegeChecker {
    fn check_privilege(&self, other: &Self) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Privilege<T> {
    Global(GlobalPrivilege<T>),
    // Some(T): tenantId
    // None: all tenants
    TenantObject(TenantObjectPrivilege, Option<T>),
}

impl<T> Display for Privilege<T>
where
    T: Id,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Global(g) => {
                write!(f, "{}", g)
            }
            Self::TenantObject(p, t) => match t {
                Some(t) => {
                    write!(f, "{} of tenant {}", p, t)
                }
                None => {
                    write!(f, "{} of all tenants", p)
                }
            },
        }
    }
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
pub enum GlobalPrivilege<T> {
    System,
    // Some(T): Administrative rights for the specify user, `T` represents the unique identifier of the user
    // None: Administrative rights for all user
    User(Option<T>),
    // Some(T): Administrative rights for the specify tenant, `T` represents the unique identifier of the tenant
    // None: Administrative rights for all tenants
    Tenant(Option<T>),
}

impl<T> Display for GlobalPrivilege<T>
where
    T: Id,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::System => {
                write!(f, "maintainer for system")
            }
            Self::User(u) => match u {
                Some(u) => {
                    write!(f, "maintainer for user {}", u)
                }
                None => {
                    write!(f, "maintainer for all users")
                }
            },
            Self::Tenant(t) => match t {
                Some(t) => {
                    write!(f, "maintainer for tenant {}", t)
                }
                None => {
                    write!(f, "maintainer for all tenants")
                }
            },
        }
    }
}

impl<T> PrivilegeChecker for GlobalPrivilege<T>
where
    T: Id,
{
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::User(None), Self::User(_)) => true,
            (Self::User(Some(lo)), Self::User(Some(ro))) => lo == ro,
            (Self::Tenant(None), Self::Tenant(_)) => true,
            (Self::Tenant(Some(lo)), Self::Tenant(Some(ro))) => lo == ro,
            (l, r) => l == r,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TenantObjectPrivilege {
    // All operation permissions related to system
    // e.g. kill querys of tenant
    System,
    // All operation permissions related to members
    MemberFull,
    // All operation permissions related to roles
    RoleFull,
    // T: database_name
    // None: all databases in this tenant
    Database(DatabasePrivilege, Option<String>),
}

impl Display for TenantObjectPrivilege {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::System => {
                write!(f, "system admin")
            }
            Self::MemberFull => {
                write!(f, "maintainer for all members")
            }
            Self::RoleFull => {
                write!(f, "maintainer for all roles")
            }
            Self::Database(p, db) => match db {
                Some(db) => {
                    write!(f, "{:?} on database {}", p, db)
                }
                None => {
                    write!(f, "{:?} on all databases", p)
                }
            },
        }
    }
}

impl PrivilegeChecker for TenantObjectPrivilege {
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::System, Self::System) => true,
            (Self::MemberFull, Self::MemberFull) => true,
            (Self::RoleFull, Self::RoleFull) => true,
            (Self::Database(s, None), Self::Database(o, _)) => s.check_privilege(o),
            (Self::Database(s, Some(s_t)), Self::Database(o, Some(o_t))) => {
                s_t == o_t && s.check_privilege(o)
            }
            (l, r) => l == r,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DatabasePrivilege {
    Read,
    Write,
    Full,
}

impl DatabasePrivilege {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Read => "Read",
            Self::Write => "Write",
            Self::Full => "All",
        }
    }
}

impl PrivilegeChecker for DatabasePrivilege {
    fn check_privilege(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Full, _) => true,
            (Self::Write, Self::Write | Self::Read) => true,
            (Self::Read, Self::Read) => true,
            (l, r) => l == r,
        }
    }
}
