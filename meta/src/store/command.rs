use std::collections::HashMap;

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::SystemTenantRole;
use models::auth::role::TenantRoleIdentifier;
use models::auth::user::UserOptions;
use models::meta_data::*;
use models::oid::Oid;
use models::schema::TenantOptions;
use models::schema::{DatabaseSchema, TableSchema};
use serde::Deserialize;
use serde::Serialize;

/******************* write command *************************/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteCommand {
    AddDataNode(String, NodeInfo),
    CreateDB(String, String, DatabaseSchema),
    CreateBucket {
        cluster: String,
        tenant: String,
        db: String,
        ts: i64,
    },

    CreateTable(String, String, TableSchema),
    UpdateTable(String, String, TableSchema),

    // cluster, user_name, user_options, is_admin
    CreateUser(String, String, UserOptions, bool),
    // cluster, user_id, user_options
    AlterUser(String, String, UserOptions),
    // cluster, old_name, new_name
    RenameUser(String, String, String),
    // cluster, user_name
    DropUser(String, String),

    // cluster, tenant_name, tenant_options
    CreateTenant(String, String, TenantOptions),
    // cluster, tenant_name, tenant_options
    AlterTenant(String, String, TenantOptions),
    // cluster, old_name, new_name
    RenameTenant(String, String, String),
    // cluster, tenant_name
    DropTenant(String, String),

    // cluster, user_id, role, tenant_name
    AddMemberToTenant(String, Oid, TenantRoleIdentifier, String),
    // cluster, user_id, tenant_name
    RemoveMemberFromTenant(String, Oid, String),
    // cluster, user_id, role, tenant_name
    ReasignMemberRole(String, Oid, TenantRoleIdentifier, String),

    // cluster, role_name, sys_role, privileges, tenant_name
    CreateRole(
        String,
        String,
        SystemTenantRole,
        HashMap<String, DatabasePrivilege>,
        String,
    ),
    // cluster, role_name, tenant_name
    DropRole(String, String, String),
    // cluster, privileges, role_name, tenant_name
    GrantPrivileges(String, Vec<(DatabasePrivilege, String)>, String, String),
    // cluster, privileges, role_name, tenant_name
    RevokePrivileges(String, Vec<(DatabasePrivilege, String)>, String, String),

    Set {
        key: String,
        value: String,
    },
}

/******************* read command *************************/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadCommand {
    DataNodes(String),              //cluster
    TenaneMetaData(String, String), // cluster tenant

    // cluster, role_name, tenant_name
    CustomRole(String, String, String),
    // cluster, tenant_name
    CustomRoles(String, String),
    // cluster, tenant_name, user_id
    MemberRole(String, String, Oid),
    // cluster, tenant_name
    Members(String, String),
    // cluster, user_name
    User(String, String),
    // cluster
    Users(String),
    // cluster, tenant_name
    Tenant(String, String),
    // cluster
    Tenants(String),
}

/******************* response  *************************/
pub const META_REQUEST_FAILED: i32 = -1;
pub const META_REQUEST_SUCCESS: i32 = 0;
pub const META_REQUEST_DB_EXIST: i32 = 1;
pub const META_REQUEST_TABLE_EXIST: i32 = 2;
pub const META_REQUEST_USER_EXIST: i32 = 3;
pub const META_REQUEST_USER_NOT_FOUND: i32 = 4;
pub const META_REQUEST_TENANT_EXIST: i32 = 5;
pub const META_REQUEST_TENANT_NOT_FOUND: i32 = 6;
pub const META_REQUEST_ROLE_EXIST: i32 = 7;
pub const META_REQUEST_ROLE_NOT_FOUND: i32 = 8;
pub const META_REQUEST_PRIVILEGE_EXIST: i32 = 9;
pub const META_REQUEST_PRIVILEGE_NOT_FOUND: i32 = 10;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct StatusResponse {
    pub code: i32,
    pub msg: String,
}

impl StatusResponse {
    pub fn new(code: i32, msg: String) -> Self {
        Self { code, msg }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TenaneMetaDataResp {
    pub status: StatusResponse,
    pub data: TenantMetaData,
}

impl TenaneMetaDataResp {
    pub fn new(code: i32, msg: String) -> Self {
        Self {
            status: StatusResponse::new(code, msg),
            data: TenantMetaData::new(),
        }
    }

    pub fn new_from_data(code: i32, msg: String, data: TenantMetaData) -> Self {
        let mut rsp = TenaneMetaDataResp::new(code, msg);
        rsp.data = data;

        rsp
    }
}

impl ToString for TenaneMetaDataResp {
    fn to_string(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum CommonResp<T> {
    Ok(T),
    Err(StatusResponse),
}

impl<T> ToString for CommonResp<T>
where
    T: Serialize,
{
    fn to_string(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}
