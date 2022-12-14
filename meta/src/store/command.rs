#![allow(clippy::field_reassign_with_default)]

use std::collections::HashMap;

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::SystemTenantRole;
use models::auth::role::TenantRoleIdentifier;
use models::auth::user::UserOptions;
use models::meta_data::*;
use models::oid::Oid;
use models::schema::TenantOptions;
use models::schema::TskvTableSchema;
use models::schema::{DatabaseSchema, TableSchema};

use serde::Deserialize;
use serde::Serialize;

/******************* write command *************************/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteCommand {
    // cluster, node info
    AddDataNode(String, NodeInfo),

    // cluster, tenant, db schema
    CreateDB(String, String, DatabaseSchema),

    // cluster, tenant, db schema
    AlterDB(String, String, DatabaseSchema),

    // cluster, tenant, db name
    DropDB(String, String, String),

    // cluster, tenant, db name, timestamp
    CreateBucket(String, String, String, i64),

    // cluster, tenant, db name, id
    DeleteBucket(String, String, String, u32),

    // cluster, tenant, table schema
    CreateTable(String, String, TableSchema),
    UpdateTable(String, String, TableSchema),
    // cluster, tenant, db name, table name
    DropTable(String, String, String, String),

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

impl ToString for StatusResponse {
    fn to_string(&self) -> String {
        serde_json::to_string(&self).unwrap()
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TenantMetaDataDelta {
    pub full_load: bool,
    pub status: StatusResponse,

    pub ver_range: (u64, u64),
    pub update: TenantMetaData,
    pub delete: TenantMetaData,
}

impl TenantMetaDataDelta {
    pub fn update_version(&mut self, ver: u64) {
        if self.ver_range.0 == 0 {
            self.ver_range.0 = ver;
        }

        if self.ver_range.1 < ver {
            self.ver_range.1 = ver;
        }
    }

    pub fn create_or_update_user(&mut self, ver: u64, user: &UserInfo) {
        self.update_version(ver);

        self.update.users.insert(user.name.clone(), user.clone());

        self.delete.users.remove(&user.name);
    }

    pub fn delete_user(&mut self, ver: u64, user: &str) {
        self.update_version(ver);

        self.update.users.remove(user);

        self.delete
            .users
            .insert(user.to_string(), UserInfo::default());
    }

    pub fn create_db(&mut self, ver: u64, schema: &DatabaseSchema) {
        self.update_version(ver);

        let db_info = self
            .update
            .dbs
            .entry(schema.database_name().to_string())
            .or_insert_with(DatabaseInfo::default);
        db_info.schema = schema.clone();

        self.delete.dbs.remove(&schema.database_name().to_string());
    }

    pub fn delete_db(&mut self, ver: u64, name: &str) {
        self.update_version(ver);

        self.update.dbs.remove(name);

        self.delete
            .dbs
            .insert(name.to_string(), DatabaseInfo::default());
    }

    pub fn update_db_schema(&mut self, ver: u64, schema: &DatabaseSchema) {
        self.update_version(ver);

        let db_info = self
            .update
            .dbs
            .entry(schema.database_name().to_string())
            .or_insert_with(DatabaseInfo::default);
        db_info.schema = schema.clone();
    }

    pub fn create_or_update_table(&mut self, ver: u64, schema: &TableSchema) {
        self.update_version(ver);

        let db_info = self
            .update
            .dbs
            .entry(schema.db())
            .or_insert_with(DatabaseInfo::default);

        db_info.tables.insert(schema.name(), schema.clone());

        if let Some(info) = self.delete.dbs.get_mut(&schema.db()) {
            info.tables.remove(&schema.name());
        }
    }

    pub fn delete_table(&mut self, ver: u64, db: &str, table: &str) {
        self.update_version(ver);

        if let Some(info) = self.update.dbs.get_mut(db) {
            info.tables.remove(table);
        }

        let db_info = self
            .delete
            .dbs
            .entry(db.to_string())
            .or_insert_with(DatabaseInfo::default);

        db_info.tables.insert(
            table.to_string(),
            TableSchema::TsKvTableSchema(TskvTableSchema::default()),
        );
    }

    pub fn create_or_update_bucket(&mut self, ver: u64, db: &str, bucket: &BucketInfo) {
        self.update_version(ver);

        let db_info = self
            .update
            .dbs
            .entry(db.to_string())
            .or_insert_with(DatabaseInfo::default);
        match db_info.buckets.binary_search_by(|v| v.id.cmp(&bucket.id)) {
            Ok(index) => db_info.buckets[index] = bucket.clone(),
            Err(index) => db_info.buckets.insert(index, bucket.clone()),
        }

        if let Some(info) = self.delete.dbs.get_mut(db) {
            if let Ok(index) = info.buckets.binary_search_by(|v| v.id.cmp(&bucket.id)) {
                info.buckets.remove(index);
            }
        }
    }

    pub fn delete_bucket(&mut self, ver: u64, db: &str, id: u32) {
        self.update_version(ver);

        if let Some(info) = self.update.dbs.get_mut(db) {
            if let Ok(index) = info.buckets.binary_search_by(|v| v.id.cmp(&id)) {
                info.buckets.remove(index);
            }
        }

        let db_info = self
            .delete
            .dbs
            .entry(db.to_string())
            .or_insert_with(DatabaseInfo::default);
        let mut bucket = BucketInfo::default();
        bucket.id = id;
        match db_info.buckets.binary_search_by(|v| v.id.cmp(&id)) {
            Ok(index) => db_info.buckets[index] = bucket,
            Err(index) => db_info.buckets.insert(index, bucket),
        }
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
