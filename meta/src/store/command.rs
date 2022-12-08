#![allow(clippy::field_reassign_with_default)]

use models::meta_data::*;
use models::schema::{DatabaseSchema, TableSchema, TskvTableSchema};
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
}

/******************* response  *************************/
pub const META_REQUEST_FAILED: i32 = -1;
pub const META_REQUEST_SUCCESS: i32 = 0;
pub const META_REQUEST_DB_EXIST: i32 = 1;
pub const META_REQUEST_TABLE_EXIST: i32 = 2;

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
