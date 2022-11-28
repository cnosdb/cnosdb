use models::meta_data::*;
use models::schema::TskvTableSchema;
use serde::Deserialize;
use serde::Serialize;

/******************* write command *************************/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteCommand {
    AddDataNode(String, NodeInfo),
    CreateDB(String, String, DatabaseInfo),
    CreateBucket {
        cluster: String,
        tenant: String,
        db: String,
        ts: i64,
    },

    CreateTable(String, String, TskvTableSchema),
    UpdateTable(String, String, TskvTableSchema),

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
    pub err_code: i32,
    pub err_msg: String,
}

impl StatusResponse {
    pub fn new(err_code: i32, err_msg: String) -> Self {
        Self { err_code, err_msg }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct TenaneMetaDataResp {
    pub err_code: i32,
    pub err_msg: String,
    pub meta_data: TenantMetaData,
}

impl TenaneMetaDataResp {
    pub fn to_string(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}
