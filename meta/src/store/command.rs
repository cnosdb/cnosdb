use models::meta_data::*;
use models::schema::DatabaseSchema;
use models::schema::TskvTableSchema;
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
