pub const CREATE_NODE_COMMAND: u32 = 1;
pub const DELETE_NODE_COMMAND: u32 = 2;
pub const CREATE_DATABASE_COMMAND: u32 = 3;
pub const DROP_DATABASE_COMMAND: u32 = 4;
pub const UPDATE_RETENTION_POLICY_COMMAND: u32 = 5;
pub const CREATE_BUCKET_COMMAND: u32 = 6;
pub const DELETE_BUCKET_COMMAND: u32 = 7;
pub const CREATE_USER_COMMAND: u32 = 8;
pub const DROP_USER_COMMAND: u32 = 9;
pub const UPDATE_USER_COMMAND: u32 = 10;
pub const CREATE_DATA_NODE_COMMAND: u32 = 11;
pub const UPDATE_DATA_NODE_COMMAND: u32 = 12;
pub const DELETE_DATA_NODE_COMMAND: u32 = 13;
pub const CREATE_META_NODE_COMMAND: u32 = 14;
pub const UPDATE_META_NODE_COMMAND: u32 = 15;
pub const DELETE_META_NODE_COMMAND: u32 = 16;
pub const DROP_VNODE_COMMAND: u32 = 17;
pub const UPDATE_VNODE_OWNERS_COMMAND: u32 = 18;
pub const WRITE_VNODE_POINT_COMMAND: u32 = 19;

#[derive(Debug, Clone)]
pub struct Resource {
    pub id: u64,
    pub cpu: u64,
    pub disk: u64,
    pub memory: u64,
}

#[derive(Debug, Clone)]
pub struct UserInfo {
    pub name: String,
    pub pwd: String,
    pub perm: u64, //read write admin bitmap
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub tcp_addr: String,
    pub http_addr: String,
    pub status: u64,
}

#[derive(Debug, Clone)]
pub struct RetentionPolicyInfo {
    pub database_duration: i64,
    pub bucket_duration: i64,
    pub replications: u32,
}

#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub id: u64,
    pub start_time: i64,
    pub end_time: i64,
    pub vnodes: Vec<VnodeInfo>,
}

impl BucketInfo {
    pub fn vnode_for(&self, id: u64) -> VnodeInfo {
        let index = id as usize % self.vnodes.len();
        return self.vnodes[index].clone();
    }
}

#[derive(Debug, Clone)]
pub struct VnodeInfo {
    pub id: u64,
    pub owners: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    pub name: String,
    pub policy: RetentionPolicyInfo,
    pub buckets: Vec<BucketInfo>,
}

#[derive(Debug, Clone)]
pub struct MetaData {
    pub version: u64,

    pub users: Vec<UserInfo>,
    pub dbs: Vec<DatabaseInfo>,
    pub data_nodes: Vec<NodeInfo>,
    pub meta_nodes: Vec<NodeInfo>,
}

impl MetaData {
    pub fn new() -> Self {
        Self {
            version: 0,
            users: vec![],
            dbs: vec![],
            data_nodes: vec![],
            meta_nodes: vec![],
        }
    }

    pub fn bucket_by_timestamp(&self, db_name: &String, ts: i64) -> Option<&BucketInfo> {
        if let Some(db) = self.dbs.iter().find(|db| db.name == *db_name) {
            if let Some(bucket) = db
                .buckets
                .iter()
                .find(|bucket| (ts >= bucket.start_time) && (ts < bucket.end_time))
            {
                return Some(bucket);
            }
        }

        return None;
    }
}
