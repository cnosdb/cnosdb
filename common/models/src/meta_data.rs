#[derive(Debug, Clone)]
pub struct CommandHeader {
    pub typ: u32,
    pub len: u32,
}

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
