use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Resource {
    pub id: u64,
    pub cpu: u64,
    pub disk: u64,
    pub memory: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct UserInfo {
    pub name: String,
    pub pwd: String,
    pub perm: u64, //read write admin bitmap
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub tcp_addr: String,
    pub http_addr: String,
    pub status: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct BucketInfo {
    pub id: u32,
    pub start_time: i64,
    pub end_time: i64,
    pub shard_group: Vec<ReplcationSet>,
}

impl BucketInfo {
    pub fn vnode_for(&self, id: u64) -> ReplcationSet {
        let index = id as usize % self.shard_group.len();
        return self.shard_group[index].clone();
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ReplcationSet {
    pub id: u32,
    pub vnodes: Vec<VnodeInfo>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct VnodeInfo {
    pub id: u32,
    pub node_id: u64,
}

// CREATE DATABASE <database_name>
// [WITH [TTL <duration>]
// [SHARD <n>]
// [VNODE_DURATION <duration>]
// [REPLICA <n>]
// [PRECISION {'ms' | 'us' | 'ns'}]]
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct DatabaseInfo {
    pub name: String,
    pub shard: u32,
    pub ttl: i64,
    pub vnode_duration: i64,
    pub replications: u32,
    pub buckets: Vec<BucketInfo>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct TenantMetaData {
    pub version: u64,
    pub users: HashMap<String, UserInfo>,
    pub dbs: HashMap<String, DatabaseInfo>,
    pub data_nodes: HashMap<String, NodeInfo>,
}

impl TenantMetaData {
    pub fn new() -> Self {
        Self {
            version: 0,
            users: HashMap::new(),
            dbs: HashMap::new(),
            data_nodes: HashMap::new(),
        }
    }

    pub fn database_min_ts(&self, name: &String) -> Option<i64> {
        if let Some(db) = self.dbs.get(name) {
            if db.ttl == 0 {
                return Some(0);
            }

            let now = crate::utils::now_timestamp();
            return Some(now - db.ttl);
        }

        None
    }

    pub fn bucket_by_timestamp(&self, db_name: &String, ts: i64) -> Option<&BucketInfo> {
        if let Some(db) = self.dbs.get(db_name) {
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

pub fn get_time_range(ts: i64, duration: i64) -> (i64, i64) {
    if duration <= 0 {
        (std::i64::MIN, std::i64::MAX)
    } else {
        (
            (ts / duration) * duration,
            (ts / duration) * duration + duration,
        )
    }
}

pub fn allocation_replication_set(
    nodes: Vec<NodeInfo>,
    shards: u32,
    replica: u32,
    begin_seq: u32,
) -> (Vec<ReplcationSet>, u32) {
    let node_count = nodes.len() as u32;
    let mut replica = replica;
    if replica == 0 {
        replica = 1
    } else if replica > node_count {
        replica = node_count
    }

    let mut incr_id = begin_seq;

    let mut index = begin_seq;
    let mut group = vec![];
    for _ in 0..shards {
        let mut repl_set = ReplcationSet {
            id: incr_id,
            vnodes: vec![],
        };
        incr_id = incr_id + 1;

        for _ in 0..replica {
            repl_set.vnodes.push(VnodeInfo {
                id: incr_id,
                node_id: nodes.get((index % node_count) as usize).unwrap().id,
            });
            incr_id = incr_id + 1;
            index = index + 1;
        }

        group.push(repl_set);
    }

    (group, incr_id - begin_seq)
}
