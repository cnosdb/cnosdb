use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::schema::{DatabaseSchema, TableSchema};

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct SysInfo {
    pub cpu_load: f64,
    pub disk_free: u64,
    pub mem_free: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ExpiredBucketInfo {
    pub tenant: String,
    pub database: String,
    pub bucket: BucketInfo,
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

        self.shard_group[index].clone()
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
    pub schema: DatabaseSchema,
    pub buckets: Vec<BucketInfo>,
    pub tables: HashMap<String, TableSchema>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct TenantMetaData {
    pub version: u64,
    pub users: HashMap<String, UserInfo>,
    pub dbs: HashMap<String, DatabaseInfo>,
}

impl TenantMetaData {
    pub fn new() -> Self {
        Self {
            version: 0,
            users: HashMap::new(),
            dbs: HashMap::new(),
        }
    }

    pub fn merge_into(&mut self, update: &TenantMetaData) {
        for (key, val) in update.users.iter() {
            self.users.insert(key.clone(), val.clone());
        }

        for (key, val) in update.dbs.iter() {
            let info = self
                .dbs
                .entry(key.clone())
                .or_insert_with(DatabaseInfo::default);

            if !val.schema.is_empty() {
                info.schema = val.schema.clone();
            }

            for item in val.buckets.iter() {
                match info.buckets.binary_search_by(|v| v.id.cmp(&item.id)) {
                    Ok(index) => info.buckets[index] = item.clone(),
                    Err(index) => info.buckets.insert(index, item.clone()),
                }
            }

            for (name, item) in val.tables.iter() {
                info.tables.insert(name.clone(), item.clone());
            }
        }
    }

    pub fn delete_from(&mut self, delete: &TenantMetaData) {
        for (key, _) in delete.users.iter() {
            self.users.remove(key);
        }

        for (key, val) in delete.dbs.iter() {
            if val.schema.is_empty() && val.buckets.is_empty() && val.tables.is_empty() {
                self.dbs.remove(key);
                continue;
            }

            let info = self
                .dbs
                .entry(key.clone())
                .or_insert_with(DatabaseInfo::default);
            for item in val.buckets.iter() {
                if let Ok(index) = info.buckets.binary_search_by(|v| v.id.cmp(&item.id)) {
                    info.buckets.remove(index);
                }
            }

            for (name, _) in val.tables.iter() {
                info.tables.remove(name);
            }
        }
    }

    pub fn table_schema(&self, db: &str, tab: &str) -> Option<TableSchema> {
        if let Some(info) = self.dbs.get(db) {
            if let Some(schema) = info.tables.get(tab) {
                return Some(schema.clone());
            }
        }

        None
    }

    pub fn database_min_ts(&self, name: &str) -> Option<i64> {
        if let Some(db) = self.dbs.get(name) {
            let ttl = db.schema.config.ttl_or_default().time_stamp();
            let now = crate::utils::now_timestamp();

            return Some(now - ttl);
        }

        None
    }

    pub fn bucket_by_timestamp(&self, db_name: &str, ts: i64) -> Option<&BucketInfo> {
        if let Some(db) = self.dbs.get(db_name) {
            if let Some(bucket) = db
                .buckets
                .iter()
                .find(|bucket| (ts >= bucket.start_time) && (ts < bucket.end_time))
            {
                return Some(bucket);
            }
        }

        None
    }

    pub fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> Vec<BucketInfo> {
        if let Some(db) = self.dbs.get(db_name) {
            let mut result = vec![];
            for item in db.buckets.iter() {
                if end < item.start_time || start > item.end_time {
                    continue;
                }

                result.push(item.clone());
            }

            return result;
        }

        vec![]
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
        incr_id += 1;

        for _ in 0..replica {
            repl_set.vnodes.push(VnodeInfo {
                id: incr_id,
                node_id: nodes.get((index % node_count) as usize).unwrap().id,
            });
            incr_id += 1;
            index += 1;
        }

        group.push(repl_set);
    }

    (group, incr_id - begin_seq)
}
