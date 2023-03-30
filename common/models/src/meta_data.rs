use std::collections::HashMap;
use std::ffi::CString;
use std::io;
use std::mem::MaybeUninit;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::errors::check_err;
use crate::predicate::domain::TimeRange;
use crate::schema::{DatabaseSchema, TableSchema};

pub type VnodeId = u32;
pub type NodeId = u64;
pub type ReplicationSetId = u32;

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
    pub id: NodeId,
    pub grpc_addr: String,
    pub http_addr: String,
    pub disk_free: u64,
    pub is_cold: bool,
    pub status: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct BucketInfo {
    pub id: u32,
    pub start_time: i64,
    pub end_time: i64,
    pub shard_group: Vec<ReplicationSet>,
}

impl BucketInfo {
    pub fn vnode_for(&self, id: u64) -> ReplicationSet {
        let index = id as usize % self.shard_group.len();

        self.shard_group[index].clone()
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ReplicationSet {
    pub id: ReplicationSetId,
    pub vnodes: Vec<VnodeInfo>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct VnodeInfo {
    pub id: VnodeId,
    pub node_id: u64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct VnodeAllInfo {
    pub vnode_id: u32,
    pub node_id: u64,
    pub repl_set_id: u32,
    pub bucket_id: u32,
    pub db_name: String,
    pub tenant: String,

    pub start_time: i64,
    pub end_time: i64,
}

pub type DatabaseInfoRef = Arc<DatabaseInfo>;

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

impl DatabaseInfo {
    pub fn time_range(&self) -> TimeRange {
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for BucketInfo {
            start_time,
            end_time,
            ..
        } in &self.buckets
        {
            (*start_time < min_ts).then(|| min_ts = *start_time);
            (*end_time > max_ts).then(|| max_ts = *end_time);
        }

        TimeRange::new(min_ts, max_ts)
    }

    // return the min timestamp value database allowed to store
    pub fn time_to_expired(&self) -> i64 {
        self.schema.time_to_expired()
    }
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
            return Some(db.time_to_expired());
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
    } else if ts >= 0 {
        let floor = ts / duration;
        (
            floor * duration,
            (floor * duration).saturating_add(duration),
        )
    } else {
        let floor = (ts + 1) / duration;
        (
            (floor * duration).saturating_sub(duration),
            floor * duration,
        )
    }
}

pub fn allocation_replication_set(
    nodes: Vec<NodeInfo>,
    shards: u32,
    replica: u32,
    begin_seq: u32,
) -> (Vec<ReplicationSet>, u32) {
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
    let mut sort_nodes = nodes;
    sort_nodes.sort_by(|a, b| b.disk_free.cmp(&a.disk_free));

    for _ in 0..shards {
        let mut repl_set = ReplicationSet {
            id: incr_id,
            vnodes: vec![],
        };
        incr_id += 1;

        for _ in 0..replica {
            repl_set.vnodes.push(VnodeInfo {
                id: incr_id,
                node_id: sort_nodes.get((index % node_count) as usize).unwrap().id,
            });
            incr_id += 1;
            index += 1;
        }

        group.push(repl_set);
    }

    (group, incr_id - begin_seq)
}

pub fn get_disk_info(path_buf: &str) -> io::Result<u64> {
    let path = path_buf;

    let mut statfs = MaybeUninit::<libc::statfs>::zeroed();
    let c_storage_path = CString::new(path).unwrap();
    check_err(unsafe { libc::statfs(c_storage_path.as_ptr() as *const i8, statfs.as_mut_ptr()) })?;

    let statfs = unsafe { statfs.assume_init() };

    Ok(statfs.f_bsize as u64 * statfs.f_bfree)
}
