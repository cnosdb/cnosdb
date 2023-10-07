use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::auth::role::{CustomTenantRole, TenantRoleIdentifier};
use crate::node_info::NodeStatus;
use crate::oid::Oid;
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

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Default, Clone)]
pub enum NodeAttribute {
    #[default]
    Hot,
    Cold,
}

impl fmt::Display for NodeAttribute {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NodeAttribute::Hot => write!(f, "HOT"),
            NodeAttribute::Cold => write!(f, "COLD"),
        }
    }
}

impl From<String> for NodeAttribute {
    fn from(node_attribute: String) -> Self {
        match node_attribute.to_uppercase().as_str() {
            "HOT" => NodeAttribute::Hot,
            "COLD" => NodeAttribute::Cold,
            _ => panic!("Invalid NodeAttribute:{}", node_attribute),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct NodeInfo {
    pub id: NodeId,
    pub grpc_addr: String,
    pub http_addr: String,
    pub attribute: NodeAttribute,
}

impl NodeInfo {
    pub fn is_cold(&self) -> bool {
        self.attribute == NodeAttribute::Cold
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct NodeMetrics {
    pub id: NodeId,
    pub disk_free: u64,
    pub time: i64,
    pub status: NodeStatus,
}

impl NodeMetrics {
    pub fn is_healthy(&self) -> bool {
        self.status == NodeStatus::Healthy
    }
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

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct ReplicationSet {
    pub id: ReplicationSetId,
    pub leader_node_id: NodeId,
    pub leader_vnode_id: VnodeId,
    pub vnodes: Vec<VnodeInfo>,
}

impl ReplicationSet {
    pub fn new(
        id: ReplicationSetId,
        leader_node_id: NodeId,
        leader_vnode_id: VnodeId,
        vnodes: Vec<VnodeInfo>,
    ) -> Self {
        Self {
            id,
            vnodes,
            leader_node_id,
            leader_vnode_id,
        }
    }

    pub fn vnode(&self, id: VnodeId) -> Option<VnodeInfo> {
        for vnode in &self.vnodes {
            if vnode.id == id {
                return Some(vnode.clone());
            }
        }

        None
    }

    pub fn by_node_id(&self, id: NodeId) -> Option<VnodeInfo> {
        for vnode in &self.vnodes {
            if vnode.node_id == id {
                return Some(vnode.clone());
            }
        }

        None
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct VnodeInfo {
    pub id: VnodeId,
    pub node_id: NodeId,
    #[serde(default = "Default::default")]
    pub status: VnodeStatus,
}

impl VnodeInfo {
    pub fn new(id: VnodeId, node_id: NodeId) -> Self {
        Self {
            id,
            node_id,
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VnodeStatus {
    #[default]
    Running,
    Copying,
    Broken,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct VnodeAllInfo {
    pub vnode_id: VnodeId,
    pub node_id: NodeId,
    pub repl_set_id: u32,
    pub bucket_id: u32,
    pub db_name: String,
    pub tenant: String,
    pub status: VnodeStatus,
    pub start_time: i64,
    pub end_time: i64,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ReplicaAllInfo {
    pub bucket_id: u32,
    pub db_name: String,
    pub tenant: String,
    pub start_time: i64,
    pub end_time: i64,
    pub replica_set: ReplicationSet,
}

impl VnodeAllInfo {
    pub fn set_status(&mut self, status: VnodeStatus) {
        self.status = status;
    }
}

impl From<VnodeAllInfo> for VnodeInfo {
    fn from(value: VnodeAllInfo) -> Self {
        VnodeInfo {
            id: value.vnode_id,
            node_id: value.node_id,
            status: value.status,
        }
    }
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
    // db_name -> database_info
    pub dbs: HashMap<String, DatabaseInfo>,
    pub roles: HashMap<String, CustomTenantRole<Oid>>,
    pub members: HashMap<String, TenantRoleIdentifier>,
}

impl TenantMetaData {
    pub fn new() -> Self {
        Self {
            version: 0,
            dbs: HashMap::new(),
            roles: HashMap::new(),
            members: HashMap::new(),
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
    let mut index = 0;
    let mut group = vec![];

    for _ in 0..shards {
        let mut repl_set = ReplicationSet {
            id: incr_id,
            vnodes: vec![],
            leader_node_id: 0,
            leader_vnode_id: 0,
        };
        incr_id += 1;

        for _ in 0..replica {
            repl_set.vnodes.push(VnodeInfo::new(
                incr_id,
                nodes.get((index % node_count) as usize).unwrap().id,
            ));
            incr_id += 1;
            index += 1;
        }
        repl_set.leader_vnode_id = repl_set.vnodes[0].id;
        repl_set.leader_node_id = repl_set.vnodes[0].node_id;

        group.push(repl_set);
    }

    (group, incr_id - begin_seq)
}

pub fn get_disk_info(path: &str) -> std::io::Result<u64> {
    use std::mem::MaybeUninit;

    #[cfg(unix)]
    {
        use std::ffi::CString;

        use crate::errors::check_err;

        let mut statfs = MaybeUninit::<libc::statfs>::zeroed();
        let c_storage_path = CString::new(path).unwrap();
        check_err(unsafe {
            libc::statfs(
                c_storage_path.as_ptr() as *const libc::c_char,
                statfs.as_mut_ptr(),
            )
        })?;

        let statfs = unsafe { statfs.assume_init() };

        Ok(statfs.f_bsize as u64 * statfs.f_bavail)
    }

    #[cfg(windows)]
    {
        use std::ffi::OsStr;

        use windows::core::HSTRING;
        use windows::Win32::Storage::FileSystem::{
            GetDiskSpaceInformationW, DISK_SPACE_INFORMATION,
        };

        let hdir = HSTRING::from(OsStr::new(path));
        let mut disk_space_info = MaybeUninit::<DISK_SPACE_INFORMATION>::zeroed();
        let disk_space_info = unsafe {
            GetDiskSpaceInformationW(&hdir, disk_space_info.as_mut_ptr())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
            disk_space_info.assume_init()
        };
        Ok(disk_space_info.ActualAvailableAllocationUnits
            * (disk_space_info.SectorsPerAllocationUnit * disk_space_info.BytesPerSector) as u64)
    }
}
