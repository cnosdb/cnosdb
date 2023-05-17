#![allow(clippy::field_reassign_with_default)]

use std::collections::{HashMap, HashSet};

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::UserOptions;
use models::meta_data::*;
use models::oid::Oid;
use models::schema::{DatabaseSchema, TableSchema, TenantOptions};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use super::key_path::KeyPath;
use crate::limiter::local_request_limiter::LocalBucketRequest;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateVnodeReplSetArgs {
    pub cluster: String,
    pub tenant: String,
    pub db_name: String,
    pub bucket_id: u32,
    pub repl_id: u32,
    pub del_info: Vec<VnodeInfo>,
    pub add_info: Vec<VnodeInfo>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UpdateVnodeArgs {
    pub cluster: String,
    pub vnode_info: VnodeAllInfo,
}

/******************* write command *************************/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WriteCommand {
    // retain increment id  cluster, count
    RetainID(String, u32),

    UpdateVnodeReplSet(UpdateVnodeReplSetArgs),

    UpdateVnode(UpdateVnodeArgs),
    // cluster, node info
    AddDataNode(String, NodeInfo),

    //cluster, node metrics
    ReportNodeMetrics(String, NodeMetrics),

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
    // cluster, tenant, requests
    LimiterRequest {
        cluster: String,
        tenant: String,
        request: LocalBucketRequest,
    },
}

/******************* read command *************************/
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ReadCommand {
    DataNodes(String),              //cluster
    TenaneMetaData(String, String), // cluster tenant

    NodeMetrics(String), //cluster

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
pub const META_REQUEST_DB_NOT_FOUND: i32 = 11;

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
        serde_json::to_string(&self).unwrap_or("".to_string())
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

pub const ENTRY_LOG_TYPE_SET: i32 = 1;
pub const ENTRY_LOG_TYPE_DEL: i32 = 2;
pub const ENTRY_LOG_TYPE_NOP: i32 = 10;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct EntryLog {
    pub tye: i32,
    pub ver: u64,
    pub key: String,
    pub val: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct WatchData {
    pub full_sync: bool,
    pub min_ver: u64,
    pub max_ver: u64,
    pub entry_logs: Vec<EntryLog>,
}

impl WatchData {
    pub fn need_return(&self, base_ver: u64) -> bool {
        if self.full_sync {
            return true;
        }

        if !self.entry_logs.is_empty() {
            return true;
        }

        if base_ver + 100 < self.max_ver {
            return true;
        }

        false
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct CircleBuf {
    count: usize,
    writer: usize,
    capacity: usize,

    buf: Vec<EntryLog>,
}

impl CircleBuf {
    pub fn new(capacity: usize) -> Self {
        let mut buf = Vec::new();
        buf.resize(capacity, EntryLog::default());

        Self {
            buf,
            count: 0,
            writer: 0,
            capacity,
        }
    }

    pub fn append(&mut self, log: EntryLog) {
        self.buf[self.writer] = log;

        self.writer += 1;
        if self.writer == self.capacity {
            self.writer = 0;
        }

        if self.count < self.capacity {
            self.count += 1;
        }
    }

    pub fn is_empty(&self) -> bool {
        if self.count == 0 {
            return true;
        }

        false
    }

    pub fn min_version(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }

        let index = if self.count == self.capacity {
            self.writer
        } else {
            0
        };

        Some(self.buf[index].ver)
    }

    pub fn max_version(&self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }

        let index = if self.writer == 0 {
            self.capacity - 1
        } else {
            self.writer - 1
        };

        Some(self.buf[index].ver)
    }

    // -1: the logs is empty
    // -2: min version < ver
    pub fn find_index(&self, ver: u64) -> i32 {
        if self.is_empty() {
            return -1;
        }

        let mut index = self.writer;
        for _ in 0..self.count {
            index = if index == 0 {
                self.capacity - 1
            } else {
                index - 1
            };

            if self.buf[index].ver <= ver {
                return index.try_into().unwrap();
            }
        }

        -2
    }

    pub fn read_entrys<F>(&self, filter: F, index: usize) -> Vec<EntryLog>
    where
        F: Fn(&EntryLog) -> bool,
    {
        let mut entrys = vec![];

        let mut index = (index + 1) % self.capacity;
        while index != self.writer {
            let entry = self.buf.get(index).unwrap();
            if filter(entry) {
                entrys.push(entry.clone());
            }

            index = (index + 1) % self.capacity;
        }

        entrys
    }
}

pub struct Watch {
    pub logs: RwLock<CircleBuf>,
    pub sender: broadcast::Sender<()>,
}

impl Watch {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1);
        Self {
            sender,
            logs: RwLock::new(CircleBuf::new(8 * 1024)),
        }
    }

    pub fn writer_log(&self, log: EntryLog) {
        self.logs.write().append(log);

        let _ = self.sender.send(());
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.sender.subscribe()
    }

    pub fn min_version(&self) -> Option<u64> {
        self.logs.read().min_version()
    }
    pub fn max_version(&self) -> Option<u64> {
        self.logs.read().max_version()
    }

    // -1: the logs is empty
    // -2: min version < ver
    pub fn read_entry_logs(
        &self,
        cluster: &str,
        tenants: &HashSet<String>,
        base_ver: u64,
    ) -> (Vec<EntryLog>, i32) {
        let filter = |entry: &EntryLog| -> bool {
            if entry.key.starts_with(&KeyPath::data_nodes(cluster)) {
                return true;
            }

            if tenants.is_empty() {
                return false;
            }

            if !entry.key.starts_with(&KeyPath::cluster_prefix(cluster)) {
                return false;
            }

            if tenants.contains(&"".to_string()) {
                return true;
            }

            let prefix = KeyPath::tenants(cluster);
            if let Some(sub_str) = entry.key.strip_prefix(&prefix) {
                if let Some((tenant, _)) = sub_str.split_once('/') {
                    if tenants.contains(tenant) {
                        return true;
                    }
                }
            }

            false
        };

        self.read_start_version(filter, base_ver)
    }

    fn read_start_version<F>(&self, filter: F, base_ver: u64) -> (Vec<EntryLog>, i32)
    where
        F: Fn(&EntryLog) -> bool,
    {
        let logs = self.logs.read();
        let index = logs.find_index(base_ver);
        if index < 0 {
            return (vec![], index);
        }

        (logs.read_entrys(filter, index as usize), 0)
    }
}

impl Default for Watch {
    fn default() -> Self {
        Self::new()
    }
}

mod test {
    use std::collections::HashSet;
    use std::sync::Arc;

    use tokio::sync::RwLock;

    use crate::store::command::Watch;

    async fn _watch_data_test(watch: Arc<RwLock<Watch>>, cluster: &str, tenant: &str, ver: u64) {
        println!("======== {}.{}", cluster, tenant);

        loop {
            let mut chan = watch.read().await.subscribe();
            let _ = chan.recv().await;

            let logs = watch.read().await.read_entry_logs(
                cluster,
                &HashSet::from([tenant.to_string()]),
                ver,
            );
            println!("=== {}.{}; {:?}", cluster, tenant, logs);
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_watch() {
        use tokio::io::AsyncBufReadExt;

        let watch = Arc::new(RwLock::new(Watch::new()));

        let w_clone = watch.clone();
        tokio::spawn(_watch_data_test(w_clone, "c", "t", 100));

        let w_clone = watch.clone();
        tokio::spawn(_watch_data_test(w_clone, "c", "", 200));

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        loop {
            let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
            let mut line = String::new();
            let _ = reader.read_line(&mut line).await;
            let strs: Vec<&str> = line.split(' ').collect();

            let entry = crate::store::command::EntryLog {
                tye: 0,
                key: strs[0].to_string(),
                ver: serde_json::from_str::<u64>(strs[1]).unwrap(),
                val: "".to_string(),
            };

            watch.write().await.writer_log(entry.clone());
            println!("=== write {:?}", entry);
        }
    }
}
