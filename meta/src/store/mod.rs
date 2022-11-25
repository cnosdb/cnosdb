#![allow(clippy::module_inception)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::Mutex;

use crate::ExampleTypeConfig;
use crate::NodeId;
use models::schema::TskvTableSchema;
use openraft::async_trait::async_trait;
use openraft::storage::LogState;
use openraft::storage::Snapshot;
use openraft::AnyError;
use openraft::EffectiveMembership;
use openraft::Entry;
use openraft::EntryPayload;
use openraft::ErrorSubject;
use openraft::ErrorVerb;
use openraft::LogId;
use openraft::RaftLogReader;
use openraft::RaftSnapshotBuilder;
use openraft::RaftStorage;
use openraft::SnapshotMeta;
use openraft::StateMachineChanges;
use openraft::StorageError;
use openraft::StorageIOError;
use openraft::Vote;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_str;
use sled::{Db, IVec};
use tokio::sync::RwLock;
use trace::info;
use tracing;

pub mod config;
pub mod store;

use crate::store::config::Config;

use models::{meta_data::*, utils};

#[derive(Debug)]
pub struct SnapshotInfo {
    pub meta: SnapshotMeta<NodeId>,
    pub data: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum KvReq {
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

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct KvResp {
    pub err_code: i32,
    pub err_msg: String,

    pub meta_data: TenantMetaData,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineContent {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: EffectiveMembership<NodeId>,
    pub data: BTreeMap<String, String>,
    pub sequance: u64,
}

pub fn children_fullpath(path: &str, map: &BTreeMap<String, String>) -> Vec<String> {
    let mut path = path.to_owned();
    if !path.ends_with('/') {
        path.push('/');
    }

    let mut list = vec![];
    for (key, _) in map.range(path.clone()..) {
        match key.strip_prefix(path.as_str()) {
            Some(val) => {
                if val.find('/').is_some() {
                    continue;
                }
                if val.is_empty() {
                    continue;
                }

                list.push(key.clone());
            }

            None => break,
        }
    }

    list
}

fn fetch_and_add_incr_id(cluster: &String, map: &mut BTreeMap<String, String>, count: u32) -> u32 {
    let id_key = KeyPath::incr_id(cluster);

    let mut id_str = "1".to_string();
    if let Some(val) = map.get(&id_key) {
        id_str = val.clone();
    }
    let id_num = from_str::<u32>(&id_str).unwrap_or(1);

    map.insert(id_key, (id_num + count).to_string());

    id_num
}

pub fn get_struct<'a, T: Deserialize<'a>>(
    key: &String,
    map: &'a BTreeMap<String, String>,
) -> Option<T> {
    let val = map.get(key)?;
    let info: T = serde_json::from_str(val).ok()?;

    Some(info)
}

pub fn children_data<'a, T: Deserialize<'a>>(
    path: &str,
    map: &'a BTreeMap<String, String>,
) -> HashMap<String, T> {
    let mut path = path.to_owned();
    if !path.ends_with('/') {
        path.push('/');
    }

    let mut result = HashMap::new();
    for it in children_fullpath(&path, map).iter() {
        if let Some(val) = get_struct::<T>(it, map) {
            if let Some(key) = it.strip_prefix(path.as_str()) {
                result.insert(key.to_string(), val);
            }
        }
    }

    result
}

// **    /cluster_name/auto_incr_id -> id
// **    /cluster_name/data_nodes/node_id -> [NodeInfo] 集群、数据节点等信息
// **    /cluster_name/tenant_name/users/name -> [UserInfo] 租户下用户信息、访问权限等
// **    /cluster_name/tenant_name/dbs/db_name -> [DatabaseInfo] db相关信息、保留策略等
// **    /cluster_name/tenant_name/dbs/db_name/buckets/id -> [BucketInfo] bucket相关信息
// **    /cluster_name/tenant_name/dbs/db_name/schemas/name -> [BucketInfo] schema相关信息
pub struct KeyPath {}
impl KeyPath {
    pub fn incr_id(cluster: &String) -> String {
        format!("/{}/auto_incr_id", cluster)
    }

    pub fn data_nodes(cluster: &String) -> String {
        format!("/{}/data_nodes", cluster)
    }

    pub fn data_node_id(cluster: &String, id: u64) -> String {
        format!("/{}/data_nodes/{}", cluster, id)
    }

    pub fn tenant_users(cluster: &String, tenant: &String) -> String {
        format!("/{}/{}/users", cluster, tenant)
    }

    pub fn tenant_user_name(cluster: &String, tenant: &String, name: &String) -> String {
        format!("/{}/{}/users/{}", cluster, tenant, name)
    }

    pub fn tenant_dbs(cluster: &String, tenant: &String) -> String {
        format!("/{}/{}/dbs", cluster, tenant)
    }

    pub fn tenant_db_name(cluster: &String, tenant: &String, db: &String) -> String {
        format!("/{}/{}/dbs/{}", cluster, tenant, db)
    }

    pub fn tenant_db_buckets(cluster: &String, tenant: &String, db: &String) -> String {
        format!("/{}/{}/dbs/{}/buckets", cluster, tenant, db)
    }

    pub fn tenant_bucket_id(cluster: &String, tenant: &String, db: &String, id: u32) -> String {
        format!("/{}/{}/dbs/{}/buckets/{}", cluster, tenant, db, id)
    }

    pub fn tenant_schemas(cluster: &String, tenant: &String, db: &String) -> String {
        format!("/{}/{}/dbs/{}/schemas", cluster, tenant, db)
    }

    pub fn tenant_schema_name(
        cluster: &String,
        tenant: &String,
        db: &String,
        name: &String,
    ) -> String {
        format!("/{}/{}/dbs/{}/schemas/{}", cluster, tenant, db, name)
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachine {
    pub last_applied_log: Option<LogId<NodeId>>,
    pub last_membership: EffectiveMembership<NodeId>,
    pub data: BTreeMap<String, String>,
}

impl StateMachine {
    pub fn to_content(&self) -> StateMachineContent {
        StateMachineContent {
            last_applied_log: self.last_applied_log,
            last_membership: self.last_membership.clone(),
            data: self.data.clone(),
            sequance: 0,
        }
    }

    pub fn from_content(&mut self, content: &StateMachineContent) {
        self.last_applied_log = content.last_applied_log;
        self.last_membership = content.last_membership.clone();
        self.data = content.data.clone();
    }

    pub fn to_tenant_meta_data(&self, cluster: &String, tenant: &String) -> TenantMetaData {
        let mut meta = TenantMetaData::new();

        if let Some(val) = self.last_applied_log {
            meta.version = val.index
        }

        meta.users = children_data::<UserInfo>(&KeyPath::tenant_users(cluster, tenant), &self.data);
        meta.data_nodes = children_data::<NodeInfo>(&KeyPath::data_nodes(cluster), &self.data);

        meta.dbs = children_data::<DatabaseInfo>(&KeyPath::tenant_dbs(cluster, tenant), &self.data);
        for (key, val) in meta.dbs.iter_mut() {
            let buckets = children_data::<BucketInfo>(
                &KeyPath::tenant_db_buckets(cluster, tenant, key),
                &self.data,
            );
            val.buckets = buckets.into_values().collect();
        }

        meta
    }
}

#[derive(Debug)]
pub struct Store {
    last_purged_log_id: RwLock<Option<LogId<NodeId>>>,

    /// The Raft log.
    pub log: sled::Tree,

    /// The Raft state machine.
    pub state_machine: RwLock<StateMachine>,

    /// The current granted vote.
    vote: sled::Tree,

    snapshot_idx: Arc<Mutex<u64>>,

    current_snapshot: RwLock<Option<SnapshotInfo>>,

    config: Config,

    pub node_id: NodeId,
}

fn get_sled_db(config: Config, node_id: NodeId) -> Db {
    let db_path = format!(
        "{}/{}-{}.binlog",
        config.journal_path, config.instance_prefix, node_id
    );
    let db = sled::open(db_path.clone()).unwrap();
    tracing::debug!("get_sled_db: created log at: {:?}", db_path);
    db
}

impl Store {
    pub fn open_create(node_id: NodeId) -> Store {
        tracing::info!("open_create, node_id: {}", node_id);

        let config = Config::default();

        let db = get_sled_db(config.clone(), node_id);

        let log = db
            .open_tree(format!("journal_entities_{}", node_id))
            .unwrap();

        let vote = db.open_tree(format!("votes_{}", node_id)).unwrap();

        let current_snapshot = RwLock::new(None);

        Store {
            last_purged_log_id: Default::default(),
            config,
            node_id,
            log,
            state_machine: Default::default(),
            vote,
            snapshot_idx: Arc::new(Mutex::new(0)),
            current_snapshot,
        }
    }
}

//Store trait for restore things from snapshot and log
#[async_trait]
pub trait Restore {
    async fn restore(&mut self);
}

#[async_trait]
impl Restore for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn restore(&mut self) {
        tracing::debug!("restore");
        let log = &self.log;

        let first = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<ExampleTypeConfig>>(&*val)
                    .unwrap()
                    .log_id
            });

        if let Some(x) = first {
            tracing::debug!("restore: first log id = {:?}", x);
            let mut ld = self.last_purged_log_id.write().await;
            *ld = Some(x);
        }

        let snapshot = self.get_current_snapshot().await.unwrap();

        if let Some(ss) = snapshot {
            self.install_snapshot(&ss.meta, ss.snapshot).await.unwrap();
        }
    }
}

#[async_trait]
impl RaftLogReader<ExampleTypeConfig> for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_state(&mut self) -> Result<LogState<ExampleTypeConfig>, StorageError<NodeId>> {
        let log = &self.log;
        let last = log
            .iter()
            .rev()
            .next()
            .map(|res| res.unwrap())
            .map(|(_, val)| {
                serde_json::from_slice::<Entry<ExampleTypeConfig>>(&*val)
                    .unwrap()
                    .log_id
            });

        let last_purged = *self.last_purged_log_id.read().await;

        let last = match last {
            None => last_purged,
            Some(x) => Some(x),
        };
        tracing::debug!(
            "get_log_state: last_purged = {:?}, last = {:?}",
            last_purged,
            last
        );
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<ExampleTypeConfig>>, StorageError<NodeId>> {
        let log = &self.log;
        let response = log
            .range(transform_range_bound(range))
            .map(|res| res.unwrap())
            .map(|(_, val)| serde_json::from_slice::<Entry<ExampleTypeConfig>>(&*val).unwrap())
            .collect();

        Ok(response)
    }
}

fn transform_range_bound<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
    range: RB,
) -> (Bound<IVec>, Bound<IVec>) {
    (
        serialize_bound(&range.start_bound()),
        serialize_bound(&range.end_bound()),
    )
}

fn serialize_bound(v: &Bound<&u64>) -> Bound<IVec> {
    match v {
        Bound::Included(v) => Bound::Included(IVec::from(&v.to_be_bytes())),
        Bound::Excluded(v) => Bound::Excluded(IVec::from(&v.to_be_bytes())),
        Bound::Unbounded => Bound::Unbounded,
    }
}

#[async_trait]
impl RaftSnapshotBuilder<ExampleTypeConfig, Cursor<Vec<u8>>> for Arc<Store> {
    #[tracing::instrument(level = "trace", skip(self))]
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<ExampleTypeConfig, Cursor<Vec<u8>>>, StorageError<NodeId>> {
        let (data, last_applied_log);

        {
            // Serialize the data of the state machine.
            let state_machine = self.state_machine.read().await;
            data = serde_json::to_vec(&*state_machine).map_err(|e| {
                StorageIOError::new(
                    ErrorSubject::StateMachine,
                    ErrorVerb::Read,
                    AnyError::new(&e),
                )
            })?;

            last_applied_log = state_machine.last_applied_log;
        }

        let last_applied_log = match last_applied_log {
            None => {
                panic!("can not compact empty state machine");
            }
            Some(x) => x,
        };

        let snapshot_idx = {
            let mut l = self.snapshot_idx.lock().unwrap();
            *l += 1;
            *l
        };

        let snapshot_id = format!(
            "{}-{}-{}",
            last_applied_log.leader_id, last_applied_log.index, snapshot_idx
        );

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            snapshot_id,
        };

        let snapshot = SnapshotInfo {
            meta: meta.clone(),
            data: data.clone(),
        };

        {
            let mut current_snapshot = self.current_snapshot.write().await;
            *current_snapshot = Some(snapshot);
        }

        self.write_snapshot().await.unwrap();

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStorage<ExampleTypeConfig> for Arc<Store> {
    type SnapshotData = Cursor<Vec<u8>>;
    type LogReader = Self;
    type SnapshotBuilder = Self;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.vote
            .insert(b"vote", IVec::from(serde_json::to_vec(vote).unwrap()))
            .unwrap();
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let value = self.vote.get(b"vote").unwrap();
        match value {
            None => Ok(None),
            Some(val) => Ok(Some(serde_json::from_slice::<Vote<NodeId>>(&*val).unwrap())),
        }
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn append_to_log(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<(), StorageError<NodeId>> {
        let log = &self.log;
        for entry in entries {
            log.insert(
                entry.log_id.index.to_be_bytes(),
                IVec::from(serde_json::to_vec(entry).unwrap()),
            )
            .unwrap();
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        let log = &self.log;
        let keys = log
            .range(transform_range_bound(log_id.index..))
            .map(|res| res.unwrap())
            .map(|(k, _v)| k); //TODO Why originally used collect instead of the iter.
        for key in keys {
            log.remove(&key).unwrap();
        }

        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        tracing::debug!("delete_log: [{:?}, +oo)", log_id);

        {
            let mut ld = self.last_purged_log_id.write().await;
            assert!(*ld <= Some(log_id));
            *ld = Some(log_id);
        }

        {
            let log = &self.log;

            let keys = log
                .range(transform_range_bound(..=log_id.index))
                .map(|res| res.unwrap())
                .map(|(k, _)| k);
            for key in keys {
                log.remove(&key).unwrap();
            }
        }

        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, EffectiveMembership<NodeId>), StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn apply_to_state_machine(
        &mut self,
        entries: &[&Entry<ExampleTypeConfig>],
    ) -> Result<Vec<KvResp>, StorageError<NodeId>> {
        let mut res = Vec::with_capacity(entries.len());

        let mut sm = self.state_machine.write().await;

        for entry in entries {
            tracing::debug!(%entry.log_id, "replicate to sm");

            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(KvResp::default()),
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership = EffectiveMembership::new(Some(entry.log_id), mem.clone());
                    res.push(KvResp::default())
                }

                EntryPayload::Normal(ref req) => match req {
                    KvReq::Set { key, value } => {
                        sm.data.insert(key.clone(), value.clone());
                        res.push(KvResp::default());

                        info!("WRITE: {} :{}", key, value);
                    }

                    KvReq::AddDataNode(cluster, node) => {
                        let key = KeyPath::data_node_id(cluster, node.id);
                        let value = serde_json::to_string(node).unwrap();
                        sm.data.insert(key.clone(), value.clone());
                        info!("WRITE: {} :{}", key, value);

                        res.push(KvResp::default());
                    }

                    KvReq::CreateDB(cluster, tenant, db) => {
                        let key = KeyPath::tenant_db_name(cluster, tenant, &db.name);
                        let value = serde_json::to_string(db).unwrap();
                        sm.data.insert(key.clone(), value.clone());
                        info!("WRITE: {} :{}", key, value);

                        let resp = KvResp {
                            err_code: 0,
                            err_msg: "".to_string(),
                            meta_data: sm.to_tenant_meta_data(cluster, tenant),
                        };

                        res.push(resp);
                    }

                    KvReq::CreateTable(cluster, tenant, schema) => {
                        let key =
                            KeyPath::tenant_schema_name(cluster, tenant, &schema.db, &schema.name);
                        let value = serde_json::to_string(schema).unwrap();
                        sm.data.insert(key.clone(), value.clone());
                        info!("WRITE: {} :{}", key, value);

                        let resp = KvResp {
                            err_code: 0,
                            err_msg: "".to_string(),
                            meta_data: sm.to_tenant_meta_data(cluster, tenant),
                        };

                        res.push(resp);
                    }

                    KvReq::UpdateTable(cluster, tenant, schema) => {
                        let key =
                            KeyPath::tenant_schema_name(cluster, tenant, &schema.db, &schema.name);
                        let value = serde_json::to_string(schema).unwrap();
                        sm.data.insert(key.clone(), value.clone());
                        info!("WRITE: {} :{}", key, value);

                        let resp = KvResp {
                            err_code: 0,
                            err_msg: "".to_string(),
                            meta_data: sm.to_tenant_meta_data(cluster, tenant),
                        };

                        res.push(resp);
                    }

                    KvReq::CreateBucket {
                        cluster,
                        tenant,
                        db,
                        ts,
                    } => {
                        let mut resp = process_create_bucket(&mut sm, cluster, tenant, db, ts);
                        resp.meta_data = sm.to_tenant_meta_data(cluster, tenant);
                        res.push(resp);
                    }
                },
            };
        }
        Ok(res)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Self::SnapshotData>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId>,
        snapshot: Box<Self::SnapshotData>,
    ) -> Result<StateMachineChanges<ExampleTypeConfig>, StorageError<NodeId>> {
        tracing::info!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );

        let new_snapshot = SnapshotInfo {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        // Update the state machine.
        {
            let updated_state_machine: StateMachine = serde_json::from_slice(&new_snapshot.data)
                .map_err(|e| {
                    StorageIOError::new(
                        ErrorSubject::Snapshot(new_snapshot.meta.clone()),
                        ErrorVerb::Read,
                        AnyError::new(&e),
                    )
                })?;
            let mut state_machine = self.state_machine.write().await;
            *state_machine = updated_state_machine;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(StateMachineChanges {
            last_applied: meta.last_log_id,
            is_snapshot: true,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<ExampleTypeConfig, Self::SnapshotData>>, StorageError<NodeId>> {
        tracing::debug!("get_current_snapshot: start");

        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => {
                let data = self.read_snapshot_file().await;
                let data = match data {
                    Ok(c) => c,
                    Err(_e) => return Ok(None),
                };

                let content: StateMachine = serde_json::from_slice(&data).unwrap();

                let last_applied_log = content.last_applied_log.unwrap();
                tracing::debug!(
                    "get_current_snapshot: last_applied_log = {:?}",
                    last_applied_log
                );

                let snapshot_idx = {
                    let mut l = self.snapshot_idx.lock().unwrap();
                    *l += 1;
                    *l
                };

                let snapshot_id = format!(
                    "{}-{}-{}",
                    last_applied_log.leader_id, last_applied_log.index, snapshot_idx
                );

                let meta = SnapshotMeta {
                    last_log_id: last_applied_log,
                    snapshot_id,
                };

                tracing::debug!("get_current_snapshot: meta {:?}", meta);

                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
        }
    }
}

fn process_create_bucket(
    sm: &mut StateMachine,
    cluster: &String,
    tenant: &String,
    db: &String,
    ts: &i64,
) -> KvResp {
    let db_path = KeyPath::tenant_db_name(cluster, tenant, db);
    let buckets = children_data::<BucketInfo>(&(db_path.clone() + "/buckets"), &sm.data);
    for (_, val) in buckets.iter() {
        if *ts >= val.start_time && *ts < val.end_time {
            return KvResp::default();
        }
    }

    let db_info = match get_struct::<DatabaseInfo>(&db_path, &sm.data) {
        Some(info) => info,
        None => {
            return KvResp {
                err_code: -1,
                meta_data: TenantMetaData::new(),
                err_msg: format!("database {} is not exist", db),
            };
        }
    };

    let node_list: Vec<NodeInfo> =
        children_data::<NodeInfo>(&KeyPath::data_nodes(cluster), &sm.data)
            .into_values()
            .collect();

    let now = utils::now_timestamp();
    if node_list.is_empty() || db_info.shard == 0 || db_info.replications > node_list.len() as u32 {
        return KvResp {
            err_code: -1,
            meta_data: TenantMetaData::new(),
            err_msg: format!("database {} attribute invalid!", db),
        };
    }

    if *ts < now - db_info.ttl {
        return KvResp {
            err_code: -1,
            meta_data: TenantMetaData::new(),
            err_msg: format!("database {} create expired bucket not permit!", db),
        };
    }

    let mut bucket = BucketInfo {
        id: fetch_and_add_incr_id(cluster, &mut sm.data, 1),
        start_time: 0,
        end_time: 0,
        shard_group: vec![],
    };
    (bucket.start_time, bucket.end_time) = get_time_range(*ts, db_info.vnode_duration);
    let (group, used) = allocation_replication_set(
        node_list,
        db_info.shard,
        db_info.replications,
        bucket.id + 1,
    );
    bucket.shard_group = group;
    fetch_and_add_incr_id(cluster, &mut sm.data, used);

    let key = KeyPath::tenant_bucket_id(cluster, tenant, db, bucket.id);
    let val = serde_json::to_string(&bucket).unwrap();

    sm.data.insert(key.clone(), val.clone());
    info!("WRITE: {} :{}", key, val);

    KvResp::default()
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Serialize};

    use crate::{client::MetaHttpClient, store::children_fullpath};

    use super::KvReq;

    #[tokio::test]
    async fn test_btree_map() {
        let mut map = BTreeMap::new();
        map.insert("/root/tenant".to_string(), "tenant_v".to_string());
        map.insert("/root/tenant/db1".to_string(), "123_v".to_string());
        map.insert("/root/tenant/db2".to_string(), "456_v".to_string());
        map.insert("/root/tenant/db1/".to_string(), "123/_v".to_string());
        map.insert("/root/tenant/db1/table1".to_string(), "123_v".to_string());
        map.insert("/root/tenant/123".to_string(), "123_v".to_string());
        map.insert("/root/tenant/456".to_string(), "456_v".to_string());

        let begin = "/root/tenant/".to_string();
        let end = "/root/tenant/|".to_string();
        for (key, value) in map.range(begin..end) {
            println!("{key}  : {value}");
        }

        let nodes = children_fullpath("/root/tenant", &map);
        println!("nodes: {:?}", nodes);
    }

    //{"Set":{"key":"foo","value":"bar111"}}
    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Command1 {
        id: u32,
        name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub struct Command2 {
        id: u32,
        name: String,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    pub enum Command {
        // Test1 { id: u32, name: String },
        // Test2 { id: u32, name: String },
        Test1(Command1),
    }

    #[tokio::test]
    async fn test_json() {
        let cmd = Command::Test1(Command1 {
            id: 100,
            name: "test".to_string(),
        });

        let str = serde_json::to_vec(&cmd).unwrap();
        print!("\n1 === {}=== \n", String::from_utf8(str).unwrap());

        let str = serde_json::to_string(&cmd).unwrap();
        print!("\n2 === {}=== \n", str);

        let tup = ("test1".to_string(), "test2".to_string());
        let str = serde_json::to_string(&tup).unwrap();
        print!("\n3 === {}=== \n", str);

        let str = serde_json::to_string(&"xxx".to_string()).unwrap();
        print!("\n4 === {}=== \n", str);
    }

    #[tokio::test]
    async fn test_meta_client() {
        let client = MetaHttpClient::new(1, "127.0.0.1:21001".to_string());
        let rsp = client.write(&KvReq::Set {
            key: "kxxxxxx".to_string(),
            value: "vxxxxxx".to_string(),
        });
        println!("write: {:#?}\n", rsp);
    }
}
