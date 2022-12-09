use crate::{ClusterNode, ClusterNodeId};
use models::schema::TskvTableSchema;
use models::schema::DatabaseSchema;
use openraft::EffectiveMembership;
use openraft::LogId;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, from_str};
use sled::Db;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use trace::info;

use crate::error::{l_r_err, s_w_err, sm_r_err, sm_w_err, StorageIOResult};
use crate::store::data;
use crate::store::key_path::KeyPath;
use models::{meta_data::*, utils};

use super::command::*;

pub type CommandResp = String;

pub fn children_fullpath(path: &str, map: Arc<sled::Db>) -> Vec<String> {
    let mut path = path.to_owned();
    if !path.ends_with('/') {
        path.push('/');
    }

    let mut list = vec![];
    for res in map.scan_prefix(path.as_bytes()) {
        match res {
            Err(_) => break,
            Ok(val) => {
                let key;
                unsafe { key = String::from_utf8_unchecked((*val.0).to_owned()) };
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
        }
    }
    list
}



fn fetch_and_add_incr_id(cluster: &str, map: Arc<sled::Db>, count: u32) -> u32 {
    let id_key = KeyPath::incr_id(cluster);

    let mut id_str = "1".to_string();
    if let Some(val) = map.get(&id_key).unwrap() {
        unsafe { id_str = String::from_utf8_unchecked((*val).to_owned()) };
    }
    let id_num = from_str::<u32>(&id_str).unwrap_or(1);

    let _ = map.insert(id_key.as_bytes(), (id_num + count).to_string().as_bytes());

    id_num
}

pub fn children_data<T>(path: &str, map: Arc<Db>) -> HashMap<String, T>
where for<'a> T: Deserialize<'a>
    {
    let mut path = path.to_owned();
    if !path.ends_with('/') {
        path.push('/');
    }
    let mut result = HashMap::new();
    for it in children_fullpath(&path, map.clone()).iter() {
        match map.get(it) {
            Err(_) => continue,
            Ok(t) => {
                if let Some(val) = t {
                    if let Some(info)= from_slice(&val).ok(){
                        if let Some(key) = it.strip_prefix(path.as_str()) {
                            result.insert(key.to_string(), info);
                        }
                    }
                }
            }

        }
    }

    result
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineContent {
    pub last_applied_log: Option<LogId<ClusterNodeId>>,
    pub last_membership: EffectiveMembership<ClusterNodeId, ClusterNode>,
    pub data: BTreeMap<String, String>,
}

impl From<&StateMachine> for StateMachineContent {
    fn from(state: &StateMachine) -> Self {
        let mut data_tree = BTreeMap::new();
        for entry_res in state.data().iter() {
            let entry = entry_res.expect("read db failed");

            let key: &[u8] = &entry.0;
            let value: &[u8] = &entry.1;
            data_tree.insert(
                String::from_utf8(key.to_vec()).expect("invalid key"),
                String::from_utf8(value.to_vec()).expect("invalid data"),
            );
        }
        Self {
            last_applied_log: state.get_last_applied_log().expect("last_applied_log"),
            last_membership: state.get_last_membership().expect("last_membership"),
            data: data_tree,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StateMachine {
    pub db: Arc<sled::Db>,
}
impl StateMachine {
    pub(crate) fn new(db: Arc<sled::Db>) -> StateMachine {
        Self { db }
    }
    fn data(&self) -> sled::Tree {
        self.db.open_tree("data").expect("data open failed")
    }
    fn state_machine(&self) -> sled::Tree {
        self.db
            .open_tree("state_machine")
            .expect("state_machine open failed")
    }

    pub(crate) fn get_last_membership(
        &self,
    ) -> StorageIOResult<EffectiveMembership<ClusterNodeId, ClusterNode>> {
        let state_machine = self.state_machine();
        state_machine
            .get(b"last_membership")
            .map_err(sm_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .unwrap_or_else(|| Ok(EffectiveMembership::default()))
            })
    }
    pub(crate) async fn set_last_membership(
        &self,
        membership: EffectiveMembership<ClusterNodeId, ClusterNode>,
    ) -> StorageIOResult<()> {
        let value = serde_json::to_vec(&membership).map_err(sm_w_err)?;
        let state_machine = self.state_machine();
        state_machine
            .insert(b"last_membership", value)
            .map_err(sm_w_err)?;

        state_machine
            .flush_async()
            .await
            .map_err(sm_w_err)
            .map(|_| ())
    }
    //todo:
    // fn set_last_membership_tx(
    //     &self,
    //     tx_state_machine: &sled::transaction::TransactionalTree,
    //     membership: EffectiveMembership<ClusterNodeId, ClusterNode>,
    // ) -> MetaResult<()> {
    //     let value = serde_json::to_vec(&membership).map_err(sm_r_err)?;
    //     tx_state_machine
    //         .insert(b"last_membership", value)
    //         .map_err(ct_err)?;
    //     Ok(())
    // }
    pub(crate) fn get_last_applied_log(&self) -> StorageIOResult<Option<LogId<ClusterNodeId>>> {
        let state_machine = self.state_machine();
        state_machine
            .get(b"last_applied_log")
            .map_err(l_r_err)
            .and_then(|value| {
                value
                    .map(|v| serde_json::from_slice(&v).map_err(sm_r_err))
                    .transpose()
            })
    }
    pub(crate) async fn set_last_applied_log(
        &self,
        log_id: LogId<ClusterNodeId>,
    ) -> StorageIOResult<()> {
        let value = serde_json::to_vec(&log_id).map_err(sm_w_err)?;
        let state_machine = self.state_machine();
        state_machine
            .insert(b"last_applied_log", value)
            .map_err(l_r_err)?;

        state_machine
            .flush_async()
            .await
            .map_err(l_r_err)
            .map(|_| ())
    }
    //todo:
    // fn set_last_applied_log_tx(
    //     &self,
    //     tx_state_machine: &sled::transaction::TransactionalTree,
    //     log_id: LogId<ClusterNodeId>,
    // ) -> MetaResult<()> {
    //     let value = serde_json::to_vec(&log_id).map_err(ct_err)?;
    //     tx_state_machine
    //         .insert(b"last_applied_log", value)
    //         .map_err(ct_err)?;
    //     Ok(())
    // }
    pub(crate) async fn from_serializable(
        sm: StateMachineContent,
        db: Arc<sled::Db>,
    ) -> StorageIOResult<Self> {
        let data_tree = data(&db);
        let mut batch = sled::Batch::default();
        for (key, value) in sm.data {
            batch.insert(key.as_bytes(), value.as_bytes())
        }
        data_tree.apply_batch(batch).map_err(sm_w_err)?;
        data_tree.flush_async().await.map_err(s_w_err)?;

        let r = Self { db };
        if let Some(log_id) = sm.last_applied_log {
            r.set_last_applied_log(log_id).await?;
        }
        r.set_last_membership(sm.last_membership).await?;

        Ok(r)
    }

    //todo:
    // fn insert_tx(
    //     &self,
    //     tx_data_tree: &sled::transaction::TransactionalTree,
    //     key: String,
    //     value: String,
    // ) -> MetaResult<()> {
    //     tx_data_tree
    //         .insert(key.as_bytes(), value.as_bytes())
    //         .map_err(ct_err)?;
    //     Ok(())
    // }
    pub fn get(&self, key: &str) -> StorageIOResult<Option<String>> {
        let key = key.as_bytes();
        let data_tree = self.data();
        data_tree
            .get(key)
            .map(|value| {
                value.map(|value| String::from_utf8(value.to_vec()).expect("invalid data"))
            })
            .map_err(sm_r_err)
    }
    pub fn get_all(&self) -> StorageIOResult<Vec<String>> {
        let data_tree = self.data();

        let data = data_tree
            .iter()
            .filter_map(|entry_res| {
                if let Ok(el) = entry_res {
                    Some(String::from_utf8(el.1.to_vec()).expect("invalid data"))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        Ok(data)
    }

    pub fn to_tenant_meta_data(&self, cluster: &str, tenant: &str) -> StorageIOResult<TenantMetaData> {
        let mut meta = TenantMetaData::new();
        //todo:
        meta.version = 1;
        meta.users = children_data::<UserInfo>(&KeyPath::tenant_users(cluster, tenant), self.db.clone());
        meta.data_nodes = children_data::<NodeInfo>(&KeyPath::data_nodes(cluster), self.db.clone());

        let db_schemas =
            children_data::<DatabaseSchema>(&KeyPath::tenant_dbs(cluster, tenant), self.db.clone());
        for (key, schema) in db_schemas.iter() {
            let buckets = children_data::<BucketInfo>(
                &KeyPath::tenant_db_buckets(cluster, tenant, key),
                self.db.clone(),
            );

            let tables = children_data::<TskvTableSchema>(
                &KeyPath::tenant_schemas(cluster, tenant, key),
                self.db.clone(),
            );

            let info = DatabaseInfo {
                tables,
                schema: schema.clone(),
                buckets: buckets.into_values().collect(),
            };

            meta.dbs.insert(key.clone(), info);
        }
        Ok(meta)
    }

    pub fn process_read_command(&self, req: &ReadCommand) -> CommandResp {
        info!("meta process read command {:?}", req);

        match req {
            ReadCommand::DataNodes(cluster) => {
                let response: Vec<NodeInfo> =
                    children_data::<NodeInfo>(&KeyPath::data_nodes(&cluster), self.db.clone())
                        .into_values()
                        .collect();

                serde_json::to_string(&response).unwrap()
            }

            ReadCommand::TenaneMetaData(cluster, tenant) => TenaneMetaDataResp::new_from_data(
                META_REQUEST_SUCCESS,
                "".to_string(),
                self.to_tenant_meta_data(cluster, tenant).unwrap(),
            )
            .to_string(),
        }
    }

    pub fn process_write_command(&mut self, req: &WriteCommand) -> CommandResp {
        info!("meta process write command {:?}", req);

        match req {
            WriteCommand::Set { key, value } => {
                self.db.insert(key.as_bytes(), value.as_bytes()).unwrap();
                info!("WRITE: {} :{}", key, value);

                CommandResp::default()
            }

            WriteCommand::AddDataNode(cluster, node) => self.process_add_date_node(cluster, node),

            WriteCommand::CreateDB(cluster, tenant, schema) => {
                self.process_create_db(cluster, tenant, schema)
            }

            WriteCommand::CreateTable(cluster, tenant, schema) => {
                self.process_create_table(cluster, tenant, schema)
            }

            WriteCommand::UpdateTable(cluster, tenant, schema) => {
                self.process_update_table(cluster, tenant, schema)
            }

            WriteCommand::CreateBucket {
                cluster,
                tenant,
                db,
                ts,
            } => self.process_create_bucket(cluster, tenant, db, ts),
        }
    }

    fn process_add_date_node(&mut self, cluster: &str, node: &NodeInfo) -> CommandResp {
        let key = KeyPath::data_node_id(cluster, node.id);
        let value = serde_json::to_string(node).unwrap();
        self.db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        info!("WRITE: {} :{}", key, value);

        serde_json::to_string(&StatusResponse::default()).unwrap()
    }

    fn process_create_db(
        &mut self,
        cluster: &str,
        tenant: &str,
        schema: &DatabaseSchema,
    ) -> CommandResp {
        let key = KeyPath::tenant_db_name(cluster, tenant, schema.database_name());
        // if self.data.contains_key(&key) {
        //     return KvResp {
        //         err_code: -1,
        //         err_msg: "database already exist".to_string(),
        //         meta_data: self.to_tenant_meta_data(cluster, tenant),
        //     };
        // }

        let value = serde_json::to_string(schema).unwrap();
        self.db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        info!("WRITE: {} :{}", key, value);

        TenaneMetaDataResp::new_from_data(
            META_REQUEST_SUCCESS,
            "".to_string(),
            self.to_tenant_meta_data(cluster, tenant).unwrap(),
        )
        .to_string()
    }

    fn process_create_table(
        &mut self,
        cluster: &str,
        tenant: &str,
        schema: &TskvTableSchema,
    ) -> CommandResp {
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db, &schema.name);
        // if self.data.contains_key(&key) {
        //     return KvResp {
        //         err_code: -1,
        //         err_msg: "table already exist".to_string(),
        //         meta_data: self.to_tenant_meta_data(cluster, tenant),
        //     };
        // }

        let value = serde_json::to_string(schema).unwrap();
        self.db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        info!("WRITE: {} :{}", key, value);

        TenaneMetaDataResp::new_from_data(
            META_REQUEST_SUCCESS,
            "".to_string(),
            self.to_tenant_meta_data(cluster, tenant).unwrap(),
        )
        .to_string()
    }

    fn process_update_table(
        &mut self,
        cluster: &str,
        tenant: &str,
        schema: &TskvTableSchema,
    ) -> CommandResp {
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db, &schema.name);
        if let Some(val) = self.db.get(&key).unwrap().and_then(|v|{from_slice::<TskvTableSchema>(&v).ok()}) {
            if val.schema_id + 1 != schema.schema_id {
                return TenaneMetaDataResp::new_from_data(
                    META_REQUEST_FAILED,
                    format!(
                        "update table schema conflict {}->{}",
                        val.schema_id, schema.schema_id
                    ),
                    self.to_tenant_meta_data(cluster, tenant).unwrap(),
                )
                .to_string();
            }
        }

        let value = serde_json::to_string(schema).unwrap();
        self.db.insert(key.as_bytes(), value.as_bytes()).unwrap();
        info!("WRITE: {} :{}", key, value);

        TenaneMetaDataResp::new_from_data(
            META_REQUEST_SUCCESS,
            "".to_string(),
            self.to_tenant_meta_data(cluster, tenant).unwrap(),
        )
        .to_string()
    }

    fn process_create_bucket(
        &mut self,
        cluster: &str,
        tenant: &str,
        db: &str,
        ts: &i64,
    ) -> CommandResp {
        let db_path = KeyPath::tenant_db_name(cluster, tenant, db);
        let buckets = children_data::<BucketInfo>(&(db_path.clone() + "/buckets"), self.db.clone());
        for (_, val) in buckets.iter() {
            if *ts >= val.start_time && *ts < val.end_time {
                return TenaneMetaDataResp::new_from_data(
                    META_REQUEST_SUCCESS,
                    "".to_string(),
                    self.to_tenant_meta_data(cluster, tenant).unwrap(),
                )
                .to_string();
            }
        }
        let res = self.db.get(&db_path).unwrap().and_then(|v|{from_slice::<DatabaseSchema>(&v).ok()});
        let db_schema = match res{
            Some(info) => info,
            None => {
                return TenaneMetaDataResp::new(
                    META_REQUEST_FAILED,
                    format!("database {} is not exist", db),
                )
                    .to_string();
            }
        };

        let node_list: Vec<NodeInfo> =
            children_data::<NodeInfo>(&KeyPath::data_nodes(cluster), self.db.clone())
                .into_values()
                .collect();

        let now = utils::now_timestamp();
        if node_list.is_empty()
            || db_schema.config.shard_num_or_default() == 0
            || db_schema.config.replica_or_default() > node_list.len() as u64
        {
            return TenaneMetaDataResp::new(
                META_REQUEST_FAILED,
                format!("database {} attribute invalid!", db),
            ).to_string();
        }

        if *ts < now - db_schema.config.ttl_or_default().time_stamp() {
            return TenaneMetaDataResp::new(
                META_REQUEST_FAILED,
                format!("database {} create expired bucket not permit!", db),
            )
            .to_string();
        }

        let mut bucket = BucketInfo {
            id: fetch_and_add_incr_id(cluster, self.db.clone(), 1),
            start_time: 0,
            end_time: 0,
            shard_group: vec![],
        };
        (bucket.start_time, bucket.end_time) = get_time_range(
            *ts,
            db_schema.config.vnode_duration_or_default().time_stamp(),
        );
        let (group, used) = allocation_replication_set(
            node_list,
            db_schema.config.shard_num_or_default() as u32,
            db_schema.config.replica_or_default() as u32,
            bucket.id + 1,
        );
        bucket.shard_group = group;
        fetch_and_add_incr_id(cluster, self.db.clone(), used);

        let key = KeyPath::tenant_bucket_id(cluster, tenant, db, bucket.id);
        let val = serde_json::to_string(&bucket).unwrap();

        self.db.insert(key.as_bytes(), val.as_bytes()).unwrap();
        info!("WRITE: {} :{}", key, val);

        TenaneMetaDataResp::new_from_data(
            META_REQUEST_SUCCESS,
            "".to_string(),
            self.to_tenant_meta_data(cluster, tenant).unwrap(),
        )
        .to_string()
    }
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;
    use std::println;

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
}
