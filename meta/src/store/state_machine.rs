use crate::NodeId;
use models::schema::{DatabaseSchema, TableSchema};

use openraft::EffectiveMembership;
use openraft::LogId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_str;

use tokio::sync::mpsc::Sender;

use std::collections::BTreeMap;
use std::collections::HashMap;
use trace::info;

use models::{meta_data::*, utils};

use super::command::*;

pub type CommandResp = String;

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

fn fetch_and_add_incr_id(cluster: &str, map: &mut BTreeMap<String, String>, count: u32) -> u32 {
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
    key: &str,
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
// **    /cluster_name/tenant_name/dbs/db_name -> [DatabaseSchema] db相关信息、保留策略等
// **    /cluster_name/tenant_name/dbs/db_name/buckets/id -> [BucketInfo] bucket相关信息
// **    /cluster_name/tenant_name/dbs/db_name/tables/name -> [TskvTableSchema] schema相关信息
pub struct KeyPath {}
impl KeyPath {
    pub fn incr_id(cluster: &str) -> String {
        format!("/{}/auto_incr_id", cluster)
    }

    pub fn data_nodes(cluster: &str) -> String {
        format!("/{}/data_nodes", cluster)
    }

    pub fn data_node_id(cluster: &str, id: u64) -> String {
        format!("/{}/data_nodes/{}", cluster, id)
    }

    pub fn tenant_users(cluster: &str, tenant: &str) -> String {
        format!("/{}/{}/users", cluster, tenant)
    }

    pub fn tenant_user_name(cluster: &str, tenant: &str, name: &str) -> String {
        format!("/{}/{}/users/{}", cluster, tenant, name)
    }

    pub fn tenant_dbs(cluster: &str, tenant: &str) -> String {
        format!("/{}/{}/dbs", cluster, tenant)
    }

    pub fn tenant_version(cluster: &str, tenant: &str) -> String {
        format!("/{}/{}/version", cluster, tenant)
    }

    pub fn tenant_db_name(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/{}/dbs/{}", cluster, tenant, db)
    }

    pub fn tenant_db_buckets(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/{}/dbs/{}/buckets", cluster, tenant, db)
    }

    pub fn tenant_bucket_id(cluster: &str, tenant: &str, db: &str, id: u32) -> String {
        format!("/{}/{}/dbs/{}/buckets/{}", cluster, tenant, db, id)
    }

    pub fn tenant_schemas(cluster: &str, tenant: &str, db: &str) -> String {
        format!("/{}/{}/dbs/{}/schemas", cluster, tenant, db)
    }

    pub fn tenant_schema_name(cluster: &str, tenant: &str, db: &str, name: &str) -> String {
        format!("/{}/{}/dbs/{}/schemas/{}", cluster, tenant, db, name)
    }
}

#[derive(Debug)]
pub struct WatchTenantMetaData {
    pub sender: Sender<bool>,

    pub cluster: String,
    pub tenant: String,

    pub delta: TenantMetaDataDelta,
}

impl WatchTenantMetaData {
    pub fn interesting(&self, cluster: &str, tenant: &str) -> bool {
        if self.cluster == cluster && self.tenant == tenant {
            return true;
        }

        false
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

    pub fn version(&self) -> u64 {
        if let Some(val) = self.last_applied_log {
            return val.index;
        }

        0
    }

    pub fn to_tenant_meta_data(&self, cluster: &str, tenant: &str) -> TenantMetaData {
        let mut meta = TenantMetaData::new();
        meta.version = self.version();
        meta.users = children_data::<UserInfo>(&KeyPath::tenant_users(cluster, tenant), &self.data);

        //
        let db_schemas =
            children_data::<DatabaseSchema>(&KeyPath::tenant_dbs(cluster, tenant), &self.data);

        for (key, schema) in db_schemas.iter() {
            let buckets = children_data::<BucketInfo>(
                &KeyPath::tenant_db_buckets(cluster, tenant, key),
                &self.data,
            );

            let tables = children_data::<TableSchema>(
                &KeyPath::tenant_schemas(cluster, tenant, key),
                &self.data,
            );

            let info = DatabaseInfo {
                tables,
                schema: schema.clone(),
                buckets: buckets.into_values().collect(),
            };

            meta.dbs.insert(key.clone(), info);
        }

        meta
    }

    pub fn process_read_command(&self, req: &ReadCommand) -> CommandResp {
        info!("meta process read command {:?}", req);

        match req {
            ReadCommand::DataNodes(cluster) => {
                let response: Vec<NodeInfo> =
                    children_data::<NodeInfo>(&KeyPath::data_nodes(cluster), &self.data)
                        .into_values()
                        .collect();

                serde_json::to_string(&response).unwrap()
            }

            ReadCommand::TenaneMetaData(cluster, tenant) => TenaneMetaDataResp::new_from_data(
                META_REQUEST_SUCCESS,
                "".to_string(),
                self.to_tenant_meta_data(cluster, tenant),
            )
            .to_string(),
        }
    }

    pub fn process_write_command(
        &mut self,
        req: &WriteCommand,
        watch: &mut HashMap<String, WatchTenantMetaData>,
    ) -> CommandResp {
        info!("meta process write command {:?}", req);

        match req {
            WriteCommand::Set { key, value } => {
                self.data.insert(key.clone(), value.clone());
                info!("WRITE: {} :{}", key, value);

                CommandResp::default()
            }

            WriteCommand::AddDataNode(cluster, node) => self.process_add_date_node(cluster, node),

            WriteCommand::CreateDB(cluster, tenant, schema) => {
                self.process_create_db(cluster, tenant, schema, watch)
            }

            WriteCommand::CreateTable(cluster, tenant, schema) => {
                self.process_create_table(cluster, tenant, schema, watch)
            }

            WriteCommand::UpdateTable(cluster, tenant, schema) => {
                self.process_update_table(cluster, tenant, schema, watch)
            }

            WriteCommand::CreateBucket {
                cluster,
                tenant,
                db,
                ts,
            } => self.process_create_bucket(cluster, tenant, db, ts, watch),
        }
    }

    fn process_add_date_node(&mut self, cluster: &str, node: &NodeInfo) -> CommandResp {
        let key = KeyPath::data_node_id(cluster, node.id);
        let value = serde_json::to_string(node).unwrap();
        self.data.insert(key.clone(), value.clone());
        info!("WRITE: {} :{}", key, value);

        serde_json::to_string(&StatusResponse::default()).unwrap()
    }

    fn process_create_db(
        &mut self,
        cluster: &str,
        tenant: &str,
        schema: &DatabaseSchema,
        watch: &mut HashMap<String, WatchTenantMetaData>,
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
        self.data.insert(key.clone(), value.clone());
        info!("WRITE: {} :{}", key, value);

        for (_, item) in watch.iter_mut() {
            if item.interesting(cluster, tenant) {
                item.delta.create_db(self.version(), schema);
                let _ = item.sender.try_send(true);
            }
        }

        TenaneMetaDataResp::new_from_data(
            META_REQUEST_SUCCESS,
            "".to_string(),
            self.to_tenant_meta_data(cluster, tenant),
        )
        .to_string()
    }

    fn process_create_table(
        &mut self,
        cluster: &str,
        tenant: &str,
        schema: &TableSchema,
        watch: &mut HashMap<String, WatchTenantMetaData>,
    ) -> CommandResp {
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db(), &schema.name());
        // if self.data.contains_key(&key) {
        //     return KvResp {
        //         err_code: -1,
        //         err_msg: "table already exist".to_string(),
        //         meta_data: self.to_tenant_meta_data(cluster, tenant),
        //     };
        // }

        let value = serde_json::to_string(schema).unwrap();
        self.data.insert(key.clone(), value.clone());
        info!("WRITE: {} :{}", key, value);

        for (_, item) in watch.iter_mut() {
            if item.interesting(cluster, tenant) {
                item.delta.create_or_update_table(self.version(), schema);
                let _ = item.sender.try_send(true);
            }
        }

        TenaneMetaDataResp::new_from_data(
            META_REQUEST_SUCCESS,
            "".to_string(),
            self.to_tenant_meta_data(cluster, tenant),
        )
        .to_string()
    }

    fn process_update_table(
        &mut self,
        cluster: &str,
        tenant: &str,
        schema: &TableSchema,
        watch: &mut HashMap<String, WatchTenantMetaData>,
    ) -> CommandResp {
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db(), &schema.name());
        if let Some(val) = get_struct::<TableSchema>(&key, &self.data) {
            match (val, schema) {
                (TableSchema::TsKvTableSchema(val), TableSchema::TsKvTableSchema(schema)) => {
                    if val.schema_id + 1 != schema.schema_id {
                        return TenaneMetaDataResp::new_from_data(
                            META_REQUEST_FAILED,
                            format!(
                                "update table schema conflict {}->{}",
                                val.schema_id, schema.schema_id
                            ),
                            self.to_tenant_meta_data(cluster, tenant),
                        )
                        .to_string();
                    }
                }
                _ => {
                    return TenaneMetaDataResp::new_from_data(
                        META_REQUEST_FAILED,
                        "not support update external table".to_string(),
                        self.to_tenant_meta_data(cluster, tenant),
                    )
                    .to_string()
                }
            }
        }

        let value = serde_json::to_string(schema).unwrap();
        self.data.insert(key.clone(), value.clone());
        info!("WRITE: {} :{}", key, value);

        for (_, item) in watch.iter_mut() {
            if item.interesting(cluster, tenant) {
                item.delta.create_or_update_table(self.version(), schema);
                let _ = item.sender.try_send(true);
            }
        }

        TenaneMetaDataResp::new_from_data(
            META_REQUEST_SUCCESS,
            "".to_string(),
            self.to_tenant_meta_data(cluster, tenant),
        )
        .to_string()
    }

    fn process_create_bucket(
        &mut self,
        cluster: &str,
        tenant: &str,
        db: &str,
        ts: &i64,
        watch: &mut HashMap<String, WatchTenantMetaData>,
    ) -> CommandResp {
        let db_path = KeyPath::tenant_db_name(cluster, tenant, db);
        let buckets = children_data::<BucketInfo>(&(db_path.clone() + "/buckets"), &self.data);
        for (_, val) in buckets.iter() {
            if *ts >= val.start_time && *ts < val.end_time {
                return TenaneMetaDataResp::new_from_data(
                    META_REQUEST_SUCCESS,
                    "".to_string(),
                    self.to_tenant_meta_data(cluster, tenant),
                )
                .to_string();
            }
        }

        let db_schema = match get_struct::<DatabaseSchema>(&db_path, &self.data) {
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
            children_data::<NodeInfo>(&KeyPath::data_nodes(cluster), &self.data)
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
            )
            .to_string();
        }

        if *ts < now - db_schema.config.ttl_or_default().time_stamp() {
            return TenaneMetaDataResp::new(
                META_REQUEST_FAILED,
                format!("database {} create expired bucket not permit!", db),
            )
            .to_string();
        }

        let mut bucket = BucketInfo {
            id: fetch_and_add_incr_id(cluster, &mut self.data, 1),
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
        fetch_and_add_incr_id(cluster, &mut self.data, used);

        let key = KeyPath::tenant_bucket_id(cluster, tenant, db, bucket.id);
        let val = serde_json::to_string(&bucket).unwrap();

        self.data.insert(key.clone(), val.clone());
        info!("WRITE: {} :{}", key, val);

        for (_, item) in watch.iter_mut() {
            if item.interesting(cluster, tenant) {
                item.delta
                    .create_or_update_bucket(self.version(), db, &bucket);
                let _ = item.sender.try_send(true);
            }
        }

        TenaneMetaDataResp::new_from_data(
            META_REQUEST_SUCCESS,
            "".to_string(),
            self.to_tenant_meta_data(cluster, tenant),
        )
        .to_string()
    }
}

#[cfg(test)]
mod test {
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;

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
