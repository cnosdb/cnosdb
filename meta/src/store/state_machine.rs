use crate::NodeId;
use models::schema::TskvTableSchema;

use openraft::EffectiveMembership;

use openraft::LogId;

use serde::Deserialize;
use serde::Serialize;
use serde_json::from_str;

use trace::info;

use std::collections::BTreeMap;
use std::collections::HashMap;

use models::{meta_data::*, utils};

use super::KvReq;
use super::KvResp;

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
// **    /cluster_name/tenant_name/dbs/db_name/schemas/name -> [TskvTableSchema] schema相关信息
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

            val.tables = children_data::<TskvTableSchema>(
                &KeyPath::tenant_schemas(cluster, tenant, key),
                &self.data,
            );
        }

        meta
    }

    pub fn process_command(&mut self, req: &KvReq) -> KvResp {
        info!("meta process command {:?}", req);

        match req {
            KvReq::Set { key, value } => {
                self.data.insert(key.clone(), value.clone());
                info!("WRITE: {} :{}", key, value);

                return KvResp::default();
            }

            KvReq::AddDataNode(cluster, node) => {
                return self.process_add_date_node(cluster, node);
            }

            KvReq::CreateDB(cluster, tenant, db) => {
                return self.process_create_db(cluster, tenant, db);
            }

            KvReq::CreateTable(cluster, tenant, schema) => {
                return self.process_create_table(cluster, tenant, schema);
            }

            KvReq::UpdateTable(cluster, tenant, schema) => {
                return self.process_update_table(cluster, tenant, schema);
            }

            KvReq::CreateBucket {
                cluster,
                tenant,
                db,
                ts,
            } => {
                return self.process_create_bucket(cluster, tenant, db, ts);
            }
        }
    }

    fn process_add_date_node(&mut self, cluster: &String, node: &NodeInfo) -> KvResp {
        let key = KeyPath::data_node_id(cluster, node.id);
        let value = serde_json::to_string(node).unwrap();
        self.data.insert(key.clone(), value.clone());
        info!("WRITE: {} :{}", key, value);

        KvResp::default()
    }

    fn process_create_db(
        &mut self,
        cluster: &String,
        tenant: &String,
        db: &DatabaseInfo,
    ) -> KvResp {
        let key = KeyPath::tenant_db_name(cluster, tenant, &db.name);
        // if self.data.contains_key(&key) {
        //     return KvResp {
        //         err_code: -1,
        //         err_msg: "database already exist".to_string(),
        //         meta_data: self.to_tenant_meta_data(cluster, tenant),
        //     };
        // }

        let value = serde_json::to_string(db).unwrap();
        self.data.insert(key.clone(), value.clone());
        info!("WRITE: {} :{}", key, value);

        KvResp {
            err_code: 0,
            err_msg: "".to_string(),
            meta_data: self.to_tenant_meta_data(cluster, tenant),
        }
    }

    fn process_create_table(
        &mut self,
        cluster: &String,
        tenant: &String,
        schema: &TskvTableSchema,
    ) -> KvResp {
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db, &schema.name);
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

        KvResp {
            err_code: 0,
            err_msg: "".to_string(),
            meta_data: self.to_tenant_meta_data(cluster, tenant),
        }
    }

    fn process_update_table(
        &mut self,
        cluster: &String,
        tenant: &String,
        schema: &TskvTableSchema,
    ) -> KvResp {
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db, &schema.name);
        if let Some(val) = get_struct::<TskvTableSchema>(&key, &self.data) {
            if val.schema_id + 1 != schema.schema_id {
                return KvResp {
                    err_code: -1,
                    err_msg: format!(
                        "update table schema conflict {}->{}",
                        val.schema_id, schema.schema_id
                    ),
                    meta_data: self.to_tenant_meta_data(cluster, tenant),
                };
            }
        }

        let value = serde_json::to_string(schema).unwrap();
        self.data.insert(key.clone(), value.clone());
        info!("WRITE: {} :{}", key, value);

        KvResp {
            err_code: 0,
            err_msg: "".to_string(),
            meta_data: self.to_tenant_meta_data(cluster, tenant),
        }
    }

    fn process_create_bucket(
        &mut self,
        cluster: &String,
        tenant: &String,
        db: &String,
        ts: &i64,
    ) -> KvResp {
        let db_path = KeyPath::tenant_db_name(cluster, tenant, db);
        let buckets = children_data::<BucketInfo>(&(db_path.clone() + "/buckets"), &self.data);
        for (_, val) in buckets.iter() {
            if *ts >= val.start_time && *ts < val.end_time {
                return KvResp {
                    err_code: 0,
                    err_msg: "".to_string(),
                    meta_data: self.to_tenant_meta_data(cluster, tenant),
                };
            }
        }

        let db_info = match get_struct::<DatabaseInfo>(&db_path, &self.data) {
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
            children_data::<NodeInfo>(&KeyPath::data_nodes(cluster), &self.data)
                .into_values()
                .collect();

        let now = utils::now_timestamp();
        if node_list.is_empty()
            || db_info.shard == 0
            || db_info.replications > node_list.len() as u32
        {
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
            id: fetch_and_add_incr_id(cluster, &mut self.data, 1),
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
        fetch_and_add_incr_id(cluster, &mut self.data, used);

        let key = KeyPath::tenant_bucket_id(cluster, tenant, db, bucket.id);
        let val = serde_json::to_string(&bucket).unwrap();

        self.data.insert(key.clone(), val.clone());
        info!("WRITE: {} :{}", key, val);

        KvResp {
            err_code: 0,
            err_msg: "".to_string(),
            meta_data: self.to_tenant_meta_data(cluster, tenant),
        }
    }
}
