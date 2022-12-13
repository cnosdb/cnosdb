use crate::NodeId;
use models::auth::privilege::DatabasePrivilege;
use models::auth::role::CustomTenantRole;
use models::auth::role::SystemTenantRole;
use models::auth::role::TenantRoleIdentifier;
use models::auth::user::UserDesc;
use models::auth::user::UserOptions;
use models::oid::Identifier;
use models::oid::Oid;
use models::oid::UuidGenerator;
use models::schema::DatabaseSchema;
use models::schema::TableSchema;
use models::schema::Tenant;
use models::schema::TenantOptions;

use openraft::EffectiveMembership;
use openraft::LogId;
use serde::Deserialize;
use serde::Serialize;
use serde_json::from_str;
use trace::debug;

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

    pub fn users(cluster: &str) -> String {
        format!("/{}/users", cluster)
    }

    pub fn user(cluster: &str, user: &str) -> String {
        format!("/{}/users/{}", cluster, user)
    }

    pub fn tenants(cluster: &str) -> String {
        format!("/{}/tenants", cluster)
    }

    pub fn tenant(cluster: &str, name: &str) -> String {
        format!("/{}/tenants/{}", cluster, name)
    }

    pub fn role(cluster: &str, tenant_name: &str, role_name: &str) -> String {
        format!("/{}/tenants/{}/roles/{}", cluster, tenant_name, role_name)
    }

    pub fn roles(cluster: &str, tenant_name: &str) -> String {
        format!("/{}/tenants/{}/roles", cluster, tenant_name)
    }

    pub fn member(cluster: &str, tenant_name: &str, user_id: &Oid) -> String {
        format!("/{}/tenants/{}/members/{}", cluster, tenant_name, user_id)
    }

    pub fn members(cluster: &str, tenant_name: &str) -> String {
        format!("/{}/tenants/{}/members", cluster, tenant_name)
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

            ReadCommand::CustomRole(cluster, role_name, tenant_name) => {
                let path = KeyPath::role(cluster, tenant_name, role_name);

                let role = get_struct::<CustomTenantRole<Oid>>(&path, &self.data);

                CommonResp::Ok(role).to_string()
            }
            ReadCommand::CustomRoles(cluster, tenant_name) => {
                let path = KeyPath::roles(cluster, tenant_name);

                let roles: Vec<CustomTenantRole<Oid>> =
                    children_data::<CustomTenantRole<Oid>>(&path, &self.data)
                        .into_values()
                        .collect();

                CommonResp::Ok(roles).to_string()
            }

            ReadCommand::MemberRole(cluster, tenant_name, user_id) => {
                let path = KeyPath::member(cluster, tenant_name, user_id);

                if let Some(member) = get_struct::<TenantRoleIdentifier>(&path, &self.data) {
                    return CommonResp::Ok(member).to_string();
                }

                let status = StatusResponse::new(
                    META_REQUEST_USER_NOT_FOUND,
                    format!("{} of tenant {}", user_id, tenant_name),
                );
                CommonResp::<TenantRoleIdentifier>::Err(status).to_string()
            }
            ReadCommand::Members(cluster, tenant_name) => {
                let path = KeyPath::members(cluster, tenant_name);

                let members: Vec<TenantRoleIdentifier> =
                    children_data::<TenantRoleIdentifier>(&path, &self.data)
                        .into_values()
                        .collect();

                CommonResp::Ok(members).to_string()
            }

            ReadCommand::User(cluster, user_name) => {
                debug!("received ReadCommand::User: {}, {}", cluster, user_name);

                let path = KeyPath::user(cluster, user_name);

                let user = get_struct::<UserDesc>(&path, &self.data);

                CommonResp::Ok(user).to_string()
            }
            ReadCommand::Users(cluster) => {
                let path = KeyPath::users(cluster);

                let users: Vec<UserDesc> = children_data::<UserDesc>(&path, &self.data)
                    .into_values()
                    .collect();

                CommonResp::Ok(users).to_string()
            }
            ReadCommand::Tenant(cluster, tenant_name) => {
                debug!("received ReadCommand::Tenant: {}, {}", cluster, tenant_name);

                let path = KeyPath::tenant(cluster, tenant_name);

                let data = get_struct::<Tenant>(&path, &self.data);

                CommonResp::Ok(data).to_string()
            }
            ReadCommand::Tenants(cluster) => {
                let path = KeyPath::tenants(cluster);

                let data: Vec<Tenant> = children_data::<Tenant>(&path, &self.data)
                    .into_values()
                    .collect();

                CommonResp::Ok(data).to_string()
            }
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

            WriteCommand::DropDB(cluster, tenant, db_name) => {
                self.process_drop_db(cluster, tenant, db_name, watch)
            }

            WriteCommand::DropTable(cluster, tenant, db_name, table_name) => {
                self.process_drop_table(cluster, tenant, db_name, table_name, watch)
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

            WriteCommand::CreateUser(cluster, name, options, is_admin) => {
                self.process_create_user(cluster, name, options, *is_admin)
            }
            WriteCommand::AlterUser(cluster, name, options) => {
                self.process_alter_user(cluster, name, options)
            }
            WriteCommand::RenameUser(cluster, old_name, new_name) => {
                self.process_rename_user(cluster, old_name, new_name)
            }
            WriteCommand::DropUser(cluster, name) => self.process_drop_user(cluster, name),

            WriteCommand::CreateTenant(cluster, name, options) => {
                self.process_create_tenant(cluster, name, options)
            }
            WriteCommand::AlterTenant(cluster, name, options) => {
                self.process_alter_tenant(cluster, name, options)
            }
            WriteCommand::RenameTenant(cluster, old_name, new_name) => {
                self.process_rename_tenant(cluster, old_name, new_name)
            }
            WriteCommand::DropTenant(cluster, name) => self.process_drop_tenant(cluster, name),

            WriteCommand::AddMemberToTenant(cluster, user_id, role, tenant_name) => {
                self.process_add_member_to_tenant(cluster, user_id, role, tenant_name)
            }
            WriteCommand::RemoveMemberFromTenant(cluster, user_id, tenant_name) => {
                self.process_remove_member_to_tenant(cluster, user_id, tenant_name)
            }
            WriteCommand::ReasignMemberRole(cluster, user_id, role, tenant_name) => {
                self.process_reasign_member_role(cluster, user_id, role, tenant_name)
            }

            WriteCommand::CreateRole(cluster, role_name, sys_role, privileges, tenant_name) => {
                self.process_create_role(cluster, role_name, sys_role, privileges, tenant_name)
            }
            WriteCommand::DropRole(cluster, role_name, tenant_name) => {
                self.process_drop_role(cluster, role_name, tenant_name)
            }
            WriteCommand::GrantPrivileges(cluster, privileges, role_name, tenant_name) => {
                self.process_grant_privileges(cluster, privileges, role_name, tenant_name)
            }
            WriteCommand::RevokePrivileges(cluster, privileges, role_name, tenant_name) => {
                self.process_revoke_privileges(cluster, privileges, role_name, tenant_name)
            }
        }
    }

    fn process_add_date_node(&mut self, cluster: &str, node: &NodeInfo) -> CommandResp {
        let key = KeyPath::data_node_id(cluster, node.id);
        let value = serde_json::to_string(node).unwrap();
        self.data.insert(key.clone(), value.clone());
        info!("WRITE: {} :{}", key, value);

        serde_json::to_string(&StatusResponse::default()).unwrap()
    }

    fn process_drop_db(
        &mut self,
        cluster: &str,
        tenant: &str,
        db_name: &str,
        watch: &mut HashMap<String, WatchTenantMetaData>,
    ) -> CommandResp {
        let key = KeyPath::tenant_db_name(cluster, tenant, db_name);
        self.data.remove(&key);

        for (_, item) in watch.iter_mut() {
            if item.interesting(cluster, tenant) {
                item.delta.delete_db(self.version(), db_name);
                let _ = item.sender.try_send(true);
            }
        }

        StatusResponse::new(META_REQUEST_SUCCESS, "".to_string()).to_string()
    }

    fn process_drop_table(
        &mut self,
        cluster: &str,
        tenant: &str,
        db_name: &str,
        table_name: &str,
        watch: &mut HashMap<String, WatchTenantMetaData>,
    ) -> CommandResp {
        let key = KeyPath::tenant_schema_name(cluster, tenant, db_name, table_name);
        self.data.remove(&key);

        for (_, item) in watch.iter_mut() {
            if item.interesting(cluster, tenant) {
                item.delta.delete_table(self.version(), db_name, table_name);
                let _ = item.sender.try_send(true);
            }
        }

        StatusResponse::new(META_REQUEST_SUCCESS, "".to_string()).to_string()
    }

    fn process_create_db(
        &mut self,
        cluster: &str,
        tenant: &str,
        schema: &DatabaseSchema,
        watch: &mut HashMap<String, WatchTenantMetaData>,
    ) -> CommandResp {
        let key = KeyPath::tenant_db_name(cluster, tenant, schema.database_name());
        if self.data.contains_key(&key) {
            return TenaneMetaDataResp::new_from_data(
                META_REQUEST_DB_EXIST,
                "database already exist".to_string(),
                self.to_tenant_meta_data(cluster, tenant),
            )
            .to_string();
        }

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
        let key = KeyPath::tenant_db_name(cluster, tenant, &schema.db());
        if !self.data.contains_key(&key) {
            return TenaneMetaDataResp::new_from_data(
                META_REQUEST_DB_EXIST,
                "database not found".to_string(),
                self.to_tenant_meta_data(cluster, tenant),
            )
            .to_string();
        }
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db(), &schema.name());
        if self.data.contains_key(&key) {
            return TenaneMetaDataResp::new_from_data(
                META_REQUEST_TABLE_EXIST,
                "table already exist".to_string(),
                self.to_tenant_meta_data(cluster, tenant),
            )
            .to_string();
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

    fn process_create_user(
        &mut self,
        cluster: &str,
        user_name: &str,
        user_options: &UserOptions,
        is_admin: bool,
    ) -> CommandResp {
        let key = KeyPath::user(cluster, user_name);

        if self.data.contains_key(&key) {
            let status = StatusResponse::new(META_REQUEST_USER_EXIST, user_name.to_string());
            return CommonResp::<Oid>::Err(status).to_string();
        }

        let oid = UuidGenerator::default().next_id();
        let user_desc = UserDesc::new(oid, user_name.to_string(), user_options.clone(), is_admin);

        match serde_json::to_string(&user_desc) {
            Ok(value) => {
                self.data.insert(key, value);
                CommonResp::Ok(oid).to_string()
            }
            Err(err) => {
                let status = StatusResponse::new(META_REQUEST_FAILED, err.to_string());
                CommonResp::<Oid>::Err(status).to_string()
            }
        }
    }

    fn process_alter_user(
        &mut self,
        cluster: &str,
        user_name: &str,
        user_options: &UserOptions,
    ) -> CommandResp {
        let key = KeyPath::user(cluster, user_name);

        let resp = if let Some(e) = self.data.remove(&key) {
            match serde_json::from_str::<UserDesc>(&e) {
                Ok(old_user_desc) => {
                    let old_options = old_user_desc.options().to_owned();
                    let new_options = old_options.merge(user_options.clone());

                    let new_user_desc = UserDesc::new(
                        *old_user_desc.id(),
                        user_name.to_string(),
                        new_options,
                        old_user_desc.is_admin(),
                    );
                    let value = serde_json::to_string(&new_user_desc).unwrap();
                    self.data.insert(key, value);

                    CommonResp::Ok(())
                }
                Err(err) => {
                    CommonResp::Err(StatusResponse::new(META_REQUEST_FAILED, err.to_string()))
                }
            }
        } else {
            CommonResp::Err(StatusResponse::new(
                META_REQUEST_USER_NOT_FOUND,
                user_name.to_string(),
            ))
        };

        resp.to_string()
    }

    fn process_rename_user(&self, _cluster: &str, _old_name: &str, _new_name: &str) -> CommandResp {
        let status = StatusResponse::new(META_REQUEST_FAILED, "Not implement".to_string());
        CommonResp::<()>::Err(status).to_string()
    }

    fn process_drop_user(&mut self, cluster: &str, user_name: &str) -> CommandResp {
        let key = KeyPath::user(cluster, user_name);

        let success = self.data.remove(&key).is_some();

        CommonResp::Ok(success).to_string()
    }

    fn process_create_tenant(
        &mut self,
        cluster: &str,
        name: &str,
        options: &TenantOptions,
    ) -> CommandResp {
        let key = KeyPath::tenant(cluster, name);

        if self.data.contains_key(&key) {
            let status = StatusResponse::new(META_REQUEST_TENANT_EXIST, name.to_string());
            return CommonResp::<Tenant>::Err(status).to_string();
        }

        let oid = UuidGenerator::default().next_id();
        let tenant = Tenant::new(oid, name.to_string(), options.clone());

        match serde_json::to_string(&tenant) {
            Ok(value) => {
                self.data.insert(key, value);
                CommonResp::Ok(tenant).to_string()
            }
            Err(err) => {
                let status = StatusResponse::new(META_REQUEST_FAILED, err.to_string());
                CommonResp::<Tenant>::Err(status).to_string()
            }
        }
    }

    fn process_alter_tenant(
        &mut self,
        cluster: &str,
        name: &str,
        options: &TenantOptions,
    ) -> CommandResp {
        let key = KeyPath::tenant(cluster, name);

        let resp = if let Some(e) = self.data.remove(&key) {
            match serde_json::from_str::<Tenant>(&e) {
                Ok(tenant) => {
                    let old_options = tenant.options().to_owned();
                    let new_options = old_options.merge(options.clone());

                    let new_tenant = Tenant::new(*tenant.id(), name.to_string(), new_options);
                    let value = serde_json::to_string(&new_tenant).unwrap();
                    self.data.insert(key, value);

                    CommonResp::Ok(())
                }
                Err(err) => {
                    CommonResp::Err(StatusResponse::new(META_REQUEST_FAILED, err.to_string()))
                }
            }
        } else {
            CommonResp::Err(StatusResponse::new(
                META_REQUEST_TENANT_NOT_FOUND,
                name.to_string(),
            ))
        };

        resp.to_string()
    }

    fn process_rename_tenant(
        &self,
        _cluster: &str,
        _old_name: &str,
        _new_name: &str,
    ) -> CommandResp {
        let status = StatusResponse::new(META_REQUEST_FAILED, "Not implement".to_string());
        CommonResp::<()>::Err(status).to_string()
    }

    fn process_drop_tenant(&mut self, cluster: &str, name: &str) -> CommandResp {
        let key = KeyPath::tenant(cluster, name);

        let success = self.data.remove(&key).is_some();

        CommonResp::Ok(success).to_string()
    }

    fn process_add_member_to_tenant(
        &mut self,
        cluster: &str,
        user_id: &Oid,
        role: &TenantRoleIdentifier,
        tenant_name: &str,
    ) -> CommandResp {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if self.data.contains_key(&key) {
            let status = StatusResponse::new(META_REQUEST_USER_EXIST, user_id.to_string());
            return CommonResp::<()>::Err(status).to_string();
        }

        match serde_json::to_string(role) {
            Ok(value) => {
                self.data.insert(key, value);
                CommonResp::Ok(()).to_string()
            }
            Err(err) => {
                let status = StatusResponse::new(META_REQUEST_FAILED, err.to_string());
                CommonResp::<()>::Err(status).to_string()
            }
        }
    }

    fn process_remove_member_to_tenant(
        &mut self,
        cluster: &str,
        user_id: &Oid,
        tenant_name: &str,
    ) -> CommandResp {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if self.data.remove(&key).is_none() {
            let status = StatusResponse::new(META_REQUEST_USER_NOT_FOUND, user_id.to_string());
            return CommonResp::<()>::Err(status).to_string();
        }

        CommonResp::Ok(()).to_string()
    }

    fn process_reasign_member_role(
        &mut self,
        cluster: &str,
        user_id: &Oid,
        role: &TenantRoleIdentifier,
        tenant_name: &str,
    ) -> CommandResp {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if self.data.remove(&key).is_none() {
            let status = StatusResponse::new(META_REQUEST_USER_NOT_FOUND, user_id.to_string());
            return CommonResp::<()>::Err(status).to_string();
        }

        match serde_json::to_string(role) {
            Ok(value) => {
                self.data.entry(key).and_modify(|e| {
                    *e = value;
                });
                CommonResp::Ok(()).to_string()
            }
            Err(err) => {
                let status = StatusResponse::new(META_REQUEST_FAILED, err.to_string());
                CommonResp::<()>::Err(status).to_string()
            }
        }
    }

    fn process_create_role(
        &mut self,
        cluster: &str,
        role_name: &str,
        sys_role: &SystemTenantRole,
        privileges: &HashMap<String, DatabasePrivilege>,
        tenant_name: &str,
    ) -> CommandResp {
        let key = KeyPath::role(cluster, tenant_name, role_name);

        if self.data.contains_key(&key) {
            let status = StatusResponse::new(
                META_REQUEST_ROLE_EXIST,
                format!("{} of tenant {}", role_name, tenant_name),
            );
            return CommonResp::<()>::Err(status).to_string();
        }

        let oid = UuidGenerator::default().next_id();
        let role = CustomTenantRole::new(
            oid,
            role_name.to_string(),
            sys_role.clone(),
            privileges.clone(),
        );

        match serde_json::to_string(&role) {
            Ok(value) => {
                self.data.insert(key, value);
                CommonResp::Ok(()).to_string()
            }
            Err(err) => {
                let status = StatusResponse::new(META_REQUEST_FAILED, err.to_string());
                CommonResp::<()>::Err(status).to_string()
            }
        }
    }

    fn process_drop_role(
        &mut self,
        cluster: &str,
        role_name: &str,
        tenant_name: &str,
    ) -> CommandResp {
        let key = KeyPath::role(cluster, tenant_name, role_name);

        let success = self.data.remove(&key).is_some();

        CommonResp::Ok(success).to_string()
    }

    fn process_grant_privileges(
        &mut self,
        cluster: &str,
        privileges: &[(DatabasePrivilege, String)],
        role_name: &str,
        tenant_name: &str,
    ) -> CommandResp {
        let key = KeyPath::role(cluster, tenant_name, role_name);

        if !self.data.contains_key(&key) {
            let status = StatusResponse::new(
                META_REQUEST_ROLE_NOT_FOUND,
                format!("{} of tenant {}", role_name, tenant_name),
            );
            return CommonResp::<()>::Err(status).to_string();
        }

        self.data.entry(key).and_modify(|e| {
            let mut old_role =
                unsafe { serde_json::from_str::<CustomTenantRole<Oid>>(e).unwrap_unchecked() };
            for (privilege, database_name) in privileges {
                let _ = old_role.grant_privilege(database_name.clone(), privilege.clone());
            }
            *e = unsafe { serde_json::to_string(&old_role).unwrap_unchecked() };
        });

        CommonResp::Ok(()).to_string()
    }

    fn process_revoke_privileges(
        &mut self,
        cluster: &str,
        privileges: &[(DatabasePrivilege, String)],
        role_name: &str,
        tenant_name: &str,
    ) -> CommandResp {
        let key = KeyPath::role(cluster, tenant_name, role_name);

        if !self.data.contains_key(&key) {
            let status = StatusResponse::new(
                META_REQUEST_ROLE_NOT_FOUND,
                format!("{} of tenant {}", role_name, tenant_name),
            );
            return CommonResp::<()>::Err(status).to_string();
        }

        self.data.entry(key).and_modify(|e| {
            let mut old_role =
                unsafe { serde_json::from_str::<CustomTenantRole<Oid>>(e).unwrap_unchecked() };
            for (privilege, database_name) in privileges {
                let _ = old_role.revoke_privilege(database_name, privilege);
            }
            *e = unsafe { serde_json::to_string(&old_role).unwrap_unchecked() };
        });

        CommonResp::Ok(()).to_string()
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
