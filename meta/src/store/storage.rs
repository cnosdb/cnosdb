use std::cmp::Reverse;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptions};
use models::meta_data::*;
use models::oid::{Identifier, Oid, UuidGenerator};
use models::schema::{DatabaseSchema, TableSchema, Tenant, TenantOptions};
use replication::errors::ReplicationResult;
use replication::{ApplyContext, ApplyStorage, Request, Response};
use serde::{Deserialize, Serialize};
use trace::{debug, error, info};

use super::command::*;
use super::key_path;
use crate::error::{MetaError, MetaResult};
use crate::limiter::local_request_limiter::{LocalBucketRequest, LocalBucketResponse};
use crate::limiter::remote_request_limiter::RemoteRequestLimiter;
use crate::store::key_path::KeyPath;

pub type CommandResp = String;

pub fn value_encode<T: Serialize>(d: &T) -> MetaResult<String> {
    serde_json::to_string(d).map_err(|e| MetaError::SerdeMsgInvalid { err: e.to_string() })
}

pub fn response_encode<T: Serialize>(d: MetaResult<T>) -> String {
    match serde_json::to_string(&d) {
        Ok(val) => val,
        Err(err) => {
            let err_rsp = MetaError::SerdeMsgInvalid {
                err: err.to_string(),
            };

            serde_json::to_string(&err_rsp).unwrap()
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct BtreeMapSnapshotData {
    pub map: BTreeMap<String, String>,
}
pub struct StateMachine {
    env: heed::Env,
    db: heed::Database<heed::types::Str, heed::types::Str>,
    pub watch: Arc<Watch>,
}

#[async_trait::async_trait]
impl ApplyStorage for StateMachine {
    async fn apply(&self, _ctx: &ApplyContext, req: &Request) -> ReplicationResult<Response> {
        let req: WriteCommand = serde_json::from_slice(req)?;

        Ok(self.process_write_command(&req).into())
    }

    async fn snapshot(&self) -> ReplicationResult<Vec<u8>> {
        let mut hash_map = BTreeMap::new();

        let reader = self.env.read_txn()?;
        let iter = self.db.iter(&reader)?;
        for pair in iter {
            let (key, val) = pair?;
            hash_map.insert(key.to_string(), val.to_string());
        }

        let data = BtreeMapSnapshotData { map: hash_map };
        let json_str = serde_json::to_string(&data).unwrap();

        Ok(json_str.as_bytes().to_vec())
    }

    async fn restore(&self, snapshot: &[u8]) -> ReplicationResult<()> {
        let data: BtreeMapSnapshotData = serde_json::from_slice(snapshot).unwrap();

        let mut writer = self.env.write_txn()?;
        self.db.clear(&mut writer)?;
        for (key, val) in data.map.iter() {
            self.db.put(&mut writer, key, val)?;
        }
        writer.commit()?;

        Ok(())
    }

    async fn destory(&self) -> ReplicationResult<()> {
        Ok(())
    }
}

impl StateMachine {
    pub fn open(path: impl AsRef<Path>) -> MetaResult<Self> {
        fs::create_dir_all(&path)?;

        let env = heed::EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(16)
            .open(path)?;

        let db: heed::Database<heed::types::Str, heed::types::Str> =
            env.create_database(Some("data"))?;
        let storage = Self {
            env,
            db,
            watch: Arc::new(Watch::new()),
        };

        Ok(storage)
    }

    pub fn is_meta_init(&self) -> MetaResult<bool> {
        self.contains_key(&KeyPath::already_init())
    }

    pub fn set_already_init(&self) -> MetaResult<()> {
        let mut writer = self.env.write_txn()?;
        self.db.put(&mut writer, &KeyPath::already_init(), "true")?;
        writer.commit()?;

        Ok(())
    }

    //********************************************************************************* */
    pub fn get(&self, key: &str) -> MetaResult<Option<String>> {
        let reader = self.env.read_txn()?;
        if let Some(data) = self.db.get(&reader, key)? {
            Ok(Some(data.to_owned()))
        } else {
            Ok(None)
        }
    }

    fn contains_key(&self, key: &str) -> MetaResult<bool> {
        if self.get(key)?.is_some() {
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn version(&self) -> MetaResult<u64> {
        let key = KeyPath::version();
        if let Some(data) = self.get(&key)? {
            Ok(data.parse::<u64>().unwrap_or(0))
        } else {
            Ok(0)
        }
    }

    fn fetch_and_add_incr_id(&self, cluster: &str, count: u32) -> MetaResult<u32> {
        let key = KeyPath::incr_id(cluster);

        let mut writer = self.env.write_txn()?;
        let data = self.db.get(&writer, &key)?.unwrap_or("1");
        let id = data.parse::<u32>().unwrap_or(1);

        self.db.put(&mut writer, &key, &(id + count).to_string())?;
        writer.commit()?;

        Ok(id)
    }

    fn insert(&self, key: &str, val: &str) -> MetaResult<()> {
        let version = self.version()? + 1;

        let mut writer = self.env.write_txn()?;
        self.db.put(&mut writer, key, val)?;
        self.db
            .put(&mut writer, &KeyPath::version(), &version.to_string())?;
        writer.commit()?;

        info!("METADATA WRITE: {} :{}", key, val);
        let log = EntryLog {
            tye: ENTRY_LOG_TYPE_SET,
            ver: version,
            key: key.to_string(),
            val: val.to_string(),
        };

        self.watch.writer_log(log);

        Ok(())
    }

    fn remove(&self, key: &str) -> MetaResult<()> {
        let version = self.version()? + 1;

        let mut writer = self.env.write_txn()?;
        self.db.delete(&mut writer, key)?;
        self.db
            .put(&mut writer, &KeyPath::version(), &version.to_string())?;
        writer.commit()?;

        info!("METADATA REMOVE: {}", key);
        let log = EntryLog {
            tye: ENTRY_LOG_TYPE_DEL,
            ver: version,
            key: key.to_string(),
            val: "".to_string(),
        };

        self.watch.writer_log(log);

        Ok(())
    }

    pub fn get_struct<T>(&self, key: &str) -> MetaResult<Option<T>>
    where
        for<'a> T: Deserialize<'a>,
    {
        let val = self.get(key)?;
        if let Some(data) = val {
            let info: T = serde_json::from_str(&data)?;
            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    pub fn children_fullpath(&self, path: &str) -> MetaResult<Vec<String>> {
        let mut path = path.to_owned();
        if !path.ends_with('/') {
            path.push('/');
        }

        let mut list = vec![];
        let reader = self.env.read_txn()?;
        let iter = self.db.prefix_iter(&reader, &path)?;
        for pair in iter {
            let (key, _) = pair?;
            match key.strip_prefix(path.as_str()) {
                Some(val) => {
                    if val.find('/').is_some() {
                        continue;
                    }
                    if val.is_empty() {
                        continue;
                    }

                    list.push(key.to_string());
                }

                None => break,
            }
        }

        Ok(list)
    }

    pub fn children_data<T>(&self, path: &str) -> MetaResult<HashMap<String, T>>
    where
        for<'a> T: Deserialize<'a>,
    {
        let mut path = path.to_owned();
        if !path.ends_with('/') {
            path.push('/');
        }

        let mut result = HashMap::new();
        let reader = self.env.read_txn()?;
        let iter = self.db.prefix_iter(&reader, &path)?;
        for pair in iter {
            let (key, val) = pair?;
            match key.strip_prefix(path.as_str()) {
                Some(sub_key) => {
                    if sub_key.find('/').is_some() {
                        continue;
                    }
                    if sub_key.is_empty() {
                        continue;
                    }

                    let info: T = serde_json::from_str(val)?;
                    result.insert(sub_key.to_string(), info);
                }

                None => break,
            }
        }

        Ok(result)
    }

    pub fn read_change_logs(
        &self,
        cluster: &str,
        tenants: &HashSet<String>,
        base_ver: u64,
    ) -> WatchData {
        let mut data = WatchData {
            full_sync: false,
            entry_logs: vec![],
            min_ver: self.watch.min_version().unwrap_or(0),
            max_ver: self.watch.max_version().unwrap_or(0),
        };

        if base_ver == self.version().unwrap_or(0) {
            return data;
        }

        let (logs, status) = self.watch.read_entry_logs(cluster, tenants, base_ver);
        if status < 0 {
            data.full_sync = true;
        } else {
            data.entry_logs = logs;
        }

        data
    }

    pub fn to_tenant_meta_data(&self, cluster: &str, tenant: &str) -> MetaResult<TenantMetaData> {
        let mut meta = TenantMetaData::new();
        meta.version = self.version()?;
        meta.roles =
            self.children_data::<CustomTenantRole<Oid>>(&KeyPath::roles(cluster, tenant))?;
        meta.members =
            self.children_data::<TenantRoleIdentifier>(&KeyPath::members(cluster, tenant))?;
        let db_schemas =
            self.children_data::<DatabaseSchema>(&KeyPath::tenant_dbs(cluster, tenant))?;

        for (key, schema) in db_schemas.iter() {
            let buckets = self
                .children_data::<BucketInfo>(&KeyPath::tenant_db_buckets(cluster, tenant, key))?;
            let tables =
                self.children_data::<TableSchema>(&KeyPath::tenant_schemas(cluster, tenant, key))?;

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
        debug!("meta process read command {:?}", req);
        match req {
            ReadCommand::DataNodes(cluster) => {
                response_encode(self.process_read_data_nodes(cluster))
            }
            ReadCommand::NodeMetrics(cluster) => {
                response_encode(self.process_read_node_metrics(cluster))
            }
            ReadCommand::TenaneMetaData(cluster, tenant) => {
                response_encode(self.to_tenant_meta_data(cluster, tenant))
            }
            ReadCommand::CustomRole(cluster, role_name, tenant_name) => {
                let path = KeyPath::role(cluster, tenant_name, role_name);
                response_encode(self.get_struct::<CustomTenantRole<Oid>>(&path))
            }
            ReadCommand::CustomRoles(cluster, tenant_name) => {
                response_encode(self.process_read_roles(cluster, tenant_name))
            }
            ReadCommand::MemberRole(cluster, tenant_name, user_id) => {
                let path = KeyPath::member(cluster, tenant_name, user_id);
                response_encode(self.get_struct::<TenantRoleIdentifier>(&path))
            }
            ReadCommand::Members(cluster, tenant_name) => {
                response_encode(self.process_read_members(cluster, tenant_name))
            }
            ReadCommand::User(cluster, user_name) => {
                let path = KeyPath::user(cluster, user_name);
                response_encode(self.get_struct::<UserDesc>(&path))
            }
            ReadCommand::Users(cluster) => response_encode(self.process_read_users(cluster)),
            ReadCommand::Tenant(cluster, tenant_name) => {
                let path = KeyPath::tenant(cluster, tenant_name);
                response_encode(self.get_struct::<Tenant>(&path))
            }
            ReadCommand::Tenants(cluster) => response_encode(self.process_read_tenants(cluster)),
            ReadCommand::TableSchema(cluster, tenant_name, db_name, table_name) => {
                let path = KeyPath::tenant_schema_name(cluster, tenant_name, db_name, table_name);
                response_encode(self.get_struct::<TableSchema>(&path))
            }
        }
    }

    pub fn process_read_data_nodes(&self, cluster: &str) -> MetaResult<(Vec<NodeInfo>, u64)> {
        let response: Vec<NodeInfo> = self
            .children_data::<NodeInfo>(&KeyPath::data_nodes(cluster))?
            .into_values()
            .collect();

        let ver = self.version()?;
        Ok((response, ver))
    }

    pub fn process_read_node_metrics(&self, cluster: &str) -> MetaResult<Vec<NodeMetrics>> {
        let response: Vec<NodeMetrics> = self
            .children_data::<NodeMetrics>(&KeyPath::data_nodes_metrics(cluster))?
            .into_values()
            .collect();

        Ok(response)
    }

    pub fn process_read_users(&self, cluster: &str) -> MetaResult<Vec<UserDesc>> {
        let path = KeyPath::users(cluster);
        let users: Vec<UserDesc> = self
            .children_data::<UserDesc>(&path)?
            .into_values()
            .collect();

        Ok(users)
    }

    pub fn process_read_tenants(&self, cluster: &str) -> MetaResult<Vec<Tenant>> {
        let path = KeyPath::tenants(cluster);
        let tenants: Vec<Tenant> = self.children_data::<Tenant>(&path)?.into_values().collect();

        Ok(tenants)
    }

    pub fn process_read_roles(
        &self,
        cluster: &str,
        tenant_name: &str,
    ) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        let path = KeyPath::roles(cluster, tenant_name);

        let roles: Vec<CustomTenantRole<Oid>> = self
            .children_data::<CustomTenantRole<Oid>>(&path)?
            .into_values()
            .collect();

        Ok(roles)
    }

    pub fn process_read_members(
        &self,
        cluster: &str,
        tenant_name: &str,
    ) -> MetaResult<HashMap<String, TenantRoleIdentifier>> {
        let path = KeyPath::members(cluster, tenant_name);

        let members = self.children_data::<TenantRoleIdentifier>(&path)?;
        let users: HashMap<String, UserDesc> = self
            .children_data::<UserDesc>(&KeyPath::users(cluster))?
            .into_values()
            .map(|desc| (format!("{}", desc.id()), desc))
            .collect();

        trace::trace!("members of path {}: {:?}", path, members);
        trace::trace!("all users: {:?}", users);

        let members: HashMap<String, TenantRoleIdentifier> = members
            .into_iter()
            .filter_map(|(id, role)| users.get(&id).map(|e| (e.name().to_string(), role)))
            .collect();

        debug!("returned members of path {}: {:?}", path, members);

        Ok(members)
    }

    pub fn process_write_command(&self, req: &WriteCommand) -> CommandResp {
        debug!("meta process write command {:?}", req);

        match req {
            WriteCommand::Set { key, value } => response_encode(self.process_write_set(key, value)),
            WriteCommand::AddDataNode(cluster, node) => {
                response_encode(self.process_add_date_node(cluster, node))
            }
            WriteCommand::ReportNodeMetrics(cluster, node_metrics) => {
                response_encode(self.process_add_node_metrics(cluster, node_metrics))
            }
            WriteCommand::CreateDB(cluster, tenant, schema) => {
                response_encode(self.process_create_db(cluster, tenant, schema))
            }
            WriteCommand::AlterDB(cluster, tenant, schema) => {
                response_encode(self.process_alter_db(cluster, tenant, schema))
            }
            WriteCommand::DropDB(cluster, tenant, db_name) => {
                response_encode(self.process_drop_db(cluster, tenant, db_name))
            }
            WriteCommand::DropTable(cluster, tenant, db_name, table_name) => {
                response_encode(self.process_drop_table(cluster, tenant, db_name, table_name))
            }
            WriteCommand::CreateTable(cluster, tenant, schema) => {
                response_encode(self.process_create_table(cluster, tenant, schema))
            }
            WriteCommand::UpdateTable(cluster, tenant, schema) => {
                response_encode(self.process_update_table(cluster, tenant, schema))
            }
            WriteCommand::CreateBucket(cluster, tenant, db, ts) => {
                response_encode(self.process_create_bucket(cluster, tenant, db, ts))
            }
            WriteCommand::DeleteBucket(cluster, tenant, db, id) => {
                response_encode(self.process_delete_bucket(cluster, tenant, db, *id))
            }
            WriteCommand::CreateUser(cluster, user) => {
                response_encode(self.process_create_user(cluster, user))
            }
            WriteCommand::AlterUser(cluster, name, options) => {
                response_encode(self.process_alter_user(cluster, name, options))
            }
            WriteCommand::RenameUser(cluster, old_name, new_name) => {
                response_encode(self.process_rename_user(cluster, old_name, new_name))
            }
            WriteCommand::DropUser(cluster, name) => {
                response_encode(self.process_drop_user(cluster, name))
            }
            WriteCommand::CreateTenant(cluster, tenant) => {
                response_encode(self.process_create_tenant(cluster, tenant))
            }
            WriteCommand::AlterTenant(cluster, name, options) => {
                response_encode(self.process_alter_tenant(cluster, name, options))
            }
            WriteCommand::RenameTenant(cluster, old_name, new_name) => {
                response_encode(self.process_rename_tenant(cluster, old_name, new_name))
            }
            WriteCommand::DropTenant(cluster, name) => {
                response_encode(self.process_drop_tenant(cluster, name))
            }
            WriteCommand::AddMemberToTenant(cluster, user_id, role, tenant_name) => {
                response_encode(self.process_add_member_to_tenant(
                    cluster,
                    user_id,
                    role,
                    tenant_name,
                ))
            }
            WriteCommand::RemoveMemberFromTenant(cluster, user_id, tenant_name) => {
                response_encode(self.process_remove_member_to_tenant(cluster, user_id, tenant_name))
            }
            WriteCommand::ReasignMemberRole(cluster, user_id, role, tenant_name) => {
                response_encode(self.process_reasign_member_role(
                    cluster,
                    user_id,
                    role,
                    tenant_name,
                ))
            }

            WriteCommand::CreateRole(cluster, role_name, sys_role, privileges, tenant_name) => {
                response_encode(self.process_create_role(
                    cluster,
                    role_name,
                    sys_role,
                    privileges,
                    tenant_name,
                ))
            }
            WriteCommand::DropRole(cluster, role_name, tenant_name) => {
                response_encode(self.process_drop_role(cluster, role_name, tenant_name))
            }
            WriteCommand::GrantPrivileges(cluster, privileges, role_name, tenant_name) => {
                response_encode(self.process_grant_privileges(
                    cluster,
                    privileges,
                    role_name,
                    tenant_name,
                ))
            }
            WriteCommand::RevokePrivileges(cluster, privileges, role_name, tenant_name) => {
                response_encode(self.process_revoke_privileges(
                    cluster,
                    privileges,
                    role_name,
                    tenant_name,
                ))
            }
            WriteCommand::RetainID(cluster, count) => {
                response_encode(self.process_retain_id(cluster, *count))
            }
            WriteCommand::UpdateVnodeReplSet(args) => {
                response_encode(self.process_update_vnode_repl_set(args))
            }
            WriteCommand::ChangeReplSetLeader(args) => {
                response_encode(self.process_change_repl_set_leader(args))
            }
            WriteCommand::UpdateVnode(args) => response_encode(self.process_update_vnode(args)),
            WriteCommand::LimiterRequest {
                cluster,
                tenant,
                request,
            } => response_encode(self.process_limiter_request(cluster, tenant, request)),
        }
    }

    fn process_write_set(&self, key: &str, val: &str) -> MetaResult<()> {
        self.insert(key, val)
    }

    fn process_update_vnode(&self, args: &UpdateVnodeArgs) -> MetaResult<()> {
        let key = key_path::KeyPath::tenant_bucket_id(
            &args.cluster,
            &args.vnode_info.tenant,
            &args.vnode_info.db_name,
            args.vnode_info.bucket_id,
        );
        let mut bucket = match self.get_struct::<BucketInfo>(&key)? {
            Some(b) => b,
            None => {
                return Err(MetaError::BucketNotFound {
                    id: args.vnode_info.bucket_id,
                });
            }
        };

        for set in bucket.shard_group.iter_mut() {
            if set.id != args.vnode_info.repl_set_id {
                continue;
            }
            for vnode in set.vnodes.iter_mut() {
                if vnode.id == args.vnode_info.vnode_id {
                    vnode.status = args.vnode_info.status;
                    break;
                }
            }
        }

        self.insert(&key, &value_encode(&bucket)?)?;
        Ok(())
    }

    fn process_update_vnode_repl_set(&self, args: &UpdateVnodeReplSetArgs) -> MetaResult<()> {
        let key = key_path::KeyPath::tenant_bucket_id(
            &args.cluster,
            &args.tenant,
            &args.db_name,
            args.bucket_id,
        );
        let mut bucket = match self.get_struct::<BucketInfo>(&key)? {
            Some(b) => b,
            None => {
                return Err(MetaError::BucketNotFound { id: args.bucket_id });
            }
        };

        for set in bucket.shard_group.iter_mut() {
            if set.id != args.repl_id {
                continue;
            }

            for info in args.del_info.iter() {
                set.vnodes
                    .retain(|item| !((item.id == info.id) && (item.node_id == info.node_id)));
            }

            for info in args.add_info.iter() {
                set.vnodes.push(info.clone());
            }

            // process if the leader is deleted....
            if set.vnode(set.leader_vnode_id).is_none() && !set.vnodes.is_empty() {
                set.leader_vnode_id = set.vnodes[0].id;
                set.leader_node_id = set.vnodes[0].node_id;
            }
        }

        // delete the vnodes is empty replication
        bucket
            .shard_group
            .retain(|replica| !replica.vnodes.is_empty());

        if bucket.shard_group.is_empty() {
            self.remove(&key)
        } else {
            self.insert(&key, &value_encode(&bucket)?)
        }
    }

    fn process_change_repl_set_leader(&self, args: &ChangeReplSetLeaderArgs) -> MetaResult<()> {
        let key = key_path::KeyPath::tenant_bucket_id(
            &args.cluster,
            &args.tenant,
            &args.db_name,
            args.bucket_id,
        );
        let mut bucket = match self.get_struct::<BucketInfo>(&key)? {
            Some(b) => b,
            None => {
                return Err(MetaError::BucketNotFound { id: args.bucket_id });
            }
        };

        for repl in bucket.shard_group.iter_mut() {
            if repl.id == args.repl_id {
                repl.leader_node_id = args.leader_node_id;
                repl.leader_vnode_id = args.leader_vnode_id;
            }
        }

        self.insert(&key, &value_encode(&bucket)?)?;
        Ok(())
    }

    fn process_retain_id(&self, cluster: &str, count: u32) -> MetaResult<u32> {
        let id = self.fetch_and_add_incr_id(cluster, count)?;

        Ok(id)
    }

    fn check_node_ip_address(&self, cluster: &str, node: &NodeInfo) -> MetaResult<bool> {
        for value in self
            .children_data::<NodeInfo>(&KeyPath::data_nodes(cluster))?
            .values()
        {
            if value.id != node.id
                && (value.http_addr == node.http_addr || value.grpc_addr == node.grpc_addr)
            {
                error!(
                    "ip address has been added, add node failed, the added node is : {:?}",
                    value
                );
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn process_add_date_node(&self, cluster: &str, node: &NodeInfo) -> MetaResult<()> {
        if !self.check_node_ip_address(cluster, node)? {
            return Err(MetaError::DataNodeExist {
                addr: format! {"{} {}", node.grpc_addr,node.http_addr},
            });
        }
        let key = KeyPath::data_node_id(cluster, node.id);
        let value = value_encode(node)?;
        self.insert(&key, &value)
    }

    fn process_add_node_metrics(
        &self,
        cluster: &str,
        node_metrics: &NodeMetrics,
    ) -> MetaResult<()> {
        let key = KeyPath::data_node_metrics(cluster, node_metrics.id);
        let value = value_encode(node_metrics)?;
        self.insert(&key, &value)
    }

    fn process_drop_db(&self, cluster: &str, tenant: &str, db_name: &str) -> MetaResult<()> {
        let key = KeyPath::tenant_db_name(cluster, tenant, db_name);
        let _ = self.remove(&key);

        let buckets_path = KeyPath::tenant_db_buckets(cluster, tenant, db_name);
        for it in self.children_fullpath(&buckets_path)?.iter() {
            let _ = self.remove(it);
        }

        let schemas_path = KeyPath::tenant_schemas(cluster, tenant, db_name);
        for it in self.children_fullpath(&schemas_path)?.iter() {
            let _ = self.remove(it);
        }

        Ok(())
    }

    fn process_drop_table(
        &self,
        cluster: &str,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::tenant_schema_name(cluster, tenant, db_name, table_name);
        if !self.contains_key(&key)? {
            return Err(MetaError::TableNotFound {
                table: table_name.to_owned(),
            });
        }

        self.remove(&key)
    }

    fn process_create_db(
        &self,
        cluster: &str,
        tenant: &str,
        schema: &DatabaseSchema,
    ) -> MetaResult<TenantMetaData> {
        let key = KeyPath::tenant_db_name(cluster, tenant, schema.database_name());
        if self.contains_key(&key)? {
            return Err(MetaError::DatabaseAlreadyExists {
                database: schema.database_name().to_string(),
            });
        }

        self.check_db_schema_valid(cluster, schema)?;
        self.insert(&key, &value_encode(schema)?)?;

        self.to_tenant_meta_data(cluster, tenant)
    }

    fn process_alter_db(
        &self,
        cluster: &str,
        tenant: &str,
        schema: &DatabaseSchema,
    ) -> MetaResult<()> {
        let key = KeyPath::tenant_db_name(cluster, tenant, schema.database_name());
        if !self.contains_key(&key)? {
            return Err(MetaError::DatabaseNotFound {
                database: schema.database_name().to_string(),
            });
        }

        self.check_db_schema_valid(cluster, schema)?;
        self.insert(&key, &value_encode(schema)?)?;
        Ok(())
    }

    fn check_db_schema_valid(&self, cluster: &str, db_schema: &DatabaseSchema) -> MetaResult<()> {
        let node_list = self.get_valid_node_list(cluster)?;
        check_node_enough(db_schema.config.replica_or_default(), &node_list)?;

        if db_schema.config.shard_num_or_default() == 0 {
            return Err(MetaError::DatabaseSchemaInvalid {
                name: db_schema.database_name().to_string(),
            });
        }

        Ok(())
    }

    fn process_create_table(
        &self,
        cluster: &str,
        tenant: &str,
        schema: &TableSchema,
    ) -> MetaResult<TenantMetaData> {
        let key = KeyPath::tenant_db_name(cluster, tenant, schema.db());
        if !self.contains_key(&key)? {
            return Err(MetaError::DatabaseNotFound {
                database: schema.db().to_string(),
            });
        }
        let key = KeyPath::tenant_schema_name(cluster, tenant, schema.db(), schema.name());
        if self.contains_key(&key)? {
            return Err(MetaError::TableAlreadyExists {
                table_name: schema.name().to_string(),
            });
        }

        self.insert(&key, &value_encode(schema)?)?;

        self.to_tenant_meta_data(cluster, tenant)
    }

    fn process_update_table(
        &self,
        cluster: &str,
        tenant: &str,
        schema: &TableSchema,
    ) -> MetaResult<()> {
        let key = KeyPath::tenant_schema_name(cluster, tenant, schema.db(), schema.name());
        if let Some(val) = self.get_struct::<TableSchema>(&key)? {
            match (val, schema) {
                (TableSchema::TsKvTableSchema(val), TableSchema::TsKvTableSchema(schema)) => {
                    if val.schema_id + 1 != schema.schema_id {
                        return Err(MetaError::UpdateTableConflict {
                            name: schema.name.clone(),
                        });
                    }
                }
                _ => {
                    return Err(MetaError::NotSupport {
                        msg: "update external table".to_string(),
                    });
                }
            }
        }

        self.insert(&key, &value_encode(schema)?)?;
        Ok(())
    }

    fn get_valid_node_list(&self, cluster: &str) -> MetaResult<Vec<NodeInfo>> {
        let node_info_list: Vec<NodeInfo> = self
            .children_data::<NodeInfo>(&KeyPath::data_nodes(cluster))?
            .into_values()
            .collect();
        let node_metrics_list: HashMap<NodeId, NodeMetrics> = self
            .children_data::<NodeMetrics>(&KeyPath::data_nodes_metrics(cluster))?
            .into_values()
            .map(|m| (m.id, m))
            .collect();

        let node_info_list = node_info_list
            .into_iter()
            .filter_map(|n| node_metrics_list.get(&n.id).map(|m| (n, m)))
            .filter(|(_, m)| m.is_healthy())
            .collect::<Vec<_>>();

        let temp_node_info_list = node_info_list
            .iter()
            .filter(|(n, _)| !n.is_cold())
            .cloned()
            .collect::<Vec<_>>();

        let mut res = if temp_node_info_list.is_empty() {
            node_info_list
        } else {
            temp_node_info_list
        };

        res.sort_by_key(|(_, m)| Reverse(m.disk_free));
        let res = res.into_iter().map(|(n, _)| n).collect();
        Ok(res)
    }

    fn process_create_bucket(
        &self,
        cluster: &str,
        tenant: &str,
        db: &str,
        ts: &i64,
    ) -> MetaResult<TenantMetaData> {
        let db_path = KeyPath::tenant_db_name(cluster, tenant, db);
        let buckets = self.children_data::<BucketInfo>(&(db_path.clone() + "/buckets"))?;
        for (_, val) in buckets.iter() {
            if *ts >= val.start_time && *ts < val.end_time {
                return self.to_tenant_meta_data(cluster, tenant);
            }
        }
        let db_schema =
            self.get_struct::<DatabaseSchema>(&db_path)?
                .ok_or(MetaError::DatabaseNotFound {
                    database: db.to_string(),
                })?;

        let node_list = self.get_valid_node_list(cluster)?;
        check_node_enough(db_schema.config.replica_or_default(), &node_list)?;

        if db_schema.config.shard_num_or_default() == 0 {
            return Err(MetaError::DatabaseSchemaInvalid {
                name: db.to_string(),
            });
        }

        if *ts < db_schema.time_to_expired() {
            return Err(MetaError::NotSupport {
                msg: "create expired bucket".to_string(),
            });
        }

        let mut bucket = BucketInfo {
            id: self.fetch_and_add_incr_id(cluster, 1)?,
            start_time: 0,
            end_time: 0,
            shard_group: vec![],
        };
        (bucket.start_time, bucket.end_time) = get_time_range(
            *ts,
            db_schema
                .config
                .vnode_duration_or_default()
                .to_precision(*db_schema.config.precision_or_default()),
        );
        let (group, used) = allocation_replication_set(
            node_list,
            db_schema.config.shard_num_or_default() as u32,
            db_schema.config.replica_or_default() as u32,
            bucket.id + 1,
        );
        bucket.shard_group = group;
        self.fetch_and_add_incr_id(cluster, used)?;

        let key = KeyPath::tenant_bucket_id(cluster, tenant, db, bucket.id);
        self.insert(&key, &value_encode(&bucket)?)?;

        self.to_tenant_meta_data(cluster, tenant)
    }

    fn process_delete_bucket(
        &self,
        cluster: &str,
        tenant: &str,
        db: &str,
        id: u32,
    ) -> MetaResult<()> {
        let key = KeyPath::tenant_bucket_id(cluster, tenant, db, id);
        self.remove(&key)
    }

    fn process_create_user(&self, cluster: &str, user_desc: &UserDesc) -> MetaResult<()> {
        let key = KeyPath::user(cluster, user_desc.name());

        if self.contains_key(&key)? {
            return Err(MetaError::UserAlreadyExists {
                user: user_desc.name().to_string(),
            });
        }

        self.insert(&key, &value_encode(&user_desc)?)?;
        Ok(())
    }

    fn process_alter_user(
        &self,
        cluster: &str,
        user_name: &str,
        user_options: &UserOptions,
    ) -> MetaResult<()> {
        let key = KeyPath::user(cluster, user_name);
        if let Some(old_user_desc) = self.get_struct::<UserDesc>(&key)? {
            let old_options = old_user_desc.options().to_owned();
            let new_options = user_options.clone().merge(old_options);

            let new_user_desc = UserDesc::new(
                *old_user_desc.id(),
                user_name.to_string(),
                new_options,
                old_user_desc.is_root_admin(),
            );

            Ok(self.insert(&key, &value_encode(&new_user_desc)?)?)
        } else {
            Err(MetaError::UserNotFound {
                user: user_name.to_string(),
            })
        }
    }

    fn process_rename_user(
        &self,
        _cluster: &str,
        _old_name: &str,
        _new_name: &str,
    ) -> MetaResult<()> {
        Err(MetaError::NotSupport {
            msg: "rename user".to_string(),
        })
    }

    fn process_drop_user(&self, cluster: &str, user_name: &str) -> MetaResult<()> {
        let key = KeyPath::user(cluster, user_name);

        self.remove(&key)
    }

    fn set_tenant_limiter(
        &self,
        cluster: &str,
        tenant: &str,
        limiter: Option<RemoteRequestLimiter>,
    ) -> MetaResult<()> {
        let key = KeyPath::limiter(cluster, tenant);

        let limiter = match limiter {
            Some(limiter) => limiter,
            None => {
                return self.remove(&key);
            }
        };

        self.insert(&key, &value_encode(&limiter)?)
    }

    fn process_create_tenant(&self, cluster: &str, tenant: &Tenant) -> MetaResult<()> {
        let key = KeyPath::tenant(cluster, tenant.name());

        if self.contains_key(&key)? {
            return Err(MetaError::TenantAlreadyExists {
                tenant: tenant.name().to_string(),
            });
        }

        let limiter = tenant
            .options()
            .request_config()
            .map(RemoteRequestLimiter::new);

        self.set_tenant_limiter(cluster, tenant.name(), limiter)?;

        self.insert(&key, &value_encode(&tenant)?)?;

        Ok(())
    }

    fn process_alter_tenant(
        &self,
        cluster: &str,
        name: &str,
        options: &TenantOptions,
    ) -> MetaResult<Tenant> {
        let key = KeyPath::tenant(cluster, name);
        if let Some(tenant) = self.get_struct::<Tenant>(&key)? {
            let new_tenant = Tenant::new(*tenant.id(), name.to_string(), options.to_owned());
            self.insert(&key, &value_encode(&new_tenant)?)?;

            let limiter = options.request_config().map(RemoteRequestLimiter::new);

            self.set_tenant_limiter(cluster, name, limiter)?;

            Ok(new_tenant)
        } else {
            Err(MetaError::TenantAlreadyExists {
                tenant: name.to_string(),
            })
        }
    }

    fn process_rename_tenant(
        &self,
        _cluster: &str,
        _old_name: &str,
        _new_name: &str,
    ) -> MetaResult<()> {
        Err(MetaError::NotSupport {
            msg: "rename tenant".to_string(),
        })
    }

    fn process_drop_tenant(&self, cluster: &str, name: &str) -> MetaResult<()> {
        let key = KeyPath::tenant(cluster, name);
        let limiter_key = KeyPath::limiter(cluster, name);

        self.remove(&key)?;
        self.remove(&limiter_key)?;

        Ok(())
    }

    fn process_add_member_to_tenant(
        &self,
        cluster: &str,
        user_id: &Oid,
        role: &TenantRoleIdentifier,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if self.contains_key(&key)? {
            return Err(MetaError::UserAlreadyExists {
                user: user_id.to_string(),
            });
        }

        self.insert(&key, &value_encode(&role)?)
    }

    fn process_remove_member_to_tenant(
        &self,
        cluster: &str,
        user_id: &Oid,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if self.contains_key(&key)? {
            self.remove(&key)?;

            Ok(())
        } else {
            Err(MetaError::UserNotFound {
                user: user_id.to_string(),
            })
        }
    }

    fn process_reasign_member_role(
        &self,
        cluster: &str,
        user_id: &Oid,
        role: &TenantRoleIdentifier,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::member(cluster, tenant_name, user_id);

        if !self.contains_key(&key)? {
            return Err(MetaError::UserNotFound {
                user: user_id.to_string(),
            });
        }

        self.insert(&key, &value_encode(&role)?)
    }

    fn process_create_role(
        &self,
        cluster: &str,
        role_name: &str,
        sys_role: &SystemTenantRole,
        privileges: &HashMap<String, DatabasePrivilege>,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::role(cluster, tenant_name, role_name);

        if self.contains_key(&key)? {
            return Err(MetaError::RoleAlreadyExists {
                role: role_name.to_string(),
            });
        }

        let oid = UuidGenerator::default().next_id();
        let role = CustomTenantRole::new(
            oid,
            role_name.to_string(),
            sys_role.clone(),
            privileges.clone(),
        );

        self.insert(&key, &value_encode(&role)?)
    }

    fn process_drop_role(
        &self,
        cluster: &str,
        role_name: &str,
        tenant_name: &str,
    ) -> MetaResult<bool> {
        let key = KeyPath::role(cluster, tenant_name, role_name);

        if !self.contains_key(&key)? {
            return Err(MetaError::RoleNotFound {
                role: role_name.to_string(),
            });
        }

        self.remove(&key)?;
        Ok(true)
    }

    fn process_grant_privileges(
        &self,
        cluster: &str,
        privileges: &[(DatabasePrivilege, String)],
        role_name: &str,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::role(cluster, tenant_name, role_name);
        if let Some(mut role) = self.get_struct::<CustomTenantRole<Oid>>(&key)? {
            for (privilege, database_name) in privileges {
                let _ = role.grant_privilege(database_name.clone(), privilege.clone());
            }

            Ok(self.insert(&key, &value_encode(&role)?)?)
        } else {
            Err(MetaError::RoleNotFound {
                role: role_name.to_string(),
            })
        }
    }

    fn process_revoke_privileges(
        &self,
        cluster: &str,
        privileges: &[(DatabasePrivilege, String)],
        role_name: &str,
        tenant_name: &str,
    ) -> MetaResult<()> {
        let key = KeyPath::role(cluster, tenant_name, role_name);
        if let Some(mut role) = self.get_struct::<CustomTenantRole<Oid>>(&key)? {
            for (privilege, database_name) in privileges {
                let _ = role.revoke_privilege(database_name, privilege);
            }

            Ok(self.insert(&key, &value_encode(&role)?)?)
        } else {
            Err(MetaError::RoleNotFound {
                role: role_name.to_string(),
            })
        }
    }

    fn process_limiter_request(
        &self,
        cluster: &str,
        tenant: &str,
        requests: &LocalBucketRequest,
    ) -> MetaResult<LocalBucketResponse> {
        let mut rsp = LocalBucketResponse {
            kind: requests.kind,
            alloc: requests.expected.max,
        };
        let key = KeyPath::limiter(cluster, tenant);

        let limiter = match self.get_struct::<RemoteRequestLimiter>(&key)? {
            Some(b) => b,
            None => {
                return Ok(rsp);
            }
        };

        let bucket = match limiter.buckets.get(&requests.kind) {
            Some(bucket) => bucket,
            None => {
                return Ok(rsp);
            }
        };
        let alloc = bucket.acquire_closed(requests.expected.max as usize);

        self.set_tenant_limiter(cluster, tenant, Some(limiter))?;
        rsp.alloc = alloc as i64;

        Ok(rsp)
    }
}

fn check_node_enough(need: u64, node_list: &[NodeInfo]) -> MetaResult<()> {
    if need > node_list.len() as u64 {
        return Err(MetaError::ValidNodeNotEnough {
            need,
            valid_node_num: node_list.len() as u32,
        });
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::println;

    use serde::{Deserialize, Serialize};

    #[test]
    fn test_btree_map() {
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

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct RequestCommand {
        key: String,
        value: String,
    }

    #[test]
    fn test_json() {
        let command = RequestCommand {
            key: "xxxxxxxk".to_string(),
            value: "xxxxxxxv".to_string(),
        };
        let data = serde_json::to_string(&command).unwrap();
        println!("{}", data);

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
