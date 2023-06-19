use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{UserDesc, UserOptions};
use models::meta_data::*;
use models::node_info::NodeStatus;
use models::oid::{Identifier, Oid, UuidGenerator};
use models::schema::{DatabaseSchema, TableSchema, Tenant, TenantOptions};
use openraft::{EffectiveMembership, LogId};
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, from_str};
use trace::{debug, error, info};

use super::command::*;
use super::key_path;
use crate::error::{l_r_err, sm_r_err, sm_w_err, MetaError, MetaResult, StorageIOResult};
use crate::limiter::local_request_limiter::{LocalBucketRequest, LocalBucketResponse};
use crate::limiter::remote_request_limiter::RemoteRequestLimiter;
use crate::store::key_path::KeyPath;
use crate::{ClusterNode, ClusterNodeId};

pub type CommandResp = String;

pub fn value_encode<T: Serialize>(d: &T) -> MetaResult<String> {
    serde_json::to_string(d).map_err(|e| MetaError::SerdeMsgEncode { err: e.to_string() })
}

pub fn response_encode<T: Serialize>(d: MetaResult<T>) -> String {
    match serde_json::to_string(&d) {
        Ok(val) => val,
        Err(err) => {
            let err_rsp = MetaError::SerdeMsgEncode {
                err: err.to_string(),
            };

            serde_json::to_string(&err_rsp).unwrap()
        }
    }
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
        for entry_res in state.data_tree.iter() {
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

pub struct StateMachine {
    pub db: Arc<sled::Db>,
    pub data_tree: sled::Tree,
    pub state_machine: sled::Tree,
    pub watch: Arc<Watch>,
}

impl StateMachine {
    pub fn new(db: Arc<sled::Db>) -> StateMachine {
        let sm = Self {
            db: db.clone(),
            data_tree: db.open_tree("data").expect("data open failed"),
            state_machine: db
                .open_tree("state_machine")
                .expect("state_machine open failed"),

            watch: Arc::new(Watch::new()),
        };

        sm.write_nop_log();

        sm
    }

    pub(crate) fn get_last_membership(
        &self,
    ) -> StorageIOResult<EffectiveMembership<ClusterNodeId, ClusterNode>> {
        self.state_machine
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
        self.state_machine
            .insert(b"last_membership", value)
            .map_err(sm_w_err)?;

        Ok(())
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
        self.state_machine
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
        self.state_machine
            .insert(b"last_applied_log", value)
            .map_err(l_r_err)?;

        Ok(())
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
        let data_tree = db.open_tree("data").expect("store open failed");
        let mut batch = sled::Batch::default();
        for (key, value) in sm.data {
            batch.insert(key.as_bytes(), value.as_bytes())
        }
        data_tree.apply_batch(batch).map_err(sm_w_err)?;

        let r = StateMachine::new(db);
        r.write_nop_log();

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

    //********************************************************************************* */
    //todo: temp it will be removed
    fn version(&self) -> u64 {
        let key = KeyPath::version();

        let mut ver_str = "0".to_string();
        if let Some(val) = self.db.get(key).unwrap() {
            unsafe { ver_str = String::from_utf8_unchecked((*val).to_owned()) };
        }

        from_str::<u64>(&ver_str).unwrap_or(0)
    }

    fn update_version(&self) -> StorageIOResult<u64> {
        let key = KeyPath::version();

        let mut ver_str = "0".to_string();
        if let Some(val) = self.db.get(&key).unwrap() {
            unsafe { ver_str = String::from_utf8_unchecked((*val).to_owned()) };
        }
        let ver = from_str::<u64>(&ver_str).unwrap_or(0) + 1;
        let val = &*(ver.to_string());
        self.db.insert(&key, val).map_err(l_r_err)?;
        info!("METADATA WRITE: {} :{}", &key, val);
        Ok(ver)
    }

    fn fetch_and_add_incr_id(&self, cluster: &str, count: u32) -> StorageIOResult<u32> {
        let id_key = KeyPath::incr_id(cluster);

        let mut id_str = "1".to_string();
        if let Some(val) = self.get(&id_key)? {
            unsafe { id_str = String::from_utf8_unchecked((*val).to_owned()) };
        }
        let id_num = from_str::<u32>(&id_str).unwrap_or(1);

        let val = &*(id_num + count).to_string();
        self.db.insert(&id_key, val).map_err(sm_w_err)?;
        info!("METADATA WRITE: {} :{}", &id_key, val);

        Ok(id_num)
    }

    pub fn write_nop_log(&self) {
        let log = EntryLog {
            tye: ENTRY_LOG_TYPE_NOP,
            ver: self.version(),
            key: "".to_string(),
            val: "".to_string(),
        };

        self.watch.writer_log(log);
    }

    fn get(&self, key: &str) -> StorageIOResult<Option<sled::IVec>> {
        let val = self.db.get(key).map_err(sm_r_err)?;

        Ok(val)
    }

    fn contains_key(&self, key: &str) -> StorageIOResult<bool> {
        let exist = self.db.contains_key(key).map_err(sm_r_err)?;

        Ok(exist)
    }

    fn insert(&self, key: &str, val: &str) -> StorageIOResult<()> {
        let version = self.update_version()?;
        self.db.insert(key, val).map_err(l_r_err)?;
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

    fn remove(&self, key: &str) -> StorageIOResult<()> {
        let version = self.update_version()?;
        self.db.remove(key).map_err(l_r_err)?;
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

        let (logs, status) = self.watch.read_entry_logs(cluster, tenants, base_ver);
        if status < 0 {
            data.full_sync = true;
        } else {
            data.entry_logs = logs;
        }

        data
    }

    pub fn get_struct<T>(&self, key: &str) -> MetaResult<Option<T>>
    where
        for<'a> T: Deserialize<'a>,
    {
        let val = self.get(key)?;
        if let Some(data) = val {
            let info: T = from_slice(&data).map_err(sm_r_err)?;
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
        for res in self.db.scan_prefix(path.as_bytes()) {
            match res {
                Err(err) => {
                    return Err(crate::error::MetaError::MetaStoreIO {
                        err: err.to_string(),
                    })
                }
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
        let keys = self.children_fullpath(&path)?;
        for it in keys.iter() {
            let val = self.get(it)?;
            if let Some(data) = val {
                if let Ok(info) = from_slice(&data) {
                    if let Some(key) = it.strip_prefix(path.as_str()) {
                        result.insert(key.to_string(), info);
                    }
                }
            }
        }

        Ok(result)
    }

    pub fn to_tenant_meta_data(&self, cluster: &str, tenant: &str) -> MetaResult<TenantMetaData> {
        let mut meta = TenantMetaData::new();
        meta.version = self.version();
        meta.users = self.children_data::<UserInfo>(&KeyPath::tenant_users(cluster, tenant))?;

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
        }
    }

    pub fn process_read_data_nodes(&self, cluster: &str) -> MetaResult<(Vec<NodeInfo>, u64)> {
        let response: Vec<NodeInfo> = self
            .children_data::<NodeInfo>(&KeyPath::data_nodes(cluster))?
            .into_values()
            .collect();

        Ok((response, self.version()))
    }

    pub fn process_read_node_metrics(&self, cluster: &str) -> MetaResult<Vec<NodeMetrics>> {
        let response: Vec<NodeMetrics> = self
            .children_data::<NodeMetrics>(&KeyPath::data_nodes(cluster))?
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
            WriteCommand::CreateUser(cluster, name, options, is_admin) => {
                response_encode(self.process_create_user(cluster, name, options, *is_admin))
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
            WriteCommand::CreateTenant(cluster, name, options) => {
                response_encode(self.process_create_tenant(cluster, name, options))
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
            WriteCommand::UpdateVnode(args) => response_encode(self.process_update_vnode(args)),
            WriteCommand::LimiterRequest {
                cluster,
                tenant,
                request,
            } => response_encode(self.process_limiter_request(cluster, tenant, request)),
        }
    }

    fn process_write_set(&self, key: &str, val: &str) -> MetaResult<()> {
        Ok(self.insert(key, val)?)
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
        Ok(self.insert(&key, &value)?)
    }

    fn process_add_node_metrics(
        &self,
        cluster: &str,
        node_metrics: &NodeMetrics,
    ) -> MetaResult<()> {
        let key = KeyPath::data_node_metrics(cluster, node_metrics.id);
        let value = value_encode(node_metrics)?;
        Ok(self.insert(&key, &value)?)
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

        Ok(self.remove(&key)?)
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
        if db_schema.config.shard_num_or_default() == 0
            || db_schema.config.replica_or_default()
                > self
                    .children_data::<NodeInfo>(&KeyPath::data_nodes(cluster))?
                    .into_values()
                    .count() as u64
        {
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
        let key = KeyPath::tenant_db_name(cluster, tenant, &schema.db());
        if !self.contains_key(&key)? {
            return Err(MetaError::DatabaseNotFound {
                database: schema.db(),
            });
        }
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db(), &schema.name());
        if self.contains_key(&key)? {
            return Err(MetaError::TableAlreadyExists {
                table_name: schema.name(),
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
        let key = KeyPath::tenant_schema_name(cluster, tenant, &schema.db(), &schema.name());
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

        let node_metrics_list: Vec<NodeMetrics> = self
            .children_data::<NodeMetrics>(&KeyPath::data_nodes_metrics(cluster))?
            .into_values()
            .collect();

        let mut filter_node_list = node_info_list;
        filter_node_list.retain(|node_info| node_info.attribute != NodeAttribute::Cold);

        let mut tmp_node_list: Vec<_> = filter_node_list
            .clone()
            .iter()
            .map(|a| {
                (
                    a.clone(),
                    node_metrics_list.iter().find(|b| b.id == a.id).unwrap(),
                )
            })
            .collect();

        if tmp_node_list.is_empty() {
            return Ok(filter_node_list);
        }

        tmp_node_list.sort_by(|(_, a), (_, b)| b.disk_free.cmp(&a.disk_free));

        let mut valid_node_list = Vec::new();

        for (node_info, node_metrics) in tmp_node_list.iter() {
            if node_metrics.status == NodeStatus::Healthy {
                valid_node_list.push(node_info.clone());
            }
        }

        Ok(valid_node_list)
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
        let res = self
            .get(&db_path)?
            .and_then(|v| from_slice::<DatabaseSchema>(&v).ok());
        let db_schema = match res {
            Some(info) => info,
            None => {
                return Err(MetaError::DatabaseNotFound {
                    database: db.to_string(),
                });
            }
        };

        let node_list = self.get_valid_node_list(cluster)?;
        if node_list.is_empty()
            || db_schema.config.shard_num_or_default() == 0
            || db_schema.config.replica_or_default() > node_list.len() as u64
        {
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
        Ok(self.remove(&key)?)
    }

    fn process_create_user(
        &self,
        cluster: &str,
        user_name: &str,
        user_options: &UserOptions,
        is_admin: bool,
    ) -> MetaResult<Oid> {
        let key = KeyPath::user(cluster, user_name);

        if self.contains_key(&key)? {
            return Err(MetaError::UserAlreadyExists {
                user: user_name.to_string(),
            });
        }

        let oid = UuidGenerator::default().next_id();
        let user_desc = UserDesc::new(oid, user_name.to_string(), user_options.clone(), is_admin);

        self.insert(&key, &value_encode(&user_desc)?)?;
        Ok(oid)
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

        Ok(self.remove(&key)?)
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
                return Ok(self.remove(&key)?);
            }
        };

        Ok(self.insert(&key, &value_encode(&limiter)?)?)
    }

    fn process_create_tenant(
        &self,
        cluster: &str,
        name: &str,
        options: &TenantOptions,
    ) -> MetaResult<Tenant> {
        let key = KeyPath::tenant(cluster, name);

        if self.db.contains_key(&key).unwrap() {
            return Err(MetaError::TenantAlreadyExists {
                tenant: name.to_string(),
            });
        }

        let oid = UuidGenerator::default().next_id();
        let tenant = Tenant::new(oid, name.to_string(), options.clone());

        let limiter = options.request_config().map(RemoteRequestLimiter::new);

        self.set_tenant_limiter(cluster, name, limiter)?;

        self.insert(&key, &value_encode(&tenant)?)?;

        Ok(tenant)
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

        Ok(self.insert(&key, &value_encode(&role)?)?)
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

        Ok(self.insert(&key, &value_encode(&role)?)?)
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

        Ok(self.insert(&key, &value_encode(&role)?)?)
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

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::println;

    use serde::{Deserialize, Serialize};

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
