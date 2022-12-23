#![allow(dead_code, unused_imports, unused_variables)]

use client::MetaHttpClient;
use config::ClusterConfig;
use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{
    CustomTenantRole, SystemTenantRole, TenantRole, TenantRoleIdentifier, UserRole,
};
use models::auth::user::{User, UserDesc};
use models::meta_data::*;
use models::oid::{Identifier, Oid};
use parking_lot::RwLock;
use rand::distributions::{Alphanumeric, DistString};
use snafu::Snafu;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::{fmt::Debug, io};
use store::command;
use tokio::net::TcpStream;

use trace::{debug, info, warn};

use crate::error::{MetaError, MetaResult};
use models::schema::{
    DatabaseSchema, ExternalTableSchema, TableSchema, Tenant, TenantOptions, TskvTableSchema,
};

use crate::store::command::{
    META_REQUEST_FAILED, META_REQUEST_PRIVILEGE_EXIST, META_REQUEST_PRIVILEGE_NOT_FOUND,
    META_REQUEST_ROLE_EXIST, META_REQUEST_ROLE_NOT_FOUND, META_REQUEST_SUCCESS,
    META_REQUEST_USER_EXIST, META_REQUEST_USER_NOT_FOUND,
};
use crate::tenant_manager::RemoteTenantManager;
use crate::user_manager::{RemoteUserManager, UserManager, UserManagerMock};
use crate::{client, store};
pub type UserManagerRef = Arc<dyn UserManager>;
pub type TenantManagerRef = Arc<dyn TenantManager>;
pub type MetaClientRef = Arc<dyn MetaClient>;
pub type AdminMetaRef = Arc<dyn AdminMeta>;
pub type MetaRef = Arc<dyn MetaManager>;

pub trait MetaManager: Send + Sync + Debug {
    fn node_id(&self) -> u64;
    fn admin_meta(&self) -> AdminMetaRef;
    fn user_manager(&self) -> UserManagerRef;
    fn tenant_manager(&self) -> TenantManagerRef;
    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo>;
    fn user_with_privileges(&self, user_name: &str, tenant_name: Option<&str>) -> MetaResult<User>;
}

pub trait TenantManager: Send + Sync + Debug {
    // tenant
    fn create_tenant(&self, name: String, options: TenantOptions) -> MetaResult<MetaClientRef>;
    fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>>;
    fn tenants(&self) -> MetaResult<Vec<Tenant>>;
    fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()>;
    fn drop_tenant(&self, name: &str) -> MetaResult<bool>;
    // tenant object meta manager
    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo>;
}

#[async_trait::async_trait]
pub trait AdminMeta: Send + Sync + Debug {
    // *数据节点上下线管理 */
    fn data_nodes(&self) -> Vec<NodeInfo>;
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()>;
    // fn del_data_node(&self, id: u64) -> MetaResult<()>;

    // fn meta_nodes(&self);
    // fn add_meta_node(&self, node: &NodeInfo) -> MetaResult<()>;
    // fn del_meta_node(&self, id: u64) -> MetaResult<()>;

    fn heartbeat(&self); // update node status

    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo>;
    async fn get_node_conn(&self, node_id: u64) -> MetaResult<TcpStream>;
    fn put_node_conn(&self, node_id: u64, conn: TcpStream);
}

pub trait MetaClient: Send + Sync + Debug {
    fn tenant(&self) -> &Tenant;
    fn tenant_mut(&mut self) -> &mut Tenant;

    //fn create_user(&self, user: &UserInfo) -> MetaResult<()>;
    //fn drop_user(&self, name: &str) -> MetaResult<()>;

    // tenant member
    // fn tenants_of_user(&mut self, user_id: &Oid) -> MetaResult<Option<&HashSet<Oid>>>;
    // fn remove_member_from_all_tenants(&mut self, user_id: &Oid) -> MetaResult<bool>;
    fn add_member_with_role(&self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()>;
    fn member_role(&self, user_id: &Oid) -> MetaResult<Option<TenantRoleIdentifier>>;
    fn members(&self) -> MetaResult<HashMap<String, TenantRoleIdentifier>>;
    fn reasign_member_role(&self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()>;
    fn remove_member(&self, user_id: Oid) -> MetaResult<()>;

    // tenant role
    fn create_custom_role(
        &self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()>;
    fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>>;
    fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>>;
    fn grant_privilege_to_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()>;
    fn revoke_privilege_from_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()>;
    fn drop_custom_role(&self, role_name: &str) -> MetaResult<bool>;

    fn create_db(&self, info: &DatabaseSchema) -> MetaResult<()>;
    fn alter_db_schema(&self, info: &DatabaseSchema) -> MetaResult<()>;
    fn get_db_schema(&self, name: &str) -> MetaResult<Option<DatabaseSchema>>;
    fn list_databases(&self) -> MetaResult<Vec<String>>;
    fn drop_db(&self, name: &str) -> MetaResult<bool>;

    fn create_table(&self, schema: &TableSchema) -> MetaResult<()>;
    fn update_table(&self, schema: &TableSchema) -> MetaResult<()>;
    fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>>;
    fn get_tskv_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TskvTableSchema>>;
    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<ExternalTableSchema>>;
    fn list_tables(&self, db: &str) -> MetaResult<Vec<String>>;
    fn drop_table(&self, db: &str, table: &str) -> MetaResult<()>;

    fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo>;
    fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()>;

    fn database_min_ts(&self, db: &str) -> Option<i64>;
    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo>;

    fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> MetaResult<Vec<BucketInfo>>;

    fn locate_replcation_set_for_write(
        &self,
        db: &str,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplcationSet>;

    fn print_data(&self) -> String;
}

#[derive(Debug)]
pub struct RemoteMetaManager {
    config: ClusterConfig,
    node_info: NodeInfo,

    admin: AdminMetaRef,
    user_manager: UserManagerRef,
    tenant_manager: TenantManagerRef,
}

impl RemoteMetaManager {
    pub fn new(config: ClusterConfig) -> Self {
        let admin: AdminMetaRef = Arc::new(RemoteAdminMeta::new(
            config.name.clone(),
            config.meta.clone(),
        ));
        let user_manager = Arc::new(RemoteUserManager::new(
            config.name.clone(),
            config.meta.clone(),
        ));
        let tenant_manager = Arc::new(RemoteTenantManager::new(
            config.name.clone(),
            config.meta.clone(),
            config.node_id,
        ));

        let node_info = NodeInfo {
            status: 0,
            id: config.node_id,
            tcp_addr: config.tcp_server.clone(),
            http_addr: config.http_server.clone(),
        };

        admin.add_data_node(&node_info).unwrap();

        Self {
            config,
            admin,
            node_info,
            user_manager,
            tenant_manager,
        }
    }
}

impl MetaManager for RemoteMetaManager {
    fn node_id(&self) -> u64 {
        self.config.node_id
    }

    fn admin_meta(&self) -> AdminMetaRef {
        self.admin.clone()
    }

    fn user_manager(&self) -> UserManagerRef {
        self.user_manager.clone()
    }

    fn tenant_manager(&self) -> TenantManagerRef {
        self.tenant_manager.clone()
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        self.tenant_manager.expired_bucket()
    }

    fn user_with_privileges(&self, user_name: &str, tenant_name: Option<&str>) -> MetaResult<User> {
        let user_desc =
            self.user_manager
                .user(user_name)?
                .ok_or_else(|| MetaError::UserNotFound {
                    user: user_name.to_string(),
                })?;

        // admin user
        if user_desc.is_admin() {
            return Ok(User::new(user_desc, UserRole::Dba.to_privileges()));
        }

        // common user & with tenant
        if let Some(tenant_name) = tenant_name {
            let client = self
                .tenant_manager
                .tenant_meta(tenant_name)
                .ok_or_else(|| MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                })?;

            let tenant_id = client.tenant().id();
            let role =
                client
                    .member_role(user_desc.id())?
                    .ok_or_else(|| MetaError::MemberNotFound {
                        member_name: user_desc.name().to_string(),
                        tenant_name: tenant_name.to_string(),
                    })?;

            let privileges = match role {
                TenantRoleIdentifier::System(sys_role) => sys_role.to_privileges(tenant_id),
                TenantRoleIdentifier::Custom(ref role_name) => client
                    .custom_role(role_name)?
                    .map(|e| e.to_privileges(tenant_id))
                    .unwrap_or_default(),
            };

            return Ok(User::new(user_desc, privileges));
        }

        // common user & without tenant
        Ok(User::new(user_desc, Default::default()))
    }
}

#[derive(Debug)]
pub struct RemoteAdminMeta {
    cluster: String,
    meta_url: String,
    data_nodes: RwLock<HashMap<u64, NodeInfo>>,
    conn_map: RwLock<HashMap<u64, VecDeque<TcpStream>>>,

    client: MetaHttpClient,
}

impl RemoteAdminMeta {
    pub fn new(cluster: String, meta_url: String) -> Self {
        Self {
            cluster,
            meta_url: meta_url.clone(),
            conn_map: RwLock::new(HashMap::new()),
            data_nodes: RwLock::new(HashMap::new()),
            client: MetaHttpClient::new(1, meta_url),
        }
    }

    pub fn sys_info() -> SysInfo {
        let mut info = SysInfo::default();

        if let Ok(val) = sys_info::disk_info() {
            info.disk_free = val.free;
        }

        if let Ok(val) = sys_info::mem_info() {
            info.mem_free = val.free;
        }

        if let Ok(val) = sys_info::loadavg() {
            info.cpu_load = val.one;
        }

        info
    }
}

#[async_trait::async_trait]
impl AdminMeta for RemoteAdminMeta {
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()> {
        let req = command::WriteCommand::AddDataNode(self.cluster.clone(), node.clone());
        let rsp = self.client.write::<command::StatusResponse>(&req)?;
        if rsp.code != command::META_REQUEST_SUCCESS {
            return Err(MetaError::CommonError {
                msg: format!("add data node err: {} {}", rsp.code, rsp.msg),
            });
        }

        Ok(())
    }

    fn data_nodes(&self) -> Vec<NodeInfo> {
        let req = command::ReadCommand::DataNodes(self.cluster.clone());
        let resp = self.client.read::<Vec<NodeInfo>>(&req).unwrap();
        {
            let mut nodes = self.data_nodes.write();
            for item in resp.iter() {
                nodes.insert(item.id, item.clone());
            }
        }

        let mut nodes = vec![];
        for (_, val) in self.data_nodes.read().iter() {
            nodes.push(val.clone())
        }

        nodes
    }

    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        if let Some(val) = self.data_nodes.read().get(&id) {
            return Ok(val.clone());
        }

        let req = command::ReadCommand::DataNodes(self.cluster.clone());
        let resp = self.client.read::<Vec<NodeInfo>>(&req)?;
        {
            let mut nodes = self.data_nodes.write();
            for item in resp.iter() {
                nodes.insert(item.id, item.clone());
            }
        }

        if let Some(val) = self.data_nodes.read().get(&id) {
            return Ok(val.clone());
        }

        Err(MetaError::NotFoundNode { id })
    }

    async fn get_node_conn(&self, node_id: u64) -> MetaResult<TcpStream> {
        {
            let mut write = self.conn_map.write();
            let entry = write
                .entry(node_id)
                .or_insert_with(|| VecDeque::with_capacity(32));
            if let Some(val) = entry.pop_front() {
                return Ok(val);
            }
        }

        let info = self.node_info_by_id(node_id)?;
        let client = TcpStream::connect(info.tcp_addr).await?;

        return Ok(client);
    }

    fn put_node_conn(&self, node_id: u64, conn: TcpStream) {
        let mut write = self.conn_map.write();
        let entry = write
            .entry(node_id)
            .or_insert_with(|| VecDeque::with_capacity(32));

        // close too more idle connection
        if entry.len() < 32 {
            entry.push_back(conn);
        }
    }

    fn heartbeat(&self) {}
}

#[derive(Debug)]
pub struct RemoteMetaClient {
    cluster: String,
    tenant: Tenant,
    meta_url: String,

    data: RwLock<TenantMetaData>,
    client: MetaHttpClient,
    client_id: String,
}

impl RemoteMetaClient {
    pub fn new(cluster: String, tenant: Tenant, meta_url: String, node_id: u64) -> Arc<Self> {
        let mut rng = rand::thread_rng();
        let random = Alphanumeric.sample_string(&mut rng, 16);

        let client_id = format!("{}.{}.{}.{}", &cluster, &tenant.name(), node_id, random);

        let client = Arc::new(Self {
            cluster,
            tenant,
            client_id,
            meta_url: meta_url.clone(),
            data: RwLock::new(TenantMetaData::new()),
            client: MetaHttpClient::new(1, meta_url),
        });

        let _ = client.sync_all_tenant_metadata();

        let client_local = client.clone();
        let hand = std::thread::spawn(|| RemoteMetaClient::watch_data(client_local));

        client
    }

    pub fn watch_data(client: Arc<RemoteMetaClient>) {
        let mut cmd = (
            client.client_id.clone(),
            client.cluster.clone(),
            client.tenant.name().to_string(),
            0,
        );

        loop {
            cmd.3 = client.data.read().version;
            match client
                .client
                .watch_tenant::<command::TenantMetaDataDelta>(&cmd)
            {
                Ok(delta) => {
                    let mut data = client.data.write();
                    if delta.full_load {
                        if delta.update.version > data.version {
                            *data = delta.update;
                        }
                    } else if data.version >= delta.ver_range.0 && data.version < delta.ver_range.1
                    {
                        data.merge_into(&delta.update);
                        data.delete_from(&delta.delete);
                        data.version = delta.ver_range.1;
                    }
                }

                Err(err) => {
                    info!("watch data result: {:?} {}", &cmd, err)
                }
            }
        }
    }

    fn sync_all_tenant_metadata(&self) -> MetaResult<()> {
        let req = command::ReadCommand::TenaneMetaData(
            self.cluster.clone(),
            self.tenant.name().to_string(),
        );
        let resp = self.client.read::<command::TenaneMetaDataResp>(&req)?;
        if resp.status.code < 0 {
            return Err(MetaError::CommonError {
                msg: format!("open meta err: {} {}", resp.status.code, resp.status.msg),
            });
        }

        let mut data = self.data.write();
        if resp.data.version > data.version {
            *data = resp.data;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl MetaClient for RemoteMetaClient {
    fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    fn tenant_mut(&mut self) -> &mut Tenant {
        &mut self.tenant
    }

    // tenant member start

    fn add_member_with_role(&self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()> {
        let req = command::WriteCommand::AddMemberToTenant(
            self.cluster.clone(),
            user_id,
            role,
            self.tenant.name().to_string(),
        );

        match self.client.write::<command::CommonResp<()>>(&req)? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_EXIST {
                    Err(MetaError::UserAlreadyExists { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    fn member_role(&self, user_id: &Oid) -> MetaResult<Option<TenantRoleIdentifier>> {
        let req = command::ReadCommand::MemberRole(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            *user_id,
        );

        match self
            .client
            .read::<command::CommonResp<Option<TenantRoleIdentifier>>>(&req)?
        {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    fn members(&self) -> MetaResult<HashMap<String, TenantRoleIdentifier>> {
        let req =
            command::ReadCommand::Members(self.cluster.clone(), self.tenant.name().to_string());

        match self
            .client
            .read::<command::CommonResp<HashMap<String, TenantRoleIdentifier>>>(&req)?
        {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                warn!(
                    "members of tenant {}, error: {}",
                    self.tenant().name(),
                    status.msg
                );
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    fn reasign_member_role(&self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()> {
        let req = command::WriteCommand::ReasignMemberRole(
            self.cluster.clone(),
            user_id,
            role,
            self.tenant.name().to_string(),
        );

        match self.client.write::<command::CommonResp<()>>(&req)? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    fn remove_member(&self, user_id: Oid) -> MetaResult<()> {
        let req = command::WriteCommand::RemoveMemberFromTenant(
            self.cluster.clone(),
            user_id,
            self.tenant.name().to_string(),
        );

        match self.client.write::<command::CommonResp<()>>(&req)? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    // tenant member end

    // tenant role start

    fn create_custom_role(
        &self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()> {
        let req = command::WriteCommand::CreateRole(
            self.cluster.clone(),
            role_name,
            system_role,
            additiona_privileges,
            self.tenant.name().to_string(),
        );

        match self.client.write::<command::CommonResp<()>>(&req)? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_ROLE_EXIST {
                    Err(MetaError::RoleAlreadyExists { role: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>> {
        let req = command::ReadCommand::CustomRole(
            self.cluster.clone(),
            role_name.to_string(),
            self.tenant.name().to_string(),
        );

        match self
            .client
            .read::<command::CommonResp<Option<CustomTenantRole<Oid>>>>(&req)?
        {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        let req =
            command::ReadCommand::CustomRoles(self.cluster.clone(), self.tenant.name().to_string());

        match self
            .client
            .read::<command::CommonResp<Vec<CustomTenantRole<Oid>>>>(&req)?
        {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                warn!("custom roles not found, {}", status.msg);
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    fn grant_privilege_to_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        let req = command::WriteCommand::GrantPrivileges(
            self.cluster.clone(),
            database_privileges,
            role_name.to_string(),
            self.tenant.name().to_string(),
        );

        match self.client.write::<command::CommonResp<()>>(&req)? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_ROLE_NOT_FOUND {
                    Err(MetaError::RoleNotFound { role: status.msg })
                } else if status.code == META_REQUEST_PRIVILEGE_EXIST {
                    Err(MetaError::PrivilegeAlreadyExists { name: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    fn revoke_privilege_from_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        let req = command::WriteCommand::RevokePrivileges(
            self.cluster.clone(),
            database_privileges,
            role_name.to_string(),
            self.tenant.name().to_string(),
        );

        match self.client.write::<command::CommonResp<()>>(&req)? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_ROLE_NOT_FOUND {
                    Err(MetaError::RoleNotFound { role: status.msg })
                } else if status.code == META_REQUEST_PRIVILEGE_NOT_FOUND {
                    Err(MetaError::PrivilegeNotFound { name: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    fn drop_custom_role(&self, role_name: &str) -> MetaResult<bool> {
        let req = command::WriteCommand::DropRole(
            self.cluster.clone(),
            role_name.to_string(),
            self.tenant.name().to_string(),
        );

        match self.client.write::<command::CommonResp<bool>>(&req)? {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    // tenant role end

    fn create_db(&self, schema: &DatabaseSchema) -> MetaResult<()> {
        let req = command::WriteCommand::CreateDB(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            schema.clone(),
        );

        let rsp = self.client.write::<command::TenaneMetaDataResp>(&req)?;
        let mut data = self.data.write();
        if rsp.data.version > data.version {
            *data = rsp.data;
        }

        if rsp.status.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else if rsp.status.code == command::META_REQUEST_DB_EXIST {
            Err(MetaError::DatabaseAlreadyExists {
                database: schema.database_name().to_string(),
            })
        } else {
            Err(MetaError::CommonError {
                msg: rsp.status.to_string(),
            })
        }
    }

    fn alter_db_schema(&self, info: &DatabaseSchema) -> MetaResult<()> {
        let req = command::WriteCommand::AlterDB(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            info.clone(),
        );

        let rsp = self.client.write::<command::StatusResponse>(&req)?;
        info!("alter db: {:?}; {:?}", req, rsp);

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else {
            Err(MetaError::CommonError {
                msg: rsp.to_string(),
            })
        }
    }

    fn get_db_schema(&self, name: &str) -> MetaResult<Option<DatabaseSchema>> {
        if let Some(db) = self.data.read().dbs.get(name) {
            return Ok(Some(db.schema.clone()));
        }

        Ok(None)
    }

    fn list_databases(&self) -> MetaResult<Vec<String>> {
        let mut list = vec![];
        for (k, _) in self.data.read().dbs.iter() {
            list.push(k.clone());
        }

        Ok(list)
    }

    fn drop_db(&self, name: &str) -> MetaResult<bool> {
        let mut exist = false;
        if self.data.read().dbs.contains_key(name) {
            exist = true;
        }

        let req = command::WriteCommand::DropDB(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            name.to_string(),
        );

        let rsp = self.client.write::<command::StatusResponse>(&req)?;
        info!("drop db: {:?}; {:?}", req, rsp);

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(exist)
        } else {
            Err(MetaError::CommonError {
                msg: rsp.to_string(),
            })
        }
    }

    fn create_table(&self, schema: &TableSchema) -> MetaResult<()> {
        let req = command::WriteCommand::CreateTable(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            schema.clone(),
        );

        debug!("create_table: {:?}", req);

        let rsp = self.client.write::<command::TenaneMetaDataResp>(&req)?;
        let mut data = self.data.write();
        if rsp.data.version > data.version {
            *data = rsp.data;
        }

        if rsp.status.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else if rsp.status.code == command::META_REQUEST_TABLE_EXIST {
            Err(MetaError::TableAlreadyExists {
                table_name: schema.name(),
            })
        } else {
            Err(MetaError::CommonError {
                msg: rsp.status.to_string(),
            })
        }
    }

    fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>> {
        return Ok(self.data.read().table_schema(db, table));
    }

    fn get_tskv_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TskvTableSchema>> {
        if let Some(TableSchema::TsKvTableSchema(val)) = self.data.read().table_schema(db, table) {
            return Ok(Some(val));
        }

        Ok(None)
    }

    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<ExternalTableSchema>> {
        if let Some(TableSchema::ExternalTableSchema(val)) =
            self.data.read().table_schema(db, table)
        {
            return Ok(Some(val));
        }

        Ok(None)
    }

    fn update_table(&self, schema: &TableSchema) -> MetaResult<()> {
        let req = command::WriteCommand::UpdateTable(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            schema.clone(),
        );

        let rsp = self.client.write::<command::TenaneMetaDataResp>(&req)?;
        let mut data = self.data.write();
        if rsp.data.version > data.version {
            *data = rsp.data;
        }

        // TODO table not exist

        Ok(())
    }

    fn list_tables(&self, db: &str) -> MetaResult<Vec<String>> {
        let mut list = vec![];
        if let Some(info) = self.data.read().dbs.get(db) {
            for (k, _) in info.tables.iter() {
                list.push(k.clone());
            }
        }

        Ok(list)
    }

    fn drop_table(&self, db: &str, table: &str) -> MetaResult<()> {
        let req = command::WriteCommand::DropTable(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            db.to_string(),
            table.to_string(),
        );

        let rsp = self.client.write::<command::StatusResponse>(&req)?;

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else {
            Err(MetaError::CommonError {
                msg: rsp.to_string(),
            })
        }
    }

    fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo> {
        let req = command::WriteCommand::CreateBucket(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            db.to_string(),
            ts,
        );

        let rsp = self.client.write::<command::TenaneMetaDataResp>(&req)?;
        {
            let mut data = self.data.write();
            if rsp.data.version > data.version {
                *data = rsp.data;
            }
        }

        if rsp.status.code < 0 {
            return Err(MetaError::MetaClientErr {
                msg: format!("create bucket err: {} {}", rsp.status.code, rsp.status.msg),
            });
        }

        if let Some(bucket) = self.data.read().bucket_by_timestamp(db, ts) {
            return Ok(bucket.clone());
        }

        Err(MetaError::CommonError {
            msg: "create bucket unknown error".to_string(),
        })
    }

    fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()> {
        let req = command::WriteCommand::DeleteBucket(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            db.to_string(),
            id,
        );

        let rsp = self.client.write::<command::StatusResponse>(&req)?;
        info!("delete bucket: {:?}; {:?}", req, rsp);

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else {
            return Err(MetaError::CommonError {
                msg: rsp.to_string(),
            });
        }
    }

    fn database_min_ts(&self, name: &str) -> Option<i64> {
        self.data.read().database_min_ts(name)
    }

    fn locate_replcation_set_for_write(
        &self,
        db: &str,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplcationSet> {
        if let Some(bucket) = self.data.read().bucket_by_timestamp(db, ts) {
            return Ok(bucket.vnode_for(hash_id));
        }

        let bucket = self.create_bucket(db, ts)?;

        Ok(bucket.vnode_for(hash_id))
    }

    fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> MetaResult<Vec<BucketInfo>> {
        let buckets = self.data.read().mapping_bucket(db_name, start, end);

        Ok(buckets)
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        let mut list = vec![];
        for (key, val) in self.data.read().dbs.iter() {
            let ttl = val.schema.config.ttl_or_default().time_stamp();
            let now = models::utils::now_timestamp();

            for bucket in val.buckets.iter() {
                if bucket.end_time < now - ttl {
                    let info = ExpiredBucketInfo {
                        tenant: self.tenant.name().to_string(),
                        database: key.clone(),
                        bucket: bucket.clone(),
                    };

                    list.push(info)
                }
            }
        }

        list
    }

    fn print_data(&self) -> String {
        info!("****** Tenant: {:?}; Meta: {}", self.tenant, self.meta_url);
        info!("****** Meta Data: {:#?}", self.data);

        format!("{:#?}", self.data.read())
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn test_sys_info() {
        let info = sys_info::disk_info();
        println!("Disk: {:?}", info);

        let info = sys_info::mem_info();
        println!("Mem: {:?}", info);

        let info = sys_info::cpu_num();
        println!("Cpu Num: {:?}", info);

        let info = sys_info::loadavg();
        println!("Cpu Num: {:?}", info);
    }
}
