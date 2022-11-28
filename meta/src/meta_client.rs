#![allow(dead_code, unused_imports, unused_variables)]

use client::MetaHttpClient;
use config::ClusterConfig;
use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRole, TenantRoleIdentifier};
use models::meta_data::*;
use models::oid::{Identifier, Oid};
use parking_lot::RwLock;
use snafu::Snafu;

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::{fmt::Debug, io};
use store::command;
use tokio::net::TcpStream;

use trace::info;

use models::schema::{Tenant, TenantOptions, TskvTableSchema};

use crate::tenant_manager::RemoteTenantManager;
use crate::user_manager::{UserManager, UserManagerMock};
use crate::{client, store};

#[derive(Snafu, Debug)]
pub enum MetaError {
    #[snafu(display("The tenant {} already exists", tenant))]
    TenantAlreadyExists { tenant: String },

    #[snafu(display("The tenant {} not found", tenant))]
    TenantNotFound { tenant: String },

    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },

    #[snafu(display("Not Found DB: {}", db))]
    NotFoundDb { db: String },

    #[snafu(display("Not Found Data Node: {}", id))]
    NotFoundNode { id: u64 },

    #[snafu(display("Request meta cluster error: {}", msg))]
    MetaClientErr { msg: String },

    #[snafu(display("Error: {}", msg))]
    CommonError { msg: String },
}

impl From<io::Error> for MetaError {
    fn from(err: io::Error) -> Self {
        MetaError::CommonError {
            msg: err.to_string(),
        }
    }
}

impl From<client::WriteError> for MetaError {
    fn from(err: client::WriteError) -> Self {
        MetaError::MetaClientErr {
            msg: err.to_string(),
        }
    }
}

pub type MetaResult<T> = Result<T, MetaError>;

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
}

pub trait TenantManager: Send + Sync + Debug {
    // tenant
    fn create_tenant(&self, name: String, options: TenantOptions) -> MetaResult<MetaClientRef>;
    fn tenant(&self, name: &str) -> MetaResult<Tenant>;
    fn alter_tenant(&self, tenant_id: Oid, options: TenantOptions) -> MetaResult<()>;
    fn drop_tenant(&self, name: &str) -> MetaResult<()>;
    // tenant object meta manager
    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;
}

#[async_trait::async_trait]
pub trait AdminMeta: Send + Sync + Debug {
    // *数据节点上下线管理 */
    // fn data_nodes(&self) -> Vec<NodeInfo>;
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()>;
    // fn del_data_node(&self, id: u64) -> MetaResult<()>;

    // fn meta_nodes(&self);
    // fn add_meta_node(&self, node: &NodeInfo) -> MetaResult<()>;
    // fn del_meta_node(&self, id: u64) -> MetaResult<()>;

    // fn heartbeat(&self); // update node status

    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo>;
    async fn get_node_conn(&self, node_id: u64) -> MetaResult<TcpStream>;
    fn put_node_conn(&self, node_id: u64, conn: TcpStream);
}

pub trait MetaClient: Send + Sync + Debug {
    fn tenant(&self) -> &Tenant;

    //fn create_user(&self, user: &UserInfo) -> MetaResult<()>;
    //fn drop_user(&self, name: &String) -> MetaResult<()>;

    // tenant member
    // fn tenants_of_user(&mut self, user_id: &Oid) -> MetaResult<Option<&HashSet<Oid>>>;
    // fn remove_member_from_all_tenants(&mut self, user_id: &Oid) -> MetaResult<bool>;
    fn add_member_with_role(&mut self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()>;
    fn member_role(&self, user_id: &Oid) -> MetaResult<TenantRole<Oid>>;
    fn members(&self) -> MetaResult<Option<HashSet<&Oid>>>;
    fn reasign_member_role(&mut self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()>;
    fn remove_member(&mut self, user_id: Oid) -> MetaResult<()>;

    // tenant role
    fn create_custom_role(
        &mut self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()>;
    fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>>;
    fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>>;
    fn grant_privilege_to_custom_role(
        &mut self,
        database_name: String,
        database_privileges: Vec<(DatabasePrivilege, Oid)>,
        role_name: &str,
    ) -> MetaResult<()>;
    fn revoke_privilege_from_custom_role(
        &mut self,
        database_name: &str,
        database_privileges: Vec<(DatabasePrivilege, Oid)>,
        role_name: &str,
    ) -> MetaResult<bool>;
    fn drop_custom_role(&mut self, role_name: &str) -> MetaResult<bool>;

    fn create_db(&self, info: &DatabaseInfo) -> MetaResult<()>;
    fn get_db_schema(&self, name: &String) -> MetaResult<Option<DatabaseInfo>>;
    fn list_databases(&self) -> MetaResult<Vec<String>>;
    fn drop_db(&self, name: &String) -> MetaResult<()>;

    fn create_table(&self, schema: &TskvTableSchema) -> MetaResult<()>;
    fn update_table(&self, schema: &TskvTableSchema) -> MetaResult<()>;
    fn get_table_schema(&self, db: &String, table: &String) -> MetaResult<Option<TskvTableSchema>>;
    fn list_tables(&self, db: &String) -> MetaResult<Vec<String>>;
    fn drop_table(&self, db: &String, table: &String) -> MetaResult<()>;

    fn create_bucket(&self, db: &String, ts: i64) -> MetaResult<BucketInfo>;
    //fn drop_bucket(&self, db: &String, id: u64) -> MetaResult<()>;

    fn database_min_ts(&self, db: &String) -> Option<i64>;

    fn mapping_bucket(&self, db_name: &String, start: i64, end: i64)
        -> MetaResult<Vec<BucketInfo>>;

    fn locate_replcation_set_for_write(
        &self,
        db: &String,
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
        // TODO
        let user_manager = Arc::new(UserManagerMock::default());
        let tenant_manager = Arc::new(RemoteTenantManager::new(
            config.name.clone(),
            config.meta.clone(),
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
            client: MetaHttpClient::new(1, meta_url.clone()),
        }
    }
}

#[async_trait::async_trait]
impl AdminMeta for RemoteAdminMeta {
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()> {
        let req = command::WriteCommand::AddDataNode(self.cluster.clone(), node.clone());
        let rsp = self.client.write::<command::StatusResponse>(&req)?;
        if rsp.err_code != command::META_REQUEST_SUCCESS {
            return Err(MetaError::CommonError {
                msg: format!("add data node err: {} {}", rsp.err_code, rsp.err_msg),
            });
        }

        Ok(())
    }

    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        if let Some(val) = self.data_nodes.read().get(&id) {
            return Ok(val.clone());
        }

        let req = command::ReadCommand::DataNodes(self.cluster.clone());
        let resp = self.client.read::<Vec<NodeInfo>>(&req)?;
        let mut nodes = self.data_nodes.write();
        for item in resp.iter() {
            nodes.insert(item.id, item.clone());
        }

        if let Some(val) = self.data_nodes.read().get(&id) {
            return Ok(val.clone());
        }

        return Err(MetaError::NotFoundNode { id });
    }

    async fn get_node_conn(&self, node_id: u64) -> MetaResult<TcpStream> {
        {
            let mut write = self.conn_map.write();
            let entry = write.entry(node_id).or_insert(VecDeque::with_capacity(32));
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
        let entry = write.entry(node_id).or_insert(VecDeque::with_capacity(32));

        // close too more idle connection
        if entry.len() < 32 {
            entry.push_back(conn);
        }
    }
}

#[derive(Debug)]
pub struct RemoteMetaClient {
    cluster: String,
    tenant: Tenant,
    meta_url: String,

    data: RwLock<TenantMetaData>,
    client: MetaHttpClient,
}

impl RemoteMetaClient {
    pub fn new(cluster: String, tenant: Tenant, meta_url: String) -> Self {
        Self {
            cluster,
            tenant,
            meta_url: meta_url.clone(),
            data: RwLock::new(TenantMetaData::new()),
            client: MetaHttpClient::new(1, meta_url.clone()),
        }
    }

    fn sync_all_tenant_metadata(&self) -> MetaResult<()> {
        let req = command::ReadCommand::TenaneMetaData(
            self.cluster.clone(),
            self.tenant.name().to_string(),
        );
        let resp = self.client.read::<command::TenaneMetaDataResp>(&req)?;
        if resp.err_code < 0 {
            return Err(MetaError::CommonError {
                msg: format!("open meta err: {} {}", resp.err_code, resp.err_msg),
            });
        }

        let mut data = self.data.write();
        if resp.meta_data.version > data.version {
            *data = resp.meta_data;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl MetaClient for RemoteMetaClient {
    fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    // tenant member start

    fn add_member_with_role(&mut self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()> {
        // TODO
        Ok(())
    }

    fn member_role(&self, user_id: &Oid) -> MetaResult<TenantRole<Oid>> {
        // TODO
        Ok(TenantRole::System(SystemTenantRole::Owner))
    }

    fn members(&self) -> MetaResult<Option<HashSet<&Oid>>> {
        // TODO
        Ok(Some(HashSet::default()))
    }

    fn reasign_member_role(&mut self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()> {
        // TODO
        Ok(())
    }

    fn remove_member(&mut self, user_id: Oid) -> MetaResult<()> {
        // TODO
        Ok(())
    }

    // tenant member end

    // tenant role start

    fn create_custom_role(
        &mut self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()> {
        // TODO
        Ok(())
    }

    fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>> {
        // TODO
        Ok(None)
    }

    fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        // TODO
        Ok(vec![])
    }

    fn grant_privilege_to_custom_role(
        &mut self,
        database_name: String,
        database_privileges: Vec<(DatabasePrivilege, Oid)>,
        role_name: &str,
    ) -> MetaResult<()> {
        // TODO
        Ok(())
    }

    fn revoke_privilege_from_custom_role(
        &mut self,
        database_name: &str,
        database_privileges: Vec<(DatabasePrivilege, Oid)>,
        role_name: &str,
    ) -> MetaResult<bool> {
        // TODO
        Ok(true)
    }

    fn drop_custom_role(&mut self, role_name: &str) -> MetaResult<bool> {
        // TODO
        Ok(true)
    }

    // tenant role end

    fn create_db(&self, info: &DatabaseInfo) -> MetaResult<()> {
        let req = command::WriteCommand::CreateDB(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            info.clone(),
        );

        let rsp = self.client.write::<command::TenaneMetaDataResp>(&req)?;
        let mut data = self.data.write();
        if rsp.meta_data.version > data.version {
            *data = rsp.meta_data;
        }

        // todo db already exist
        // if rsp.err_code != command::META_REQUEST_SUCCESS {
        //     return Err(MetaError::CommonError {
        //         msg: format!("add data node err: {} {}", rsp.err_code, rsp.err_msg),
        //     });
        // }

        Ok(())
    }

    fn get_db_schema(&self, name: &String) -> MetaResult<Option<DatabaseInfo>> {
        if let Some(db) = self.data.read().dbs.get(name) {
            return Ok(Some(db.clone()));
        }

        self.sync_all_tenant_metadata()?;
        if let Some(db) = self.data.read().dbs.get(name) {
            return Ok(Some(db.clone()));
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

    fn drop_db(&self, name: &String) -> MetaResult<()> {
        todo!()
    }

    fn create_table(&self, schema: &TskvTableSchema) -> MetaResult<()> {
        let req = command::WriteCommand::CreateTable(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            schema.clone(),
        );

        let rsp = self.client.write::<command::TenaneMetaDataResp>(&req)?;
        let mut data = self.data.write();
        if rsp.meta_data.version > data.version {
            *data = rsp.meta_data;
        }

        // todo table already exist

        Ok(())
    }

    fn get_table_schema(&self, db: &String, table: &String) -> MetaResult<Option<TskvTableSchema>> {
        if let Some(val) = self.data.read().table_schema(db, table) {
            return Ok(Some(val));
        }

        self.sync_all_tenant_metadata()?;
        let val = self.data.read().table_schema(db, table);
        Ok(val)
    }

    fn update_table(&self, schema: &TskvTableSchema) -> MetaResult<()> {
        let req = command::WriteCommand::UpdateTable(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            schema.clone(),
        );

        let rsp = self.client.write::<command::TenaneMetaDataResp>(&req)?;
        let mut data = self.data.write();
        if rsp.meta_data.version > data.version {
            *data = rsp.meta_data;
        }

        // todo table not exist

        Ok(())
    }

    fn list_tables(&self, db: &String) -> MetaResult<Vec<String>> {
        let mut list = vec![];
        if let Some(info) = self.data.read().dbs.get(db) {
            for (k, _) in info.tables.iter() {
                list.push(k.clone());
            }
        }

        Ok(list)
    }

    fn drop_table(&self, db: &String, table: &String) -> MetaResult<()> {
        todo!()
    }

    fn create_bucket(&self, db: &String, ts: i64) -> MetaResult<BucketInfo> {
        let req = command::WriteCommand::CreateBucket {
            cluster: self.cluster.clone(),
            tenant: self.tenant.name().to_string(),
            db: db.clone(),
            ts,
        };

        let rsp = self.client.write::<command::TenaneMetaDataResp>(&req)?;
        let mut data = self.data.write();
        if rsp.meta_data.version > data.version {
            *data = rsp.meta_data;
        }

        if rsp.err_code < 0 {
            return Err(MetaError::MetaClientErr {
                msg: format!("create bucket err: {} {}", rsp.err_code, rsp.err_msg),
            });
        }

        if let Some(bucket) = self.data.read().bucket_by_timestamp(db, ts) {
            return Ok(bucket.clone());
        }

        return Err(MetaError::CommonError {
            msg: format!("create bucket unknown error"),
        });
    }

    fn database_min_ts(&self, name: &String) -> Option<i64> {
        self.data.read().database_min_ts(name)
    }

    fn locate_replcation_set_for_write(
        &self,
        db: &String,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplcationSet> {
        if let Some(bucket) = self.data.read().bucket_by_timestamp(db, ts) {
            return Ok(bucket.vnode_for(hash_id));
        }

        let bucket = self.create_bucket(db, ts)?;
        return Ok(bucket.vnode_for(hash_id));
    }

    fn mapping_bucket(
        &self,
        db_name: &String,
        start: i64,
        end: i64,
    ) -> MetaResult<Vec<BucketInfo>> {
        //todo improve performence,watch the meta
        self.sync_all_tenant_metadata().unwrap();

        let buckets = self.data.read().mapping_bucket(db_name, start, end);
        return Ok(buckets);
    }

    fn print_data(&self) -> String {
        info!("****** Tenant: {:?}; Meta: {}", self.tenant, self.meta_url);
        info!("****** Meta Data: {:#?}", self.data);

        format!("{:#?}", self.data.read())
    }
}
