use config::ClusterConfig;
use futures::future::ok;
use meta::client::MetaHttpClient;
use meta::store::KvReq;
use models::meta_data::*;
use parking_lot::{RwLock, RwLockReadGuard};
use snafu::Snafu;
use std::borrow::BorrowMut;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::{fmt::Debug, io};
use tokio::net::TcpStream;

use trace::info;

use models::schema::{DatabaseSchema, TableSchema, TskvTableSchema};

#[derive(Snafu, Debug)]
pub enum MetaError {
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

pub type MetaResult<T> = Result<T, MetaError>;

pub type MetaClientRef = Arc<dyn MetaClient>;
pub type AdminMetaRef = Arc<dyn AdminMeta>;
pub type MetaRef = Arc<dyn MetaManager>;

#[async_trait::async_trait]
pub trait MetaManager: Send + Sync + Debug {
    fn node_id(&self) -> u64;
    fn admin_meta(&self) -> AdminMetaRef;
    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef>;
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

#[async_trait::async_trait]
pub trait MetaClient: Send + Sync + Debug {
    fn sync_data(&self) -> MetaResult<()>;
    fn tenant_name(&self) -> &str;
    //fn create_user(&self, user: &UserInfo) -> MetaResult<()>;
    //fn drop_user(&self, name: &String) -> MetaResult<()>;

    fn create_db(&self, info: &DatabaseInfo) -> MetaResult<()>;
    fn get_db_schema(&self, name: &String) -> MetaResult<Option<DatabaseInfo>>;
    fn list_databases(&self) -> MetaResult<Vec<String>>;
    fn drop_db(&self, name: &String) -> MetaResult<()>;

    fn create_table(&self, schema: &TskvTableSchema) -> MetaResult<()>;
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
    tenants: RwLock<HashMap<String, MetaClientRef>>,
}

impl RemoteMetaManager {
    pub fn new(config: ClusterConfig) -> Self {
        let admin: AdminMetaRef = Arc::new(RemoteAdminMeta::new(
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
            tenants: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait::async_trait]
impl MetaManager for RemoteMetaManager {
    fn node_id(&self) -> u64 {
        self.config.node_id
    }

    fn admin_meta(&self) -> AdminMetaRef {
        self.admin.clone()
    }

    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        if let Some(client) = self.tenants.read().get(tenant) {
            return Some(client.clone());
        }

        let client: MetaClientRef = Arc::new(RemoteMetaClient::new(
            self.config.name.clone(),
            tenant.clone(),
            self.config.meta.clone(),
        ));

        self.tenants.write().insert(tenant.clone(), client.clone());

        return Some(client);
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
        let req = meta::store::KvReq::AddDataNode(self.cluster.clone(), node.clone());

        let rsp = self
            .client
            .write(&req)
            .map_err(|err| MetaError::CommonError {
                msg: format!("add data node err: {}", err.to_string()),
            })?;

        if rsp.err_code < 0 {
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

        match self.client.read_data_nodes(&self.cluster) {
            Ok(val) => {
                let mut nodes = self.data_nodes.write();
                for item in val.iter() {
                    nodes.insert(item.id, item.clone());
                }
            }

            Err(err) => {
                return Err(MetaError::CommonError {
                    msg: err.to_string(),
                });
            }
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
    tenant: String,
    meta_url: String,

    data: RwLock<TenantMetaData>,
    client: MetaHttpClient,
}

impl RemoteMetaClient {
    pub fn new(cluster: String, tenant: String, meta_url: String) -> Self {
        Self {
            cluster,
            tenant,
            meta_url: meta_url.clone(),
            data: RwLock::new(TenantMetaData::new()),
            client: MetaHttpClient::new(1, meta_url.clone()),
        }
    }
}

#[async_trait::async_trait]
impl MetaClient for RemoteMetaClient {
    fn sync_data(&self) -> MetaResult<()> {
        let rsp = self
            .client
            .read_tenant_meta(&(self.cluster.clone(), self.tenant.clone()))
            .map_err(|err| MetaError::CommonError {
                msg: format!("open meta err: {}", err.to_string()),
            })?;

        if rsp.err_code < 0 {
            return Err(MetaError::CommonError {
                msg: format!("open meta err: {} {}", rsp.err_code, rsp.err_msg),
            });
        }

        let mut data = self.data.write();
        if rsp.meta_data.version > data.version {
            *data = rsp.meta_data;
        }

        Ok(())
    }
    fn tenant_name(&self) -> &str {
        return &self.tenant;
    }

    fn create_db(&self, info: &DatabaseInfo) -> MetaResult<()> {
        let req = KvReq::CreateDB(self.cluster.clone(), self.tenant.clone(), info.clone());

        let rsp = self
            .client
            .write(&req)
            .map_err(|err| MetaError::CommonError {
                msg: format!("create db err: {}", err.to_string()),
            })?;

        if rsp.err_code < 0 {
            return Err(MetaError::CommonError {
                msg: format!("create db err: {} {}", rsp.err_code, rsp.err_msg),
            });
        }

        let mut data = self.data.write();
        if rsp.meta_data.version > data.version {
            *data = rsp.meta_data;
        }

        return Ok(());
    }

    fn get_db_schema(&self, name: &String) -> MetaResult<Option<DatabaseInfo>> {
        if let Some(db) = self.data.read().dbs.get(name) {
            return Ok(Some(db.clone()));
        }

        self.sync_data()?;
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
        let req = KvReq::CreateTable(self.cluster.clone(), self.tenant.clone(), schema.clone());

        let rsp = self
            .client
            .write(&req)
            .map_err(|err| MetaError::CommonError {
                msg: format!("create table err: {}", err.to_string()),
            })?;

        if rsp.err_code < 0 {
            return Err(MetaError::CommonError {
                msg: format!("create table err: {} {}", rsp.err_code, rsp.err_msg),
            });
        }

        let mut data = self.data.write();
        if rsp.meta_data.version > data.version {
            *data = rsp.meta_data;
        }

        return Ok(());
    }

    fn get_table_schema(&self, db: &String, table: &String) -> MetaResult<Option<TskvTableSchema>> {
        if let Some(val) = self.data.read().table_schema(db, table) {
            return Ok(Some(val));
        }

        self.sync_data()?;
        let val = self.data.read().table_schema(db, table);
        Ok(val)
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
        let req = meta::store::KvReq::CreateBucket {
            cluster: self.cluster.clone(),
            tenant: self.tenant.clone(),
            db: db.clone(),
            ts,
        };

        let rsp = self
            .client
            .write(&req)
            .map_err(|err| MetaError::CommonError {
                msg: format!("create bucket err: {}", err.to_string()),
            })?;

        if rsp.err_code < 0 {
            return Err(MetaError::CommonError {
                msg: format!("create bucket err: {} {}", rsp.err_code, rsp.err_msg),
            });
        }

        let mut data = self.data.write();
        if rsp.meta_data.version > data.version {
            *data = rsp.meta_data;
        }

        if let Some(bucket) = data.bucket_by_timestamp(db, ts) {
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
        //todo improve performence
        self.sync_data().unwrap();

        let buckets = self.data.read().mapping_bucket(db_name, start, end);
        return Ok(buckets);
    }

    fn print_data(&self) -> String {
        info!("****** Tenant: {}; Meta: {}", self.tenant, self.meta_url);
        info!("****** Meta Data: {:#?}", self.data);

        format!("{:#?}", self.data.read())
    }
}
