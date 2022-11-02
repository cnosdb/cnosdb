use config::ClusterConfig;
use meta::client::MetaHttpClient;
use models::meta_data::*;
use parking_lot::{RwLock, RwLockReadGuard};
use snafu::Snafu;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

use trace::info;

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

    #[snafu(display("Error: {}", msg))]
    CommonError { msg: String },
}

pub type MetaResult<T> = Result<T, MetaError>;

pub type MetaClientRef = Arc<Box<dyn MetaClient + Send + Sync>>;
pub type AdminMetaClientRef = Arc<Box<dyn AdminMetaClient + Send + Sync>>;

#[async_trait::async_trait]
pub trait AdminMetaClient {
    //*数据节点上下线管理 */
    fn data_nodes(&self) -> Vec<NodeInfo>;
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()>;
    fn del_data_node(&self, id: u64) -> MetaResult<()>;

    fn meta_nodes(&self);
    fn add_meta_node(&self, node: &NodeInfo) -> MetaResult<()>;
    fn del_meta_node(&self, id: u64) -> MetaResult<()>;
    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo>;

    fn heartbeat(&self); // update node status
}

pub struct LocalAdminMetaClient {
    cluster: String,
    meta_url: String,
    data_nodes: RwLock<HashMap<u64, NodeInfo>>,

    client: MetaHttpClient,
}

impl LocalAdminMetaClient {
    pub fn new(cluster: String, meta_url: String, runtime: Arc<Runtime>) -> Self {
        Self {
            cluster,
            meta_url,
            data_nodes: RwLock::new(HashMap::new()),
            client: MetaHttpClient::new(1, "127.0.0.1:21001".to_string(), runtime),
        }
    }
}

#[async_trait::async_trait]
impl AdminMetaClient for LocalAdminMetaClient {
    fn data_nodes(&self) -> Vec<NodeInfo> {
        todo!()
    }
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()> {
        todo!()
    }
    fn del_data_node(&self, id: u64) -> MetaResult<()> {
        todo!()
    }

    fn meta_nodes(&self) {
        todo!()
    }
    fn add_meta_node(&self, node: &NodeInfo) -> MetaResult<()> {
        todo!()
    }
    fn del_meta_node(&self, id: u64) -> MetaResult<()> {
        todo!()
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

    fn heartbeat(&self) {
        todo!()
    }
}

#[async_trait::async_trait]
pub trait MetaClient {
    fn tenant_name(&self) -> &str;
    fn create_user(&self, user: &UserInfo) -> MetaResult<()>;
    fn drop_user(&self, name: &String) -> MetaResult<()>;

    fn create_db(&self, name: &String, policy: &DatabaseInfo) -> MetaResult<()>;
    fn drop_db(&self, name: &String) -> MetaResult<()>;

    fn create_bucket(&self, db: &String, ts: i64) -> MetaResult<&BucketInfo>;
    fn drop_bucket(&self, db: &String, id: u64) -> MetaResult<()>;

    fn bucket_by_timestamp(&self, db: &String, ts: i64) -> Option<&BucketInfo>;
    fn databases(&self) -> Vec<String>;
    fn database_min_ts(&self, db: &String) -> Option<i64>;

    fn alloc_data_resource(&self, res: &Resource) -> MetaResult<()>;
    fn release_data_resource(&self, id: u64) -> MetaResult<()>;
    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo>;

    fn tenant_node(&self) -> Vec<NodeInfo>;

    fn locate_replcation_set_for_write(
        &self,
        db: &String,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplcationSet>;

    fn create_table(&self);
    fn drop_table(&self);
    fn get_table_schema(&self);
    fn print_data(&self);
}

pub struct LocalMetaClient {
    cluster: String,
    tenant: String,
    meta_url: String,

    data: TenantMetaData,
    client: MetaHttpClient,
}

impl LocalMetaClient {
    pub fn new(cluster: String, tenant: String, meta_url: String, runtime: Arc<Runtime>) -> Self {
        Self {
            cluster,
            tenant,
            meta_url,
            data: TenantMetaData::new(),
            client: MetaHttpClient::new(1, "127.0.0.1:21001".to_string(), runtime),
        }
    }
}

#[async_trait::async_trait]
impl MetaClient for LocalMetaClient {
    fn tenant_name(&self) -> &str {
        return &self.tenant;
    }

    fn create_user(&self, user: &UserInfo) -> MetaResult<()> {
        todo!()
    }

    fn drop_user(&self, name: &String) -> MetaResult<()> {
        todo!()
    }

    fn create_db(&self, name: &String, policy: &DatabaseInfo) -> MetaResult<()> {
        todo!()
    }

    fn drop_db(&self, name: &String) -> MetaResult<()> {
        todo!()
    }

    fn create_bucket(&self, db: &String, ts: i64) -> MetaResult<&BucketInfo> {
        todo!()
    }

    fn drop_bucket(&self, db: &String, id: u64) -> MetaResult<()> {
        todo!()
    }

    fn bucket_by_timestamp(&self, db: &String, ts: i64) -> Option<&BucketInfo> {
        todo!()
    }

    fn databases(&self) -> Vec<String> {
        todo!()
    }

    fn alloc_data_resource(&self, res: &Resource) -> MetaResult<()> {
        todo!()
    }

    fn release_data_resource(&self, id: u64) -> MetaResult<()> {
        todo!()
    }

    fn tenant_node(&self) -> Vec<NodeInfo> {
        todo!()
    }

    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        todo!()
    }

    fn create_table(&self) {}
    fn drop_table(&self) {}
    fn get_table_schema(&self) {}

    fn database_min_ts(&self, name: &String) -> Option<i64> {
        self.data.database_min_ts(name)
    }

    fn locate_replcation_set_for_write(
        &self,
        db: &String,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplcationSet> {
        if let Some(bucket) = self.data.bucket_by_timestamp(db, ts) {
            return Ok(bucket.vnode_for(hash_id));
        }

        let bucket = self.create_bucket(db, ts)?;
        return Ok(bucket.vnode_for(hash_id));
    }

    fn print_data(&self) {
        info!("****** Tenant: {}; Meta: {}", self.tenant, self.meta_url);
        info!("****** Meta Data: {:#?}", self.data);
    }
}

pub struct MetaClientManager {
    runtime: Arc<Runtime>,
    config: ClusterConfig,

    admin: AdminMetaClientRef,
    tenants: RwLock<HashMap<String, MetaClientRef>>,
}

impl MetaClientManager {
    pub fn new(config: ClusterConfig, runtime: Arc<Runtime>) -> Self {
        let admin: AdminMetaClientRef = Arc::new(Box::new(LocalAdminMetaClient::new(
            config.name.clone(),
            config.meta.clone(),
            runtime.clone(),
        )));

        Self {
            config,
            admin,
            runtime,
            tenants: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_admin_meta_client(&self) -> AdminMetaClientRef {
        self.admin.clone()
    }

    pub fn get_meta_client(&self, tenant: &String) -> Option<MetaClientRef> {
        if let Some(client) = self.tenants.read().get(tenant) {
            return Some(client.clone());
        }

        let client: MetaClientRef = Arc::new(Box::new(LocalMetaClient::new(
            self.config.name.clone(),
            tenant.clone(),
            self.config.meta.clone(),
            self.runtime.clone(),
        )));

        self.tenants.write().insert(tenant.clone(), client.clone());

        return Some(client);
    }
}
