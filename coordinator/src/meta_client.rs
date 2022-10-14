use models::meta_data::*;
use snafu::Snafu;
use std::sync::Arc;

#[derive(Snafu, Debug)]
pub enum MetaError {
    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },
}

pub type MetaResult<T> = Result<T, MetaError>;

pub type MetaClientRef = Arc<Box<dyn MetaClient + Send + Sync>>;

#[async_trait::async_trait]
pub trait AdminMetaClient {
    //*数据节点上下线管理 */
    fn data_nodes(&self) -> Vec<NodeInfo>;
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()>;
    fn del_data_node(&self, id: u64) -> MetaResult<()>;

    fn meta_nodes();
    fn add_meta_node(&self, node: &NodeInfo) -> MetaResult<()>;
    fn del_meta_node(&self, id: u64) -> MetaResult<()>;

    fn heartbeat(); // update node status
}

#[async_trait::async_trait]
pub trait MetaClient {
    fn tenant_name(&self) -> &str;
    fn create_user(&self, user: &UserInfo) -> MetaResult<()>;
    fn drop_user(&self, name: &String) -> MetaResult<()>;

    fn create_db(&self, name: &String, policy: &RetentionPolicyInfo) -> MetaResult<()>;
    fn drop_db(&self, name: &String) -> MetaResult<()>;

    fn create_bucket(&self, db: &String, ts: i64) -> MetaResult<&BucketInfo>;
    fn drop_bucket(&self, db: &String, id: u64) -> MetaResult<()>;

    fn bucket_by_timestamp(&self, db: &String, ts: i64) -> Option<&BucketInfo>;
    fn databases(&self) -> Vec<String>;
    fn database_min_ts(&self, db: &String) -> Option<i64>;

    fn alloc_data_resource(&self, res: &Resource) -> MetaResult<()>;
    fn release_data_resource(&self, id: u64) -> MetaResult<()>;

    fn tenant_node(&self) -> Vec<NodeInfo>;
    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo>;

    fn locate_db_ts_for_write(&self, db: &String, ts: i64) -> MetaResult<ReplcationSet>;

    fn create_table(&self);
    fn drop_table(&self);
    fn get_table_schema(&self);
}

pub struct LocalMetaClient {
    tenant: String,
    meta_url: String,

    data: MetaData,
}

impl LocalMetaClient {
    pub fn new(tenant: String, meta_url: String) -> Self {
        Self {
            tenant,
            meta_url,
            data: MetaData::new(),
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

    fn create_db(&self, name: &String, policy: &RetentionPolicyInfo) -> MetaResult<()> {
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

    fn locate_db_ts_for_write(&self, db: &String, ts: i64) -> MetaResult<ReplcationSet> {
        todo!()
    }

    fn create_table(&self) {}
    fn drop_table(&self) {}
    fn get_table_schema(&self) {}

    fn database_min_ts(&self, name: &String) -> Option<i64> {
        for db in &self.data.dbs {
            if db.name == *name {
                if db.policy.database_duration == 0 {
                    return Some(0);
                }

                let now = models::utils::now_timestamp();
                return Some(now - db.policy.database_duration);
            }
        }

        None
    }
}
