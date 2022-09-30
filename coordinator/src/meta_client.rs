use models::meta_data::*;

use snafu::Snafu;

#[derive(Snafu, Debug)]
pub enum MetaError {
    #[snafu(display("Not Found Field"))]
    NotFoundField,

    #[snafu(display("index storage error: {}", msg))]
    IndexStroage { msg: String },
}

pub type MetaResult<T> = Result<T, MetaError>;

pub trait MetaClient {
    //*数据节点上下线管理 */
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()>;
    fn del_data_node(&self, id: u64) -> MetaResult<()>;

    fn create_user(&mut self, user: &UserInfo) -> MetaResult<()>;
    fn drop_user(&mut self, name: &String) -> MetaResult<()>;

    fn create_db(&mut self, name: &String, policy: &RetentionPolicyInfo) -> MetaResult<()>;
    fn drop_db(&mut self, name: &String) -> MetaResult<()>;

    fn create_bucket(&mut self, db: &String, ts: i64) -> MetaResult<&BucketInfo>;
    fn drop_bucket(&mut self, db: &String, id: u64) -> MetaResult<()>;

    fn bucket_by_timestamp(&self, db: &String, ts: i64) -> Option<&BucketInfo>;
    fn databases(&self) -> Vec<String>;

    fn alloc_data_resource(&mut self, res: &Resource) -> MetaResult<()>;
    fn release_data_resource(&mut self, id: u64) -> MetaResult<()>;

    fn all_nodes(&self) -> Vec<NodeInfo>;
    fn tenant_node(&self) -> Vec<NodeInfo>;
    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo>;

    // fn heartbeat(); // update node status
    // fn create_table();
    // fn drop_table();
    // fn meta_nodes();
}

pub struct LocalMetaClient {
    tenant: String,

    data: MetaData,
}

impl MetaClient for LocalMetaClient {
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()> {
        todo!()
    }

    fn del_data_node(&self, id: u64) -> MetaResult<()> {
        todo!()
    }

    fn create_user(&mut self, user: &UserInfo) -> MetaResult<()> {
        todo!()
    }

    fn drop_user(&mut self, name: &String) -> MetaResult<()> {
        todo!()
    }

    fn create_db(&mut self, name: &String, policy: &RetentionPolicyInfo) -> MetaResult<()> {
        todo!()
    }

    fn drop_db(&mut self, name: &String) -> MetaResult<()> {
        todo!()
    }

    fn create_bucket(&mut self, db: &String, ts: i64) -> MetaResult<&BucketInfo> {
        todo!()
    }

    fn drop_bucket(&mut self, db: &String, id: u64) -> MetaResult<()> {
        todo!()
    }

    fn bucket_by_timestamp(&self, db: &String, ts: i64) -> Option<&BucketInfo> {
        todo!()
    }

    fn databases(&self) -> Vec<String> {
        todo!()
    }

    fn alloc_data_resource(&mut self, res: &Resource) -> MetaResult<()> {
        todo!()
    }

    fn release_data_resource(&mut self, id: u64) -> MetaResult<()> {
        todo!()
    }

    fn all_nodes(&self) -> Vec<NodeInfo> {
        todo!()
    }

    fn tenant_node(&self) -> Vec<NodeInfo> {
        todo!()
    }

    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        todo!()
    }
}
