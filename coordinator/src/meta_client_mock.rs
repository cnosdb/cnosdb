use std::sync::Arc;

use models::meta_data::{BucketInfo, DatabaseInfo, NodeInfo, ReplcationSet};
use tokio::net::TcpStream;

use crate::meta_client::{
    AdminMeta, AdminMetaRef, MetaClient, MetaClientRef, MetaError, MetaManager, MetaResult,
};

#[derive(Default)]
pub struct MockAdminMeta {}
#[async_trait::async_trait]
impl AdminMeta for MockAdminMeta {
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()> {
        Ok(())
    }

    fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        Ok(NodeInfo::default())
    }

    async fn get_node_conn(&self, node_id: u64) -> MetaResult<TcpStream> {
        Err(MetaError::CommonError {
            msg: "mock not implement node conn".to_string(),
        })
    }

    fn put_node_conn(&self, node_id: u64, conn: TcpStream) {}
}

#[derive(Default)]
pub struct MockMetaClient {}
#[async_trait::async_trait]
impl MetaClient for MockMetaClient {
    fn sync_data(&self) -> MetaResult<()> {
        Ok(())
    }
    fn tenant_name(&self) -> &str {
        ""
    }

    fn create_db(&self, name: &String, policy: &DatabaseInfo) -> MetaResult<()> {
        Ok(())
    }

    fn create_bucket(&self, db: &String, ts: i64) -> MetaResult<BucketInfo> {
        Ok(BucketInfo::default())
    }

    fn database_min_ts(&self, db: &String) -> Option<i64> {
        Some(0)
    }

    fn locate_replcation_set_for_write(
        &self,
        db: &String,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplcationSet> {
        Ok(ReplcationSet::default())
    }

    fn mapping_bucket(
        &self,
        db_name: &String,
        start: i64,
        end: i64,
    ) -> MetaResult<Vec<BucketInfo>> {
        Ok(vec![])
    }

    fn print_data(&self) -> String {
        "".to_string()
    }
}

#[derive(Default)]
pub struct MockMetaManager {}
#[async_trait::async_trait]
impl MetaManager for MockMetaManager {
    fn node_id(&self) -> u64 {
        0
    }

    fn admin_meta(&self) -> AdminMetaRef {
        Arc::new(MockAdminMeta::default())
    }

    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        Some(Arc::new(MockMetaClient::default()))
    }
}
