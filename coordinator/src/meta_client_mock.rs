use std::sync::Arc;

use models::meta_data::{BucketInfo, DatabaseInfo, NodeInfo, ReplcationSet};

use crate::meta_client::{
    AdminMeta, AdminMetaRef, MetaClient, MetaClientRef, MetaManager, MetaResult,
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
}

#[derive(Default)]
pub struct MockMetaClient {}
#[async_trait::async_trait]
impl MetaClient for MockMetaClient {
    fn open(&self) -> MetaResult<()> {
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

    fn print_data(&self) -> String {
        "".to_string()
    }
}

#[derive(Default)]
pub struct MockMetaManager {}
#[async_trait::async_trait]
impl MetaManager for MockMetaManager {
    fn admin_meta(&self) -> AdminMetaRef {
        Arc::new(MockAdminMeta::default())
    }

    fn tenant_meta(&self, tenant: &String) -> Option<MetaClientRef> {
        Some(Arc::new(MockMetaClient::default()))
    }
}
