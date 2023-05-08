#![allow(dead_code, unused_variables)]

use std::collections::HashMap;
use std::sync::Arc;

use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRoleIdentifier};
use models::meta_data::{
    BucketInfo, DatabaseInfo, ExpiredBucketInfo, NodeInfo, ReplicationSet, VnodeAllInfo, VnodeInfo,
};
use models::oid::Oid;
use models::schema::{
    DatabaseSchema, ExternalTableSchema, TableSchema, Tenant, TenantOptions, TskvTableSchema,
};
use tonic::transport::Channel;

use crate::error::MetaResult;
use crate::limiter::RequestLimiter;
use crate::model::user_manager_mock::UserManagerMock;
use crate::model::{
    AdminMeta, AdminMetaRef, MetaClient, MetaClientRef, MetaManager, TenantManager,
    TenantManagerRef, UserManagerRef,
};
use crate::store::command::EntryLog;

#[derive(Default, Debug)]
pub struct MockAdminMeta {}
#[async_trait::async_trait]
impl AdminMeta for MockAdminMeta {
    async fn sync_all(&self) -> MetaResult<u64> {
        Ok(0)
    }

    async fn add_data_node(&self) -> MetaResult<()> {
        Ok(())
    }

    async fn data_nodes(&self) -> Vec<NodeInfo> {
        vec![]
    }

    async fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        Ok(NodeInfo::default())
    }

    async fn get_node_conn(&self, node_id: u64) -> MetaResult<Channel> {
        todo!()
    }

    async fn retain_id(&self, count: u32) -> MetaResult<u32> {
        Ok(0)
    }

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        Ok(())
    }

    async fn report_node_metrics(&self) -> MetaResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct MockMetaClient {
    tenant: Tenant,
}

impl Default for MockMetaClient {
    fn default() -> Self {
        let tenant = Tenant::new(0_u128, "mock".to_string(), TenantOptions::default());
        Self { tenant }
    }
}

#[async_trait::async_trait]
impl MetaClient for MockMetaClient {
    fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    async fn create_db(&self, info: DatabaseSchema) -> MetaResult<()> {
        Ok(())
    }
    async fn alter_db_schema(&self, info: &DatabaseSchema) -> MetaResult<()> {
        Ok(())
    }

    fn get_db_schema(&self, name: &str) -> MetaResult<Option<DatabaseSchema>> {
        Ok(Some(DatabaseSchema::default()))
    }

    fn list_databases(&self) -> MetaResult<Vec<String>> {
        Ok(vec![])
    }

    async fn drop_db(&self, name: &str) -> MetaResult<bool> {
        Ok(false)
    }

    async fn create_table(&self, schema: &TableSchema) -> MetaResult<()> {
        Ok(())
    }

    async fn update_table(&self, schema: &TableSchema) -> MetaResult<()> {
        Ok(())
    }

    fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>> {
        Ok(None)
    }

    fn get_tskv_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<Arc<TskvTableSchema>>> {
        Ok(Some(Arc::new(TskvTableSchema::new_test())))
    }

    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<Arc<ExternalTableSchema>>> {
        Ok(None)
    }

    fn list_tables(&self, db: &str) -> MetaResult<Vec<String>> {
        Ok(vec![])
    }

    async fn drop_table(&self, db: &str, table: &str) -> MetaResult<()> {
        Ok(())
    }

    async fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo> {
        Ok(BucketInfo::default())
    }

    async fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()> {
        Ok(())
    }

    fn database_min_ts(&self, db: &str) -> Option<i64> {
        Some(0)
    }

    async fn locate_replication_set_for_write(
        &self,
        db: &str,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplicationSet> {
        Ok(ReplicationSet::default())
    }

    fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> MetaResult<Vec<BucketInfo>> {
        Ok(vec![])
    }

    fn print_data(&self) -> String {
        "".to_string()
    }

    async fn add_member_with_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn member_role(&self, user_id: &Oid) -> MetaResult<Option<TenantRoleIdentifier>> {
        todo!()
    }

    async fn members(&self) -> MetaResult<HashMap<String, TenantRoleIdentifier>> {
        todo!()
    }

    async fn reassign_member_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn remove_member(&self, user_id: Oid) -> MetaResult<()> {
        todo!()
    }

    async fn create_custom_role(
        &self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>> {
        todo!()
    }

    async fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        todo!()
    }

    async fn grant_privilege_to_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn revoke_privilege_from_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        todo!()
    }

    async fn drop_custom_role(&self, role_name: &str) -> MetaResult<bool> {
        todo!()
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        vec![]
    }

    fn get_replication_set(&self, repl_id: u32) -> Option<ReplicationSet> {
        todo!()
    }

    async fn update_replication_set(
        &self,
        db: &str,
        bucket_id: u32,
        repl_id: u32,
        del_info: &[VnodeInfo],
        add_info: &[VnodeInfo],
    ) -> MetaResult<()> {
        Ok(())
    }

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        Ok(())
    }

    async fn version(&self) -> u64 {
        0
    }

    async fn update_vnode(&self, info: &VnodeAllInfo) -> MetaResult<()> {
        Ok(())
    }

    fn get_vnode_all_info(&self, id: u32) -> Option<VnodeAllInfo> {
        None
    }

    fn get_vnode_repl_set(&self, id: u32) -> Option<ReplicationSet> {
        None
    }

    fn get_db_info(&self, name: &str) -> MetaResult<Option<DatabaseInfo>> {
        #[allow(clippy::inconsistent_digit_grouping)]
        let db_info = if name.eq("with_nonempty_database") {
            DatabaseInfo {
                buckets: vec![
                    BucketInfo {
                        id: 1,
                        // 2023-01-01 00:00:00.000000000
                        start_time: 1672502400_000_000_000_i64,
                        // 2023-07-01 00:00:00.000000000
                        end_time: 1688140800_000_000_000_i64,
                        ..Default::default()
                    },
                    BucketInfo {
                        id: 2,
                        // 2023-07-01 00:00:00.000000000
                        start_time: 1688140800_000_000_000_i64,
                        // 2024-01-01 00:00:00.000000000
                        end_time: 1704038400_000_000_000_i64,
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }
        } else {
            DatabaseInfo::default()
        };
        Ok(Some(db_info))
    }
}

#[derive(Default, Debug)]
pub struct MockMetaManager {}

#[async_trait::async_trait]
impl MetaManager for MockMetaManager {
    fn node_id(&self) -> u64 {
        0
    }

    async fn use_tenant(&self, val: &str) -> MetaResult<()> {
        Ok(())
    }

    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        vec![]
    }

    fn admin_meta(&self) -> AdminMetaRef {
        Arc::new(MockAdminMeta::default())
    }

    fn user_manager(&self) -> UserManagerRef {
        Arc::new(UserManagerMock::default())
    }

    fn tenant_manager(&self) -> TenantManagerRef {
        Arc::new(TenantManagerMock::default())
    }

    async fn user_with_privileges(
        &self,
        user_name: &str,
        tenant_name: Option<&str>,
    ) -> MetaResult<models::auth::user::User> {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct TenantManagerMock {}

#[async_trait::async_trait]
impl TenantManager for TenantManagerMock {
    async fn clear(&self) {}
    async fn create_tenant(
        &self,
        name: String,
        options: TenantOptions,
    ) -> MetaResult<MetaClientRef> {
        todo!()
    }

    async fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>> {
        todo!()
    }

    async fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()> {
        todo!()
    }

    async fn drop_tenant(&self, name: &str) -> MetaResult<bool> {
        todo!()
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        todo!()
    }

    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        vec![]
    }

    async fn tenants(&self) -> MetaResult<Vec<Tenant>> {
        todo!()
    }

    async fn get_tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        todo!()
    }

    async fn limiter(&self, tenant: &str) -> Arc<dyn RequestLimiter> {
        todo!()
    }
}
