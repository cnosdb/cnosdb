#![allow(dead_code, unused_imports, unused_variables)]

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use models::{
    auth::{
        privilege::DatabasePrivilege,
        role::{CustomTenantRole, SystemTenantRole, TenantRole, TenantRoleIdentifier},
        user::UserDesc,
    },
    meta_data::{BucketInfo, DatabaseInfo, ExpiredBucketInfo, NodeInfo, ReplicationSet},
    oid::Oid,
    schema::{
        DatabaseSchema, ExternalTableSchema, TableSchema, Tenant, TenantOptions, TskvTableSchema,
    },
};
use models::{limiter::LimiterConfig, meta_data::VnodeInfo};
use models::{meta_data::VnodeAllInfo, oid::Identifier};
use tokio::net::TcpStream;

use crate::error::{MetaError, MetaResult};
use crate::limiter::{Limiter, LimiterImpl};
use crate::{
    meta_client::{
        AdminMeta, AdminMetaRef, MetaClient, MetaClientRef, MetaManager, TenantManager,
        TenantManagerRef, UserManagerRef,
    },
    user_manager::UserManagerMock,
};

#[derive(Default, Debug)]
pub struct MockAdminMeta {}
#[async_trait::async_trait]
impl AdminMeta for MockAdminMeta {
    fn add_data_node(&self, node: &NodeInfo) -> MetaResult<()> {
        Ok(())
    }

    fn data_nodes(&self) -> Vec<NodeInfo> {
        vec![]
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

    fn heartbeat(&self) {}

    fn retain_id(&self, count: u32) -> MetaResult<u32> {
        Ok(0)
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

impl MetaClient for MockMetaClient {
    fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    fn create_db(&self, info: DatabaseSchema) -> MetaResult<()> {
        Ok(())
    }
    fn alter_db_schema(&self, info: &DatabaseSchema) -> MetaResult<()> {
        Ok(())
    }

    fn get_db_schema(&self, name: &str) -> MetaResult<Option<DatabaseSchema>> {
        Ok(Some(DatabaseSchema::default()))
    }

    fn list_databases(&self) -> MetaResult<Vec<String>> {
        Ok(vec![])
    }

    fn drop_db(&self, name: &str) -> MetaResult<bool> {
        Ok(false)
    }

    fn create_table(&self, schema: &TableSchema) -> MetaResult<()> {
        Ok(())
    }

    fn update_table(&self, schema: &TableSchema) -> MetaResult<()> {
        Ok(())
    }

    fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>> {
        Ok(None)
    }

    fn get_tskv_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TskvTableSchema>> {
        Ok(Some(TskvTableSchema::default()))
    }

    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<ExternalTableSchema>> {
        Ok(None)
    }

    fn list_tables(&self, db: &str) -> MetaResult<Vec<String>> {
        Ok(vec![])
    }

    fn drop_table(&self, db: &str, table: &str) -> MetaResult<()> {
        Ok(())
    }

    fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo> {
        Ok(BucketInfo::default())
    }

    fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()> {
        Ok(())
    }

    fn database_min_ts(&self, db: &str) -> Option<i64> {
        Some(0)
    }

    fn locate_replcation_set_for_write(
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

    fn add_member_with_role(&self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()> {
        todo!()
    }

    fn member_role(&self, user_id: &Oid) -> MetaResult<Option<TenantRoleIdentifier>> {
        todo!()
    }

    fn members(&self) -> MetaResult<HashMap<String, TenantRoleIdentifier>> {
        todo!()
    }

    fn reasign_member_role(&self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()> {
        todo!()
    }

    fn remove_member(&self, user_id: Oid) -> MetaResult<()> {
        todo!()
    }

    fn create_custom_role(
        &self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()> {
        todo!()
    }

    fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>> {
        todo!()
    }

    fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        todo!()
    }

    fn grant_privilege_to_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        todo!()
    }

    fn revoke_privilege_from_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        todo!()
    }

    fn drop_custom_role(&self, role_name: &str) -> MetaResult<bool> {
        todo!()
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        vec![]
    }

    fn update_replication_set(
        &self,
        db: &str,
        bucket_id: u32,
        repl_id: u32,
        del_info: &[VnodeInfo],
        add_info: &[VnodeInfo],
    ) -> MetaResult<()> {
        Ok(())
    }

    fn get_vnode_all_info(&self, id: u32) -> Option<VnodeAllInfo> {
        None
    }

    fn limiter(&self) -> Arc<dyn Limiter> {
        todo!()
    }

    fn get_db_info(&self, name: &str) -> MetaResult<Option<DatabaseInfo>> {
        todo!()
    }
}

#[derive(Default, Debug)]
pub struct MockMetaManager {}

impl MetaManager for MockMetaManager {
    fn node_id(&self) -> u64 {
        0
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
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

    fn user_with_privileges(
        &self,
        user_name: &str,
        tenant_name: Option<&str>,
    ) -> MetaResult<models::auth::user::User> {
        todo!()
    }
}

#[derive(Debug, Default)]
pub struct TenantManagerMock {}

impl TenantManager for TenantManagerMock {
    fn create_tenant(&self, name: String, options: TenantOptions) -> MetaResult<MetaClientRef> {
        todo!()
    }

    fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>> {
        todo!()
    }

    fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()> {
        todo!()
    }

    fn drop_tenant(&self, name: &str) -> MetaResult<bool> {
        todo!()
    }

    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        todo!()
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        vec![]
    }

    fn tenants(&self) -> MetaResult<Vec<Tenant>> {
        todo!()
    }

    fn tenant_set_limiter(
        &self,
        tenant_name: &str,
        limiter_config: Option<LimiterConfig>,
    ) -> MetaResult<()> {
        todo!()
    }
}
