#![allow(dead_code, unused_imports, unused_variables)]

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use models::{
    auth::{
        privilege::DatabasePrivilege,
        role::{CustomTenantRole, SystemTenantRole, TenantRole, TenantRoleIdentifier},
    },
    meta_data::{BucketInfo, DatabaseInfo, NodeInfo, ReplcationSet},
    oid::Oid,
    schema::{Tenant, TenantOptions, TskvTableSchema},
};
use tokio::net::TcpStream;

use crate::{
    meta_client::{
        AdminMeta, AdminMetaRef, MetaClient, MetaClientRef, MetaError, MetaManager, MetaResult,
        TenantManager, TenantManagerRef, UserManagerRef,
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

    fn create_db(&self, info: &DatabaseInfo) -> MetaResult<()> {
        Ok(())
    }

    fn get_db_schema(&self, name: &String) -> MetaResult<Option<DatabaseInfo>> {
        Ok(Some(DatabaseInfo::default()))
    }

    fn list_databases(&self) -> MetaResult<Vec<String>> {
        Ok(vec![])
    }

    fn drop_db(&self, name: &String) -> MetaResult<()> {
        Ok(())
    }

    fn create_table(&self, schema: &TskvTableSchema) -> MetaResult<()> {
        Ok(())
    }

    fn update_table(&self, schema: &TskvTableSchema) -> MetaResult<()> {
        Ok(())
    }

    fn get_table_schema(&self, db: &String, table: &String) -> MetaResult<Option<TskvTableSchema>> {
        Ok(Some(TskvTableSchema::default()))
    }

    fn list_tables(&self, db: &String) -> MetaResult<Vec<String>> {
        Ok(vec![])
    }

    fn drop_table(&self, db: &String, table: &String) -> MetaResult<()> {
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

    fn add_member_with_role(&mut self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()> {
        todo!()
    }

    fn member_role(&self, user_id: &Oid) -> MetaResult<TenantRole<Oid>> {
        todo!()
    }

    fn members(&self) -> MetaResult<Option<HashSet<&Oid>>> {
        todo!()
    }

    fn reasign_member_role(&mut self, user_id: Oid, role: TenantRoleIdentifier) -> MetaResult<()> {
        todo!()
    }

    fn remove_member(&mut self, user_id: Oid) -> MetaResult<()> {
        todo!()
    }

    fn create_custom_role(
        &mut self,
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
        &mut self,
        database_name: String,
        database_privileges: Vec<(DatabasePrivilege, Oid)>,
        role_name: &str,
    ) -> MetaResult<()> {
        todo!()
    }

    fn revoke_privilege_from_custom_role(
        &mut self,
        database_name: &str,
        database_privileges: Vec<(DatabasePrivilege, Oid)>,
        role_name: &str,
    ) -> MetaResult<bool> {
        todo!()
    }

    fn drop_custom_role(&mut self, role_name: &str) -> MetaResult<bool> {
        todo!()
    }
}

#[derive(Default, Debug)]
pub struct MockMetaManager {}

impl MetaManager for MockMetaManager {
    fn node_id(&self) -> u64 {
        0
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
}

#[derive(Debug, Default)]
pub struct TenantManagerMock {}

impl TenantManager for TenantManagerMock {
    fn create_tenant(&self, name: String, options: TenantOptions) -> MetaResult<MetaClientRef> {
        todo!()
    }

    fn tenant(&self, name: &str) -> MetaResult<Tenant> {
        todo!()
    }

    fn alter_tenant(&self, tenant_id: Oid, options: TenantOptions) -> MetaResult<()> {
        todo!()
    }

    fn drop_tenant(&self, name: &str) -> MetaResult<()> {
        todo!()
    }

    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        todo!()
    }
}
