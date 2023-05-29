use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::{User, UserDesc, UserOptions};
use models::meta_data::{
    BucketInfo, DatabaseInfo, ExpiredBucketInfo, NodeInfo, ReplicationSet, VnodeAllInfo, VnodeInfo,
};
use models::oid::{Identifier, Oid};
use models::schema::{
    DatabaseSchema, ExternalTableSchema, TableSchema, Tenant, TenantOptions, TskvTableSchema,
};
use tonic::transport::Channel;

use crate::error::MetaResult;
use crate::limiter::RequestLimiter;
use crate::store::command::EntryLog;

pub mod meta_admin;
pub mod meta_client;
pub mod meta_client_mock;
pub mod meta_manager;
pub mod tenant_manager;
pub mod user_manager;
pub mod user_manager_mock;

pub type UserManagerRef = Arc<dyn UserManager>;
pub type TenantManagerRef = Arc<dyn TenantManager>;
pub type MetaClientRef = Arc<dyn MetaClient>;
pub type AdminMetaRef = Arc<dyn AdminMeta>;
pub type MetaRef = Arc<dyn MetaManager>;

#[async_trait]
pub trait AdminMeta: Send + Sync + Debug {
    // *数据节点上下线管理 */
    async fn sync_all(&self) -> MetaResult<u64>;
    async fn data_nodes(&self) -> Vec<NodeInfo>;
    async fn add_data_node(&self) -> MetaResult<()>;
    async fn report_node_metrics(&self) -> MetaResult<()>;
    // fn del_data_node(&self, id: u64) -> MetaResult<()>;

    // fn meta_nodes(&self);
    // fn add_meta_node(&self, node: &NodeInfo) -> MetaResult<()>;
    // fn del_meta_node(&self, id: u64) -> MetaResult<()>;

    async fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo>;
    async fn get_node_conn(&self, node_id: u64) -> MetaResult<Channel>;
    async fn retain_id(&self, count: u32) -> MetaResult<u32>;
    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()>;
}

#[async_trait]
pub trait MetaClient: Send + Sync + Debug {
    fn tenant(&self) -> &Tenant;
    fn tenant_name(&self) -> String {
        self.tenant().name().to_string()
    }
    //fn create_user(&self, user: &UserInfo) -> MetaResult<()>;
    //fn drop_user(&self, name: &str) -> MetaResult<()>;

    // tenant member
    // fn tenants_of_user(&mut self, user_id: &Oid) -> MetaResult<Option<&HashSet<Oid>>>;
    // fn remove_member_from_all_tenants(&mut self, user_id: &Oid) -> MetaResult<bool>;
    async fn add_member_with_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()>;
    async fn member_role(&self, user_id: &Oid) -> MetaResult<Option<TenantRoleIdentifier>>;
    async fn members(&self) -> MetaResult<HashMap<String, TenantRoleIdentifier>>;
    async fn reassign_member_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()>;
    async fn remove_member(&self, user_id: Oid) -> MetaResult<()>;

    // tenant role
    async fn create_custom_role(
        &self,
        role_name: String,
        system_role: SystemTenantRole,
        additional_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()>;
    async fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>>;
    async fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>>;
    async fn grant_privilege_to_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()>;
    async fn revoke_privilege_from_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()>;
    async fn drop_custom_role(&self, role_name: &str) -> MetaResult<bool>;

    async fn create_db(&self, info: DatabaseSchema) -> MetaResult<()>;
    async fn alter_db_schema(&self, info: &DatabaseSchema) -> MetaResult<()>;
    fn get_db_schema(&self, name: &str) -> MetaResult<Option<DatabaseSchema>>;
    fn get_db_info(&self, name: &str) -> MetaResult<Option<DatabaseInfo>>;
    fn list_databases(&self) -> MetaResult<Vec<String>>;
    async fn drop_db(&self, name: &str) -> MetaResult<bool>;

    async fn create_table(&self, schema: &TableSchema) -> MetaResult<()>;
    async fn update_table(&self, schema: &TableSchema) -> MetaResult<()>;
    fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>>;
    fn get_tskv_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<Arc<TskvTableSchema>>>;
    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<Arc<ExternalTableSchema>>>;
    fn list_tables(&self, db: &str) -> MetaResult<Vec<String>>;
    async fn drop_table(&self, db: &str, table: &str) -> MetaResult<()>;

    async fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo>;
    async fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()>;

    fn database_min_ts(&self, db: &str) -> Option<i64>;
    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo>;

    fn get_vnode_all_info(&self, vnode_id: u32) -> Option<VnodeAllInfo>;
    fn get_vnode_repl_set(&self, vnode_id: u32) -> Option<ReplicationSet>;

    fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> MetaResult<Vec<BucketInfo>>;

    async fn locate_replication_set_for_write(
        &self,
        db: &str,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplicationSet>;

    fn get_replication_set(&self, repl_id: u32) -> Option<ReplicationSet>;

    async fn update_replication_set(
        &self,
        db: &str,
        bucket_id: u32,
        repl_id: u32,
        del_info: &[VnodeInfo],
        add_info: &[VnodeInfo],
    ) -> MetaResult<()>;

    async fn version(&self) -> u64;

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()>;

    fn print_data(&self) -> String;

    async fn update_vnode(&self, info: &VnodeAllInfo) -> MetaResult<()>;
}

#[async_trait]
pub trait MetaManager: Send + Sync + Debug {
    fn node_id(&self) -> u64;
    fn admin_meta(&self) -> AdminMetaRef;
    fn user_manager(&self) -> UserManagerRef;
    fn tenant_manager(&self) -> TenantManagerRef;
    async fn use_tenant(&self, val: &str) -> MetaResult<()>;
    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo>;
    async fn user_with_privileges(
        &self,
        user_name: &str,
        tenant_name: Option<&str>,
    ) -> MetaResult<User>;
}

#[async_trait]
pub trait TenantManager: Send + Sync + Debug {
    async fn clear(&self);
    // tenant
    async fn create_tenant(
        &self,
        name: String,
        options: TenantOptions,
    ) -> MetaResult<MetaClientRef>;
    async fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>>;
    async fn tenants(&self) -> MetaResult<Vec<Tenant>>;
    async fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()>;
    async fn drop_tenant(&self, name: &str) -> MetaResult<bool>;
    // tenant object meta manager
    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;

    async fn get_tenant_meta(&self, tenant: &str) -> Option<MetaClientRef>;

    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo>;

    async fn limiter(&self, tenant: &str) -> Arc<dyn RequestLimiter>;
}

#[async_trait]
pub trait UserManager: Send + Sync + Debug {
    // user
    async fn create_user(
        &self,
        name: String,
        options: UserOptions,
        is_admin: bool,
    ) -> MetaResult<Oid>;
    async fn user(&self, name: &str) -> MetaResult<Option<UserDesc>>;
    // fn user_with_privileges(&self, name: &str) -> Result<Option<User>>;
    async fn users(&self) -> MetaResult<Vec<UserDesc>>;
    async fn alter_user(&self, name: &str, options: UserOptions) -> MetaResult<()>;
    async fn drop_user(&self, name: &str) -> MetaResult<bool>;
    async fn rename_user(&self, old_name: &str, new_name: String) -> MetaResult<()>;
}
