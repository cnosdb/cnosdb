#![allow(dead_code, unused_imports, unused_variables, clippy::if_same_then_else)]

use async_trait::async_trait;
use client::MetaHttpClient;
use config::ClusterConfig;
use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{
    CustomTenantRole, SystemTenantRole, TenantRole, TenantRoleIdentifier, UserRole,
};
use models::auth::user::{User, UserDesc};
use models::meta_data::*;
use models::oid::{Identifier, Oid};
use models::utils::min_num;
use parking_lot::RwLock;
use rand::distributions::{Alphanumeric, DistString};
use snafu::Snafu;
use tokio::sync::mpsc::{self, Receiver};

use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::DerefMut;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use std::{fmt::Debug, io};
use store::command;
use tokio::net::TcpStream;

use trace::{debug, error, info, warn};

use crate::error::{MetaError, MetaResult};
use crate::store::key_path;
use models::schema::{
    DatabaseSchema, ExternalTableSchema, LimiterConfig, TableColumn, TableSchema, Tenant,
    TenantOptions, TskvTableSchema,
};

use crate::limiter::{Limiter, LimiterImpl, NoneLimiter};
use crate::store::command::{
    EntryLog, META_REQUEST_FAILED, META_REQUEST_PRIVILEGE_EXIST, META_REQUEST_PRIVILEGE_NOT_FOUND,
    META_REQUEST_ROLE_EXIST, META_REQUEST_ROLE_NOT_FOUND, META_REQUEST_SUCCESS,
    META_REQUEST_TENANT_NOT_FOUND, META_REQUEST_USER_EXIST, META_REQUEST_USER_NOT_FOUND,
};
use crate::tenant_manager::RemoteTenantManager;
use crate::user_manager::{RemoteUserManager, UserManager, UserManagerMock};
use crate::{client, store};

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
    async fn reasign_member_role(&self, user_id: Oid, role: TenantRoleIdentifier)
        -> MetaResult<()>;
    async fn remove_member(&self, user_id: Oid) -> MetaResult<()>;

    // tenant role
    async fn create_custom_role(
        &self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
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
    fn get_tskv_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TskvTableSchema>>;
    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<ExternalTableSchema>>;
    fn list_tables(&self, db: &str) -> MetaResult<Vec<String>>;
    async fn drop_table(&self, db: &str, table: &str) -> MetaResult<()>;

    async fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo>;
    async fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()>;

    fn database_min_ts(&self, db: &str) -> Option<i64>;
    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo>;

    fn get_vnode_all_info(&self, id: u32) -> Option<VnodeAllInfo>;
    fn get_vnode_repl_set(&self, id: u32) -> Option<ReplicationSet>;

    fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> MetaResult<Vec<BucketInfo>>;

    async fn locate_replcation_set_for_write(
        &self,
        db: &str,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplicationSet>;

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

    fn limiter(&self) -> Arc<dyn Limiter>;
}

#[derive(Debug)]
pub struct RemoteMetaClient {
    cluster: String,
    tenant: Tenant,
    meta_url: String,
    limiter: Arc<dyn Limiter>,

    data: RwLock<TenantMetaData>,
    client: MetaHttpClient,
}

impl RemoteMetaClient {
    pub async fn new(
        cluster: String,
        tenant: Tenant,
        meta_url: String,
        node_id: u64,
    ) -> MetaResult<Arc<Self>> {
        let limiter: Arc<dyn Limiter> = match &tenant.options().limiter_config {
            Some(config) => Arc::new(LimiterImpl::from(config)),
            None => Arc::new(NoneLimiter),
        };

        let client = Arc::new(Self {
            cluster,
            tenant,
            limiter,
            meta_url: meta_url.clone(),
            data: RwLock::new(TenantMetaData::new()),
            client: MetaHttpClient::new(1, meta_url),
        });

        client.sync_all_tenant_metadata().await?;

        Ok(client)
    }

    async fn sync_all_tenant_metadata(&self) -> MetaResult<()> {
        let req = command::ReadCommand::TenaneMetaData(self.cluster.clone(), self.tenant_name());
        let resp = self
            .client
            .read::<command::TenaneMetaDataResp>(&req)
            .await?;
        if resp.status.code < 0 {
            return Err(MetaError::CommonError {
                msg: format!("open meta err: {} {}", resp.status.code, resp.status.msg),
            });
        }

        let mut data = self.data.write();
        if resp.data.version > data.version {
            *data = resp.data;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl MetaClient for RemoteMetaClient {
    fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    async fn version(&self) -> u64 {
        self.data.read().version
    }

    // tenant member start

    async fn add_member_with_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()> {
        {
            let user_number = self.data.read().users.len();
            self.limiter().check_add_user(user_number)?;
        }

        let req = command::WriteCommand::AddMemberToTenant(
            self.cluster.clone(),
            user_id,
            role,
            self.tenant().name().to_string(),
        );

        match self.client.write::<command::CommonResp<()>>(&req).await? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_EXIST {
                    Err(MetaError::UserAlreadyExists { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn member_role(&self, user_id: &Oid) -> MetaResult<Option<TenantRoleIdentifier>> {
        let req = command::ReadCommand::MemberRole(
            self.cluster.clone(),
            self.tenant().name().to_string(),
            *user_id,
        );

        match self
            .client
            .read::<command::CommonResp<Option<TenantRoleIdentifier>>>(&req)
            .await?
        {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn members(&self) -> MetaResult<HashMap<String, TenantRoleIdentifier>> {
        let req = command::ReadCommand::Members(self.cluster.clone(), self.tenant_name());

        match self
            .client
            .read::<command::CommonResp<HashMap<String, TenantRoleIdentifier>>>(&req)
            .await?
        {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                warn!(
                    "members of tenant {}, error: {}",
                    self.tenant().name(),
                    status.msg
                );
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    async fn reasign_member_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()> {
        let req = command::WriteCommand::ReasignMemberRole(
            self.cluster.clone(),
            user_id,
            role,
            self.tenant_name(),
        );

        match self.client.write::<command::CommonResp<()>>(&req).await? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn remove_member(&self, user_id: Oid) -> MetaResult<()> {
        let req = command::WriteCommand::RemoveMemberFromTenant(
            self.cluster.clone(),
            user_id,
            self.tenant_name(),
        );

        match self.client.write::<command::CommonResp<()>>(&req).await? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_USER_NOT_FOUND {
                    Err(MetaError::UserNotFound { user: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    // tenant member end

    // tenant role start

    async fn create_custom_role(
        &self,
        role_name: String,
        system_role: SystemTenantRole,
        additiona_privileges: HashMap<String, DatabasePrivilege>,
    ) -> MetaResult<()> {
        let req = command::WriteCommand::CreateRole(
            self.cluster.clone(),
            role_name,
            system_role,
            additiona_privileges,
            self.tenant_name(),
        );

        match self.client.write::<command::CommonResp<()>>(&req).await? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_ROLE_EXIST {
                    Err(MetaError::RoleAlreadyExists { role: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>> {
        let req = command::ReadCommand::CustomRole(
            self.cluster.clone(),
            role_name.to_string(),
            self.tenant_name(),
        );

        match self
            .client
            .read::<command::CommonResp<Option<CustomTenantRole<Oid>>>>(&req)
            .await?
        {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    async fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        let req = command::ReadCommand::CustomRoles(self.cluster.clone(), self.tenant_name());

        match self
            .client
            .read::<command::CommonResp<Vec<CustomTenantRole<Oid>>>>(&req)
            .await?
        {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                warn!("custom roles not found, {}", status.msg);
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    async fn grant_privilege_to_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        let req = command::WriteCommand::GrantPrivileges(
            self.cluster.clone(),
            database_privileges,
            role_name.to_string(),
            self.tenant_name(),
        );

        match self.client.write::<command::CommonResp<()>>(&req).await? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_ROLE_NOT_FOUND {
                    Err(MetaError::RoleNotFound { role: status.msg })
                } else if status.code == META_REQUEST_PRIVILEGE_EXIST {
                    Err(MetaError::PrivilegeAlreadyExists { name: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn revoke_privilege_from_custom_role(
        &self,
        database_privileges: Vec<(DatabasePrivilege, String)>,
        role_name: &str,
    ) -> MetaResult<()> {
        let req = command::WriteCommand::RevokePrivileges(
            self.cluster.clone(),
            database_privileges,
            role_name.to_string(),
            self.tenant_name(),
        );

        match self.client.write::<command::CommonResp<()>>(&req).await? {
            command::CommonResp::Ok(_) => Ok(()),
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_ROLE_NOT_FOUND {
                    Err(MetaError::RoleNotFound { role: status.msg })
                } else if status.code == META_REQUEST_PRIVILEGE_NOT_FOUND {
                    Err(MetaError::PrivilegeNotFound { name: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn drop_custom_role(&self, role_name: &str) -> MetaResult<bool> {
        let req = command::WriteCommand::DropRole(
            self.cluster.clone(),
            role_name.to_string(),
            self.tenant_name(),
        );

        match self.client.write::<command::CommonResp<bool>>(&req).await? {
            command::CommonResp::Ok(e) => Ok(e),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    // tenant role end

    async fn create_db(&self, mut schema: DatabaseSchema) -> MetaResult<()> {
        let db_number = self.data.read().dbs.len();
        self.limiter().check_create_db(db_number, &mut schema)?;
        let req = command::WriteCommand::CreateDB(
            self.cluster.clone(),
            self.tenant_name(),
            schema.clone(),
        );

        let rsp = self
            .client
            .write::<command::TenaneMetaDataResp>(&req)
            .await?;
        let mut data = self.data.write();
        if rsp.data.version > data.version {
            *data = rsp.data;
        }

        if rsp.status.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else if rsp.status.code == command::META_REQUEST_DB_EXIST {
            Err(MetaError::DatabaseAlreadyExists {
                database: schema.database_name().to_string(),
            })
        } else {
            Err(MetaError::CommonError {
                msg: rsp.status.to_string(),
            })
        }
    }

    async fn alter_db_schema(&self, info: &DatabaseSchema) -> MetaResult<()> {
        let req =
            command::WriteCommand::AlterDB(self.cluster.clone(), self.tenant_name(), info.clone());

        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        info!("alter db: {:?}; {:?}", req, rsp);

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else {
            Err(MetaError::CommonError {
                msg: rsp.to_string(),
            })
        }
    }

    fn get_db_schema(&self, name: &str) -> MetaResult<Option<DatabaseSchema>> {
        if let Some(db) = self.data.read().dbs.get(name) {
            return Ok(Some(db.schema.clone()));
        }

        Ok(None)
    }

    fn get_db_info(&self, name: &str) -> MetaResult<Option<DatabaseInfo>> {
        Ok(self.data.read().dbs.get(name).cloned())
    }

    fn list_databases(&self) -> MetaResult<Vec<String>> {
        let mut list = vec![];
        for (k, _) in self.data.read().dbs.iter() {
            list.push(k.clone());
        }

        Ok(list)
    }

    async fn drop_db(&self, name: &str) -> MetaResult<bool> {
        let mut exist = false;
        if self.data.read().dbs.contains_key(name) {
            exist = true;
        }

        let req = command::WriteCommand::DropDB(
            self.cluster.clone(),
            self.tenant_name(),
            name.to_string(),
        );

        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        info!("drop db: {:?}; {:?}", req, rsp);

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(exist)
        } else {
            Err(MetaError::CommonError {
                msg: rsp.to_string(),
            })
        }
    }

    async fn create_table(&self, schema: &TableSchema) -> MetaResult<()> {
        let req = command::WriteCommand::CreateTable(
            self.cluster.clone(),
            self.tenant_name(),
            schema.clone(),
        );

        debug!("create_table: {:?}", req);

        let rsp = self
            .client
            .write::<command::TenaneMetaDataResp>(&req)
            .await?;
        let mut data = self.data.write();
        if rsp.data.version > data.version {
            *data = rsp.data;
        }

        if rsp.status.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else if rsp.status.code == command::META_REQUEST_DB_NOT_FOUND {
            Err(MetaError::DatabaseNotFound {
                database: schema.db(),
            })
        } else if rsp.status.code == command::META_REQUEST_TABLE_EXIST {
            Err(MetaError::TableAlreadyExists {
                table_name: schema.name(),
            })
        } else {
            Err(MetaError::CommonError {
                msg: rsp.status.to_string(),
            })
        }
    }

    fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>> {
        return Ok(self.data.read().table_schema(db, table));
    }

    fn get_tskv_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TskvTableSchema>> {
        if let Some(TableSchema::TsKvTableSchema(val)) = self.data.read().table_schema(db, table) {
            return Ok(Some(val));
        }
        Ok(None)
    }

    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<ExternalTableSchema>> {
        if let Some(TableSchema::ExternalTableSchema(val)) =
            self.data.read().table_schema(db, table)
        {
            return Ok(Some(val));
        }

        Ok(None)
    }

    async fn update_table(&self, schema: &TableSchema) -> MetaResult<()> {
        let req = command::WriteCommand::UpdateTable(
            self.cluster.clone(),
            self.tenant_name(),
            schema.clone(),
        );

        let rsp = self
            .client
            .write::<command::TenaneMetaDataResp>(&req)
            .await?;
        let mut data = self.data.write();
        if rsp.data.version > data.version {
            *data = rsp.data;
        }

        // TODO table not exist

        Ok(())
    }

    fn list_tables(&self, db: &str) -> MetaResult<Vec<String>> {
        let mut list = vec![];
        if let Some(info) = self.data.read().dbs.get(db) {
            for (k, _) in info.tables.iter() {
                list.push(k.clone());
            }
        }

        Ok(list)
    }

    async fn drop_table(&self, db: &str, table: &str) -> MetaResult<()> {
        let req = command::WriteCommand::DropTable(
            self.cluster.clone(),
            self.tenant_name(),
            db.to_string(),
            table.to_string(),
        );

        let rsp = self.client.write::<command::StatusResponse>(&req).await?;

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else {
            Err(MetaError::CommonError {
                msg: rsp.to_string(),
            })
        }
    }

    async fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo> {
        let req = command::WriteCommand::CreateBucket(
            self.cluster.clone(),
            self.tenant_name(),
            db.to_string(),
            ts,
        );

        let rsp = self
            .client
            .write::<command::TenaneMetaDataResp>(&req)
            .await?;
        {
            let mut data = self.data.write();
            if rsp.data.version > data.version {
                *data = rsp.data;
            }
        }

        if rsp.status.code < 0 {
            return Err(MetaError::MetaClientErr {
                msg: format!("create bucket err: {} {}", rsp.status.code, rsp.status.msg),
            });
        }

        if let Some(bucket) = self.data.read().bucket_by_timestamp(db, ts) {
            return Ok(bucket.clone());
        }

        Err(MetaError::CommonError {
            msg: format!("create bucket unknown error db:{} {}", db, ts),
        })
    }

    async fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()> {
        let req = command::WriteCommand::DeleteBucket(
            self.cluster.clone(),
            self.tenant_name(),
            db.to_string(),
            id,
        );

        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        info!("delete bucket: {:?}; {:?}", req, rsp);

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else {
            Err(MetaError::CommonError {
                msg: rsp.to_string(),
            })
        }
    }

    async fn update_replication_set(
        &self,
        db: &str,
        bucket_id: u32,
        repl_id: u32,
        del_info: &[VnodeInfo],
        add_info: &[VnodeInfo],
    ) -> MetaResult<()> {
        let args = command::UpdateVnodeReplSetArgs {
            cluster: self.cluster.clone(),
            tenant: self.tenant_name(),
            db_name: db.to_string(),
            bucket_id,
            repl_id,
            del_info: del_info.to_vec(),
            add_info: add_info.to_vec(),
        };
        let req = command::WriteCommand::UpdateVnodeReplSet(args);

        let rsp = self.client.write::<command::StatusResponse>(&req).await?;
        info!("update replication set: {:?}; {:?}", req, rsp);

        if rsp.code == command::META_REQUEST_SUCCESS {
            Ok(())
        } else {
            Err(MetaError::CommonError {
                msg: rsp.to_string(),
            })
        }
    }

    fn get_vnode_all_info(&self, id: u32) -> Option<VnodeAllInfo> {
        let data = self.data.read();
        for (db_name, db_info) in data.dbs.iter() {
            for bucket in db_info.buckets.iter() {
                for repl_set in bucket.shard_group.iter() {
                    for vnode_info in repl_set.vnodes.iter() {
                        if vnode_info.id == id {
                            return Some(VnodeAllInfo {
                                vnode_id: vnode_info.id,
                                node_id: vnode_info.node_id,
                                repl_set_id: repl_set.id,
                                bucket_id: bucket.id,
                                db_name: db_name.clone(),
                                tenant: self.tenant_name(),

                                start_time: bucket.start_time,
                                end_time: bucket.end_time,
                            });
                        }
                    }
                }
            }
        }

        None
    }

    fn get_vnode_repl_set(&self, id: u32) -> Option<ReplicationSet> {
        let data = self.data.read();
        for (db_name, db_info) in data.dbs.iter() {
            for bucket in db_info.buckets.iter() {
                for repl_set in bucket.shard_group.iter() {
                    for vnode_info in repl_set.vnodes.iter() {
                        if vnode_info.id == id {
                            return Some(repl_set.clone());
                        }
                    }
                }
            }
        }

        None
    }

    fn database_min_ts(&self, name: &str) -> Option<i64> {
        self.data.read().database_min_ts(name)
    }

    async fn locate_replcation_set_for_write(
        &self,
        db: &str,
        hash_id: u64,
        ts: i64,
    ) -> MetaResult<ReplicationSet> {
        if let Some(bucket) = self.data.read().bucket_by_timestamp(db, ts) {
            return Ok(bucket.vnode_for(hash_id));
        }

        let bucket = self.create_bucket(db, ts).await?;

        Ok(bucket.vnode_for(hash_id))
    }

    fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> MetaResult<Vec<BucketInfo>> {
        let buckets = self.data.read().mapping_bucket(db_name, start, end);

        Ok(buckets)
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        let mut list = vec![];
        for (key, val) in self.data.read().dbs.iter() {
            let ttl = val.schema.config.ttl_or_default().to_nanoseconds();
            let now = models::utils::now_timestamp();

            for bucket in val.buckets.iter() {
                if bucket.end_time < now - ttl {
                    let info = ExpiredBucketInfo {
                        tenant: self.tenant_name(),
                        database: key.clone(),
                        bucket: bucket.clone(),
                    };

                    list.push(info)
                }
            }
        }

        list
    }

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        let strs: Vec<&str> = entry.key.split('/').collect();

        let len = strs.len();
        if len == 8
            && strs[6] == key_path::SCHEMAS
            && strs[4] == key_path::DBS
            && strs[2] == key_path::TENANTS
        {
            let tenant = strs[3];
            let db_name = strs[5];
            let tab_name = strs[7];
            if let Some(db) = self.data.write().dbs.get_mut(db_name) {
                if entry.tye == command::ENTRY_LOG_TYPE_SET {
                    if let Ok(info) = serde_json::from_str::<TableSchema>(&entry.val) {
                        db.tables.insert(tab_name.to_string(), info);
                    }
                } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                    db.tables.remove(tab_name);
                }
            }
        } else if len == 8
            && strs[6] == key_path::BUCKETS
            && strs[4] == key_path::DBS
            && strs[2] == key_path::TENANTS
        {
            let tenant = strs[3];
            let db_name = strs[5];
            if let Some(db) = self.data.write().dbs.get_mut(db_name) {
                if let Ok(bucket_id) = serde_json::from_str::<u32>(strs[7]) {
                    db.buckets.sort_by(|a, b| a.id.cmp(&b.id));
                    if entry.tye == command::ENTRY_LOG_TYPE_SET {
                        if let Ok(info) = serde_json::from_str::<BucketInfo>(&entry.val) {
                            match db.buckets.binary_search_by(|v| v.id.cmp(&bucket_id)) {
                                Ok(index) => db.buckets[index] = info,
                                Err(index) => db.buckets.insert(index, info),
                            }
                        }
                    } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                        if let Ok(index) = db.buckets.binary_search_by(|v| v.id.cmp(&bucket_id)) {
                            db.buckets.remove(index);
                        }
                    }
                }
            }
        } else if len == 6 && strs[4] == key_path::DBS && strs[2] == key_path::TENANTS {
            let tenant = strs[3];
            let db_name = strs[5];
            let mut data = self.data.write();
            if entry.tye == command::ENTRY_LOG_TYPE_SET {
                if let Ok(info) = serde_json::from_str::<DatabaseSchema>(&entry.val) {
                    let db = data
                        .dbs
                        .entry(db_name.to_string())
                        .or_insert_with(DatabaseInfo::default);

                    db.schema = info;
                }
            } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                data.dbs.remove(db_name);
            }
        } else if len == 6 && strs[4] == key_path::USERS && strs[2] == key_path::TENANTS {
        } else if len == 6 && strs[4] == key_path::MEMBERS && strs[2] == key_path::TENANTS {
        } else if len == 6 && strs[4] == key_path::ROLES && strs[2] == key_path::TENANTS {
        }

        Ok(())
    }

    fn print_data(&self) -> String {
        info!("****** Tenant: {:?}; Meta: {}", self.tenant, self.meta_url);
        info!("****** Meta Data: {:#?}", self.data);

        format!("{:#?}", self.data.read())
    }

    fn limiter(&self) -> Arc<dyn Limiter> {
        self.limiter.clone()
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn test_sys_info() {
        let info = sys_info::disk_info();
        println!("Disk: {:?}", info);

        let info = sys_info::mem_info();
        println!("Mem: {:?}", info);

        let info = sys_info::cpu_num();
        println!("Cpu Num: {:?}", info);

        let info = sys_info::loadavg();
        println!("Cpu Num: {:?}", info);
    }
}
