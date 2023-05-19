#![allow(dead_code, clippy::if_same_then_else)]

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use client::MetaHttpClient;
use config::TenantObjectLimiterConfig;
use models::auth::privilege::DatabasePrivilege;
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRoleIdentifier};
use models::meta_data::*;
use models::oid::{Identifier, Oid};
use models::schema::{DatabaseSchema, ExternalTableSchema, TableSchema, Tenant, TskvTableSchema};
use parking_lot::RwLock;
use store::command;
use trace::{debug, info, warn};

use crate::error::{MetaError, MetaResult};
use crate::model::MetaClient;
use crate::store::command::{
    EntryLog, META_REQUEST_PRIVILEGE_EXIST, META_REQUEST_PRIVILEGE_NOT_FOUND,
    META_REQUEST_ROLE_EXIST, META_REQUEST_ROLE_NOT_FOUND, META_REQUEST_USER_EXIST,
    META_REQUEST_USER_NOT_FOUND,
};
use crate::store::key_path;
use crate::{client, store};

#[derive(Debug)]
pub struct RemoteMetaClient {
    cluster: String,
    tenant: Tenant,
    meta_url: String,

    data: RwLock<TenantMetaData>,
    client: MetaHttpClient,
}

impl RemoteMetaClient {
    pub async fn new(
        cluster: String,
        tenant: Tenant,
        meta_url: String,
        _node_id: u64,
    ) -> MetaResult<Arc<Self>> {
        let client = Arc::new(Self {
            cluster,
            tenant,
            meta_url: meta_url.clone(),
            data: RwLock::new(TenantMetaData::new()),
            client: MetaHttpClient::new(meta_url),
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

    fn check_create_db(&self, db_schema: &mut DatabaseSchema) -> MetaResult<()> {
        let limiter_config = match self.tenant.options().object_config() {
            Some(config) => config,
            None => return Ok(()),
        };

        let TenantObjectLimiterConfig {
            max_databases,
            max_replicate_number,
            max_retention_time,
            max_shard_number,
            ..
        } = limiter_config;

        let db_num = self.data.read().dbs.len();
        if let Some(max) = max_databases {
            if db_num >= *max {
                return Err(MetaError::ObjectLimit {
                    msg: format!(
                        "Create database failed, the maximum number of database is {}",
                        max
                    ),
                });
            }
        }

        let replica = db_schema.config.replica_or_default();
        if let Some(max) = max_replicate_number {
            if replica as usize > *max {
                return Err(MetaError::ObjectLimit {
                    msg: format!(
                        "Create database failed, the maximum number of database's replica is {}",
                        max
                    ),
                });
            }
        }

        let shard = db_schema.config.shard_num_or_default();
        if let Some(max) = max_shard_number {
            if shard as usize > *max {
                return Err(MetaError::ObjectLimit {
                    msg: format!(
                        "Create database failed, the maximum number of database's shards is {}",
                        max
                    ),
                });
            }
        }

        match (db_schema.config.ttl(), max_retention_time) {
            (Some(ttl), Some(day)) => {
                let ttl = ttl.to_nanoseconds();
                let max = models::schema::Duration::new_with_day(*day as u64);
                if ttl > max.to_nanoseconds() {
                    return Err(MetaError::ObjectLimit {
                        msg: format!("TTL reached limit, max is {} days", day),
                    });
                }
            }
            (None, Some(day)) => db_schema
                .config
                .with_ttl(models::schema::Duration::new_with_day(*day as u64)),
            _ => {}
        }

        Ok(())
    }

    fn check_add_user(&self) -> MetaResult<()> {
        let limiter_config = match self.tenant.options().object_config() {
            Some(config) => config,
            None => return Ok(()),
        };

        let TenantObjectLimiterConfig {
            max_users_number, ..
        } = limiter_config;

        let user_number = self.data.read().users.len();

        if let Some(max) = max_users_number {
            if user_number >= *max {
                return Err(MetaError::ObjectLimit {
                    msg: format!("users reached limit, max is {}", max),
                });
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl MetaClient for RemoteMetaClient {
    fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    async fn add_member_with_role(
        &self,
        user_id: Oid,
        role: TenantRoleIdentifier,
    ) -> MetaResult<()> {
        self.check_add_user()?;
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

    // tenant member start

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

    // tenant member end

    // tenant role start

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

    async fn create_db(&self, mut schema: DatabaseSchema) -> MetaResult<()> {
        self.check_create_db(&mut schema)?;

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

    // tenant role end

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

    fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>> {
        return Ok(self.data.read().table_schema(db, table));
    }

    fn get_tskv_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<Arc<TskvTableSchema>>> {
        if let Some(TableSchema::TsKvTableSchema(val)) = self.data.read().table_schema(db, table) {
            return Ok(Some(val));
        }
        Ok(None)
    }

    fn get_external_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<Arc<ExternalTableSchema>>> {
        if let Some(TableSchema::ExternalTableSchema(val)) =
            self.data.read().table_schema(db, table)
        {
            return Ok(Some(val));
        }

        Ok(None)
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

    fn database_min_ts(&self, name: &str) -> Option<i64> {
        self.data.read().database_min_ts(name)
    }

    fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        let mut list = vec![];
        for (key, val) in self.data.read().dbs.iter() {
            for bucket in val.buckets.iter() {
                if bucket.end_time < val.schema.time_to_expired() {
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
                                status: vnode_info.status.clone(),
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
        for (_db_name, db_info) in data.dbs.iter() {
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

    fn mapping_bucket(&self, db_name: &str, start: i64, end: i64) -> MetaResult<Vec<BucketInfo>> {
        let buckets = self.data.read().mapping_bucket(db_name, start, end);

        Ok(buckets)
    }

    async fn locate_replication_set_for_write(
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

    async fn update_vnode(&self, info: &VnodeAllInfo) -> MetaResult<()> {
        let args = command::UpdateVnodeArgs {
            cluster: self.cluster.clone(),
            vnode_info: info.clone(),
        };
        let req = command::WriteCommand::UpdateVnode(args);

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

    async fn version(&self) -> u64 {
        self.data.read().version
    }

    async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        let strs: Vec<&str> = entry.key.split('/').collect();

        let len = strs.len();
        if len == 8
            && strs[6] == key_path::SCHEMAS
            && strs[4] == key_path::DBS
            && strs[2] == key_path::TENANTS
        {
            let _tenant = strs[3];
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
            let _tenant = strs[3];
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
            let _tenant = strs[3];
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
