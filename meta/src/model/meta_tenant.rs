#![allow(dead_code, clippy::if_same_then_else)]

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::Arc;

use client::MetaHttpClient;
use config::TenantObjectLimiterConfig;
use models::auth::privilege::{DatabasePrivilege, Privilege};
use models::auth::role::{CustomTenantRole, SystemTenantRole, TenantRoleIdentifier};
use models::auth::user::UserDesc;
use models::meta_data::*;
use models::oid::{Identifier, Oid};
use models::schema::{
    DatabaseSchema, ExternalTableSchema, TableSchema, Tenant, TskvTableSchemaRef,
};
use parking_lot::RwLock;
use store::command;
use trace::info;

use crate::error::{MetaError, MetaResult};
use crate::store::command::{EntryLog, ReadCommand};
use crate::store::key_path;
use crate::{client, store};

#[derive(Debug)]
pub struct TenantMeta {
    cluster: String,
    tenant: Tenant,
    meta_url: String,

    data: RwLock<TenantMetaData>,
    client: MetaHttpClient,
}

impl TenantMeta {
    pub fn mock() -> Self {
        Self {
            cluster: "".to_string(),
            tenant: Tenant::default(),
            meta_url: "".to_string(),
            data: RwLock::new(TenantMetaData::new()),
            client: MetaHttpClient::new(""),
        }
    }

    pub async fn new(cluster: String, tenant: Tenant, meta_url: String) -> MetaResult<Arc<Self>> {
        let client = Arc::new(Self {
            cluster,
            tenant,
            meta_url: meta_url.clone(),
            data: RwLock::new(TenantMetaData::new()),
            client: MetaHttpClient::new(&meta_url),
        });

        client.sync_all_tenant_metadata().await?;

        Ok(client)
    }

    pub async fn sync_all_tenant_metadata(&self) -> MetaResult<()> {
        let req = command::ReadCommand::TenaneMetaData(self.cluster.clone(), self.tenant_name());
        let resp = self.client.read::<TenantMetaData>(&req).await?;

        let mut data = self.data.write();
        if resp.version > data.version {
            *data = resp;
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

        let user_number = self.data.read().members.len();

        if let Some(max) = max_users_number {
            if user_number >= *max {
                return Err(MetaError::ObjectLimit {
                    msg: format!("users reached limit, max is {}", max),
                });
            }
        }
        Ok(())
    }

    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    pub fn tenant_name(&self) -> String {
        self.tenant().name().to_string()
    }

    pub async fn add_member_with_role(
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

        self.client.write::<()>(&req).await
    }

    pub async fn user_privileges(
        &self,
        user_desc: &UserDesc,
    ) -> MetaResult<HashSet<Privilege<Oid>>> {
        let role = {
            let cache = self
                .data
                .read()
                .members
                .get(&user_desc.id().to_string())
                .cloned();
            if let Some(role) = cache {
                role.clone()
            } else {
                self.member_role(user_desc.id()).await?.ok_or_else(|| {
                    MetaError::MemberNotFound {
                        member_name: user_desc.name().to_string(),
                        tenant_name: self.tenant_name(),
                    }
                })?
            }
        };

        let tenant_id = self.tenant().id();
        let privileges = match role {
            TenantRoleIdentifier::System(sys_role) => sys_role.to_privileges(tenant_id),
            TenantRoleIdentifier::Custom(ref role_name) => {
                let cache = self
                    .data
                    .read()
                    .roles
                    .get(&user_desc.id().to_string())
                    .cloned();
                if let Some(role) = cache {
                    role.to_privileges(tenant_id)
                } else {
                    self.custom_role(role_name)
                        .await?
                        .map(|e| e.to_privileges(tenant_id))
                        .unwrap_or_default()
                }
            }
        };

        Ok(privileges)
    }

    // tenant member start

    pub async fn member_role(&self, user_id: &Oid) -> MetaResult<Option<TenantRoleIdentifier>> {
        let req = command::ReadCommand::MemberRole(
            self.cluster.clone(),
            self.tenant().name().to_string(),
            *user_id,
        );

        self.client.read::<Option<TenantRoleIdentifier>>(&req).await
    }

    pub async fn members(&self) -> MetaResult<HashMap<String, TenantRoleIdentifier>> {
        let req = command::ReadCommand::Members(self.cluster.clone(), self.tenant_name());

        self.client
            .read::<HashMap<String, TenantRoleIdentifier>>(&req)
            .await
    }

    pub async fn reassign_member_role(
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

        self.client.write::<()>(&req).await
    }

    pub async fn remove_member(&self, user_id: Oid) -> MetaResult<()> {
        let req = command::WriteCommand::RemoveMemberFromTenant(
            self.cluster.clone(),
            user_id,
            self.tenant_name(),
        );

        self.client.write::<()>(&req).await
    }

    pub async fn create_custom_role(
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

        self.client.write::<()>(&req).await
    }

    // tenant member end

    // tenant role start

    pub async fn custom_role(&self, role_name: &str) -> MetaResult<Option<CustomTenantRole<Oid>>> {
        let req = command::ReadCommand::CustomRole(
            self.cluster.clone(),
            role_name.to_string(),
            self.tenant_name(),
        );

        self.client
            .read::<Option<CustomTenantRole<Oid>>>(&req)
            .await
    }

    pub async fn custom_roles(&self) -> MetaResult<Vec<CustomTenantRole<Oid>>> {
        let req = command::ReadCommand::CustomRoles(self.cluster.clone(), self.tenant_name());

        self.client.read::<Vec<CustomTenantRole<Oid>>>(&req).await
    }

    pub async fn grant_privilege_to_custom_role(
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

        self.client.write::<()>(&req).await
    }

    pub async fn revoke_privilege_from_custom_role(
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

        self.client.write::<()>(&req).await
    }

    pub async fn drop_custom_role(&self, role_name: &str) -> MetaResult<bool> {
        let req = command::WriteCommand::DropRole(
            self.cluster.clone(),
            role_name.to_string(),
            self.tenant_name(),
        );

        let rsp = self.client.write::<bool>(&req).await;
        if let Err(MetaError::RoleNotFound { role: _ }) = rsp {
            Ok(false)
        } else {
            rsp
        }
    }

    pub async fn create_db(&self, mut schema: DatabaseSchema) -> MetaResult<()> {
        self.check_create_db(&mut schema)?;

        let req = command::WriteCommand::CreateDB(
            self.cluster.clone(),
            self.tenant_name(),
            schema.clone(),
        );

        let rsp = self.client.write::<TenantMetaData>(&req).await?;
        {
            let mut data = self.data.write();
            if rsp.version > data.version {
                *data = rsp;
            }
        }

        Ok(())
    }

    // tenant role end

    pub async fn alter_db_schema(&self, info: &DatabaseSchema) -> MetaResult<()> {
        let req =
            command::WriteCommand::AlterDB(self.cluster.clone(), self.tenant_name(), info.clone());

        self.client.write::<()>(&req).await
    }

    pub fn get_db_schema(&self, name: &str) -> MetaResult<Option<DatabaseSchema>> {
        if let Some(db) = self.data.read().dbs.get(name) {
            return Ok(Some(db.schema.clone()));
        }

        Ok(None)
    }

    pub fn get_db_info(&self, name: &str) -> MetaResult<Option<DatabaseInfo>> {
        Ok(self.data.read().dbs.get(name).cloned())
    }

    pub fn list_databases(&self) -> MetaResult<Vec<String>> {
        let mut list = vec![];
        for (k, _) in self.data.read().dbs.iter() {
            list.push(k.clone());
        }

        Ok(list)
    }

    pub async fn drop_db(&self, name: &str) -> MetaResult<bool> {
        let mut exist = false;
        if self.data.read().dbs.contains_key(name) {
            exist = true;
        }

        let req = command::WriteCommand::DropDB(
            self.cluster.clone(),
            self.tenant_name(),
            name.to_string(),
        );

        self.client.write::<()>(&req).await?;
        Ok(exist)
    }

    pub async fn create_table(&self, schema: &TableSchema) -> MetaResult<()> {
        let req = command::WriteCommand::CreateTable(
            self.cluster.clone(),
            self.tenant_name(),
            schema.clone(),
        );

        let rsp = self.client.write::<TenantMetaData>(&req).await?;
        {
            let mut data = self.data.write();
            if rsp.version > data.version {
                *data = rsp;
            }
        }

        Ok(())
    }

    pub async fn update_table(&self, schema: &TableSchema) -> MetaResult<()> {
        let req = command::WriteCommand::UpdateTable(
            self.cluster.clone(),
            self.tenant_name(),
            schema.clone(),
        );

        self.client.write::<()>(&req).await
    }

    pub fn get_table_schema(&self, db: &str, table: &str) -> MetaResult<Option<TableSchema>> {
        return Ok(self.data.read().table_schema(db, table));
    }

    pub fn get_tskv_table_schema(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<TskvTableSchemaRef>> {
        if let Some(TableSchema::TsKvTableSchema(val)) = self.data.read().table_schema(db, table) {
            return Ok(Some(val));
        }
        Ok(None)
    }

    pub async fn get_tskv_table_schema_by_meta(
        &self,
        db: &str,
        table: &str,
    ) -> MetaResult<Option<TskvTableSchemaRef>> {
        let req = ReadCommand::TableSchema(
            self.cluster.clone(),
            self.tenant.name().to_string(),
            db.to_string(),
            table.to_string(),
        );
        let rsp = self.client.read::<Option<TableSchema>>(&req).await?;
        if let Some(TableSchema::TsKvTableSchema(val)) = rsp {
            let mut data_w = self.data.write();
            let db = data_w
                .dbs
                .get_mut(&val.db)
                .ok_or(MetaError::DatabaseNotFound {
                    database: val.db.clone(),
                })?;
            db.tables
                .insert(val.name.clone(), TableSchema::TsKvTableSchema(val.clone()));
            return Ok(Some(val));
        }
        Ok(None)
    }

    pub fn get_external_table_schema(
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

    pub fn list_tables(&self, db: &str) -> MetaResult<Vec<String>> {
        let mut list = vec![];
        if let Some(info) = self.data.read().dbs.get(db) {
            for (k, _) in info.tables.iter() {
                list.push(k.clone());
            }
        }

        Ok(list)
    }

    pub async fn drop_table(&self, db: &str, table: &str) -> MetaResult<()> {
        let req = command::WriteCommand::DropTable(
            self.cluster.clone(),
            self.tenant_name(),
            db.to_string(),
            table.to_string(),
        );

        self.client.write::<()>(&req).await
    }

    pub async fn create_bucket(&self, db: &str, ts: i64) -> MetaResult<BucketInfo> {
        let req = command::WriteCommand::CreateBucket(
            self.cluster.clone(),
            self.tenant_name(),
            db.to_string(),
            ts,
        );

        let rsp = self.client.write::<TenantMetaData>(&req).await?;
        {
            let mut data = self.data.write();
            if rsp.version > data.version {
                *data = rsp;
            }
        }

        if let Some(bucket) = self.data.read().bucket_by_timestamp(db, ts) {
            return Ok(bucket.clone());
        }

        Err(MetaError::CommonError {
            msg: format!("create bucket unknown error db:{} {}", db, ts),
        })
    }

    pub async fn delete_bucket(&self, db: &str, id: u32) -> MetaResult<()> {
        let req = command::WriteCommand::DeleteBucket(
            self.cluster.clone(),
            self.tenant_name(),
            db.to_string(),
            id,
        );

        self.client.write::<()>(&req).await
    }

    pub fn database_min_ts(&self, name: &str) -> Option<i64> {
        self.data.read().database_min_ts(name)
    }

    pub fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
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

    pub fn get_vnode_all_info(&self, id: u32) -> Option<VnodeAllInfo> {
        let data = self.data.read();
        for (db_name, db_info) in data.dbs.iter() {
            for bucket in db_info.buckets.iter() {
                for repl_set in bucket.shard_group.iter() {
                    for vnode_info in repl_set.vnodes.iter() {
                        if vnode_info.id == id {
                            return Some(VnodeAllInfo {
                                vnode_id: vnode_info.id,
                                node_id: vnode_info.node_id,
                                status: vnode_info.status,
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

    pub fn get_vnode_repl_set(&self, vnode_id: u32) -> Option<ReplicationSet> {
        let data = self.data.read();
        for (_db_name, db_info) in data.dbs.iter() {
            for bucket in db_info.buckets.iter() {
                for repl_set in bucket.shard_group.iter() {
                    for vnode_info in repl_set.vnodes.iter() {
                        if vnode_info.id == vnode_id {
                            return Some(repl_set.clone());
                        }
                    }
                }
            }
        }

        None
    }

    pub fn mapping_bucket(
        &self,
        db_name: &str,
        start: i64,
        end: i64,
    ) -> MetaResult<Vec<BucketInfo>> {
        let buckets = self.data.read().mapping_bucket(db_name, start, end);

        Ok(buckets)
    }

    pub async fn locate_replication_set_for_write(
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

    pub fn get_replication_set(&self, repl_id: u32) -> Option<ReplicationSet> {
        let data = self.data.read();
        for (_db_name, db_info) in data.dbs.iter() {
            for bucket in db_info.buckets.iter() {
                for repl_set in bucket.shard_group.iter() {
                    if repl_set.id == repl_id {
                        return Some(repl_set.clone());
                    }
                }
            }
        }

        None
    }

    pub async fn update_vnode(&self, info: &VnodeAllInfo) -> MetaResult<()> {
        let args = command::UpdateVnodeArgs {
            cluster: self.cluster.clone(),
            vnode_info: info.clone(),
        };

        let req = command::WriteCommand::UpdateVnode(args);
        self.client.write::<()>(&req).await
    }

    pub fn change_local_vnode_status(&self, id: u32, status: VnodeStatus) -> MetaResult<()> {
        let mut data = self.data.write();
        for (_db_name, db_info) in data.dbs.iter_mut() {
            for bucket in db_info.buckets.iter_mut() {
                for repl_set in bucket.shard_group.iter_mut() {
                    for vnode in repl_set.vnodes.iter_mut() {
                        if vnode.id == id {
                            vnode.status = status;
                            return Ok(());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn update_replication_set(
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
        self.client.write::<()>(&req).await
    }

    pub async fn version(&self) -> u64 {
        self.data.read().version
    }

    // **[6]    /cluster_name/tenants/tenant/roles/name -> [CustomTenantRole<Oid>]
    // **[6]    /cluster_name/tenants/tenant/members/oid -> [TenantRoleIdentifier]
    pub async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
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
        } else if len == 6 && strs[4] == key_path::MEMBERS && strs[2] == key_path::TENANTS {
            let key = strs[5];
            let mut data = self.data.write();
            if entry.tye == command::ENTRY_LOG_TYPE_SET {
                if let Ok(info) = serde_json::from_str::<TenantRoleIdentifier>(&entry.val) {
                    data.members.insert(key.to_owned(), info);
                }
            } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                data.members.remove(key);
            }
        } else if len == 6 && strs[4] == key_path::ROLES && strs[2] == key_path::TENANTS {
            let key = strs[5];
            let mut data = self.data.write();
            if entry.tye == command::ENTRY_LOG_TYPE_SET {
                if let Ok(info) = serde_json::from_str::<CustomTenantRole<Oid>>(&entry.val) {
                    data.roles.insert(key.to_owned(), info);
                }
            } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                data.roles.remove(key);
            }
        }

        Ok(())
    }

    pub fn print_data(&self) -> String {
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
