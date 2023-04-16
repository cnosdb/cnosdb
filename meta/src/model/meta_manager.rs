#![allow(clippy::if_same_then_else)]

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use config::Config;
use models::auth::role::TenantRoleIdentifier;
use models::auth::user::{admin_user, User};
use models::meta_data::*;
use models::oid::Identifier;
use models::schema::{DatabaseSchema, TableSchema};
use models::utils::min_num;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tokio::sync::mpsc::{self, Receiver, Sender};
use trace::info;

use super::MetaClientRef;
use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::model::meta_admin::RemoteAdminMeta;
use crate::model::tenant_manager::{
    RemoteTenantManager, UseTenantInfo, USE_TENANT_ACTION_ADD, USE_TENANT_ACTION_DEL,
};
use crate::model::user_manager::RemoteUserManager;
use crate::model::{AdminMetaRef, MetaManager, TenantManagerRef, UserManagerRef};
use crate::store::command::EntryLog;
use crate::store::{command, key_path};

#[derive(Debug)]
pub struct RemoteMetaManager {
    config: Config,

    watch_version: Arc<AtomicU64>,
    tenant_change_sender: Sender<UseTenantInfo>,
    watch_tenants: Arc<RwLock<HashSet<String>>>,

    admin: AdminMetaRef,
    user_manager: UserManagerRef,
    tenant_manager: TenantManagerRef,

    sub_change_sender: broadcast::Sender<SubOperationLog>,
}

impl RemoteMetaManager {
    pub async fn new(config: Config, path: String) -> Arc<Self> {
        let (tenant_change_sender, tenant_change_receiver) = mpsc::channel(1024);

        let admin: AdminMetaRef = Arc::new(RemoteAdminMeta::new(config.clone(), path));
        let base_ver = admin.sync_all().await.unwrap();
        let cluster_meta = config.cluster.meta_service_addr.clone().join(";");
        let user_manager = Arc::new(RemoteUserManager::new(
            config.cluster.name.clone(),
            cluster_meta.clone(),
        ));
        let tenant_manager = Arc::new(RemoteTenantManager::new(
            config.cluster.name.clone(),
            cluster_meta,
            config.node_id,
            tenant_change_sender.clone(),
        ));

        let (sub_change_sender, _) = broadcast::channel(1024);

        let manager = Arc::new(Self {
            config,
            admin,
            user_manager,
            tenant_manager,
            sub_change_sender,
            tenant_change_sender,
            watch_tenants: Arc::new(RwLock::new(HashSet::new())),
            watch_version: Arc::new(AtomicU64::new(base_ver)),
        });

        tokio::spawn(RemoteMetaManager::watch_task_manager(
            manager.clone(),
            tenant_change_receiver,
        ));

        manager
    }

    pub async fn watch_task_manager(
        mgr: Arc<RemoteMetaManager>,
        mut tenant_change_receiver: Receiver<UseTenantInfo>,
    ) {
        let mut base_ver = mgr.watch_version.load(Ordering::Relaxed);
        let mut task_handle: Option<tokio::task::JoinHandle<()>> = None;
        loop {
            if let Some(handle) = task_handle {
                handle.abort();
            }

            let handle = tokio::spawn(RemoteMetaManager::watch_data_task(mgr.clone(), base_ver));
            task_handle = Some(handle);

            //wait version change
            let version = match tenant_change_receiver.recv().await {
                Some(info) => {
                    let mut tenants = mgr.watch_tenants.write();
                    if info.action == USE_TENANT_ACTION_ADD {
                        if info.name.is_empty() {
                            tenants.clear();
                        }
                        tenants.insert(info.name.clone());

                        for (db_name, db) in info.data.dbs.iter() {
                            for (_, sub) in db.subs.iter() {
                                let _ = mgr.sub_change_sender.send(SubOperationLog {
                                    info: sub.clone(),
                                    opt_type: OPERATION_TYPE_ADD,
                                    tenant: info.name.clone(),
                                    db_name: db_name.clone(),
                                });
                            }
                        }
                    } else if info.action == USE_TENANT_ACTION_DEL {
                        if info.name.is_empty() {
                            tenants.clear();
                        }
                        tenants.remove(&info.name);
                    }
                    info.data.version
                }

                None => {
                    trace::error!("version change channel closed, watch task manager exit");
                    break;
                }
            };

            base_ver = min_num(mgr.watch_version.load(Ordering::Relaxed), version);
            mgr.watch_version.store(base_ver, Ordering::Relaxed);
        }
    }

    pub async fn watch_data_task(mgr: Arc<RemoteMetaManager>, base_ver: u64) {
        let tenants = mgr.watch_tenants.read().clone();

        let client_id = format!("watch.{}", mgr.config.node_id);
        let mut request = (
            client_id,
            mgr.config.cluster.name.clone(),
            tenants,
            base_ver,
        );

        let cluster_meta = mgr.config.cluster.meta_service_addr.clone().join(";");
        let client = MetaHttpClient::new(cluster_meta);
        loop {
            if let Ok(watch_data) = client.watch::<command::WatchData>(&request).await {
                if watch_data.full_sync {
                    let base_ver = mgr.process_full_sync().await;
                    mgr.watch_version.store(base_ver, Ordering::Relaxed);
                    request.3 = base_ver;
                    continue;
                }

                mgr.process_watch_data(&watch_data).await;
                mgr.watch_version
                    .store(watch_data.max_ver, Ordering::Relaxed);

                request.3 = watch_data.max_ver;
            } else {
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }

    pub async fn process_full_sync(&self) -> u64 {
        let base_ver;
        loop {
            if let Ok(ver) = self.admin.sync_all().await {
                base_ver = ver;
                break;
            } else {
                info!("sync data node failed");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }

        self.tenant_manager.clear().await;

        base_ver
    }

    pub async fn process_watch_data(&self, watch_data: &command::WatchData) {
        for entry in watch_data.entry_logs.iter() {
            if entry.tye == command::ENTRY_LOG_TYPE_NOP {
                continue;
            }

            let strs: Vec<&str> = entry.key.split('/').collect();
            let len = strs.len();
            if len < 2 || strs[1] != self.config.cluster.name {
                continue;
            }

            if len > 3 && strs[2] == key_path::TENANTS {
                let tenant_name = strs[3];
                if let Some(client) = self.tenant_manager.get_tenant_meta(tenant_name).await {
                    let _ = self.process_tenant_meta_data(client, entry).await;
                }
            } else if len == 4 && strs[2] == key_path::DATA_NODES {
                let _ = self.admin_meta().process_watch_log(entry).await;
            } else if len == 3 && strs[2] == key_path::AUTO_INCR_ID {
            } else if len == 4 && strs[2] == key_path::USERS {
            }
        }
    }

    async fn process_tenant_meta_data(
        &self,
        client: MetaClientRef,
        entry: &EntryLog,
    ) -> MetaResult<()> {
        let strs: Vec<&str> = entry.key.split('/').collect();

        let mut data = client.get_mut_data();

        let len = strs.len();
        if len == 8
            && strs[6] == key_path::SCHEMAS
            && strs[4] == key_path::DBS
            && strs[2] == key_path::TENANTS
        {
            let _tenant = strs[3];
            let db_name = strs[5];
            let tab_name = strs[7];
            if let Some(db) = data.dbs.get_mut(db_name) {
                if entry.tye == command::ENTRY_LOG_TYPE_SET {
                    if let Ok(info) = serde_json::from_str::<TableSchema>(&entry.val) {
                        db.tables.insert(tab_name.to_string(), info);
                    }
                } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                    db.tables.remove(tab_name);
                }
            }
        } else if len == 8
            && strs[6] == key_path::SUBS
            && strs[4] == key_path::DBS
            && strs[2] == key_path::TENANTS
        {
            let tenant = strs[3];
            let db_name = strs[5];
            let sub_name = strs[7];
            if let Some(db) = data.dbs.get_mut(db_name) {
                if entry.tye == command::ENTRY_LOG_TYPE_SET {
                    if let Ok(info) = serde_json::from_str::<SubscriptionInfo>(&entry.val) {
                        db.subs.insert(sub_name.to_string(), info.clone());

                        let _ = self.sub_change_sender.send(SubOperationLog {
                            info,
                            opt_type: OPERATION_TYPE_UPDATE,
                            tenant: tenant.to_string(),
                            db_name: db_name.to_string(),
                        });
                    }
                } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                    db.subs.remove(sub_name);

                    let _ = self.sub_change_sender.send(SubOperationLog {
                        info: SubscriptionInfo::default(),
                        opt_type: OPERATION_TYPE_DELETE,
                        tenant: tenant.to_string(),
                        db_name: db_name.to_string(),
                    });
                }
            }
        } else if len == 8
            && strs[6] == key_path::BUCKETS
            && strs[4] == key_path::DBS
            && strs[2] == key_path::TENANTS
        {
            let _tenant = strs[3];
            let db_name = strs[5];
            if let Some(db) = data.dbs.get_mut(db_name) {
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
}

// **[4]    /cluster_name/users/user ->
// **[3]    /cluster_name/auto_incr_id -> id

// **[4]    /cluster_name/data_nodes/node_id -> [NodeInfo] 集群、数据节点等信息

// **[6]    /cluster_name/tenants/tenant/roles/roles ->
// **[6]    /cluster_name/tenants/tenant/members/user_id ->
// **[6]    /cluster_name/tenants/tenant/users/name -> [UserInfo] 租户下用户信息、访问权限等       -- delete
// **[6]    /cluster_name/tenants/tenant/dbs/db_name -> [DatabaseInfo] db相关信息、保留策略等
// **[8]    /cluster_name/tenants/tenant/dbs/db_name/buckets/id -> [BucketInfo] bucket相关信息
// **[8]    /cluster_name/tenants/tenant/dbs/db_name/schemas/name -> [TskvTableSchema] schema相关信息
// **[8]  0 /     1      /   2   /   3  / 4 /    5  /   6   /  7
#[async_trait::async_trait]
impl MetaManager for RemoteMetaManager {
    fn node_id(&self) -> u64 {
        self.config.node_id
    }

    fn admin_meta(&self) -> AdminMetaRef {
        self.admin.clone()
    }

    fn user_manager(&self) -> UserManagerRef {
        self.user_manager.clone()
    }

    fn tenant_manager(&self) -> TenantManagerRef {
        self.tenant_manager.clone()
    }

    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        self.tenant_manager.expired_bucket().await
    }

    async fn subscribe_sub_change(&self) -> broadcast::Receiver<SubOperationLog> {
        self.sub_change_sender.subscribe()
    }

    async fn use_tenant(&self, name: &str) -> MetaResult<()> {
        if self.watch_tenants.read().contains(name) {
            return Ok(());
        }

        if self.watch_tenants.read().contains(&"".to_string()) {
            return Ok(());
        }

        if !name.is_empty() {
            self.tenant_manager()
                .tenant_meta(name)
                .await
                .ok_or_else(|| MetaError::TenantNotFound {
                    tenant: name.to_string(),
                })?;

            return Ok(());
        }

        let info = UseTenantInfo {
            name: name.to_string(),
            action: USE_TENANT_ACTION_ADD,
            data: TenantMetaData {
                version: u64::MAX,
                users: HashMap::new(),
                dbs: HashMap::new(),
            },
        };

        self.tenant_change_sender
            .send(info)
            .await
            .expect("use tenant channel failed");

        Ok(())
    }

    async fn user_with_privileges(
        &self,
        user_name: &str,
        tenant_name: Option<&str>,
    ) -> MetaResult<User> {
        let user_desc =
            self.user_manager
                .user(user_name)
                .await?
                .ok_or_else(|| MetaError::UserNotFound {
                    user: user_name.to_string(),
                })?;

        // admin user
        if user_desc.is_admin() {
            return Ok(admin_user(user_desc));
        }

        // common user & with tenant
        if let Some(tenant_name) = tenant_name {
            let client = self
                .tenant_manager
                .tenant_meta(tenant_name)
                .await
                .ok_or_else(|| MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                })?;

            let tenant_id = *client.tenant().id();
            let role = client.member_role(user_desc.id()).await?.ok_or_else(|| {
                MetaError::MemberNotFound {
                    member_name: user_desc.name().to_string(),
                    tenant_name: tenant_name.to_string(),
                }
            })?;

            let privileges = match role {
                TenantRoleIdentifier::System(sys_role) => sys_role.to_privileges(&tenant_id),
                TenantRoleIdentifier::Custom(ref role_name) => client
                    .custom_role(role_name)
                    .await?
                    .map(|e| e.to_privileges(&tenant_id))
                    .unwrap_or_default(),
            };

            return Ok(User::new(user_desc, privileges));
        }

        // common user & without tenant
        Ok(User::new(user_desc, Default::default()))
    }
}
