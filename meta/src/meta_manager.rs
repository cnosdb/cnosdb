#![allow(clippy::if_same_then_else)]

use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use config::ClusterConfig;
use models::auth::role::{TenantRoleIdentifier, UserRole};
use models::auth::user::User;
use models::meta_data::*;
use models::oid::Identifier;
use models::utils::min_num;
use tokio::sync::mpsc::{self, Receiver};
use trace::info;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::meta_admin::RemoteAdminMeta;
use crate::store::{command, key_path};
use crate::tenant_manager::RemoteTenantManager;
use crate::user_manager::RemoteUserManager;
use crate::{AdminMetaRef, TenantManagerRef, UserManagerRef};

#[async_trait]
pub trait MetaManager: Send + Sync + Debug {
    fn node_id(&self) -> u64;
    fn admin_meta(&self) -> AdminMetaRef;
    fn user_manager(&self) -> UserManagerRef;
    fn tenant_manager(&self) -> TenantManagerRef;
    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo>;
    async fn user_with_privileges(
        &self,
        user_name: &str,
        tenant_name: Option<&str>,
    ) -> MetaResult<User>;
}

#[derive(Debug)]
pub struct RemoteMetaManager {
    config: ClusterConfig,

    watch_version: Arc<AtomicU64>,

    admin: AdminMetaRef,
    user_manager: UserManagerRef,
    tenant_manager: TenantManagerRef,
}

impl RemoteMetaManager {
    pub async fn new(config: ClusterConfig) -> Arc<Self> {
        let (ver_change_sender, ver_change_receiver) = mpsc::channel(1024);

        let (admin, version) = RemoteAdminMeta::new(config.clone()).await.unwrap();
        let admin: AdminMetaRef = Arc::new(admin);

        let user_manager = Arc::new(RemoteUserManager::new(
            config.name.clone(),
            config.meta_service_addr.clone(),
        ));
        let tenant_manager = Arc::new(RemoteTenantManager::new(
            config.name.clone(),
            config.meta_service_addr.clone(),
            config.node_id,
            ver_change_sender,
        ));

        let manager = Arc::new(Self {
            config,
            admin,
            user_manager,
            tenant_manager,
            watch_version: Arc::new(AtomicU64::new(version)),
        });

        tokio::spawn(RemoteMetaManager::watch_task_manager(
            manager.clone(),
            ver_change_receiver,
        ));

        manager
    }

    pub async fn watch_task_manager(mgr: Arc<RemoteMetaManager>, mut recver: Receiver<u64>) {
        let mut base_ver = mgr.watch_version.load(Ordering::Relaxed);
        let mut task_handle: Option<tokio::task::JoinHandle<()>> = None;
        loop {
            if let Some(handle) = task_handle {
                handle.abort();
            }

            let handle = tokio::spawn(RemoteMetaManager::watch_data_task(mgr.clone(), base_ver));
            task_handle = Some(handle);

            //wait version change
            let version = match recver.recv().await {
                Some(val) => {
                    let mut min_ver = val;
                    while let Ok(val) = recver.try_recv() {
                        min_ver = min_num(val, min_ver);
                    }

                    min_ver
                }

                None => {
                    trace::error!("watch task manager exit");
                    break;
                }
            };

            base_ver = min_num(mgr.watch_version.load(Ordering::Relaxed), version);
            mgr.watch_version.store(base_ver, Ordering::Relaxed);
        }
    }

    pub async fn watch_data_task(mgr: Arc<RemoteMetaManager>, base_ver: u64) {
        let client_id = format!("{}.{}", mgr.config.tenant, mgr.config.node_id);

        let mut request = (
            client_id,
            mgr.config.name.clone(),
            mgr.config.tenant.clone(),
            base_ver,
        );

        let client = MetaHttpClient::new(1, mgr.config.meta_service_addr.clone());
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
            if len < 2 || strs[1] != self.config.name {
                continue;
            }

            if len > 3 && strs[2] == key_path::TENANTS {
                let tenant_name = strs[3];
                if let Some(client) = self.tenant_manager.get_tenant_meta(tenant_name).await {
                    let _ = client.process_watch_log(entry).await;
                }
            } else if len == 4 && strs[2] == key_path::DATA_NODES {
                let _ = self.admin_meta().process_watch_log(entry).await;
            } else if len == 3 && strs[2] == key_path::AUTO_INCR_ID {
            } else if len == 4 && strs[2] == key_path::USERS {
            }
        }
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
            return Ok(User::new(user_desc, UserRole::Dba.to_privileges()));
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
