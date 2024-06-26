#![allow(dead_code, clippy::if_same_then_else)]

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use config::tskv::Config;
use metrics::metric_register::MetricsRegister;
use models::auth::user::{admin_user, User, UserDesc, UserOptions};
use models::meta_data::*;
use models::node_info::NodeStatus;
use models::oid::{Identifier, Oid, UuidGenerator};
use models::schema::resource_info::{ResourceInfo, ResourceStatus};
use models::schema::tenant::{Tenant, TenantOptions};
use models::utils::{build_address_with_optional_addr, now_timestamp_secs};
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tonic::transport::{Channel, Endpoint};
use trace::error;
use tracing::info;

use super::meta_tenant::TenantMeta;
use super::MetaClientRef;
use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::limiter::limiter_factory::{LimiterFactory, LocalRequestLimiterFactory};
use crate::limiter::limiter_manager::{LimiterKey, LimiterManager};
use crate::limiter::{LimiterConfig, LimiterType, RequestLimiter};
use crate::store::command::{self, EntryLog};
use crate::store::key_path;

pub const USE_TENANT_ACTION_ADD: i32 = 1;
pub const USE_TENANT_ACTION_DEL: i32 = 2;

#[derive(Debug)]
enum TenantAction {
    Add,
    Del,
}

#[derive(Debug)]
struct UseTenantInfo {
    pub name: String,
    pub version: u64,
    pub action: TenantAction,
}

type ReceiverType = Arc<Mutex<Option<Receiver<MetaModifyType>>>>;
#[derive(Debug)]
pub struct AdminMeta {
    config: Config,
    client: MetaHttpClient,

    watch_version: AtomicU64,
    watch_tenants: RwLock<HashSet<String>>,
    watch_notify: Sender<UseTenantInfo>,

    users: RwLock<HashMap<String, UserDesc>>,
    conn_map: RwLock<HashMap<u64, Channel>>,
    data_nodes: RwLock<HashMap<u64, NodeInfo>>,

    tenants: RwLock<HashMap<String, Arc<TenantMeta>>>,
    limiters: Arc<LimiterManager>,

    resource_tx_rx: (Sender<MetaModifyType>, ReceiverType),
    metrics_register: Arc<MetricsRegister>,
}

impl AdminMeta {
    pub fn mock() -> Self {
        let (watch_notify, _) = mpsc::channel(1024);
        let client = MetaHttpClient::new("", Arc::new(MetricsRegister::default()));
        let config = Config::default();

        let limiters = LimiterManager::new(HashMap::new());
        let (tx, rx) = mpsc::channel::<MetaModifyType>(1024);

        Self {
            config,
            watch_notify,
            client,
            users: RwLock::new(HashMap::new()),
            conn_map: RwLock::new(HashMap::new()),
            data_nodes: RwLock::new(HashMap::new()),
            tenants: RwLock::new(HashMap::new()),
            limiters: Arc::new(limiters),

            watch_version: AtomicU64::new(0),
            watch_tenants: RwLock::new(HashSet::new()),
            resource_tx_rx: (tx, Arc::new(Mutex::new(Some(rx)))),
            metrics_register: Arc::new(MetricsRegister::default()),
        }
    }

    pub async fn new(config: Config, metrics_register: Arc<MetricsRegister>) -> Arc<Self> {
        let meta_service_addr = config.meta.service_addr.clone();
        let meta_url = meta_service_addr.join(";");
        let (watch_notify, receiver) = mpsc::channel(1024);

        let client = MetaHttpClient::new(&meta_url, metrics_register.clone());
        let limiters = Arc::new(LimiterManager::new({
            let mut map = HashMap::new();
            map.insert(
                LimiterType::Tenant,
                Arc::new(LocalRequestLimiterFactory::new(
                    config.global.cluster_name.clone(),
                    client.clone(),
                )) as Arc<dyn LimiterFactory>,
            );
            map
        }));
        let (tx, rx) = mpsc::channel::<MetaModifyType>(1024);

        let admin = Arc::new(Self {
            config,
            watch_notify,
            client,
            users: RwLock::new(HashMap::new()),
            conn_map: RwLock::new(HashMap::new()),
            data_nodes: RwLock::new(HashMap::new()),
            tenants: RwLock::new(HashMap::new()),
            limiters,
            watch_version: AtomicU64::new(0),
            watch_tenants: RwLock::new(HashSet::new()),
            resource_tx_rx: (tx, Arc::new(Mutex::new(Some(rx)))),
            metrics_register,
        });

        let base_ver = admin.sync_gobal_info().await.unwrap();
        admin.watch_version.store(base_ver, Ordering::Relaxed);

        tokio::spawn(AdminMeta::watch_task_manager(admin.clone(), receiver));
        tokio::spawn(AdminMeta::watch_meta_node_change(admin.clone()));

        admin
    }

    pub fn cluster(&self) -> String {
        self.config.global.cluster_name.clone()
    }

    pub fn node_id(&self) -> u64 {
        self.config.global.node_id
    }

    pub fn deployment_mode(&self) -> String {
        self.config.deployment.mode.clone()
    }

    fn meta_addrs(&self) -> String {
        self.config.meta.service_addr.join(";")
    }

    pub async fn meta_leader(&self) -> MetaResult<String> {
        self.client.meta_leader().await
    }

    pub fn sys_info() -> SysInfo {
        let mut info = SysInfo::default();

        if let Ok(val) = sys_info::disk_info() {
            info.disk_free = val.free;
        }

        if let Ok(val) = sys_info::mem_info() {
            info.mem_free = val.free;
        }

        if let Ok(val) = sys_info::loadavg() {
            info.cpu_load = val.one;
        }

        info
    }

    pub async fn node_info_by_id(&self, id: u64) -> MetaResult<NodeInfo> {
        if let Some(val) = self.data_nodes.read().get(&id) {
            return Ok(val.clone());
        }

        Err(MetaError::NotFoundNode { id })
    }

    pub async fn get_node_conn(&self, node_id: u64) -> MetaResult<Channel> {
        if let Some(val) = self.conn_map.read().get(&node_id) {
            return Ok(val.clone());
        }

        let info = self.node_info_by_id(node_id).await?;
        let connector = Endpoint::from_shared(format!("http://{}", info.grpc_addr.clone()))
            .map_err(|err| MetaError::ConnectServerError {
                addr: info.grpc_addr.clone(),
                msg: err.to_string(),
            })?;

        let channel = connector
            .connect()
            .await
            .map_err(|err| MetaError::ConnectServerError {
                addr: info.grpc_addr,
                msg: err.to_string(),
            })?;

        self.conn_map.write().insert(node_id, channel.clone());

        Ok(channel)
    }

    pub async fn retain_id(&self, count: u32) -> MetaResult<u32> {
        let req = command::WriteCommand::RetainID(self.config.global.cluster_name.clone(), count);
        let id = self.client.write::<u32>(&req).await?;

        Ok(id)
    }

    pub async fn sync_gobal_info(&self) -> MetaResult<u64> {
        let req = command::ReadCommand::DataNodes(self.config.global.cluster_name.clone());
        let (resp, version) = self.client.read::<(Vec<NodeInfo>, u64)>(&req).await?;
        {
            let mut nodes = self.data_nodes.write();
            nodes.clear();
            for item in resp.iter() {
                nodes.insert(item.id, item.clone());
            }
        }

        let req = command::ReadCommand::Users(self.cluster());
        let resp = self.client.read::<Vec<UserDesc>>(&req).await?;
        {
            let mut users = self.users.write();
            users.clear();
            for item in resp.iter() {
                users.insert(item.name().to_owned(), item.clone());
            }
        }

        Ok(version)
    }

    /******************** Watch Meta Data Change Begin *********************/
    pub async fn use_tenant(&self, name: &str) -> MetaResult<()> {
        if self.watch_tenants.read().contains(name) {
            return Ok(());
        }

        if self.watch_tenants.read().contains(&"".to_string()) {
            return Ok(());
        }

        if !name.is_empty() {
            self.tenant_meta(name)
                .await
                .ok_or_else(|| MetaError::TenantNotFound {
                    tenant: name.to_string(),
                })?;

            return Ok(());
        }

        let info = UseTenantInfo {
            name: name.to_string(),
            version: u64::MAX,
            action: TenantAction::Add,
        };

        let _ = self.watch_notify.send(info).await;

        Ok(())
    }

    async fn watch_meta_node_change(admin: Arc<AdminMeta>) {
        loop {
            let res = admin.client.watch_meta_membership().await;
            if let Ok(mut new_addrs) = res {
                let r_addrs = admin.client.addrs.read();
                let mut old_addr = (*r_addrs).clone();
                drop(r_addrs);
                old_addr.sort();
                new_addrs.sort();

                if !old_addr.eq(&new_addrs) {
                    admin.client.change_meta_membership(new_addrs.clone());
                    let w_tenants = admin.tenants.write();
                    w_tenants.values().for_each(|tenant_meta| {
                        tenant_meta.client.change_meta_membership(new_addrs.clone());
                    });
                }
            } else {
                info!("watch meta node change wrong {:?}", res);
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }

    async fn watch_task_manager(admin: Arc<AdminMeta>, mut receiver: Receiver<UseTenantInfo>) {
        let mut task_handle: Option<tokio::task::JoinHandle<()>>;

        loop {
            let handle = tokio::spawn(AdminMeta::watch_data_task(admin.clone()));
            task_handle = Some(handle);

            if let Some(info) = receiver.recv().await {
                if let Some(handle) = task_handle {
                    handle.abort();
                }

                let base_ver = admin
                    .watch_version
                    .fetch_min(info.version, Ordering::Relaxed);
                admin.watch_version.store(base_ver, Ordering::Relaxed);

                let mut tenants = admin.watch_tenants.write();
                if info.name.is_empty() {
                    tenants.clear();
                }

                match info.action {
                    TenantAction::Add => {
                        tenants.insert(info.name);
                    }
                    TenantAction::Del => {
                        tenants.remove(&info.name);
                    }
                }
            } else {
                trace::error!("channel closed, watch task manager exit");
                break;
            }
        }
    }

    pub async fn watch_data_task(admin: Arc<AdminMeta>) {
        let tenants = admin.watch_tenants.read().clone();
        let base_ver = admin.watch_version.load(Ordering::Relaxed);

        let client_id = format!("watch.{}", admin.node_id());
        let mut request = (client_id, admin.cluster(), tenants, base_ver);

        loop {
            let watch_rsp = admin.client.watch::<command::WatchData>(&request).await;
            if let Ok(watch_data) = watch_rsp {
                if watch_data.full_sync {
                    let base_ver = admin.process_full_sync().await;
                    admin.watch_version.store(base_ver, Ordering::Relaxed);
                    request.3 = base_ver;
                    continue;
                }

                admin.process_watch_data(&watch_data).await;
                admin
                    .watch_version
                    .store(watch_data.max_ver, Ordering::Relaxed);

                request.3 = watch_data.max_ver;
            } else {
                info!("watch response wrong {:?}", watch_rsp);
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            }
        }
    }

    pub async fn process_full_sync(&self) -> u64 {
        loop {
            if let Ok(base_ver) = self.sync_gobal_info().await {
                self.tenants.write().clear();
                return base_ver;
            } else {
                info!("sync all data node failed");
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        }
    }

    pub async fn process_watch_data(&self, watch_data: &command::WatchData) {
        for entry in watch_data.entry_logs.iter() {
            if entry.tye == command::ENTRY_LOG_TYPE_NOP {
                continue;
            }

            let strs: Vec<&str> = entry.key.split('/').collect();
            let len = strs.len();
            if len < 2 || strs[1] != self.config.global.cluster_name {
                continue;
            }

            if len > 3 && strs[2] == key_path::TENANTS {
                let tenant_name = strs[3];
                let opt_client = self.tenants.read().get(tenant_name).cloned();
                let _ = self.limiters.process_watch_log(tenant_name, entry).await;
                if let Some(client) = opt_client {
                    let _ = client.process_watch_log(entry).await;
                }
            } else if len == 3 && strs[2] == key_path::AUTO_INCR_ID {
            } else if len == 4
                && (strs[2] == key_path::USERS
                    || strs[2] == key_path::RESOURCE_INFOS
                    || strs[2] == key_path::DATA_NODES
                    || strs[2] == key_path::DATA_NODES_METRICS)
            {
                let _ = self.process_watch_log(entry).await;
            }
        }
    }

    pub async fn process_watch_log(&self, entry: &EntryLog) -> MetaResult<()> {
        let strs: Vec<&str> = entry.key.split('/').collect();

        let len = strs.len();
        if len == 4 && strs[2] == key_path::DATA_NODES {
            if let Ok(node_id) = serde_json::from_str::<u64>(strs[3]) {
                if entry.tye == command::ENTRY_LOG_TYPE_SET {
                    if let Ok(info) = serde_json::from_str::<NodeInfo>(&entry.val) {
                        self.data_nodes.write().insert(node_id, info);
                    }
                } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                    self.data_nodes.write().remove(&node_id);
                    self.conn_map.write().remove(&node_id);
                }
            }
        } else if len == 4 && strs[2] == key_path::USERS {
            if entry.tye == command::ENTRY_LOG_TYPE_SET {
                if let Ok(user) = serde_json::from_str::<UserDesc>(&entry.val) {
                    self.users.write().insert(strs[3].to_owned(), user);
                }
            } else if entry.tye == command::ENTRY_LOG_TYPE_DEL {
                self.users.write().remove(strs[3]);
            }
        } else if len == 4
            && strs[2] == key_path::RESOURCE_INFOS
            && entry.tye == command::ENTRY_LOG_TYPE_SET
        {
            if let Ok(res_info) = serde_json::from_str::<ResourceInfo>(&entry.val) {
                if *res_info.get_status() == ResourceStatus::Schedule
                    || *res_info.get_status() == ResourceStatus::Executing
                    || *res_info.get_status() == ResourceStatus::Failed
                    || *res_info.get_status() == ResourceStatus::Cancel
                {
                    let _ = self
                        .resource_tx_rx
                        .0
                        .send(MetaModifyType::ResourceInfo(Box::new(res_info)))
                        .await;
                }
            }
        } else if len == 4 && strs[2] == key_path::DATA_NODES_METRICS {
            if let Ok(node_metrics) = serde_json::from_str::<NodeMetrics>(&entry.val) {
                if node_metrics.status == NodeStatus::Unreachable {
                    let _ = self
                        .resource_tx_rx
                        .0
                        .send(MetaModifyType::NodeMetrics(node_metrics))
                        .await;
                }
            }
        }

        Ok(())
    }

    // **[3]    /cluster_name/auto_incr_id -> id
    // **[4]    /cluster_name/users/name -> [UserDesc]
    // **[4]    /cluster_name/data_nodes/node_id -> [NodeInfo] 集群、数据节点等信息

    // **[6]    /cluster_name/tenants/tenant/roles/name -> [CustomTenantRole<Oid>]
    // **[6]    /cluster_name/tenants/tenant/members/oid -> [TenantRoleIdentifier]
    // **[6]    /cluster_name/tenants/tenant/dbs/db_name -> [DatabaseInfo] db相关信息、保留策略等
    // **[8]    /cluster_name/tenants/tenant/dbs/db_name/buckets/id -> [BucketInfo] bucket相关信息
    // **[8]    /cluster_name/tenants/tenant/dbs/db_name/schemas/name -> [TskvTableSchema] schema相关信息
    // **[8]  0 /     1      /   2   /   3  / 4 /    5  /   6   /  7

    /******************** Watch Meta Data Change End *********************/

    /******************** Data Node Operation Begin *********************/
    pub async fn add_data_node(&self) -> MetaResult<()> {
        let grpc_addr = build_address_with_optional_addr(
            &self.config.global.host,
            self.config.service.grpc_listen_port,
        );

        let node = NodeInfo {
            id: self.config.global.node_id,
            grpc_addr,
        };

        let cluster_name = self.config.global.cluster_name.clone();
        let req = command::WriteCommand::AddDataNode(cluster_name, node.clone());
        self.client.write::<()>(&req).await?;
        self.report_node_metrics().await?;

        self.data_nodes.write().insert(node.id, node);

        Ok(())
    }

    pub async fn data_nodes(&self) -> Vec<NodeInfo> {
        let mut nodes = vec![];
        for (_, val) in self.data_nodes.read().iter() {
            nodes.push(val.clone())
        }

        nodes
    }

    pub async fn report_node_metrics(&self) -> MetaResult<()> {
        let disk_free = match get_disk_info(&self.config.storage.path) {
            Ok(size) => size,
            Err(e) => {
                error!(
                    "Failed to get disk info '{}': {}",
                    self.config.storage.path, e
                );
                0
            }
        };

        let mut status = NodeStatus::default();
        if disk_free < self.config.storage.reserve_space {
            status = NodeStatus::NoDiskSpace;
        }

        let node_metrics = NodeMetrics {
            id: self.config.global.node_id,
            disk_free,
            time: now_timestamp_secs(),
            status,
        };

        let req = command::WriteCommand::ReportNodeMetrics(
            self.config.global.cluster_name.clone(),
            node_metrics.clone(),
        );

        self.client.write::<()>(&req).await
    }
    /******************** Data Node Operation End *********************/

    /******************** User Operation Begin *********************/
    pub async fn create_user(
        &self,
        name: String,
        options: UserOptions,
        is_admin: bool,
    ) -> MetaResult<Oid> {
        let oid = UuidGenerator::default().next_id();
        let user_desc = UserDesc::new(oid, name.clone(), options.clone(), is_admin);
        let req = command::WriteCommand::CreateUser(self.cluster(), user_desc);

        self.client.write::<()>(&req).await?;

        Ok(oid)
    }

    pub async fn user(&self, name: &str) -> MetaResult<Option<UserDesc>> {
        let req = command::ReadCommand::User(self.cluster(), name.to_string());

        self.client.read::<Option<UserDesc>>(&req).await
    }

    pub async fn users(&self) -> MetaResult<Vec<UserDesc>> {
        let req = command::ReadCommand::Users(self.cluster());

        self.client.read::<Vec<UserDesc>>(&req).await
    }

    pub async fn alter_user(&self, name: &str, options: UserOptions) -> MetaResult<()> {
        let req = command::WriteCommand::AlterUser(self.cluster(), name.to_string(), options);

        self.client.write::<()>(&req).await
    }

    pub async fn drop_user(&self, name: &str) -> MetaResult<bool> {
        let req = command::WriteCommand::DropUser(self.cluster(), name.to_string());

        self.client.write::<bool>(&req).await
    }

    pub async fn rename_user(&self, old_name: &str, new_name: String) -> MetaResult<()> {
        let req = command::WriteCommand::RenameUser(self.cluster(), old_name.to_string(), new_name);

        self.client.write::<()>(&req).await
    }

    pub async fn user_with_privileges(
        &self,
        user_name: &str,
        tenant_name: &str,
    ) -> MetaResult<User> {
        let user_desc = {
            let cache = self.users.read().get(user_name).cloned();
            if let Some(user) = cache {
                user.clone()
            } else {
                self.user(user_name)
                    .await?
                    .ok_or_else(|| MetaError::UserNotFound {
                        user: user_name.to_string(),
                    })?
            }
        };

        let client =
            self.tenant_meta(tenant_name)
                .await
                .ok_or_else(|| MetaError::TenantNotFound {
                    tenant: tenant_name.to_string(),
                })?;

        let role = client.member_role(user_desc.id(), true).await?;

        let user = if user_desc.is_admin() {
            admin_user(user_desc, role)
        } else {
            let privileges = client.user_privileges(&user_desc).await?;
            User::new(user_desc, privileges, role)
        };

        Ok(user)
    }

    /******************** User Operation End *********************/

    /******************** Tenant Limiter Operation Begin *********************/
    pub async fn create_tenant_meta(&self, tenant_info: Tenant) -> MetaResult<MetaClientRef> {
        let tenant_name = tenant_info.name().to_string();

        let limiter_key = LimiterKey(LimiterType::Tenant, tenant_name.clone());
        let config = LimiterConfig::TenantRequestLimiterConfig {
            tenant: tenant_name.clone(),
            config: Box::new(tenant_info.options().request_config().cloned()),
        };

        self.limiters.create_limiter(limiter_key, config).await?;

        let client = TenantMeta::new(
            self.cluster(),
            tenant_info,
            self.meta_addrs(),
            self.metrics_register.clone(),
        )
        .await?;

        self.tenants
            .write()
            .insert(tenant_name.clone(), client.clone());

        let info = UseTenantInfo {
            name: tenant_name,
            version: client.version().await,
            action: TenantAction::Add,
        };
        let _ = self.watch_notify.send(info).await;

        Ok(client)
    }

    pub async fn create_tenant(
        &self,
        name: String,
        options: TenantOptions,
    ) -> MetaResult<MetaClientRef> {
        let oid = UuidGenerator::default().next_id();
        let tenant = Tenant::new(oid, name.to_string(), options.clone());
        let req = command::WriteCommand::CreateTenant(self.cluster(), tenant.clone());

        self.client.write::<()>(&req).await?;
        let meta_client = self.create_tenant_meta(tenant).await?;
        Ok(meta_client)
    }

    pub async fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>> {
        if let Some(client) = self.tenants.read().get(name) {
            return Ok(Some(client.tenant().clone()));
        }

        let req = command::ReadCommand::Tenant(self.cluster(), name.to_string(), true);

        self.client.read::<Option<Tenant>>(&req).await
    }

    // for drop/recover tenant and restart case
    pub async fn tenant_for_special(&self, name: &str) -> MetaResult<Option<Tenant>> {
        if let Some(client) = self.tenants.read().get(name) {
            return Ok(Some(client.tenant().clone()));
        }

        let req = command::ReadCommand::Tenant(self.cluster(), name.to_string(), false);
        self.client.read::<Option<Tenant>>(&req).await
    }

    pub async fn tenants(&self) -> MetaResult<Vec<Tenant>> {
        let req = command::ReadCommand::Tenants(self.cluster());
        self.client.read::<Vec<Tenant>>(&req).await
    }

    pub async fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()> {
        let req = command::WriteCommand::AlterTenant(self.cluster(), name.to_string(), options);

        let tenant = self.client.write::<Tenant>(&req).await?;

        let tenant_meta = self.create_tenant_meta(tenant).await?;

        self.tenants.write().insert(name.to_string(), tenant_meta);

        Ok(())
    }

    pub async fn set_tenant_is_hidden(&self, name: &str, tenant_is_hidden: bool) -> MetaResult<()> {
        let req = command::WriteCommand::SetTenantIsHidden(
            self.cluster(),
            name.to_string(),
            tenant_is_hidden,
        );
        let tenant = self.client.write::<Tenant>(&req).await?;
        let tenant_meta = self.create_tenant_meta(tenant).await?;
        self.tenants.write().insert(name.to_string(), tenant_meta);
        Ok(())
    }

    pub async fn drop_tenant(&self, name: &str) -> MetaResult<bool> {
        // notice: can't move it to if clause
        let exist = self.tenants.write().remove(name).is_some();
        if exist {
            let req = command::WriteCommand::DropTenant(self.cluster(), name.to_string());

            self.client.write::<()>(&req).await?;
            let limiter_key = LimiterKey::tenant_key(name.to_string());
            self.limiters.remove_limiter(&limiter_key);
        }

        Ok(exist)
    }

    pub async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        if let Some(client) = self.tenants.read().get(tenant) {
            return Some(client.clone());
        }

        if let Ok(Some(tenant_info)) = self.tenant(tenant).await {
            return self.create_tenant_meta(tenant_info).await.ok();
        }

        None
    }

    pub fn try_change_local_vnode_status(&self, tenant: &str, id: u32, status: VnodeStatus) {
        if let Some(client) = self.tenants.read().get(tenant) {
            info!("local change vnode status {} {:?}", id, status);
            let _ = client.change_local_vnode_status(id, status);
        }
    }

    pub async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        let mut list = vec![];
        for (_key, val) in self.tenants.write().iter() {
            list.append(&mut val.expired_bucket());
        }
        list
    }

    pub async fn limiter(&self, tenant: &str) -> MetaResult<Arc<dyn RequestLimiter>> {
        let key = LimiterKey(LimiterType::Tenant, tenant.to_string());
        self.limiters.get_limiter_or_create(key).await
    }

    /******************** Tenant Limiter Operation End *********************/

    pub async fn write_resourceinfo(&self, name: &str, res_info: ResourceInfo) -> MetaResult<()> {
        let req = command::WriteCommand::ResourceInfo(self.cluster(), name.to_string(), res_info);

        self.client.write::<()>(&req).await?;

        Ok(())
    }

    pub async fn read_resourceinfo_by_name(&self, name: &str) -> MetaResult<Option<ResourceInfo>> {
        let req = command::ReadCommand::ResourceInfo(self.cluster(), name.to_string());

        self.client.read::<Option<ResourceInfo>>(&req).await
    }

    pub async fn read_resourceinfos(&self) -> MetaResult<Vec<ResourceInfo>> {
        let req = command::ReadCommand::ResourceInfos(self.cluster());

        self.client.read::<Vec<ResourceInfo>>(&req).await
    }

    pub async fn write_resourceinfos_mark(&self, node_id: NodeId, is_lock: bool) -> MetaResult<()> {
        let req = command::WriteCommand::ResourceInfosMark(self.cluster(), node_id, is_lock);

        self.client.write::<()>(&req).await?;

        Ok(())
    }

    pub async fn read_resourceinfos_mark(&self) -> MetaResult<(NodeId, bool)> {
        let req = command::ReadCommand::ResourceInfosMark(self.cluster());

        self.client.read::<(NodeId, bool)>(&req).await
    }

    pub fn take_resourceinfo_rx(&self) -> Option<Receiver<MetaModifyType>> {
        self.resource_tx_rx.1.lock().take()
    }

    pub async fn write_queryinfo(
        &self,
        node_id: NodeId,
        query_id: u64,
        query_info: Vec<u8>,
    ) -> MetaResult<()> {
        let req =
            command::WriteCommand::WriteQueryInfo(self.cluster(), node_id, query_id, query_info);

        self.client.write::<()>(&req).await?;

        Ok(())
    }

    pub async fn read_queryinfos(&self, node_id: NodeId) -> MetaResult<Vec<Vec<u8>>> {
        let req = command::ReadCommand::ReadQueryInfos(self.cluster(), node_id);

        self.client.read::<Vec<Vec<u8>>>(&req).await
    }

    pub async fn remove_queryinfo(&self, node_id: NodeId, query_id: u64) -> MetaResult<()> {
        let req = command::WriteCommand::RemoveQueryInfo(self.cluster(), node_id, query_id);

        self.client.write::<()>(&req).await
    }
}
