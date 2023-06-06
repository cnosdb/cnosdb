#![allow(dead_code, clippy::collapsible_match)]
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use models::meta_data::ExpiredBucketInfo;
use models::oid::Identifier;
use models::schema::{Tenant, TenantOptions};
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

use crate::client::MetaHttpClient;
use crate::error::MetaResult;
use crate::limiter::{LocalRequestLimiter, NoneLimiter, RequestLimiter};
use crate::model::meta_client::RemoteMetaClient;
use crate::model::{MetaClient, MetaClientRef, TenantManager};
use crate::store::command;

pub const USE_TENANT_ACTION_ADD: i32 = 1;
pub const USE_TENANT_ACTION_DEL: i32 = 2;
#[derive(Debug, Clone)]
pub struct UseTenantInfo {
    pub name: String,
    pub version: u64,
    pub action: i32, //1: add, 2: del
}

#[derive(Debug)]
pub struct RemoteTenantManager {
    client: MetaHttpClient,

    cluster_name: String,
    cluster_meta: String,
    node_id: u64,
    tenant_change_sender: Sender<UseTenantInfo>,

    tenants: RwLock<HashMap<String, MetaClientRef>>,
    limiters: RwLock<HashMap<String, Arc<dyn RequestLimiter>>>,
}

impl RemoteTenantManager {
    pub fn new(
        cluster_name: String,
        cluster_meta: String,
        id: u64,
        tenant_change_sender: Sender<UseTenantInfo>,
    ) -> Self {
        Self {
            tenant_change_sender,
            client: MetaHttpClient::new(cluster_meta.clone()),
            cluster_name,
            cluster_meta,
            node_id: id,
            limiters: Default::default(),
            tenants: Default::default(),
        }
    }

    async fn create_tenant_meta(&self, tenant_info: Tenant) -> MetaResult<MetaClientRef> {
        let option = tenant_info.options().clone();
        let tenant_name = tenant_info.name().to_string();

        let client = RemoteMetaClient::new(
            self.cluster_name.clone(),
            tenant_info,
            self.cluster_meta.clone(),
            self.node_id,
        )
        .await?;

        self.tenants
            .write()
            .await
            .insert(tenant_name.clone(), client.clone());

        let limiter = self.new_limiter(&self.cluster_name, &tenant_name, &option);
        self.limiters
            .write()
            .await
            .insert(tenant_name.clone(), limiter);

        let info = UseTenantInfo {
            name: tenant_name,
            version: client.version().await,
            action: USE_TENANT_ACTION_ADD,
        };
        let _ = self.tenant_change_sender.send(info).await;

        Ok(client)
    }

    pub fn new_limiter(
        &self,
        _cluster_name: &str,
        tenant_name: &str,
        options: &TenantOptions,
    ) -> Arc<dyn RequestLimiter> {
        match options.request_config() {
            Some(config) => Arc::new(LocalRequestLimiter::new(
                self.cluster_name.as_str(),
                tenant_name,
                config,
                self.client.clone(),
            )),
            None => Arc::new(NoneLimiter {}),
        }
    }
}

#[async_trait::async_trait]
impl TenantManager for RemoteTenantManager {
    async fn clear(&self) {
        self.tenants.write().await.clear();
    }

    async fn create_tenant(
        &self,
        name: String,
        options: TenantOptions,
    ) -> MetaResult<MetaClientRef> {
        let limiter = self.new_limiter(&self.cluster_name, &name, &options);
        let req =
            command::WriteCommand::CreateTenant(self.cluster_name.clone(), name.clone(), options);

        let tenant = self.client.write::<Tenant>(&req).await?;
        let meta_client = self.create_tenant_meta(tenant).await;
        self.limiters
            .write()
            .await
            .insert(name.to_string(), limiter);
        Ok(meta_client?)
    }

    async fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>> {
        if let Some(client) = self.tenants.read().await.get(name) {
            return Ok(Some(client.tenant().clone()));
        }

        let req = command::ReadCommand::Tenant(self.cluster_name.clone(), name.to_string());
        self.client.read::<Option<Tenant>>(&req).await
    }

    async fn tenants(&self) -> MetaResult<Vec<Tenant>> {
        let req = command::ReadCommand::Tenants(self.cluster_name.clone());
        self.client.read::<Vec<Tenant>>(&req).await
    }

    async fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()> {
        let limiter = self.new_limiter(&self.cluster_name, name, &options);

        let req = command::WriteCommand::AlterTenant(
            self.cluster_name.clone(),
            name.to_string(),
            options,
        );

        let tenant = self.client.write::<Tenant>(&req).await?;

        let tenant_meta = self.create_tenant_meta(tenant).await?;
        self.limiters
            .write()
            .await
            .insert(name.to_string(), limiter);
        self.tenants
            .write()
            .await
            .insert(name.to_string(), tenant_meta);
        Ok(())
    }

    async fn drop_tenant(&self, name: &str) -> MetaResult<bool> {
        if self.tenants.write().await.remove(name).is_some() {
            let req =
                command::WriteCommand::DropTenant(self.cluster_name.clone(), name.to_string());

            let exist = self.client.write::<bool>(&req).await?;
            self.limiters.write().await.remove(name);

            Ok(exist)
        } else {
            Ok(false)
        }
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        if let Some(client) = self.tenants.read().await.get(tenant) {
            return Some(client.clone());
        }

        let _tenant_name = tenant.to_string();
        if let Ok(tenant_opt) = self.tenant(tenant).await {
            if let Some(tenant_info) = tenant_opt {
                return self.create_tenant_meta(tenant_info).await.ok();
            }
        }

        None
    }

    async fn get_tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        if let Some(client) = self.tenants.read().await.get(tenant) {
            return Some(client.clone());
        }

        None
    }

    async fn expired_bucket(&self) -> Vec<ExpiredBucketInfo> {
        let mut list = vec![];
        for (_key, val) in self.tenants.write().await.iter() {
            list.append(&mut val.expired_bucket());
        }
        list
    }

    async fn limiter(&self, tenant: &str) -> Arc<dyn RequestLimiter> {
        match self.limiters.read().await.get(tenant) {
            Some(limiter) => limiter.clone(),
            None => Arc::new(NoneLimiter),
        }
    }
}
