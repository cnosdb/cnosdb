#![allow(dead_code, unused_imports, unused_variables, clippy::collapsible_match)]
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use config::TenantLimiterConfig;
use models::meta_data::ExpiredBucketInfo;
use models::oid::{Identifier, Oid};
use models::schema::{Tenant, TenantOptions};
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use trace::info;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::limiter::local_request_limiter::{LocalBucketRequest, LocalBucketResponse};
use crate::limiter::{LocalRequestLimiter, NoneLimiter, RequestLimiter};
use crate::meta_client::{MetaClient, RemoteMetaClient};
use crate::store::command::{
    self, WriteCommand, META_REQUEST_FAILED, META_REQUEST_TENANT_EXIST,
    META_REQUEST_TENANT_NOT_FOUND,
};
use crate::MetaClientRef;

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

#[derive(Debug)]
pub struct RemoteTenantManager {
    client: MetaHttpClient,

    cluster_name: String,
    cluster_meta: String,
    node_id: u64,
    ver_sender: Sender<u64>,

    tenants: RwLock<HashMap<String, MetaClientRef>>,
    limiters: RwLock<HashMap<String, Arc<dyn RequestLimiter>>>,
}

impl RemoteTenantManager {
    pub fn new(
        cluster_name: String,
        cluster_meta: String,
        id: u64,
        ver_sender: Sender<u64>,
    ) -> Self {
        Self {
            ver_sender,
            client: MetaHttpClient::new(1, cluster_meta.clone()),
            cluster_name,
            cluster_meta,
            node_id: id,
            limiters: Default::default(),
            tenants: Default::default(),
        }
    }

    pub fn new_limiter(
        &self,
        cluster_name: &str,
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
        let req = command::WriteCommand::CreateTenant(self.cluster_name.clone(), name, options);

        match self
            .client
            .write::<command::CommonResp<Tenant>>(&req)
            .await?
        {
            command::CommonResp::Ok(tenant) => {
                let client: MetaClientRef = RemoteMetaClient::new(
                    self.cluster_name.clone(),
                    tenant,
                    self.cluster_meta.clone(),
                    self.node_id,
                )
                .await?;

                self.tenants
                    .write()
                    .await
                    .insert(client.tenant().name().to_string(), client.clone());
                self.limiters
                    .write()
                    .await
                    .insert(client.tenant().name().to_string(), limiter);

                Ok(client)
            }
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_TENANT_EXIST {
                    Err(MetaError::TenantAlreadyExists { tenant: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>> {
        if let Some(client) = self.tenants.read().await.get(name) {
            return Ok(Some(client.tenant().clone()));
        }

        let req = command::ReadCommand::Tenant(self.cluster_name.clone(), name.to_string());

        match self
            .client
            .read::<command::CommonResp<Option<Tenant>>>(&req)
            .await?
        {
            command::CommonResp::Ok(data) => Ok(data),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    async fn tenants(&self) -> MetaResult<Vec<Tenant>> {
        let req = command::ReadCommand::Tenants(self.cluster_name.clone());
        match self
            .client
            .read::<command::CommonResp<Vec<Tenant>>>(&req)
            .await?
        {
            command::CommonResp::Ok(data) => Ok(data),
            command::CommonResp::Err(status) => Err(MetaError::CommonError { msg: status.msg }),
        }
    }

    async fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()> {
        let limiter = self.new_limiter(&self.cluster_name, name, &options);

        let req = command::WriteCommand::AlterTenant(
            self.cluster_name.clone(),
            name.to_string(),
            options,
        );

        match self
            .client
            .write::<command::CommonResp<Tenant>>(&req)
            .await?
        {
            command::CommonResp::Ok(data) => {
                let client = RemoteMetaClient::new(
                    self.cluster_name.clone(),
                    data,
                    self.cluster_meta.clone(),
                    self.node_id,
                )
                .await?;
                self.tenants
                    .write()
                    .await
                    .insert(client.tenant().name().to_string(), client);

                self.limiters
                    .write()
                    .await
                    .insert(name.to_string(), limiter);
                Ok(())
            }
            command::CommonResp::Err(status) => {
                // TODO improve response
                if status.code == META_REQUEST_TENANT_NOT_FOUND {
                    Err(MetaError::TenantNotFound { tenant: status.msg })
                } else {
                    Err(MetaError::CommonError { msg: status.msg })
                }
            }
        }
    }

    async fn drop_tenant(&self, name: &str) -> MetaResult<bool> {
        if self.tenants.write().await.remove(name).is_some() {
            let req =
                command::WriteCommand::DropTenant(self.cluster_name.clone(), name.to_string());

            return match self.client.write::<command::CommonResp<bool>>(&req).await? {
                command::CommonResp::Ok(e) => {
                    self.limiters.write().await.remove(name);
                    Ok(e)
                }
                command::CommonResp::Err(status) => {
                    // TODO improve response
                    Err(MetaError::CommonError { msg: status.msg })
                }
            };
        }

        Ok(false)
    }

    async fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        if let Some(client) = self.tenants.read().await.get(tenant) {
            return Some(client.clone());
        }

        let tenant_name = tenant.to_string();
        if let Ok(tenant_opt) = self.tenant(tenant).await {
            if let Some(tenant_info) = tenant_opt {
                if let Ok(client) = RemoteMetaClient::new(
                    self.cluster_name.clone(),
                    tenant_info,
                    self.cluster_meta.clone(),
                    self.node_id,
                )
                .await
                {
                    let mut tenants = self.tenants.write().await;
                    if let Some(client) = tenants.get(&tenant_name) {
                        return Some(client.clone());
                    } else {
                        tenants.insert(tenant_name, client.clone());

                        let _ = self.ver_sender.send(client.version().await).await;

                        return Some(client);
                    }
                }
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
        for (key, val) in self.tenants.write().await.iter() {
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
