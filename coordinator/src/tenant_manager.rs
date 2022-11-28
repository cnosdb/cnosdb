use std::{collections::HashMap, sync::Arc};

use models::{
    oid::{Identifier, Oid},
    schema::{Tenant, TenantOptions},
};
use parking_lot::RwLock;

use crate::meta_client::{MetaClientRef, MetaError, MetaResult, RemoteMetaClient, TenantManager};

#[derive(Debug, Default)]
pub struct RemoteTenantManager {
    cluster_name: String,
    cluster_meta: String,

    tenants: RwLock<HashMap<String, MetaClientRef>>,
}

impl RemoteTenantManager {
    pub fn new(cluster_name: String, cluster_meta: String) -> Self {
        Self {
            cluster_name,
            cluster_meta,
            tenants: Default::default(),
        }
    }
}

impl TenantManager for RemoteTenantManager {
    fn create_tenant(&self, name: String, options: TenantOptions) -> MetaResult<MetaClientRef> {
        // TODO 元数据库中创建tenant，获取Tenant，此处暂时直接构造一个
        let tenant = Tenant::new(0_u128, name, options);
        let client: MetaClientRef = Arc::new(RemoteMetaClient::new(
            self.cluster_name.clone(),
            tenant,
            self.cluster_meta.clone(),
        ));

        self.tenants
            .write()
            .insert(client.tenant().name().to_string(), client.clone());

        Ok(client)
    }

    fn tenant(&self, name: &str) -> MetaResult<Tenant> {
        self.tenants
            .read()
            .get(name)
            .map(|e| e.tenant().clone())
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: name.to_string(),
            })
    }

    fn alter_tenant(&self, tenant_id: Oid, options: TenantOptions) -> MetaResult<()> {
        // TODO
        Ok(())
    }

    fn drop_tenant(&self, name: &str) -> MetaResult<()> {
        // TODO 元数据库中删除
        self.tenants
            .write()
            .remove(name)
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: name.to_string(),
            })?;
        Ok(())
    }

    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        if let Some(client) = self.tenants.read().get(tenant) {
            return Some(client.clone());
        }

        // TODO 临时从meta获取
        let tenant = Tenant::new(0_u128, "cnosdb".to_string(), TenantOptions::default());
        let client: MetaClientRef = Arc::new(RemoteMetaClient::new(
            self.cluster_name.clone(),
            tenant,
            self.cluster_meta.clone(),
        ));

        self.tenants
            .write()
            .insert(client.tenant().name().to_string(), client.clone());

        return Some(client);
    }
}
