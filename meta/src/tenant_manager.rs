#![allow(dead_code, unused_imports, unused_variables)]
use std::{collections::HashMap, sync::Arc};

use models::{
    oid::{Identifier, Oid},
    schema::{Tenant, TenantOptions},
};
use parking_lot::RwLock;

use crate::error::{MetaError, MetaResult};
use crate::{
    client::MetaHttpClient,
    meta_client::{MetaClient, MetaClientRef, RemoteMetaClient, TenantManager},
    store::command::{
        self, META_REQUEST_FAILED, META_REQUEST_TENANT_EXIST, META_REQUEST_TENANT_NOT_FOUND,
    },
};

#[derive(Debug)]
pub struct RemoteTenantManager {
    client: MetaHttpClient,

    cluster_name: String,
    cluster_meta: String,
    node_id: u64,

    tenants: RwLock<HashMap<String, MetaClientRef>>,
}

impl RemoteTenantManager {
    pub fn new(cluster_name: String, cluster_meta: String, id: u64) -> Self {
        Self {
            client: MetaHttpClient::new(1, cluster_meta.clone()),
            cluster_name,
            cluster_meta,
            node_id: id,
            tenants: Default::default(),
        }
    }
}

impl TenantManager for RemoteTenantManager {
    fn create_tenant(&self, name: String, options: TenantOptions) -> MetaResult<MetaClientRef> {
        let req = command::WriteCommand::CreateTenant(self.cluster_name.clone(), name, options);

        match self.client.write::<command::CommonResp<Tenant>>(&req)? {
            command::CommonResp::Ok(tenant) => {
                let client: MetaClientRef = RemoteMetaClient::new(
                    self.cluster_name.clone(),
                    tenant,
                    self.cluster_meta.clone(),
                    self.node_id,
                );

                self.tenants
                    .write()
                    .insert(client.tenant().name().to_string(), client.clone());

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

    fn tenant(&self, name: &str) -> MetaResult<Option<Tenant>> {
        if let Some(client) = self.tenants.read().get(name) {
            return Ok(Some(client.tenant().clone()));
        }

        let req = command::ReadCommand::Tenant(self.cluster_name.clone(), name.to_string());

        match self
            .client
            .read::<command::CommonResp<Option<Tenant>>>(&req)?
        {
            command::CommonResp::Ok(data) => Ok(data),
            command::CommonResp::Err(status) => {
                // TODO improve response
                Err(MetaError::CommonError { msg: status.msg })
            }
        }
    }

    fn alter_tenant(&self, name: &str, options: TenantOptions) -> MetaResult<()> {
        let req = command::WriteCommand::AlterTenant(
            self.cluster_name.clone(),
            name.to_string(),
            options,
        );

        match self.client.write::<command::CommonResp<Tenant>>(&req)? {
            command::CommonResp::Ok(data) => {
                let client = RemoteMetaClient::new(
                    self.cluster_name.clone(),
                    data,
                    self.cluster_meta.clone(),
                    self.node_id,
                );
                self.tenants
                    .write()
                    .insert(client.tenant().name().to_string(), client);

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

    fn drop_tenant(&self, name: &str) -> MetaResult<bool> {
        if self.tenants.write().remove(name).is_some() {
            let req =
                command::WriteCommand::DropTenant(self.cluster_name.clone(), name.to_string());

            return match self.client.write::<command::CommonResp<bool>>(&req)? {
                command::CommonResp::Ok(e) => Ok(e),
                command::CommonResp::Err(status) => {
                    // TODO improve response
                    Err(MetaError::CommonError { msg: status.msg })
                }
            };
        }

        Ok(false)
    }

    fn tenant_meta(&self, tenant: &str) -> Option<MetaClientRef> {
        if let Some(client) = self.tenants.read().get(tenant) {
            return Some(client.clone());
        }

        self.tenant(tenant).ok().unwrap_or_default().map(|tenant| {
            RemoteMetaClient::new(
                self.cluster_name.clone(),
                tenant,
                self.cluster_meta.clone(),
                self.node_id,
            ) as MetaClientRef
        })
    }
}
