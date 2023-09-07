use std::collections::HashMap;
use std::sync::Arc;

use config::RequestLimiterConfig;
use models::schema::Tenant;
use parking_lot::RwLock;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::limiter::{LocalRequestLimiter, RequestLimiter};
use crate::store::command::ReadCommand;

#[derive(Debug)]
pub struct LimiterManager {
    cluster_name: String,
    limiters: RwLock<HashMap<String, Arc<dyn RequestLimiter>>>,
    meta_http_client: MetaHttpClient,
}

impl LimiterManager {
    pub fn new(meta_http_client: MetaHttpClient, cluster_name: String) -> Self {
        Self {
            cluster_name,
            limiters: RwLock::new(HashMap::new()),
            meta_http_client,
        }
    }

    fn new_limiter(
        &self,
        tenant_name: &str,
        config: Option<&RequestLimiterConfig>,
    ) -> Arc<dyn RequestLimiter> {
        Arc::new(LocalRequestLimiter::new(
            self.cluster_name.as_str(),
            tenant_name,
            config,
            self.meta_http_client.clone(),
        ))
    }

    pub fn create_limiter(
        &self,
        tenant_name: &str,
        limiter_config: Option<&RequestLimiterConfig>,
    ) -> Arc<dyn RequestLimiter> {
        let limiter = self.new_limiter(tenant_name, limiter_config);
        self.insert_limiter(tenant_name.into(), limiter.clone());
        limiter
    }

    fn insert_limiter(&self, tenant: String, limiter: Arc<dyn RequestLimiter>) {
        self.limiters.write().insert(tenant, limiter);
    }

    pub fn remove_limiter(&self, tenant: &str) -> Option<Arc<dyn RequestLimiter>> {
        self.limiters.write().remove(tenant)
    }

    fn get_limiter(&self, tenant: &str) -> Option<Arc<dyn RequestLimiter>> {
        self.limiters.read().get(tenant).cloned()
    }

    pub async fn get_limiter_or_create(
        &self,
        tenant_name: &str,
    ) -> MetaResult<Arc<dyn RequestLimiter>> {
        match self.get_limiter(tenant_name) {
            None => {
                let command =
                    ReadCommand::Tenant(self.cluster_name.clone(), tenant_name.to_string());
                let tenant = self
                    .meta_http_client
                    .read::<Option<Tenant>>(&command)
                    .await?
                    .ok_or_else(|| MetaError::TenantNotFound {
                        tenant: tenant_name.into(),
                    })?;
                let limiter = self.new_limiter(tenant_name, tenant.options().request_config());
                self.insert_limiter(tenant_name.into(), limiter.clone());
                Ok(limiter)
            }
            Some(l) => Ok(l.clone()),
        }
    }
}
