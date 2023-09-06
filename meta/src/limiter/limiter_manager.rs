use std::collections::HashMap;
use std::sync::Arc;

use models::schema::Tenant;
use parking_lot::RwLock;

use crate::error::{MetaError, MetaResult};
use crate::limiter::limiter_factory::LimiterFactory;
use crate::limiter::{LimiterConfig, LimiterType, RequestLimiter};
use crate::store::command::{EntryLog, ENTRY_LOG_TYPE_DEL, ENTRY_LOG_TYPE_NOP, ENTRY_LOG_TYPE_SET};

#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct LimiterKey(pub LimiterType, pub String);
impl LimiterKey {
    pub fn tenant_key(tenant: String) -> Self {
        LimiterKey(LimiterType::Tenant, tenant)
    }
}

#[derive(Debug)]
pub struct LimiterManager {
    limiters: RwLock<HashMap<LimiterKey, Arc<dyn RequestLimiter>>>,
    factories: HashMap<LimiterType, Arc<dyn LimiterFactory>>,
}
unsafe impl Send for LimiterManager {}
unsafe impl Sync for LimiterManager {}

impl LimiterManager {
    pub fn new(factories: HashMap<LimiterType, Arc<dyn LimiterFactory>>) -> Self {
        Self {
            limiters: RwLock::new(HashMap::new()),
            factories,
        }
    }

    pub async fn create_limiter(
        &self,
        key: LimiterKey,
        config: LimiterConfig,
    ) -> MetaResult<Arc<dyn RequestLimiter>> {
        let limiter_type = key.0;
        let factory =
            self.factories
                .get(&limiter_type)
                .ok_or_else(|| MetaError::LimiterCreate {
                    msg: format!("couldn't found factory of {:?}", limiter_type),
                })?;
        let limiter = factory.create_limiter(config).await?;
        self.insert_limiter(key, limiter.clone());
        Ok(limiter)
    }

    fn insert_limiter(&self, key: LimiterKey, limiter: Arc<dyn RequestLimiter>) {
        self.limiters.write().insert(key, limiter);
    }

    pub fn remove_limiter(&self, key: &LimiterKey) -> Option<Arc<dyn RequestLimiter>> {
        self.limiters.write().remove(key)
    }

    fn get_limiter(&self, key: &LimiterKey) -> Option<Arc<dyn RequestLimiter>> {
        self.limiters.read().get(key).cloned()
    }

    pub async fn get_limiter_or_create(
        &self,
        key: LimiterKey,
    ) -> MetaResult<Arc<dyn RequestLimiter>> {
        match self.get_limiter(&key) {
            None => {
                let factories =
                    self.factories
                        .get(&key.0)
                        .ok_or_else(|| MetaError::LimiterCreate {
                            msg: format!("couldn't found factory of {:?}", key.0),
                        })?;
                let limiter = factories.create_default(key.clone()).await?;
                self.insert_limiter(key.clone(), limiter.clone());
                Ok(limiter)
            }
            Some(l) => Ok(l.clone()),
        }
    }

    pub async fn process_watch_log(&self, tenant_name: &str, entry: &EntryLog) -> MetaResult<()> {
        if entry.tye == ENTRY_LOG_TYPE_NOP {
            return Ok(());
        }
        let tenant: Tenant =
            serde_json::from_str(&entry.val).map_err(|err| MetaError::SerdeMsgInvalid {
                err: err.to_string(),
            })?;
        let key = LimiterKey::tenant_key(tenant_name.into());
        if entry.tye == ENTRY_LOG_TYPE_SET {
            let limiter_config = LimiterConfig::TenantRequestLimiterConfig {
                tenant: tenant_name.to_string(),
                config: Box::new(tenant.options().request_config().cloned()),
            };
            match self.get_limiter(&key) {
                Some(limiter) => {
                    limiter.change_self(limiter_config).await?;
                }
                None => {
                    self.create_limiter(key, limiter_config).await?;
                }
            };
        } else if entry.tye == ENTRY_LOG_TYPE_DEL {
            let limiter_config = LimiterConfig::TenantRequestLimiterConfig {
                tenant: tenant_name.to_string(),
                config: Box::new(None),
            };
            if let Some(limiter) = self.remove_limiter(&key) {
                limiter.change_self(limiter_config).await?;
            };
        }
        Ok(())
    }
}
