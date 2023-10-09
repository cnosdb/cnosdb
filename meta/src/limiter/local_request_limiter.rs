use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use config::{Bucket, RequestLimiterConfig};
use limiter_bucket::CountBucket;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard, RwLock};
use tracing::debug;

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::limiter::limiter_kind::RequestLimiterKind;
use crate::limiter::{LimiterConfig, RequestLimiter};

pub enum RequireResult {
    Fail,
    Success,
    RequestMeta { min: i64, max: i64 },
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub struct ExpectedRequest {
    pub min: i64,
    pub max: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalBucketRequest {
    pub kind: RequestLimiterKind,
    pub expected: ExpectedRequest,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct LocalBucketResponse {
    pub kind: RequestLimiterKind,
    pub alloc: i64,
    /// -1 means infinity
    pub remote_remain: i64,
}

#[derive(Debug)]
pub struct LocalRequestLimiter {
    cluster: String,
    tenant: String,
    meta_http_client: MetaHttpClient,
    buckets: RwLock<HashMap<RequestLimiterKind, Arc<Mutex<CountBucket>>>>,
}

impl LocalRequestLimiter {
    pub fn new(
        cluster: &str,
        tenant: &str,
        value: Option<&RequestLimiterConfig>,
        meta_http_client: MetaHttpClient,
    ) -> Self {
        Self {
            cluster: cluster.to_string(),
            tenant: tenant.to_string(),
            buckets: RwLock::new(Self::load_config(value)),
            meta_http_client,
        }
    }

    fn load_config(
        limiter_config: Option<&RequestLimiterConfig>,
    ) -> HashMap<RequestLimiterKind, Arc<Mutex<CountBucket>>> {
        use RequestLimiterKind::*;
        match limiter_config {
            Some(config) => {
                let mut buckets = HashMap::new();
                insert_local_bucket(&mut buckets, CoordDataIn, config.coord_data_in.as_ref());
                insert_local_bucket(&mut buckets, CoordDataOut, config.coord_data_out.as_ref());
                insert_local_bucket(&mut buckets, CoordWrites, config.coord_writes.as_ref());
                insert_local_bucket(&mut buckets, CoordQueries, config.coord_queries.as_ref());
                insert_local_bucket(&mut buckets, HttpDataIn, config.http_data_in.as_ref());
                insert_local_bucket(&mut buckets, HttpDataOut, config.http_data_out.as_ref());
                insert_local_bucket(&mut buckets, HttpQueries, config.http_queries.as_ref());
                insert_local_bucket(&mut buckets, HttpWrites, config.http_writes.as_ref());
                buckets
            }
            None => HashMap::new(),
        }
    }

    fn require(&self, bucket_guard: &mut MutexGuard<CountBucket>, require: i64) -> RequireResult {
        if require < 0 {
            return RequireResult::Fail;
        }

        let now_count = bucket_guard.fetch();

        if now_count > require {
            bucket_guard.dec(require);
            return RequireResult::Success;
        }

        // If the local_bucket does not specify `max`,
        // the remote_bucket must be requested every time
        let max = match bucket_guard.max() {
            Some(max) => max,
            None => {
                return RequireResult::RequestMeta {
                    min: require,
                    max: require,
                }
            }
        };

        if now_count <= 0 && require <= max {
            RequireResult::RequestMeta {
                min: now_count.abs() + require,
                max: now_count.abs() + max,
            }
        } else if now_count > 0 && now_count + max > require {
            bucket_guard.dec(require);
            RequireResult::Success
        } else {
            RequireResult::Fail
        }
    }

    async fn remote_requre(
        &self,
        kind: RequestLimiterKind,
        bucket_guard: &mut MutexGuard<'_, CountBucket>,
        data_len: i64,
        min: i64,
        max: i64,
    ) -> MetaResult<()> {
        let request = LocalBucketRequest {
            kind,
            expected: ExpectedRequest { min, max },
        };
        let LocalBucketResponse {
            alloc,
            remote_remain,
            ..
        } = self
            .meta_http_client
            .limiter_request(&self.cluster, &self.tenant, request)
            .await?;
        bucket_guard.inc(alloc);
        debug!("Tenant {} Limiter {:?} local get {} tokens from remote, local remain {}, remote remain {}",
            &self.tenant, kind, alloc, bucket_guard.fetch(), remote_remain);
        if bucket_guard.fetch() > data_len {
            bucket_guard.dec(data_len);
            debug!(
                "Tenant {} Limiter {:?}: local consume {} tokens, local remain {}",
                self.tenant.as_str(),
                kind,
                data_len,
                bucket_guard.fetch()
            );
            Ok(())
        } else {
            Err(MetaError::RequestLimit { kind })
        }
    }

    async fn get_buket(&self, kind: RequestLimiterKind) -> Option<Arc<Mutex<CountBucket>>> {
        self.buckets.read().await.get(&kind).cloned()
    }

    async fn check_bucket(&self, kind: RequestLimiterKind, data_len: usize) -> MetaResult<()> {
        let bucket = match self.get_buket(kind).await {
            Some(bucket) => bucket,
            None => return Ok(()),
        };
        let mut bucket_guard = bucket.lock().await;

        let data_len = data_len as i64;

        match self.require(&mut bucket_guard, data_len) {
            RequireResult::Success => {
                debug!(
                    "Tenant {} Limiter {:?}: local consume {} tokens, local remain {}",
                    self.tenant.as_str(),
                    kind,
                    data_len,
                    bucket_guard.fetch()
                );
                Ok(())
            }

            RequireResult::RequestMeta { min, max } => {
                self.remote_requre(kind, &mut bucket_guard, data_len, min, max)
                    .await
            }
            RequireResult::Fail => Err(MetaError::RequestLimit { kind }),
        }
    }

    pub async fn clear(&self) {
        self.buckets.write().await.clear()
    }

    pub async fn change(&self, limiter_config: Option<&RequestLimiterConfig>) -> MetaResult<()> {
        let mut buckets_guard = self.buckets.write().await;
        *buckets_guard = Self::load_config(limiter_config);
        Ok(())
    }
}

#[async_trait]
impl RequestLimiter for LocalRequestLimiter {
    async fn check_coord_data_in(&self, data_len: usize) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::CoordDataIn, data_len)
            .await
    }

    async fn check_coord_data_out(&self, data_len: usize) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::CoordDataOut, data_len)
            .await
    }

    async fn check_coord_queries(&self) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::CoordQueries, 1).await
    }

    async fn check_coord_writes(&self) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::CoordWrites, 1).await
    }

    async fn check_http_data_in(&self, data_len: usize) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::HttpDataIn, data_len)
            .await
    }

    async fn check_http_data_out(&self, data_len: usize) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::HttpDataOut, data_len)
            .await
    }

    async fn check_http_queries(&self) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::HttpQueries, 1).await
    }

    async fn check_http_writes(&self) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::HttpWrites, 1).await
    }

    async fn change_self(&self, limiter_config: LimiterConfig) -> MetaResult<()> {
        match limiter_config {
            LimiterConfig::TenantRequestLimiterConfig { config, .. } => {
                let map = LocalRequestLimiter::load_config(config.as_ref().as_ref());
                *(self.buckets.write().await) = map;
            }
            _ => {
                return Err(MetaError::LimiterCreate {
                    msg: "config invalid".to_string(),
                });
            }
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

fn insert_local_bucket(
    buckets_guard: &mut HashMap<RequestLimiterKind, Arc<Mutex<CountBucket>>>,
    limiter_kind: RequestLimiterKind,
    bucket_config: Option<&Bucket>,
) {
    if let Some(bucket) = bucket_config {
        buckets_guard.insert(
            limiter_kind,
            Arc::new(Mutex::new(CountBucket::from(&bucket.local_bucket))),
        );
    }
}
