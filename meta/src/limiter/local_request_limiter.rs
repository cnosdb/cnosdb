use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use config::{Bucket, RequestLimiterConfig};
use limiter_bucket::CountBucket;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard, RwLock};

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::limiter::limiter_kind::RequestLimiterKind;
use crate::limiter::RequestLimiter;

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
}

#[derive(Debug)]
pub struct LocalRequestLimiter {
    cluster: String,
    tenant: String,
    meta_http_client: MetaHttpClient,
    buckets: RwLock<HashMap<RequestLimiterKind, Arc<Mutex<CountBucket>>>>,
}
pub fn down_cast_to_local_request_limiter(
    local_request_limiter: &dyn RequestLimiter,
) -> &LocalRequestLimiter {
    // # Safety
    // only [`LocalRequestLimiter`] impl [`RequestLimiter`]
    unsafe {
        local_request_limiter
            .as_any()
            .downcast_ref()
            .unwrap_unchecked()
    }
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
        let LocalBucketResponse { alloc, .. } = self
            .meta_http_client
            .limiter_request(&self.cluster, &self.tenant, request)
            .await?;
        bucket_guard.inc(alloc);
        if bucket_guard.fetch() > data_len {
            bucket_guard.dec(data_len);
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
            RequireResult::Success => Ok(()),

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
