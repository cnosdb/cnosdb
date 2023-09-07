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
                insert_local_bucket(&mut buckets, DataIn, config.data_in.as_ref());
                insert_local_bucket(&mut buckets, DataOut, config.data_out.as_ref());
                insert_local_bucket(&mut buckets, Writes, config.writes.as_ref());
                insert_local_bucket(&mut buckets, Queries, config.queries.as_ref());
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
}

#[async_trait]
impl RequestLimiter for LocalRequestLimiter {
    async fn check_data_in(&self, data_len: usize) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::DataIn, data_len)
            .await
    }

    async fn check_data_out(&self, data_len: usize) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::DataOut, data_len)
            .await
    }

    async fn check_query(&self) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::Queries, 1).await
    }

    async fn check_write(&self) -> MetaResult<()> {
        self.check_bucket(RequestLimiterKind::Writes, 1).await
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
