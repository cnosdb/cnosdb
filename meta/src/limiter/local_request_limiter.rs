use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use config::RequestLimiterConfig;
use limiter_bucket::CountBucket;
use serde::{Deserialize, Serialize};

use crate::client::MetaHttpClient;
use crate::error::{MetaError, MetaResult};
use crate::limiter::limiter_kind::RequestLimiterKind;
use crate::limiter::RequestLimiter;

pub enum RequreResult {
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
    pub(crate) buckets: HashMap<RequestLimiterKind, Arc<tokio::sync::Mutex<CountBucket>>>,
}

impl LocalRequestLimiter {
    pub fn new(
        cluster: &str,
        tenant: &str,
        value: &RequestLimiterConfig,
        meta_http_client: MetaHttpClient,
    ) -> Self {
        let mut buckets = HashMap::new();

        if let Some(ref config) = value.data_in {
            buckets.insert(
                RequestLimiterKind::DataIn,
                Arc::new(tokio::sync::Mutex::new(CountBucket::from(
                    &config.local_bucket,
                ))),
            );
        }

        if let Some(ref config) = value.data_out {
            buckets.insert(
                RequestLimiterKind::DataOut,
                Arc::new(tokio::sync::Mutex::new(CountBucket::from(
                    &config.local_bucket,
                ))),
            );
        }

        if let Some(ref config) = value.queries {
            buckets.insert(
                RequestLimiterKind::Queries,
                Arc::new(tokio::sync::Mutex::new(CountBucket::from(
                    &config.local_bucket,
                ))),
            );
        }

        if let Some(ref config) = value.writes {
            buckets.insert(
                RequestLimiterKind::Writes,
                Arc::new(tokio::sync::Mutex::new(CountBucket::from(
                    &config.local_bucket,
                ))),
            );
        }

        Self {
            cluster: cluster.to_string(),
            tenant: tenant.to_string(),
            buckets,
            meta_http_client,
        }
    }

    fn requre(
        &self,
        bucket_guard: &mut tokio::sync::MutexGuard<CountBucket>,
        requre: i64,
    ) -> RequreResult {
        if requre < 0 {
            return RequreResult::Fail;
        }

        let now_count = bucket_guard.fetch();

        if now_count > requre {
            bucket_guard.dec(requre);
            return RequreResult::Success;
        }

        // If the local_bucket does not specify `max`,
        // the remote_bucket must be requested every time
        let max = match bucket_guard.max() {
            Some(max) => max,
            None => {
                return RequreResult::RequestMeta {
                    min: requre,
                    max: requre,
                }
            }
        };

        if now_count <= 0 && requre <= max {
            RequreResult::RequestMeta {
                min: now_count.abs() + requre,
                max: now_count.abs() + max,
            }
        } else if now_count > 0 && now_count + max > requre {
            bucket_guard.dec(requre);
            RequreResult::Success
        } else {
            RequreResult::Fail
        }
    }

    async fn remote_requre(
        &self,
        kind: RequestLimiterKind,
        bucket_guard: &mut tokio::sync::MutexGuard<'_, CountBucket>,
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

    async fn check_bucket(&self, kind: RequestLimiterKind, data_len: usize) -> MetaResult<()> {
        let mut bucket_guard = match self.buckets.get(&kind) {
            Some(bucket) => bucket.lock().await,
            None => return Ok(()),
        };
        let data_len = data_len as i64;

        match self.requre(&mut bucket_guard, data_len) {
            RequreResult::Success => Ok(()),

            RequreResult::RequestMeta { min, max } => {
                self.remote_requre(kind, &mut bucket_guard, data_len, min, max)
                    .await
            }
            RequreResult::Fail => Err(MetaError::RequestLimit { kind }),
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
        self.check_bucket(RequestLimiterKind::Queries, 1).await
    }
}
