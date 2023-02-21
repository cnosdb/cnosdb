use std::collections::HashMap;

use config::RequestLimiterConfig;
use limiter_bucket::RateBucket;
use serde::{Deserialize, Serialize};

use crate::limiter::limiter_kind::RequestLimiterKind;

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteRequestLimiter {
    pub buckets: HashMap<RequestLimiterKind, RateBucket>,
}

impl RemoteRequestLimiter {
    pub fn new(limit_config: &RequestLimiterConfig) -> Self {
        let mut buckets = HashMap::new();
        if let Some(bucket) = limit_config.data_in {
            buckets.insert(
                RequestLimiterKind::DataIn,
                RateBucket::from(&bucket.remote_bucket),
            );
        }
        if let Some(bucket) = limit_config.data_out {
            buckets.insert(
                RequestLimiterKind::DataOut,
                RateBucket::from(&bucket.remote_bucket),
            );
        }
        if let Some(bucket) = limit_config.queries {
            buckets.insert(
                RequestLimiterKind::Queries,
                RateBucket::from(&bucket.remote_bucket),
            );
        }
        if let Some(bucket) = limit_config.writes {
            buckets.insert(
                RequestLimiterKind::Writes,
                RateBucket::from(&bucket.remote_bucket),
            );
        }
        Self { buckets }
    }
}
