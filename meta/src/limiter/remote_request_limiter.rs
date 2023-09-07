use std::collections::HashMap;

use config::{Bucket, RequestLimiterConfig};
use limiter_bucket::RateBucket;
use serde::{Deserialize, Serialize};

use crate::limiter::limiter_kind::RequestLimiterKind;

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteRequestLimiter {
    pub buckets: HashMap<RequestLimiterKind, RateBucket>,
}

impl RemoteRequestLimiter {
    fn load_config(
        limiter_config: Option<&RequestLimiterConfig>,
    ) -> HashMap<RequestLimiterKind, RateBucket> {
        match limiter_config {
            Some(config) => {
                use RequestLimiterKind::*;
                let mut buckets = HashMap::new();
                insert_remote_bucket(&mut buckets, DataIn, config.data_in.as_ref());
                insert_remote_bucket(&mut buckets, DataOut, config.data_out.as_ref());
                insert_remote_bucket(&mut buckets, Queries, config.queries.as_ref());
                insert_remote_bucket(&mut buckets, Writes, config.writes.as_ref());
                buckets
            }
            None => HashMap::new(),
        }
    }

    pub fn new(limit_config: &RequestLimiterConfig) -> Self {
        let buckets = Self::load_config(Some(limit_config));
        Self { buckets }
    }
}

fn insert_remote_bucket(
    buckets: &mut HashMap<RequestLimiterKind, RateBucket>,
    kind: RequestLimiterKind,
    bucket: Option<&Bucket>,
) {
    if let Some(bucket) = bucket {
        buckets.insert(kind, RateBucket::from(&bucket.remote_bucket));
    }
}
