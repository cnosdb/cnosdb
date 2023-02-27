use std::sync::Arc;

use parking_lot::Mutex;

use crate::metric_type::MetricType;
use crate::metric_value::{HistogramValue, MetricValue, ValueBucket};
use crate::{CreateMetricRecorder, MetricRecorder};

/// Determines the bucketing used by the `U64Histogram`
#[derive(Debug, Clone)]
pub struct U64HistogramOptions {
    buckets: Vec<u64>,
}

impl U64HistogramOptions {
    pub fn new(thresholds: impl IntoIterator<Item = u64>) -> Self {
        let mut buckets: Vec<_> = thresholds.into_iter().collect();
        buckets.sort_unstable();
        Self { buckets }
    }
}

#[derive(Debug, Clone)]
pub struct U64Histogram {
    shared: Arc<Mutex<HistogramValue<u64>>>,
}

impl U64Histogram {
    pub(crate) fn new(sorted_buckets: impl Iterator<Item = u64>) -> Self {
        let buckets = sorted_buckets
            .into_iter()
            .map(|le| ValueBucket {
                le,
                count: Default::default(),
            })
            .collect();

        Self {
            shared: Arc::new(Mutex::new(HistogramValue {
                total: Default::default(),
                buckets,
            })),
        }
    }

    pub fn fetch(&self) -> HistogramValue<u64> {
        self.shared.lock().clone()
    }

    pub fn record(&self, value: u64) {
        self.record_multiple(value, 1)
    }

    pub fn record_multiple(&self, value: u64, count: u64) {
        let mut state = self.shared.lock();
        if let Some(bucket) = state
            .buckets
            .iter_mut()
            .find(|bucket| value <= bucket.le)
            .as_mut()
        {
            bucket.count = bucket.count.wrapping_add(count);
            state.total = state.total.wrapping_add(value * count);
        }
    }
}

impl CreateMetricRecorder for U64Histogram {
    type Options = U64HistogramOptions;

    fn create(option: &Self::Options) -> Self {
        U64Histogram::new(option.buckets.clone().into_iter())
    }
}

impl MetricRecorder for U64Histogram {
    type Recorder = Self;

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn metric_type() -> MetricType {
        MetricType::U64Histogram
    }

    fn value(&self) -> MetricValue {
        MetricValue::U64Histogram(self.fetch())
    }
}
