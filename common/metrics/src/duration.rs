use std::time::Duration;

use crate::count::U64Counter;
use crate::gauge::U64Gauge;
use crate::histogram::U64Histogram;
use crate::metric_type::MetricType;
use crate::metric_value::{HistogramValue, MetricValue, ValueBucket};
use crate::{CreateMetricRecorder, MetricRecorder};

pub const DURATION_MAX: Duration = Duration::from_nanos(u64::MAX);

#[derive(Debug, Clone, Default)]
pub struct DurationGauge {
    inner: U64Gauge,
}

impl DurationGauge {
    pub fn inc(&self, duration: Duration) {
        self.inner.inc(
            duration
                .as_nanos()
                .try_into()
                .expect("cannot fit duration into u64"),
        )
    }

    pub fn fetch(&self) -> Duration {
        Duration::from_nanos(self.inner.fetch())
    }
}

impl MetricRecorder for DurationGauge {
    type Recorder = Self;

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn metric_type() -> MetricType {
        MetricType::DurationGauge
    }

    fn value(&self) -> MetricValue {
        MetricValue::DurationGauge(self.fetch())
    }
}

#[derive(Debug, Clone, Default)]
pub struct DurationCounter {
    inner: U64Counter,
}

impl DurationCounter {
    pub fn inc(&self, duration: Duration) {
        self.inner.inc(
            duration
                .as_nanos()
                .try_into()
                .expect("cannot fit duration into u64"),
        )
    }

    pub fn fetch(&self) -> Duration {
        Duration::from_nanos(self.inner.fetch())
    }
}

impl MetricRecorder for DurationCounter {
    type Recorder = Self;

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn metric_type() -> MetricType {
        MetricType::DurationCounter
    }

    fn value(&self) -> MetricValue {
        MetricValue::DurationCounter(self.fetch())
    }
}

#[derive(Debug, Clone)]
pub struct DurationHistogram {
    inner: U64Histogram,
}

impl DurationHistogram {
    pub fn fetch(&self) -> HistogramValue<Duration> {
        let inner = self.inner.fetch();

        HistogramValue {
            total: Duration::from_nanos(inner.total),
            buckets: inner
                .buckets
                .into_iter()
                .map(|bucket| ValueBucket {
                    le: Duration::from_nanos(bucket.le),
                    count: bucket.count,
                })
                .collect(),
        }
    }

    pub fn record(&self, value: Duration) {
        self.record_multiple(value, 1)
    }

    pub fn record_multiple(&self, value: Duration, count: u64) {
        self.inner.record_multiple(
            value
                .as_nanos()
                .try_into()
                .expect("cannot fit duration into u64"),
            count,
        )
    }
}

#[derive(Debug, Clone)]
pub struct DurationHistogramOptions {
    buckets: Vec<Duration>,
}

impl DurationHistogramOptions {
    /// Create a new `DurationHistogramOptions` with a list of thresholds to delimit the buckets
    pub fn new(thresholds: impl IntoIterator<Item = Duration>) -> Self {
        let mut buckets: Vec<_> = thresholds.into_iter().collect();
        buckets.sort_unstable();
        Self { buckets }
    }
}

impl Default for DurationHistogramOptions {
    fn default() -> Self {
        Self {
            buckets: vec![
                Duration::from_millis(1),
                Duration::from_micros(2_500),
                Duration::from_millis(5),
                Duration::from_millis(10),
                Duration::from_millis(25),
                Duration::from_millis(50),
                Duration::from_millis(100),
                Duration::from_millis(250),
                Duration::from_millis(500),
                Duration::from_millis(1000),
                Duration::from_millis(2500),
                Duration::from_millis(5000),
                Duration::from_millis(10000),
                DURATION_MAX,
            ],
        }
    }
}

impl CreateMetricRecorder for DurationHistogram {
    type Options = DurationHistogramOptions;

    fn create(options: &Self::Options) -> Self {
        let buckets = options
            .buckets
            .clone()
            .into_iter()
            .map(|b| b.as_nanos().try_into().expect(""));
        DurationHistogram {
            inner: U64Histogram::new(buckets),
        }
    }
}

impl MetricRecorder for DurationHistogram {
    type Recorder = Self;

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn metric_type() -> MetricType {
        MetricType::DurationHistogram
    }

    fn value(&self) -> MetricValue {
        MetricValue::DurationHistogram(self.fetch())
    }
}
