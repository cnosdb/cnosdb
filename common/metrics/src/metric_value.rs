use std::time::Duration;

use crate::metric_type::MetricType;

#[derive(Clone, Debug)]
pub enum MetricValue {
    U64Counter(u64),
    U64Gauge(u64),
    DurationCounter(Duration),
    DurationGauge(Duration),
    U64Histogram(HistogramValue<u64>),
    DurationHistogram(HistogramValue<Duration>),
    Null,
}

impl MetricValue {
    pub fn metric_type(&self) -> MetricType {
        match self {
            Self::U64Gauge(_) => MetricType::U64Gauge,
            Self::U64Counter(_) => MetricType::U64Counter,
            Self::DurationCounter(_) => MetricType::DurationCounter,
            Self::DurationGauge(_) => MetricType::DurationGauge,
            Self::U64Histogram(_) => MetricType::U64Histogram,
            Self::DurationHistogram(_) => MetricType::DurationHistogram,
            Self::Null => MetricType::UnTyped,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HistogramValue<T> {
    /// The sum of all observations
    pub total: T,
    /// The buckets
    pub buckets: Vec<ValueBucket<T>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ValueBucket<T> {
    pub le: T,
    pub count: u64,
}
