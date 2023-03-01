#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MetricType {
    U64Counter,
    U64Gauge,
    U64Histogram,
    DurationCounter,
    DurationGauge,
    DurationHistogram,
    UnTyped,
}
