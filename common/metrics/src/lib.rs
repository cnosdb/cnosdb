pub mod count;
pub mod duration;
pub mod gauge;
pub mod histogram;
pub mod label;
pub mod metric;
pub mod metric_register;
pub mod metric_type;
pub mod metric_value;
pub mod prom_reporter;
pub mod reporter;

use std::any::Any;
use std::fmt::Debug;
use std::ops::Not;

pub use duration::DURATION_MAX;
use once_cell::sync::Lazy;
use prometheus::{
    default_registry, gather, linear_buckets, register_histogram_vec, register_int_counter_vec,
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, Opts,
};
use trace::error;

// note: metrics references influxdb_iox
// https://github.com/influxdata/influxdb_iox/tree/main/metrics
use crate::metric::Metric;
use crate::metric_value::MetricValue;
use crate::reporter::Reporter;

pub trait Measure: Debug {
    fn report(&self, reporter: &mut dyn Reporter);
    fn as_any(&self) -> &dyn Any;
}

pub trait CreateMetricRecorder {
    type Options: Sized + Send + Sync + Debug;
    fn create(option: &Self::Options) -> Self;
}

impl<T: Default> CreateMetricRecorder for T {
    type Options = ();
    fn create(_: &Self::Options) -> Self {
        T::default()
    }
}

/// example:
///
/// ```rust
/// use metrics::histogram::{U64Histogram, U64HistogramOptions};
/// use metrics::metric_register::MetricsRegister;
///
///fn example()  {
///    let register = MetricsRegister::default();
///    let options = U64HistogramOptions::new(vec![1, 10, 20, 30, 40, 50, 60, 70, 80, 90]);
///    let metric = register.register_metric::<U64Histogram>("example", "example metrics", options);
///    let histogram = metric.recorder([("database", "test"), ("tenant", "cnosdb")]);
///    histogram.record(5);
/// }
///
/// ```

pub trait MetricRecorder: CreateMetricRecorder + Clone + Debug {
    type Recorder;
    fn recorder(&self) -> Self::Recorder;
    fn metric_type() -> metric_type::MetricType;
    fn value(&self) -> MetricValue;
}

impl<T: MetricRecorder + 'static> Measure for Metric<T> {
    fn report(&self, reporter: &mut dyn Reporter) {
        reporter.start(
            self.name.clone(),
            self.description.clone(),
            self.metric_type,
        );
        self.shard.report(&self.labels, reporter);
        reporter.stop()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub const NAMESPACE: &str = "cnosdb";

pub const SERVER_SUBSYSTEM: &str = "server";
pub const SERVER_HTTP: &str = "server_http";
pub const SERVER_HTTP_WRITE: &str = "server_http_write";
pub const SERVER_HTTP_QUERY: &str = "server_http_query";

pub const SERVER_GRPC: &str = "grpc";
pub const TSKV_SUBSYSTEM: &str = "tskv";

pub static QUERY_SUCCESS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        Opts::new("success", "num of query success requests")
            .namespace(NAMESPACE)
            .subsystem(SERVER_HTTP_QUERY),
        &["user", "db"]
    )
    .expect("query metric cannot be created")
});

pub static QUERY_FAILED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        Opts::new("failure", "num of query failures")
            .namespace(NAMESPACE)
            .subsystem(SERVER_HTTP_QUERY),
        &["user", "db"]
    )
    .expect("query metric cannot be created")
});

pub static WRITE_SUCCESS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        Opts::new("write_success", "total num of point write success",)
            .namespace(NAMESPACE)
            .subsystem(SERVER_HTTP_WRITE),
        &["user", "db"]
    )
    .expect("query metric cannot be created")
});

pub static WRITE_FAILURE: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        Opts::new("write_failure", "total num of point write failed",)
            .namespace(NAMESPACE)
            .subsystem(SERVER_HTTP_WRITE),
        &["user", "db"]
    )
    .expect("query metric cannot be created")
});

pub static QUERY_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        HistogramOpts::new("milliseconds", "total latency distribution of query read",)
            .namespace(NAMESPACE)
            .subsystem(SERVER_HTTP_QUERY)
            .buckets(linear_buckets(0.0, 200.0, 2000).unwrap()),
        &["user", "db"],
    )
    .expect("query metric cannot be created")
});

pub static WRITE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        HistogramOpts::new("milliseconds", "total latency distribution of point write",)
            .namespace(NAMESPACE)
            .subsystem(SERVER_HTTP_WRITE)
            .buckets(linear_buckets(0.0, 200.0, 2000).unwrap()),
        &["user", "db"]
    )
    .expect("query metric cannot be created")
});

pub fn sample_query_read_duration(user: &str, db: &str, success: bool, delta: f64) {
    if success {
        QUERY_SUCCESS.with_label_values(&[db, user]).inc()
    } else {
        QUERY_FAILED.with_label_values(&[db, user]).inc()
    }
    if delta.eq(&0.0).not() {
        QUERY_DURATION.with_label_values(&[user, db]).observe(delta)
    }
}

pub fn sample_point_write_duration(user: &str, db: &str, success: bool, delta: f64) {
    if success {
        WRITE_SUCCESS.with_label_values(&[user, db]).inc()
    } else {
        WRITE_FAILURE.with_label_values(&[user, db]).inc()
    }
    WRITE_DURATION.with_label_values(&[user, db]).observe(delta)
}

pub static COMPACTION_SUCCESS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "compaction_success_total",
            "total success num of compaction",
        )
        .namespace(NAMESPACE)
        .subsystem(TSKV_SUBSYSTEM),
    )
    .expect("tskv metric cannot be created")
});

pub static COMPACTION_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new("compaction_failed_total", "total failed num of compaction")
            .namespace(NAMESPACE)
            .subsystem(TSKV_SUBSYSTEM),
    )
    .expect("tskv metric cannot be created")
});

pub static COMPACTION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "compaction_duration_seconds",
            "total duration distribution of compaction",
        )
        .namespace(NAMESPACE)
        .subsystem(TSKV_SUBSYSTEM)
        .buckets(linear_buckets(0.0, 300.0, 2400).unwrap()),
        &["db", "ts_family", "in_level", "out_level"],
    )
    .expect("tskv metric cannot be created")
});

pub fn init_tskv_metrics_recorder() {
    default_registry()
        .register(Box::new(COMPACTION_SUCCESS.clone()))
        .expect("tskv metrics collector cannot be registered");
    default_registry()
        .register(Box::new(COMPACTION_FAILED.clone()))
        .expect("tskv metrics collector cannot be registered");
    default_registry()
        .register(Box::new(COMPACTION_DURATION.clone()))
        .expect("tskv metrics collector cannot be registered");
}

pub fn incr_compaction_success() {
    COMPACTION_SUCCESS.inc();
}

pub fn incr_compaction_failed() {
    COMPACTION_FAILED.inc();
}

pub fn sample_tskv_compaction_duration(
    db: &str,
    ts_family: &str,
    in_level: &str,
    out_level: &str,
    delta: f64,
) {
    COMPACTION_DURATION
        .with_label_values(&[db, ts_family, in_level, out_level])
        .observe(delta)
}

pub fn gather_metrics() -> Vec<u8> {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Some(e) = encoder.encode(gather().as_ref(), &mut buffer).err() {
        error!("could not encode metrics: {}", e)
    }
    buffer
}
