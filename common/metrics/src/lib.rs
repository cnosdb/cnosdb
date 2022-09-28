use once_cell::sync::Lazy;
use prometheus::local::LocalHistogram;
use prometheus::Registry;
use prometheus::{
    exponential_buckets, linear_buckets, Encoder, HistogramOpts, HistogramTimer, HistogramVec,
    IntCounter, IntGauge, Opts,
};

pub const SERVER_NAMESPACE: &str = "server";

pub const TSKV_SUBSYSTEM: &str = "tskv";

pub const QUERY_SUBSYSTEM: &str = "query";

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static QUERY_READ_SUCCESS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "query_read_success_total",
            "total num of query success requests",
        )
        .namespace(SERVER_NAMESPACE)
        .subsystem(QUERY_SUBSYSTEM),
    )
    .expect("query metric cannot be created")
});

pub static QUERY_READ_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new("query_read_failure_total", "total num of query failures")
            .namespace(SERVER_NAMESPACE)
            .subsystem(QUERY_SUBSYSTEM),
    )
    .expect("query metric cannot be created")
});

pub static POINT_WRITE_SUCCESS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "point_write_success_total",
            "total num of point write success",
        )
        .namespace(SERVER_NAMESPACE)
        .subsystem(QUERY_SUBSYSTEM),
    )
    .expect("query metric cannot be created")
});

pub static POINT_WRITE_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "point_write_failure_total",
            "total num of point write failed",
        )
        .namespace(SERVER_NAMESPACE)
        .subsystem(QUERY_SUBSYSTEM),
    )
    .expect("query metric cannot be created")
});

pub static QUERY_READ_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "query_read_milliseconds",
            "total latency distribution of query read",
        )
        .namespace(SERVER_NAMESPACE)
        .subsystem(QUERY_SUBSYSTEM)
        .buckets(linear_buckets(0.0, 200.0, 2000).unwrap()),
        &["tenant", "db"],
    )
    .expect("query metric cannot be created")
});

pub static POINT_WRITE_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "point_write_milliseconds",
            "total latency distribution of point write",
        )
        .namespace(SERVER_NAMESPACE)
        .subsystem(QUERY_SUBSYSTEM)
        .buckets(linear_buckets(0.0, 200.0, 2000).unwrap()),
        &["tenant", "db"],
    )
    .expect("query metric cannot be created")
});

pub fn init_query_metrics_recorder() {
    REGISTRY
        .register(Box::new(QUERY_READ_LATENCY.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY
        .register(Box::new(QUERY_READ_FAILED.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY
        .register(Box::new(QUERY_READ_SUCCESS.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY
        .register(Box::new(POINT_WRITE_LATENCY.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY
        .register(Box::new(POINT_WRITE_FAILED.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY
        .register(Box::new(POINT_WRITE_SUCCESS.clone()))
        .expect("query metrics collector cannot be registered");
}

pub fn sample_query_read_latency(tenant: &str, db: &str, delta: f64) {
    QUERY_READ_LATENCY
        .with_label_values(&[tenant, db])
        .observe(delta)
}

pub fn sample_point_write_latency(tenant: &str, db: &str, delta: f64) {
    POINT_WRITE_LATENCY
        .with_label_values(&[tenant, db])
        .observe(delta)
}

pub fn incr_query_read_success() {
    QUERY_READ_SUCCESS.inc()
}

pub fn incr_query_read_failed() {
    QUERY_READ_FAILED.inc()
}

pub fn incr_point_write_failed() {
    POINT_WRITE_FAILED.inc()
}

pub fn incr_point_write_success() {
    POINT_WRITE_SUCCESS.inc();
}

pub static COMPACTION_SUCCESS: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "compaction_success_total",
            "total success num of compaction",
        )
        .namespace(SERVER_NAMESPACE)
        .subsystem(TSKV_SUBSYSTEM),
    )
    .expect("tskv metric cannot be created")
});

pub static COMPACTION_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new("compaction_failed_total", "total failed num of compaction")
            .namespace(SERVER_NAMESPACE)
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
        .namespace(SERVER_NAMESPACE)
        .subsystem(TSKV_SUBSYSTEM)
        .buckets(linear_buckets(0.0, 10.0, 200).unwrap()),
        &["db", "ts_family", "level"],
    )
    .expect("tskv metric cannot be created")
});

pub fn init_tskv_metrics_recorder() {
    REGISTRY
        .register(Box::new(COMPACTION_SUCCESS.clone()))
        .expect("tskv metrics collector cannot be registered");
    REGISTRY
        .register(Box::new(COMPACTION_FAILED.clone()))
        .expect("tskv metrics collector cannot be registered");
    REGISTRY
        .register(Box::new(COMPACTION_DURATION.clone()))
        .expect("tskv metrics collector cannot be registered");
}

pub fn incr_compaction_success() {
    COMPACTION_SUCCESS.inc();
}

pub fn incr_compaction_failed() {
    COMPACTION_FAILED.inc();
}

pub fn sample_tskv_compaction_duration(db: &str, ts_family: &str, level: &str, delta: f64) {
    COMPACTION_DURATION
        .with_label_values(&[db, ts_family, level])
        .observe(delta)
}

pub fn gather_metrics_as_prometheus_string() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
        error!("could not encode metrics: {}", e);
        return String::default();
    };

    match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    }
}
