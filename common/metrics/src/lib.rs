use std::os::linux::raw::stat;
use once_cell::sync::Lazy;
use prometheus::{Encoder, exponential_buckets, HistogramOpts, HistogramTimer, HistogramVec, IntCounter, IntGauge, linear_buckets, Opts};
use prometheus::local::LocalHistogram;
use prometheus::Registry;

pub const SERVER_NAMESPACE: &str = "server";

pub const TSKV_SUBSYSTEM: &str = "tskv";

pub const QUERY_SUBSYSTEM: &str = "query";

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static QUERY_READ_SUCCESS: Lazy<IntCounter> = Lazy::new( || {
    IntCounter::with_opts(
        Opts::new(
            "query_read_success_total",
            "total num of query success requests",
        ).namespace(SERVER_NAMESPACE)
            .subsystem(QUERY_SUBSYSTEM),
    ).expect("query metric cannot be created")
});

pub static QUERY_READ_FAILED: Lazy<IntCounter> = Lazy::new( || {
    IntCounter::with_opts(
        Opts::new(
            "query_read_failure_total",
            "total num of query failures",
        ).namespace(SERVER_NAMESPACE)
            .subsystem(QUERY_SUBSYSTEM),
    ).expect("query metric cannot be created")
});

pub static POINT_WRITE_SUCCESS: Lazy<IntCounter> = Lazy::new( || {
    IntCounter::with_opts(
        Opts::new(
            "point_write_success_total",
            "total num of point write success",
        ).namespace(SERVER_NAMESPACE)
            .subsystem(QUERY_SUBSYSTEM),
    ).expect("query metric cannot be created")
});

pub static POINT_WRITE_FAILED: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "point_write_failure_total",
            "total num of point write failed",
        ).namespace(SERVER_NAMESPACE)
            .subsystem(QUERY_SUBSYSTEM),
    ).expect("query metric cannot be created")
});

pub static QUERY_READ_LATENCY: Lazy<HistogramVec> = Lazy::new( || {
    HistogramVec::new(
        HistogramOpts::new(
            "query_read_milliseconds",
            "total latency distribution of query read"
        ).namespace(SERVER_NAMESPACE)
            .subsystem(QUERY_SUBSYSTEM)
            .buckets(linear_buckets(0.0, 200.0, 2000).unwrap()),
        &["tenant","db"],
    ).expect("query metric cannot be created")
});

pub static POINT_WRITE_LATENCY: Lazy<HistogramVec> = Lazy::new( || {
    HistogramVec::new(
        HistogramOpts::new(
            "point_write_milliseconds",
            "total latency distribution of point write"
        ).namespace(SERVER_NAMESPACE)
            .subsystem(QUERY_SUBSYSTEM)
            .buckets(linear_buckets(0.0, 200.0, 2000).unwrap()),
        &["tenant","db"],
    ).expect("query metric cannot be created")
});


pub fn init_query_recorder(){
    REGISTRY.register(Box::new(QUERY_READ_LATENCY.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY.register(Box::new(QUERY_READ_FAILED.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY.register(Box::new(QUERY_READ_SUCCESS.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY.register(Box::new(POINT_WRITE_LATENCY.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY.register(Box::new(POINT_WRITE_FAILED.clone()))
        .expect("query metrics collector cannot be registered");

    REGISTRY.register(Box::new(POINT_WRITE_SUCCESS.clone()))
        .expect("query metrics collector cannot be registered");
}

pub fn sample_query_metrics_read_latency(tenant :&str,db: &str,  delta: f64) {
    QUERY_READ_LATENCY.with_label_values(&[tenant, db]).observe(delta)
}

pub fn sample_query_metrics_write_latency( tenant :&str, db: &str, delta: f64) {
    POINT_WRITE_LATENCY.with_label_values(&[tenant, db]).observe(delta)
}

pub fn incr_query_read_success() {
    QUERY_READ_SUCCESS.inc()
}

pub fn incr_query_read_failed(){
    QUERY_READ_FAILED.inc()
}

pub fn incr_point_write_failed(){
    POINT_WRITE_FAILED.inc()
}

pub fn incr_point_write_success(){
    POINT_WRITE_SUCCESS.inc();
}

pub static COMPACTION_NUM: Lazy<IntCounter> = Lazy::new(|| {
    IntCounter::with_opts(
        Opts::new(
            "compaction_total",
            "total num of compaction",
        ).namespace(SERVER_NAMESPACE)
            .subsystem(TSKV_SUBSYSTEM),
    ).expect("tskv metric cannot be created")
});



pub fn init_compaction_recorder(){
    REGISTRY.register(Box::new(COMPACTION_NUM.clone()))
        .expect("tskv metrics collector cannot be registered");
}

pub fn incr_compaction_num(){
    COMPACTION_NUM.inc();
}

pub fn gather_metrics_as_prometheus_string() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();

    let mut buffer = Vec::new();
    encoder.encode(&REGISTRY.gather(), &mut buffer)?;

    match String::from_utf8(buffer.clone()) {
        Ok(v) => v,
        Err(e) => {
            error!("metrics could not be from_utf8'd: {}", e);
            String::default()
        }
    }
}
