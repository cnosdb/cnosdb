use std::time::Instant;

use once_cell::sync::Lazy;
use prometheus::core::Collector;
use prometheus::{default_registry, linear_buckets, HistogramOpts, HistogramVec};

use crate::registered::{FakeMetricStore, MetricStore};
use crate::time_unit::TimeUnit;
use crate::{NAMESPACE, TSKV_SUBSYSTEM};

static UNIT: &str = "unit";
static SAMPLE_SIZE: &str = "sample_size";
static NODE_ID: &str = "node_id";
static VNODE_ID: &str = "vnode_id";
static TYPE: &str = "type";

#[derive(Debug, Clone, Copy)]
pub enum MetricCompactionType {
    Normal,
    Delta,
    Manual,
}

impl MetricCompactionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MetricCompactionType::Normal => "normal",
            MetricCompactionType::Delta => "delta",
            MetricCompactionType::Manual => "manual",
        }
    }
}

impl std::fmt::Display for MetricCompactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

fn register<T: Collector + 'static>(collector: T) {
    if let Err(e) = default_registry().register(Box::new(collector)) {
        panic!("Failed to register collector: {e}");
    }
}

pub fn init() {
    register(COMPACTION_MERGE_FIELD_DURATION_FINAL_SUM.clone());
    register(COMPACTION_MERGE_FIELD_DURATION_FINAL_AVG.clone());
    register(COMPACTION_MERGE_FIELD_DURATION_FINAL_MIN.clone());
    register(COMPACTION_MERGE_FIELD_DURATION_FINAL_MAX.clone());
    register(COMPACTION_MERGE_FIELD_DURATION_SUM.clone());
    register(COMPACTION_MERGE_FIELD_DURATION_AVG.clone());
    register(COMPACTION_MERGE_FIELD_DURATION_MIN.clone());
    register(COMPACTION_MERGE_FIELD_DURATION_MAX.clone());

    register(COMPACTION_READ_DATA_DURATION_FINAL_SUM.clone());
    register(COMPACTION_READ_DATA_DURATION_FINAL_AVG.clone());
    register(COMPACTION_READ_DATA_DURATION_FINAL_MIN.clone());
    register(COMPACTION_READ_DATA_DURATION_FINAL_MAX.clone());
    register(COMPACTION_READ_DATA_DURATION_SUM.clone());
    register(COMPACTION_READ_DATA_DURATION_AVG.clone());
    register(COMPACTION_READ_DATA_DURATION_MIN.clone());
    register(COMPACTION_READ_DATA_DURATION_MAX.clone());

    register(COMPACTION_WRITE_DATA_DURATION_FINAL_SUM.clone());
    register(COMPACTION_WRITE_DATA_DURATION_FINAL_AVG.clone());
    register(COMPACTION_WRITE_DATA_DURATION_FINAL_MIN.clone());
    register(COMPACTION_WRITE_DATA_DURATION_FINAL_MAX.clone());
    register(COMPACTION_WRITE_DATA_DURATION_SUM.clone());
    register(COMPACTION_WRITE_DATA_DURATION_AVG.clone());
    register(COMPACTION_WRITE_DATA_DURATION_MIN.clone());
    register(COMPACTION_WRITE_DATA_DURATION_MAX.clone());
}

fn histogram_vec_for_tskv_compaction_final<N, H>(name: N, help: H) -> HistogramVec
where
    N: Into<String>,
    H: Into<String>,
{
    let name = name.into();
    match HistogramVec::new(
        HistogramOpts::new(&name, help)
            .namespace(NAMESPACE)
            .subsystem(TSKV_SUBSYSTEM)
            .buckets(linear_buckets(0.0, 100.0, 100).unwrap()),
        &[UNIT, SAMPLE_SIZE, NODE_ID, VNODE_ID, TYPE],
    ) {
        Ok(hv) => hv,
        Err(e) => panic!("Failed to create metric '{name}': {e}"),
    }
}

fn histogram_vec_for_tskv_compaction_samples<N, H>(name: N, help: H) -> HistogramVec
where
    N: Into<String>,
    H: Into<String>,
{
    let name = name.into();
    match HistogramVec::new(
        HistogramOpts::new(&name, help)
            .namespace(NAMESPACE)
            .subsystem(TSKV_SUBSYSTEM)
            .buckets(linear_buckets(0.0, 100.0, 100).unwrap()),
        &[UNIT, SAMPLE_SIZE, NODE_ID, VNODE_ID, TYPE],
    ) {
        Ok(hv) => hv,
        Err(e) => panic!("Failed to create metric '{name}': {e}"),
    }
}

static COMPACTION_MERGE_FIELD_DURATION_FINAL_SUM: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_merge_field_final_sum",
        "the sum of all merge field durations in a compaction",
    )
});
static COMPACTION_MERGE_FIELD_DURATION_FINAL_AVG: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_merge_field_final_avg",
        "the avg of all merge field durations in a compaction",
    )
});
static COMPACTION_MERGE_FIELD_DURATION_FINAL_MIN: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_merge_field_final_min",
        "the min of all merge field durations in a compaction",
    )
});
static COMPACTION_MERGE_FIELD_DURATION_FINAL_MAX: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_merge_field_final_max",
        "the max of all merge field durations in a compaction",
    )
});
static COMPACTION_MERGE_FIELD_DURATION_SUM: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_merge_field_sum",
        "the sum of a batch of merge field durations",
    )
});
static COMPACTION_MERGE_FIELD_DURATION_AVG: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_merge_field_avg",
        "the avg of a batch of merge field durations",
    )
});
static COMPACTION_MERGE_FIELD_DURATION_MIN: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_merge_field_min",
        "the min of a batch of merge field durations",
    )
});
static COMPACTION_MERGE_FIELD_DURATION_MAX: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_merge_field_max",
        "the max of a batch of merge field durations",
    )
});

static COMPACTION_READ_DATA_DURATION_FINAL_SUM: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_read_data_final_sum",
        "the sum of all read data durations in a compaction",
    )
});
static COMPACTION_READ_DATA_DURATION_FINAL_AVG: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_read_data_final_avg",
        "the avg of all read data durations in a compaction",
    )
});
static COMPACTION_READ_DATA_DURATION_FINAL_MIN: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_read_data_final_min",
        "the min of all read data durations in a compaction",
    )
});
static COMPACTION_READ_DATA_DURATION_FINAL_MAX: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_read_data_final_max",
        "the max of all read data durations in a compaction",
    )
});
static COMPACTION_READ_DATA_DURATION_SUM: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_read_data_sum",
        "the sum of a batch of read data durations",
    )
});
static COMPACTION_READ_DATA_DURATION_AVG: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_read_data_avg",
        "the avg of a batch of read data durations",
    )
});
static COMPACTION_READ_DATA_DURATION_MIN: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_read_data_min",
        "the min of a batch of read data durations",
    )
});
static COMPACTION_READ_DATA_DURATION_MAX: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_read_data_max",
        "the max of a batch of read data durations",
    )
});

static COMPACTION_WRITE_DATA_DURATION_FINAL_SUM: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_write_data_final_sum",
        "the sum of all write data durations in a compaction",
    )
});
static COMPACTION_WRITE_DATA_DURATION_FINAL_AVG: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_write_data_final_avg",
        "the avg of all write data durations in a compaction",
    )
});
static COMPACTION_WRITE_DATA_DURATION_FINAL_MIN: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_write_data_final_min",
        "the min of all write data durations in a compaction",
    )
});
static COMPACTION_WRITE_DATA_DURATION_FINAL_MAX: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_final(
        "compaction_write_data_final_max",
        "the max of all write data durations in a compaction",
    )
});
static COMPACTION_WRITE_DATA_DURATION_SUM: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_write_data_sum",
        "the sum of a batch of write data durations",
    )
});
static COMPACTION_WRITE_DATA_DURATION_AVG: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_write_data_avg",
        "the avg of a batch of write data durations",
    )
});
static COMPACTION_WRITE_DATA_DURATION_MIN: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_write_data_min",
        "the min of a batch of write data durations",
    )
});
static COMPACTION_WRITE_DATA_DURATION_MAX: Lazy<HistogramVec> = Lazy::new(|| {
    histogram_vec_for_tskv_compaction_samples(
        "compaction_write_data_max",
        "the max of a batch of write data durations",
    )
});

pub struct TskvVnodeCompactionReporter<'a> {
    merge_field_final_reporter: Box<dyn MetricStore + 'a>,
    merge_field_reporter: Box<dyn MetricStore + 'a>,
    read_data_final_reporter: Box<dyn MetricStore + 'a>,
    read_data_reporter: Box<dyn MetricStore + 'a>,
    write_data_final_reporter: Box<dyn MetricStore + 'a>,
    write_data_reporter: Box<dyn MetricStore + 'a>,
}

impl<'a> TskvVnodeCompactionReporter<'a> {
    pub fn fake() -> Self {
        Self {
            merge_field_final_reporter: Box::new(FakeMetricStore),
            merge_field_reporter: Box::new(FakeMetricStore),
            read_data_final_reporter: Box::new(FakeMetricStore),
            read_data_reporter: Box::new(FakeMetricStore),
            write_data_final_reporter: Box::new(FakeMetricStore),
            write_data_reporter: Box::new(FakeMetricStore),
        }
    }

    pub fn new(node_id: u64, vnode_id: u32, compaction_type: MetricCompactionType) -> Self {
        Self {
            merge_field_final_reporter: Box::new(
                VnodeCompactionMetricStoreBuilder::new(
                    TimeUnit::Microsecond,
                    1,
                    node_id,
                    vnode_id,
                    compaction_type,
                )
                .build(
                    &COMPACTION_MERGE_FIELD_DURATION_FINAL_SUM,
                    &COMPACTION_MERGE_FIELD_DURATION_FINAL_AVG,
                    &COMPACTION_MERGE_FIELD_DURATION_FINAL_MIN,
                    &COMPACTION_MERGE_FIELD_DURATION_FINAL_MAX,
                ),
            ),
            merge_field_reporter: Box::new(
                VnodeCompactionMetricStoreBuilder::new(
                    TimeUnit::Microsecond,
                    5000,
                    node_id,
                    vnode_id,
                    compaction_type,
                )
                .build(
                    &COMPACTION_MERGE_FIELD_DURATION_SUM,
                    &COMPACTION_MERGE_FIELD_DURATION_AVG,
                    &COMPACTION_MERGE_FIELD_DURATION_MIN,
                    &COMPACTION_MERGE_FIELD_DURATION_MAX,
                ),
            ),
            read_data_final_reporter: Box::new(
                VnodeCompactionMetricStoreBuilder::new(
                    TimeUnit::Microsecond,
                    1,
                    node_id,
                    vnode_id,
                    compaction_type,
                )
                .build(
                    &COMPACTION_READ_DATA_DURATION_FINAL_SUM,
                    &COMPACTION_READ_DATA_DURATION_FINAL_AVG,
                    &COMPACTION_READ_DATA_DURATION_FINAL_MIN,
                    &COMPACTION_READ_DATA_DURATION_FINAL_MAX,
                ),
            ),
            read_data_reporter: Box::new(
                VnodeCompactionMetricStoreBuilder::new(
                    TimeUnit::Microsecond,
                    10000,
                    node_id,
                    vnode_id,
                    compaction_type,
                )
                .build(
                    &COMPACTION_READ_DATA_DURATION_SUM,
                    &COMPACTION_READ_DATA_DURATION_AVG,
                    &COMPACTION_READ_DATA_DURATION_MIN,
                    &COMPACTION_READ_DATA_DURATION_MAX,
                ),
            ),
            write_data_final_reporter: Box::new(
                VnodeCompactionMetricStoreBuilder::new(
                    TimeUnit::Microsecond,
                    1,
                    node_id,
                    vnode_id,
                    compaction_type,
                )
                .build(
                    &COMPACTION_WRITE_DATA_DURATION_FINAL_SUM,
                    &COMPACTION_WRITE_DATA_DURATION_FINAL_AVG,
                    &COMPACTION_WRITE_DATA_DURATION_FINAL_MIN,
                    &COMPACTION_WRITE_DATA_DURATION_FINAL_MAX,
                ),
            ),
            write_data_reporter: Box::new(
                VnodeCompactionMetricStoreBuilder::new(
                    TimeUnit::Microsecond,
                    10000,
                    node_id,
                    vnode_id,
                    compaction_type,
                )
                .build(
                    &COMPACTION_WRITE_DATA_DURATION_FINAL_SUM,
                    &COMPACTION_WRITE_DATA_DURATION_FINAL_AVG,
                    &COMPACTION_WRITE_DATA_DURATION_FINAL_MIN,
                    &COMPACTION_WRITE_DATA_DURATION_FINAL_MAX,
                ),
            ),
        }
    }

    pub fn merge_field_begin(&mut self) {
        self.merge_field_reporter.begin();
    }

    pub fn merge_field_end(&mut self) {
        self.merge_field_reporter.end(false);
    }

    pub fn read_data_begin(&mut self) {
        self.read_data_reporter.begin();
    }

    pub fn read_data_end(&mut self) {
        self.read_data_reporter.end(false);
    }

    pub fn write_data_begin(&mut self) {
        self.write_data_reporter.begin();
    }

    pub fn write_data_end(&mut self) {
        self.write_data_reporter.end(false);
    }

    pub fn begin(&mut self) {
        self.merge_field_final_reporter.begin();
        self.read_data_final_reporter.begin();
        self.write_data_final_reporter.begin();
    }

    pub fn end(&mut self) {
        self.merge_field_final_reporter.end(true);
        self.read_data_final_reporter.end(true);
        self.write_data_final_reporter.end(true);
    }
}

trait Observe: Send {
    fn observe(&mut self, sum: f64, avg: f64, min: f64, max: f64);
}

struct PrometheuObserver<'a> {
    sum_reporter: &'a HistogramVec,
    avg_reporter: &'a HistogramVec,
    min_reporter: &'a HistogramVec,
    max_reporter: &'a HistogramVec,
    label_values_borrowed: Vec<&'a str>,
}

impl<'a> Observe for PrometheuObserver<'a> {
    fn observe(&mut self, sum: f64, avg: f64, min: f64, max: f64) {
        self.sum_reporter
            .with_label_values(&self.label_values_borrowed)
            .observe(sum);
        self.avg_reporter
            .with_label_values(&self.label_values_borrowed)
            .observe(avg);
        self.min_reporter
            .with_label_values(&self.label_values_borrowed)
            .observe(min);
        self.max_reporter
            .with_label_values(&self.label_values_borrowed)
            .observe(max);
    }
}

struct VnodeCompactionMetricStore<'a> {
    observer: Box<dyn Observe + 'a>,
    _label_values_owned: Vec<String>,

    time_unit: TimeUnit,
    sample_size: usize,
    instant: std::time::Instant,
    cost_buf: Vec<f64>,
    cost_min: f64,
    cost_max: f64,
}

impl<'a> VnodeCompactionMetricStore<'a> {
    const DEFAULT_MIN_MAX: (f64, f64) = (f64::MAX, 0_f64);

    fn flush(&mut self, force_flush: bool) {
        let cost_t = self.time_unit.parse_duration(self.instant.elapsed());
        self.cost_min = self.cost_min.min(cost_t);
        self.cost_max = self.cost_max.max(cost_t);
        self.cost_buf.push(cost_t);
        if force_flush || self.cost_buf.len() >= self.sample_size {
            let (sum, avg) = if !self.cost_buf.is_empty() {
                let sum = self.cost_buf.iter().sum::<f64>();
                let avg = sum / self.cost_buf.len() as f64;
                self.cost_buf.clear();
                (sum, avg)
            } else {
                (0.0, 0.0)
            };
            self.observe(sum, avg, self.cost_min, self.cost_max);
            (self.cost_min, self.cost_max) = Self::DEFAULT_MIN_MAX;
        }
    }

    pub fn observe(&mut self, sum: f64, avg: f64, min: f64, max: f64) {
        self.observer.observe(sum, avg, min, max);
    }
}

impl<'a> MetricStore for VnodeCompactionMetricStore<'a> {
    fn begin(&mut self) {
        self.instant = std::time::Instant::now();
    }

    fn end(&mut self, is_finished: bool) {
        self.flush(is_finished);
    }
}

#[derive(Debug)]
struct VnodeCompactionMetricStoreBuilder {
    time_unit: TimeUnit,
    sample_size: usize,
    node_id: u64,
    vnode_id: u32,
    compaction_type: MetricCompactionType,
}

impl VnodeCompactionMetricStoreBuilder {
    fn new(
        time_unit: TimeUnit,
        sample_size: usize,
        node_id: u64,
        vnode_id: u32,
        compaction_type: MetricCompactionType,
    ) -> Self {
        Self {
            node_id,
            compaction_type,
            time_unit,
            vnode_id,
            sample_size,
        }
    }

    fn build_labels(&self) -> Vec<String> {
        vec![
            self.time_unit.to_string(),
            self.sample_size.to_string(),
            self.node_id.to_string(),
            self.vnode_id.to_string(),
            self.compaction_type.to_string(),
        ]
    }

    fn build<'a>(
        self,
        sum_reporter: &'a HistogramVec,
        avg_reporter: &'a HistogramVec,
        min_reporter: &'a HistogramVec,
        max_reporter: &'a HistogramVec,
    ) -> VnodeCompactionMetricStore<'a> {
        let label_values_owned = self.build_labels();
        let label_values_borrowed: Vec<&str> = label_values_owned
            .iter()
            .map(|s| unsafe {
                let sli = std::slice::from_raw_parts(s.as_ptr(), s.len());
                std::str::from_utf8_unchecked(sli)
            })
            .collect::<Vec<_>>();
        VnodeCompactionMetricStore {
            observer: Box::new(PrometheuObserver {
                sum_reporter,
                avg_reporter,
                min_reporter,
                max_reporter,
                label_values_borrowed,
            }),
            _label_values_owned: label_values_owned,
            time_unit: self.time_unit,
            sample_size: self.sample_size,
            instant: Instant::now(),
            cost_buf: Vec::with_capacity(self.sample_size),
            cost_min: f64::MAX,
            cost_max: 0.0_f64,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use parking_lot::Mutex;

    use super::*;

    #[derive(Debug, Clone)]
    pub struct TestObserver {
        sum: Arc<Mutex<f64>>,
        avg: Arc<Mutex<f64>>,
        min: Arc<Mutex<f64>>,
        max: Arc<Mutex<f64>>,
    }

    impl TestObserver {
        pub fn new() -> Self {
            Self {
                sum: Arc::new(Mutex::new(0.0)),
                avg: Arc::new(Mutex::new(0.0)),
                min: Arc::new(Mutex::new(0.0)),
                max: Arc::new(Mutex::new(0.0)),
            }
        }

        pub fn sum(&self) -> f64 {
            *self.sum.lock()
        }

        pub fn avg(&self) -> f64 {
            *self.avg.lock()
        }

        pub fn min(&self) -> f64 {
            *self.min.lock()
        }

        pub fn max(&self) -> f64 {
            *self.max.lock()
        }
    }

    impl Observe for TestObserver {
        fn observe(&mut self, sum: f64, avg: f64, min: f64, max: f64) {
            *self.sum.lock() = sum;
            *self.avg.lock() = avg;
            *self.min.lock() = min;
            *self.max.lock() = max;
        }
    }

    #[test]
    fn test_vnode_compaction_metrics_store() {
        let mut reporter = VnodeCompactionMetricStoreBuilder::new(
            TimeUnit::Millisecond,
            1000,
            1,
            2,
            MetricCompactionType::Normal,
        )
        .build(
            &COMPACTION_READ_DATA_DURATION_SUM,
            &COMPACTION_READ_DATA_DURATION_AVG,
            &COMPACTION_READ_DATA_DURATION_MIN,
            &COMPACTION_READ_DATA_DURATION_MAX,
        );
        let observer = TestObserver::new();
        reporter.observer = Box::new(observer.clone());
        for i in 0..1000 {
            reporter.begin();
            if i % 2 == 0 {
                // Sleep a duration > 10ms, and assume that it < 99ms.
                sleep(Duration::from_millis(10));
            } else {
                // Sleep a duration > 20ms, and assume that it < 99ms.
                sleep(Duration::from_millis(20));
            }
            reporter.end(false);
        }
        let sum = observer.sum();
        let avg = observer.avg();
        let min = observer.min();
        let max = observer.max();
        println!("sum={sum},avg={avg},min={min},max={max}");
        assert!(
            (15000.0..30000.0).contains(&sum),
            "sum: {sum} not as expected"
        );
        assert!((15.0..99.0).contains(&avg), "avg: {avg} not as expected");
        assert!((10.0..99.0).contains(&min), "min: {min} not as expected");
        assert!((20.0..99.0).contains(&max), "max: {max} not as expected");
    }

    #[test]
    fn test_vnode_compaction_reporter() {
        let mut reporter = TskvVnodeCompactionReporter::new(1, 2, MetricCompactionType::Normal);
        reporter.begin();
        for _ in 0..10000 {
            reporter.merge_field_begin();
            reporter.merge_field_end();
            reporter.read_data_begin();
            reporter.read_data_end();
            reporter.write_data_begin();
            reporter.write_data_end();
        }
        reporter.end();
    }

    #[test]
    fn test_fake_vnode_compaction_reporter() {
        let mut reporter = TskvVnodeCompactionReporter::fake();
        reporter.begin();
        for _ in 0..100 {
            reporter.merge_field_begin();
            reporter.merge_field_end();
            reporter.read_data_begin();
            reporter.read_data_end();
            reporter.write_data_begin();
            reporter.write_data_end();
        }
        reporter.end();
    }
}
