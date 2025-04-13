use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use metrics::duration::{DurationHistogram, DurationHistogramOptions};
use metrics::label::Labels;
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;
use metrics::DURATION_MAX;
use models::meta_data::{NodeId, VnodeId};

static UNIT: &str = "unit";
static SAMPLE_SIZE: &str = "sample_size";
static NODE_ID: &str = "node_id";
static VNODE_ID: &str = "vnode_id";
static TYPE: &str = "type";

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl TimeUnit {
    /// Parse a duration to a f64 number.
    ///
    /// # Example
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// let sec_1 = TimeUnit::Second.parse_f64(Duration::from_secs(1)); // 1.0
    /// assert!(0.999 < sec_1 && sec_1 < 1.001);
    /// let sec_1_021 = TimeUnit::Second.parse_f64(Duration::from_millis(1021)); // 1.021
    /// assert!(1.020 < sec_1_021 && sec_1_021 < 1.022);
    /// let ms_1021 = TimeUnit::Millisecond.parse_f64(Duration::from_millis(1021)); // 1021
    /// assert!(1020.999 < ms_1021 && ms_1021 < 1021.001);
    /// let ns_100 = TimeUnit::Nanosecond.parse_f64(Duration::from_nanos(100)); // 100
    /// assert!(99.999 < ns_100 && ns_100 < 100.001);
    /// ```
    pub fn parse_f64(&self, duration: Duration) -> f64 {
        match self {
            Self::Second => duration.as_secs_f64(),
            Self::Millisecond => duration.as_millis() as f64,
            Self::Microsecond => duration.as_micros() as f64,
            Self::Nanosecond => duration.as_nanos() as f64,
        }
    }

    const S_MS: f64 = 1_000.0;
    const S_US: f64 = 1_000_000.0;
    const S_NS: f64 = 1_000_000_000.0;

    /// Parse a f64 number to a duration. If the number is negative or overflow, return None.
    ///
    /// # Example
    /// ```ignore
    /// assert_eq!(TimeUnit::Second.parse_duration(1.0), Some(Duration::from_secs(1)));
    /// assert_eq!(TimeUnit::Second.parse_duration(1.021), Some(Duration::from_millis(1021)));
    /// assert_eq!(TimeUnit::Millisecond.parse_duration(1021.0), Some(Duration::from_millis(1021)));
    /// assert_eq!(TimeUnit::Nanosecond.parse_duration(100.0), Some(Duration::from_nanos(100)));
    /// ```
    pub fn parse_duration(&self, n: f64) -> Option<Duration> {
        let s = match self {
            TimeUnit::Second => n,
            TimeUnit::Millisecond => n / Self::S_MS,
            TimeUnit::Microsecond => n / Self::S_US,
            TimeUnit::Nanosecond => n / Self::S_NS,
        };
        Duration::try_from_secs_f64(s).ok()
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Second => "s",
            Self::Millisecond => "ms",
            Self::Microsecond => "us",
            Self::Nanosecond => "ns",
        }
    }
}

impl std::fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CompactionType {
    /// Normal compaction triggered by system.
    Normal,
    /// Manual compaction triggered by user.
    Manual,
}

impl CompactionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompactionType::Normal => "normal",
            CompactionType::Manual => "manual",
        }
    }
}

impl std::fmt::Display for CompactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

pub trait DurationMetricRecorder: Send {
    fn begin(&mut self);

    fn end(&mut self, is_finished: bool);
}

struct FakeDurationMetricsRecorder;

impl DurationMetricRecorder for FakeDurationMetricsRecorder {
    fn begin(&mut self) {}

    fn end(&mut self, _: bool) {}
}

/// All metrics of a vnode compaction.
pub struct VnodeCompactionMetrics {
    total: Box<dyn DurationMetricRecorder>,
    read: Box<dyn DurationMetricRecorder>,
    merge: Box<dyn DurationMetricRecorder>,
    write: Box<dyn DurationMetricRecorder>,
}

impl DurationMetricRecorder for VnodeCompactionMetrics {
    fn begin(&mut self) {
        self.total.begin();
    }

    fn end(&mut self, is_finished: bool) {
        if is_finished {
            self.total.end(is_finished);
        }
        self.read.end(is_finished);
        self.merge.end(is_finished);
        self.write.end(is_finished);
    }
}

impl VnodeCompactionMetrics {
    /// Create a new metrics recorder, if `collect_compaction_metrics` is false, only record the total time cost of a vnode compaction.
    /// Otherwise, record the total time cost, read, merge, and write time cost of a vnode compaction.
    pub fn new(
        registry: &Arc<MetricsRegister>,
        node_id: NodeId,
        vnode_id: VnodeId,
        compaction_type: CompactionType,
        collect_compaction_metrics: bool,
    ) -> Self {
        if collect_compaction_metrics {
            Self::new_standard(registry, node_id, vnode_id, compaction_type)
        } else {
            Self::new_minimal(registry, node_id, vnode_id, compaction_type)
        }
    }

    const DEFAULT_TIME_UNIT: TimeUnit = TimeUnit::Microsecond;
    const DEFAULT_SAMPLE_SIZE: usize = 10_000;

    /// Create a standard metrics recorder, record the total time cost, read, merge, and write time cost of a vnode compaction.
    pub fn new_standard(
        registry: &Arc<MetricsRegister>,
        node_id: NodeId,
        vnode_id: VnodeId,
        compaction_type: CompactionType,
    ) -> Self {
        let total = MetricRecorderOptions::new(
            Self::DEFAULT_TIME_UNIT,
            1,
            node_id,
            vnode_id,
            compaction_type,
        )
        .build(
            registry,
            "compaction_total".to_string(),
            format!("The total time taken of compaction: {compaction_type}({node_id}.{vnode_id})"),
            total_duration_buckets(),
        );

        let read = MetricRecorderOptions::new(
            Self::DEFAULT_TIME_UNIT,
            Self::DEFAULT_SAMPLE_SIZE,
            node_id,
            vnode_id,
            compaction_type,
        )
        .build_aggregated(
            registry,
            (
                "compaction_read_avg".to_string(),
                format!("The average time taken to read data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            (
                "compaction_read_min".to_string(),
                format!("The minimum time taken to read data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            (
                "compaction_read_max".to_string(),
                format!("The maximum time taken to read data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            read_duration_buckets(),
        );
        let merge = MetricRecorderOptions::new(
            Self::DEFAULT_TIME_UNIT,
            Self::DEFAULT_SAMPLE_SIZE,
            node_id,
            vnode_id,
            compaction_type,
        )
        .build_aggregated(
            registry,
            (
                "compaction_merge_avg".to_string(),
                format!("The average time taken to merge data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            (
                "compaction_merge_min".to_string(),
                format!("The minimum time taken to merge data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            (
                "compaction_merge_max".to_string(),
                format!("The maximum time taken to merge data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            merge_duration_buckets(),
        );
        let write = MetricRecorderOptions::new(
            Self::DEFAULT_TIME_UNIT,
            Self::DEFAULT_SAMPLE_SIZE,
            node_id,
            vnode_id,
            compaction_type,
        )
        .build_aggregated(
            registry,
            (
                "compaction_write_avg".to_string(),
                format!("The average time taken to write data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            (
                "compaction_write_min".to_string(),
                format!("The minimum time taken to write data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            (
                "compaction_write_max".to_string(),
                format!("The maximum time taken to write data {SAMPLE_SIZE} times in compaction: {compaction_type}({node_id}.{vnode_id})"),
            ),
            write_duration_buckets(),
        );
        Self {
            total: Box::new(total),
            read: Box::new(read),
            merge: Box::new(merge),
            write: Box::new(write),
        }
    }

    /// Create a minimal metrics recorder, only record the total time cost of a vnode compaction.
    pub fn new_minimal(
        registry: &Arc<MetricsRegister>,
        node_id: NodeId,
        vnode_id: VnodeId,
        compaction_type: CompactionType,
    ) -> Self {
        let total = MetricRecorderOptions::new(
            Self::DEFAULT_TIME_UNIT,
            1,
            node_id,
            vnode_id,
            compaction_type,
        )
        .build(
            registry,
            "compaction_total".to_string(),
            format!("The total time taken of compaction: {compaction_type}({node_id}.{vnode_id})"),
            total_duration_buckets(),
        );
        Self {
            total: Box::new(total),
            read: Box::new(FakeDurationMetricsRecorder),
            merge: Box::new(FakeDurationMetricsRecorder),
            write: Box::new(FakeDurationMetricsRecorder),
        }
    }

    /// Create a fake metrics recorder.
    #[allow(dead_code)]
    pub fn fake() -> Self {
        Self {
            total: Box::new(FakeDurationMetricsRecorder),
            read: Box::new(FakeDurationMetricsRecorder),
            merge: Box::new(FakeDurationMetricsRecorder),
            write: Box::new(FakeDurationMetricsRecorder),
        }
    }

    pub fn read_begin(&mut self) {
        self.read.begin();
    }

    pub fn read_end(&mut self) {
        self.read.end(false);
    }

    pub fn merge_begin(&mut self) {
        self.merge.begin();
    }

    pub fn merge_end(&mut self) {
        self.merge.end(false);
    }

    pub fn write_begin(&mut self) {
        self.write.begin();
    }

    pub fn write_end(&mut self) {
        self.write.end(false);
    }
}

impl Drop for VnodeCompactionMetrics {
    fn drop(&mut self) {
        self.end(true);
    }
}

struct MetricRecorderOptions {
    labels: Labels,
    aggregator: Aggregator,
}

impl MetricRecorderOptions {
    pub fn new(
        time_unit: TimeUnit,
        sample_size: usize,
        node_id: u64,
        vnode_id: u32,
        compaction_type: CompactionType,
    ) -> Self {
        let mut labels = Labels::default();
        labels.extend([
            (UNIT, Cow::Owned(time_unit.to_string())),
            (SAMPLE_SIZE, Cow::Owned(sample_size.to_string())),
            (NODE_ID, Cow::Owned(node_id.to_string())),
            (VNODE_ID, Cow::Owned(vnode_id.to_string())),
            (TYPE, Cow::Owned(compaction_type.to_string())),
        ]);

        Self {
            labels,
            aggregator: Aggregator::new(time_unit, sample_size),
        }
    }

    /// Build a metric recorder.
    pub fn build(
        self,
        registry: &Arc<MetricsRegister>,
        metric_name: String,
        metric_desc: String,
        buckets: DurationHistogramOptions,
    ) -> MetricsRecorder {
        let total_metric =
            registry.register_metric::<DurationHistogram>(metric_name, metric_desc, buckets);
        let total_recorder = total_metric.recorder(self.labels.clone());
        MetricsRecorder {
            labels: self.labels,
            metric: total_metric,
            instant: Instant::now(),
            recorder: total_recorder,
        }
    }

    /// Build a metric recorder that records aggregated values(avg, min, max).
    pub fn build_aggregated(
        self,
        registry: &Arc<MetricsRegister>,
        avg: (String, String),
        min: (String, String),
        max: (String, String),
        buckets: DurationHistogramOptions,
    ) -> AggregatedMetricsRecorder {
        let avg_metric =
            registry.register_metric::<DurationHistogram>(avg.0, avg.1, buckets.clone());
        let avg_recorder = avg_metric.recorder(self.labels.clone());
        let min_metric =
            registry.register_metric::<DurationHistogram>(min.0, min.1, buckets.clone());
        let min_recorder = min_metric.recorder(self.labels.clone());
        let max_metric = registry.register_metric::<DurationHistogram>(max.0, max.1, buckets);
        let max_recorder = max_metric.recorder(self.labels.clone());
        AggregatedMetricsRecorder {
            labels: self.labels,
            avg_metric,
            min_metric,
            max_metric,
            aggregator: self.aggregator,
            avg_recorder,
            min_recorder,
            max_recorder,
        }
    }
}

struct MetricsRecorder {
    labels: Labels,
    metric: Metric<DurationHistogram>,

    instant: Instant,
    recorder: DurationHistogram,
}

unsafe impl Send for MetricsRecorder {}

impl DurationMetricRecorder for MetricsRecorder {
    fn begin(&mut self) {
        self.instant = std::time::Instant::now();
    }

    fn end(&mut self, _is_finished: bool) {
        self.recorder.record(self.instant.elapsed());
    }
}

impl Drop for MetricsRecorder {
    fn drop(&mut self) {
        self.metric.remove(self.labels.clone());
    }
}

struct AggregatedMetricsRecorder {
    labels: Labels,
    avg_metric: Metric<DurationHistogram>,
    min_metric: Metric<DurationHistogram>,
    max_metric: Metric<DurationHistogram>,

    aggregator: Aggregator,
    avg_recorder: DurationHistogram,
    min_recorder: DurationHistogram,
    max_recorder: DurationHistogram,
}

unsafe impl Send for AggregatedMetricsRecorder {}

impl DurationMetricRecorder for AggregatedMetricsRecorder {
    fn begin(&mut self) {
        self.aggregator.goto_now();
    }

    fn end(&mut self, is_finished: bool) {
        if self.aggregator.goto_next_tick(is_finished) {
            self.avg_recorder.record(self.aggregator.avg_to_duration());
            self.min_recorder.record(self.aggregator.min_to_duration());
            self.max_recorder.record(self.aggregator.max_to_duration());
        }
    }
}

impl Drop for AggregatedMetricsRecorder {
    fn drop(&mut self) {
        self.avg_metric.remove(self.labels.clone());
        self.min_metric.remove(self.labels.clone());
        self.max_metric.remove(self.labels.clone());
    }
}

struct Aggregator {
    time_unit: TimeUnit,
    sample_size: usize,

    instant: std::time::Instant,
    cost_buf: Vec<f64>,
    cost_min: f64,
    cost_max: f64,

    /// The avg value of latest state.
    state_avg: f64,
    /// The min value of latest state.
    state_min: f64,
    /// The max value of latest state.
    state_max: f64,
}

impl Aggregator {
    const DEFAULT_MIN_MAX: (f64, f64) = (f64::MAX, 0_f64);

    pub fn new(time_unit: TimeUnit, sample_size: usize) -> Self {
        Self {
            time_unit,
            sample_size,
            instant: Instant::now(),
            cost_buf: Vec::with_capacity(sample_size),
            cost_min: Self::DEFAULT_MIN_MAX.0,
            cost_max: Self::DEFAULT_MIN_MAX.1,
            state_avg: 0_f64,
            state_min: 0_f64,
            state_max: 0_f64,
        }
    }

    /// Update the instant to now.
    pub fn goto_now(&mut self) {
        self.instant = Instant::now();
    }

    /// Update the state if the sample size is reached or `update_state` is true.
    /// Return true if the state is updated.
    pub fn goto_next_tick(&mut self, update_state: bool) -> bool {
        let cost_t = self.time_unit.parse_f64(self.instant.elapsed());
        self.cost_min = self.cost_min.min(cost_t);
        self.cost_max = self.cost_max.max(cost_t);
        self.cost_buf.push(cost_t);
        if update_state || self.cost_buf.len() >= self.sample_size {
            self.update_state();
            return true;
        }

        false
    }

    fn update_state(&mut self) {
        let avg = if !self.cost_buf.is_empty() {
            let sum = self.cost_buf.iter().sum::<f64>();
            let avg = sum / self.cost_buf.len() as f64;
            self.cost_buf.clear();
            avg
        } else {
            0.0
        };
        self.state_avg = avg;
        self.state_min = self.cost_min;
        self.state_max = self.cost_max;
        (self.cost_min, self.cost_max) = Self::DEFAULT_MIN_MAX;
    }

    /// Get the avg value of the latest state, parsed as duration.
    /// If parsing cased overflow, return Duration::MAX.
    pub fn avg_to_duration(&self) -> Duration {
        self.time_unit
            .parse_duration(self.state_avg)
            .unwrap_or(Duration::MAX)
    }

    /// Get the min value of the latest state, parsed as duration.
    /// If parsing cased overflow, return Duration::MAX.
    pub fn min_to_duration(&self) -> Duration {
        self.time_unit
            .parse_duration(self.state_min)
            .unwrap_or(Duration::MAX)
    }

    /// Get the max value of the latest state, parsed as duration.
    /// If parsing cased overflow, return Duration::MAX.
    pub fn max_to_duration(&self) -> Duration {
        self.time_unit
            .parse_duration(self.state_max)
            .unwrap_or(Duration::MAX)
    }
}

fn total_duration_buckets() -> DurationHistogramOptions {
    DurationHistogramOptions::new(vec![
        Duration::from_secs(10),
        Duration::from_secs(30),
        Duration::from_secs(60),
        Duration::from_secs(300),
        Duration::from_secs(600),
        Duration::from_secs(1800),
        Duration::from_secs(3600),
        DURATION_MAX,
    ])
}

fn read_duration_buckets() -> DurationHistogramOptions {
    DurationHistogramOptions::new(vec![
        Duration::from_micros(1),
        Duration::from_micros(500),
        Duration::from_millis(1),
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_secs(5),
        Duration::from_secs(10),
        DURATION_MAX,
    ])
}

fn merge_duration_buckets() -> DurationHistogramOptions {
    DurationHistogramOptions::new(vec![
        Duration::from_nanos(250),
        Duration::from_nanos(500),
        Duration::from_micros(1),
        Duration::from_micros(250),
        Duration::from_micros(500),
        Duration::from_millis(1),
        Duration::from_secs(1),
        DURATION_MAX,
    ])
}

fn write_duration_buckets() -> DurationHistogramOptions {
    DurationHistogramOptions::new(vec![
        Duration::from_micros(1),
        Duration::from_micros(500),
        Duration::from_millis(1),
        Duration::from_millis(500),
        Duration::from_secs(1),
        Duration::from_secs(5),
        Duration::from_secs(10),
        DURATION_MAX,
    ])
}

#[derive(Clone, Default)]
pub struct FlushMetrics {
    pub min_seq: u64,
    pub max_seq: u64,
    pub flush_use_time: u64,
    pub flush_index_time: u64,
    pub flush_series_count: u64,
    pub convert_to_page_time: u64,
    pub writer_pages_time: u64,
    pub writer_finish_time: u64,
    pub write_tsm_pages_size: u64,
    pub write_tsm_file_size: u64,
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use std::time::Duration;

    use assert_float_eq::assert_f64_near;

    use super::*;

    #[test]
    fn test_time_unit() {
        {
            let s = TimeUnit::Second;
            assert_eq!(s.as_str(), "s");
            assert_f64_near!(s.parse_f64(Duration::from_secs(1)), 1.0);
            assert_eq!(s.parse_duration(1.0), Some(Duration::from_secs(1)));
            assert_f64_near!(s.parse_f64(Duration::from_secs(1_000)), 1000.0);
            assert_eq!(s.parse_duration(1000.0), Some(Duration::from_secs(1_000)));
            assert_f64_near!(s.parse_f64(Duration::MAX), 18446744073709551200.0);
            assert_eq!(s.parse_duration(u64::MAX as f64), None);
        }
        {
            let ms = TimeUnit::Millisecond;
            assert_eq!(ms.as_str(), "ms");
            assert_f64_near!(ms.parse_f64(Duration::from_millis(1)), 1.0);
            assert_eq!(ms.parse_duration(1.0), Some(Duration::from_millis(1)));
            assert_f64_near!(ms.parse_f64(Duration::from_millis(1_000)), 1000.0);
            assert_eq!(
                ms.parse_duration(1000.0),
                Some(Duration::from_millis(1_000))
            );
        }
        {
            let us = TimeUnit::Microsecond;
            assert_eq!(us.as_str(), "us");
            assert_f64_near!(us.parse_f64(Duration::from_micros(1)), 1.0);
            assert_eq!(us.parse_duration(1.0), Some(Duration::from_micros(1)));
            assert_f64_near!(us.parse_f64(Duration::from_micros(1_000)), 1000.0);
            assert_eq!(
                us.parse_duration(1000.0),
                Some(Duration::from_micros(1_000))
            );
        }
        {
            let ns = TimeUnit::Nanosecond;
            assert_eq!(ns.as_str(), "ns");
            assert_f64_near!(ns.parse_f64(Duration::from_nanos(1)), 1.0);
            assert_eq!(ns.parse_duration(1.0), Some(Duration::from_nanos(1)));
            assert_f64_near!(ns.parse_f64(Duration::from_nanos(1_000)), 1000.0);
            assert_eq!(ns.parse_duration(1000.0), Some(Duration::from_nanos(1_000)));
        }
    }

    #[test]
    fn test_aggregator_group_size_1() {
        let mut aggregator = Aggregator::new(TimeUnit::Millisecond, 1);
        for i in 0..1000 {
            aggregator.goto_now();
            if i % 2 == 0 {
                // Sleep a duration > 10ms, and assume that it < 99ms.
                sleep(Duration::from_millis(10));
                aggregator.goto_next_tick(false);
                let avg = aggregator.state_avg;
                let min = aggregator.state_min;
                let max = aggregator.state_max;
                assert!((10.0..49.0).contains(&avg), "avg: {avg} not as expected");
                assert!((10.0..49.0).contains(&min), "min: {min} not as expected");
                assert!((10.0..49.0).contains(&max), "max: {max} not as expected");
            } else {
                // Sleep a duration > 20ms, and assume that it < 99ms.
                sleep(Duration::from_millis(20));
                aggregator.goto_next_tick(false);
                let avg = aggregator.state_avg;
                let min = aggregator.state_min;
                let max = aggregator.state_max;
                assert!((20.0..99.0).contains(&avg), "avg: {avg} not as expected");
                assert!((20.0..99.0).contains(&min), "min: {min} not as expected");
                assert!((20.0..99.0).contains(&max), "max: {max} not as expected");
            }
        }
    }

    #[test]
    fn test_aggregator_group_size_1000() {
        let sample_size = 1000;
        let mut aggregator = Aggregator::new(TimeUnit::Millisecond, sample_size);
        for i in 0..sample_size {
            aggregator.goto_now();
            if i % 2 == 0 {
                // Sleep a duration > 10ms, and assume that it < 99ms.
                sleep(Duration::from_millis(10));
            } else {
                // Sleep a duration > 20ms, and assume that it < 99ms.
                sleep(Duration::from_millis(20));
            }
            aggregator.goto_next_tick(false);
        }
        let avg = aggregator.state_avg;
        let min = aggregator.state_min;
        let max = aggregator.state_max;
        println!("avg={avg},min={min},max={max}");
        assert!((15.0..99.0).contains(&avg), "avg: {avg} not as expected");
        assert!((10.0..99.0).contains(&min), "min: {min} not as expected");
        assert!((20.0..99.0).contains(&max), "max: {max} not as expected");
    }
}
