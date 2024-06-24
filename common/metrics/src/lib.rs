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

pub use duration::DURATION_MAX;

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
