//! # Example
//!
//! ```rust
//! use metrics::histogram::{U64Histogram, U64HistogramOptions};
//! use metrics::label::Labels;
//! use metrics::metric_register::MetricsRegister;
//! use metrics::prom_reporter::PromReporter;
//! use metrics::reporter::Reporter;
//! use metrics::Measure;
//!
//! fn example()  {
//!    let register = std::sync::Arc::new(MetricsRegister::default());
//!
//!    let histogram_buckets = vec![1, 10, 20, 30, 40, 50, 60, 70, 80, 90];
//!    let histogram_options = U64HistogramOptions::new(histogram_buckets);
//!    let metric_name = "example";
//!    let metric_description = "example metrics";
//!    // Create a new metric with the given name, description, and options.
//!    let example_metric = register.register_metric::<U64Histogram>(metric_name, metric_description, histogram_options);
//!    // Get the recorder of the metric with labels `database=test,tenant=cnosdb`.
//!    let metric_labels: Labels = [("database", "test"), ("tenant", "cnosdb")].into();
//!    let histogram = example_metric.recorder(metric_labels.clone());
//!
//!    // Store some values in the histogram.
//!    histogram.record(5);
//!    histogram.record(15);
//!
//!    // Create a buffer and a reporter to fill the buffer.
//!    let mut buffer = Vec::new();
//!    let mut reporter = PromReporter::new(&mut buffer);
//!
//!    // Report the metric to reporter.
//!    example_metric.report(&mut reporter);
//!
//!    // Report all registered metrics to reporter.
//!    register.report(&mut reporter);
//!
//!    // Release the metric.
//!    example_metric.remove(metric_labels);
//! }
//! ```

pub mod average;
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

/// Reportable metrics. Downcast to Metric<T> in further use.
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
