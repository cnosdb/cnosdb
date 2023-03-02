use prometheus::proto::{Bucket, Counter, Gauge, Histogram, LabelPair, MetricFamily};
use prometheus::{Encoder, TextEncoder};
use trace::error;

use crate::label::Labels;
use crate::metric_type::MetricType;
use crate::metric_value::MetricValue;
use crate::reporter::Reporter;

#[derive(Debug)]
pub struct PromReporter<'a> {
    metric: Option<MetricFamily>,
    encoder: TextEncoder,
    buffer: &'a mut Vec<u8>,
}

impl<'a> PromReporter<'a> {
    pub fn new(buffer: &'a mut Vec<u8>) -> Self {
        Self {
            metric: None,
            encoder: TextEncoder::new(),
            buffer,
        }
    }
}

impl<'a> Reporter for PromReporter<'a> {
    fn start(&mut self, name: &'static str, description: &'static str, metrics_type: MetricType) {
        let (name, prom_type) = match metrics_type {
            MetricType::U64Gauge => (
                format!("{name}_total"),
                prometheus::proto::MetricType::GAUGE,
            ),
            MetricType::U64Counter => (name.to_string(), prometheus::proto::MetricType::COUNTER),
            MetricType::U64Histogram => {
                (name.to_string(), prometheus::proto::MetricType::HISTOGRAM)
            }

            MetricType::DurationCounter => (
                format!("{name}_seconds_total"),
                prometheus::proto::MetricType::COUNTER,
            ),
            MetricType::DurationGauge => (
                format!("{name}_seconds"),
                prometheus::proto::MetricType::GAUGE,
            ),

            MetricType::DurationHistogram => (
                format!("{name}_seconds"),
                prometheus::proto::MetricType::HISTOGRAM,
            ),
            MetricType::UnTyped => return,
        };
        let mut family = MetricFamily::default();
        family.set_name(name);
        family.set_help(description.to_string());
        family.set_field_type(prom_type);
        self.metric = Some(family)
    }

    fn report(&mut self, labels: &Labels, metrics_value: MetricValue) {
        let family = match self.metric.as_mut() {
            Some(f) => f,
            None => return,
        };

        let metrics = family.mut_metric();

        let mut metric = prometheus::proto::Metric::default();
        metric.set_label(
            labels
                .0
                .iter()
                .map(|(name, value)| {
                    let mut pair = LabelPair::default();
                    pair.set_name(name.to_string());
                    pair.set_value(value.to_string());
                    pair
                })
                .collect(),
        );

        match metrics_value {
            MetricValue::U64Counter(v) => {
                let mut counter = Counter::default();
                counter.set_value(v as f64);
                metric.set_counter(counter)
            }

            MetricValue::U64Gauge(g) => {
                let mut gauge = Gauge::default();
                gauge.set_value(g as f64);
                metric.set_gauge(gauge)
            }

            MetricValue::DurationCounter(v) => {
                let mut counter = Counter::default();
                counter.set_value(v.as_secs_f64());
                metric.set_counter(counter)
            }

            MetricValue::DurationGauge(v) => {
                let mut gauge = Gauge::default();
                gauge.set_value(v.as_secs_f64());
                metric.set_gauge(gauge)
            }

            MetricValue::U64Histogram(v) => {
                let mut histogram = Histogram::default();
                let mut cumulative_count = 0;

                histogram.set_bucket(
                    v.buckets
                        .into_iter()
                        .map(|value| {
                            cumulative_count += value.count;

                            let mut bucket = Bucket::default();
                            let le = match value.le {
                                u64::MAX => f64::INFINITY,
                                v => v as f64,
                            };

                            bucket.set_upper_bound(le);
                            bucket.set_cumulative_count(cumulative_count);
                            bucket
                        })
                        .collect(),
                );

                histogram.set_sample_count(cumulative_count);
                histogram.set_sample_sum(v.total as f64);
                metric.set_histogram(histogram)
            }
            MetricValue::DurationHistogram(v) => {
                let mut histogram = Histogram::default();
                let mut cumulative_count = 0;

                histogram.set_bucket(
                    v.buckets
                        .into_iter()
                        .map(|observation| {
                            cumulative_count += observation.count;

                            let mut bucket = Bucket::default();
                            let le = match observation.le {
                                crate::DURATION_MAX => f64::INFINITY,
                                v => v.as_secs_f64(),
                            };

                            bucket.set_upper_bound(le);
                            bucket.set_cumulative_count(cumulative_count);
                            bucket
                        })
                        .collect(),
                );

                histogram.set_sample_count(cumulative_count);
                histogram.set_sample_sum(v.total.as_secs_f64());
                metric.set_histogram(histogram)
            }

            MetricValue::Null => {
                return;
            }
        }

        metrics.push(metric);
    }

    fn stop(&mut self) {
        if let Some(family) = self.metric.take() {
            if family.get_metric().is_empty() {
                return;
            }
            match self.encoder.encode(&[family], self.buffer) {
                Ok(_) => {}
                Err(e) => error!(%e, "error encoding metric family"),
            }
        }
    }
}
