use std::borrow::Cow;
use std::fmt::Debug;

use metrics::label::Labels;
use metrics::metric_type::MetricType;
use metrics::metric_value::MetricValue;
use metrics::reporter::Reporter;
use models::utils::now_timestamp_nanos;
use protocol_parser::Line;
use protos::FieldValue;

#[derive(Debug, Clone)]
pub struct LPLine {
    measure: Cow<'static, str>,
    labels: Labels,
    value: FieldValue,
}

impl LPLine {
    pub fn new(measure: impl Into<Cow<'static, str>>, tags: Labels, value: FieldValue) -> Self {
        Self {
            measure: measure.into(),
            labels: tags,
            value,
        }
    }

    pub fn to_line(&self) -> Line {
        let tags = self
            .labels
            .0
            .iter()
            .map(|(k, v)| (Cow::Borrowed(*k), Cow::Borrowed(&**v)))
            .collect();

        Line::new(
            Cow::Borrowed(&*self.measure),
            tags,
            vec![(Cow::Borrowed("value"), self.value.clone())],
            now_timestamp_nanos(),
        )
    }
}

#[derive(Debug)]
pub struct LPReporter<'a> {
    current_measure: Option<(Vec<LPLine>, Cow<'static, str>, MetricType)>,
    points_buffer: &'a mut Vec<Vec<LPLine>>,
}

impl<'a> LPReporter<'a> {
    pub fn new(points_buffer: &'a mut Vec<Vec<LPLine>>) -> Self {
        Self {
            current_measure: None,
            points_buffer,
        }
    }

    pub fn current_measure(&self) -> Option<&(Vec<LPLine>, Cow<'static, str>, MetricType)> {
        self.current_measure.as_ref()
    }
}

impl<'a> Reporter for LPReporter<'a> {
    fn start(
        &mut self,
        name: Cow<'static, str>,
        _description: Cow<'static, str>,
        metrics_type: MetricType,
    ) {
        if metrics_type.eq(&MetricType::UnTyped) {
            return;
        }
        self.current_measure = Some((vec![], name, metrics_type))
    }
    fn report(&mut self, labels: &Labels, metrics_value: MetricValue) {
        let (lines, name, metric_type) = self.current_measure.as_mut().expect("start reporter");
        assert_eq!(
            metrics_value.metric_type(),
            *metric_type,
            "metric type error"
        );

        match metrics_value {
            MetricValue::U64Counter(c) => {
                let value = FieldValue::U64(c);
                lines.push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::U64Gauge(g) => {
                let value = FieldValue::U64(g);
                lines.push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::DurationGauge(g) => {
                let value = FieldValue::F64(g.as_secs_f64());
                lines.push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::DurationCounter(c) => {
                let value = FieldValue::F64(c.as_secs_f64());
                lines.push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::U64Histogram(histogram) => {
                for bucket in histogram.buckets {
                    let mut labels = labels.clone();
                    labels.insert(("le", bucket.le.to_string().into()));
                    let value = FieldValue::U64(bucket.count);
                    lines.push(LPLine::new(name.clone(), labels, value));
                }
                let value = FieldValue::U64(histogram.total);
                lines.push(LPLine::new(name.clone(), labels.clone(), value))
            }
            MetricValue::DurationHistogram(histogram) => {
                for bucket in histogram.buckets {
                    let mut labels = labels.clone();
                    labels.insert(("le", bucket.le.as_secs_f64().to_string().into()));
                    // sum type must be same as count type
                    // so this is f64
                    let value = FieldValue::F64(bucket.count as f64);
                    lines.push(LPLine::new(name.clone(), labels, value));
                }
                let value = FieldValue::F64(histogram.total.as_secs_f64());
                lines.push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::Null => (),
        }
    }

    fn stop(&mut self) {
        if let Some((lines, _name, _metrics_type)) = &self.current_measure {
            self.points_buffer.push(lines.clone());
        }
    }
}
