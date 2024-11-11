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
    current_measure: Option<(Cow<'static, str>, MetricType)>,
    lines_buffer: &'a mut Vec<LPLine>,
}

impl<'a> LPReporter<'a> {
    pub fn new(lines_buffer: &'a mut Vec<LPLine>) -> Self {
        Self {
            current_measure: None,
            lines_buffer,
        }
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
        self.current_measure = Some((name, metrics_type))
    }

    fn report(&mut self, labels: &Labels, metrics_value: MetricValue) {
        let (name, metric_type) = self
            .current_measure
            .as_ref()
            .expect("Has called Reporter::start()");
        assert_eq!(
            metrics_value.metric_type(),
            *metric_type,
            "metric type error"
        );

        match metrics_value {
            MetricValue::U64Counter(c) => {
                let value = FieldValue::U64(c);
                self.lines_buffer
                    .push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::U64Average(c) => {
                let value = FieldValue::U64(c);
                self.lines_buffer
                    .push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::U64Gauge(g) => {
                let value = FieldValue::U64(g);
                self.lines_buffer
                    .push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::DurationGauge(g) => {
                let value = FieldValue::F64(g.as_secs_f64());
                self.lines_buffer
                    .push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::DurationCounter(c) => {
                let value = FieldValue::F64(c.as_secs_f64());
                self.lines_buffer
                    .push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::U64Histogram(histogram) => {
                for bucket in histogram.buckets {
                    let mut labels = labels.clone();
                    labels.insert(("le", bucket.le.to_string().into()));
                    let value = FieldValue::U64(bucket.count);
                    self.lines_buffer
                        .push(LPLine::new(name.clone(), labels, value));
                }
                let value = FieldValue::U64(histogram.total);
                self.lines_buffer
                    .push(LPLine::new(name.clone(), labels.clone(), value))
            }
            MetricValue::DurationHistogram(histogram) => {
                for bucket in histogram.buckets {
                    let mut labels = labels.clone();
                    labels.insert(("le", bucket.le.as_secs_f64().to_string().into()));
                    // sum type must be same as count type
                    // so this is f64
                    let value = FieldValue::F64(bucket.count as f64);
                    self.lines_buffer
                        .push(LPLine::new(name.clone(), labels, value));
                }
                let value = FieldValue::F64(histogram.total.as_secs_f64());
                self.lines_buffer
                    .push(LPLine::new(name.clone(), labels.clone(), value));
            }
            MetricValue::Null => (),
        }
    }

    fn stop(&mut self) {}
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use metrics::metric_value::{HistogramValue, ValueBucket};
    use metrics::DURATION_MAX;

    use super::*;

    #[derive(Debug)]
    struct LinePartialComparator<'a>(Line<'a>);

    impl<'a> PartialEq for LinePartialComparator<'a> {
        fn eq(&self, other: &Self) -> bool {
            self.0.table == other.0.table
                && self.0.tags == other.0.tags
                && self.0.fields == other.0.fields
        }
    }

    #[test]
    #[rustfmt::skip]
    fn test_lp_reporter() {
        let mut lines_buffer = vec![];
        let mut reporter = LPReporter::new(&mut lines_buffer);

        // Report U64Counter
        let mut labels_1 = Labels::default();
        labels_1.insert(("t1_1", "t1_a".into()));
        labels_1.insert(("t1_2", "t1_b".into()));
        reporter.start("m1".into(), "d1".into(), MetricType::U64Counter);
        reporter.report(&labels_1, MetricValue::U64Counter(1));
        reporter.report(&labels_1, MetricValue::U64Counter(2));
        reporter.stop();
        // Report DurationHistogram
        let mut labels_2 = Labels::default();
        labels_2.insert(("t2_1", "t2_a".into()));
        labels_2.insert(("t2_2", "t2_b".into()));
        let buckets_2 = vec![
            ValueBucket { le: Duration::from_secs(1), count: 1 },
            ValueBucket { le: Duration::from_secs(10), count: 1 },
            ValueBucket { le: Duration::from_secs(100), count: 1 },
            ValueBucket { le: DURATION_MAX, count: 1 },
        ];
        reporter.start("m2".into(), "d2".into(), MetricType::DurationHistogram);
        reporter.report(&labels_2, MetricValue::DurationHistogram(HistogramValue {
            total: Duration::from_secs(3 + 30 + 300), buckets: buckets_2.clone(),
        }));
        reporter.stop();

        for line in lines_buffer.iter() {
            println!("{line:?}");
        }

        let lines = vec![
            Line::new(
                "m1".into(), vec![("t1_1".into(), "t1_a".into()), ("t1_2".into(), "t1_b".into())], vec![(Cow::Borrowed("value"), FieldValue::U64(1))], 0,
            ),
            Line::new(
                "m1".into(), vec![("t1_1".into(), "t1_a".into()), ("t1_2".into(), "t1_b".into())], vec![(Cow::Borrowed("value"), FieldValue::U64(2))], 0,
            ),
            Line::new(
                "m2".into(), vec![("le".into(), "1".into()), ("t2_1".into(), "t2_a".into()), ("t2_2".into(), "t2_b".into())], vec![(Cow::Borrowed("value"), FieldValue::F64(1.0))], 0,
            ),
            Line::new(
                "m2".into(), vec![("le".into(), "10".into()), ("t2_1".into(), "t2_a".into()), ("t2_2".into(), "t2_b".into())], vec![(Cow::Borrowed("value"), FieldValue::F64(1.0))], 0,
            ),
            Line::new(
                "m2".into(), vec![("le".into(), "100".into()), ("t2_1".into(), "t2_a".into()), ("t2_2".into(), "t2_b".into())], vec![(Cow::Borrowed("value"), FieldValue::F64(1.0))], 0,
            ),
            Line::new(
                "m2".into(), vec![
                    ("le".into(), DURATION_MAX.as_secs_f64().to_string().into()),
                    ("t2_1".into(), "t2_a".into()),
                    ("t2_2".into(), "t2_b".into())
                ], vec![(Cow::Borrowed("value"), FieldValue::F64(1.0))], 0,
            ),
            Line::new(
                "m2".into(), vec![("t2_1".into(), "t2_a".into()), ("t2_2".into(), "t2_b".into())], vec![(Cow::Borrowed("value"), FieldValue::F64(333.0))], 0,
            ),
        ];
        assert_eq!(lines_buffer.len(), lines.len());
        for (lp_line, line_exp) in lines_buffer.into_iter().zip(lines.into_iter()) {
            assert_eq!(
                LinePartialComparator(lp_line.to_line()),
                LinePartialComparator(line_exp)
            );
        }
    }
}
