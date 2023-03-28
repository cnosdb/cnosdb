use std::fmt::Debug;

use metrics::label::Labels;
use metrics::metric_type::MetricType;
use metrics::metric_value::MetricValue;
use metrics::reporter::Reporter;
use protocol_parser::lines_convert::parse_lines_to_points;
use protocol_parser::Line;
use protos::FieldValue;

#[derive(Debug)]
struct LPLine {
    measure: &'static str,
    labels: Labels,
    value: FieldValue,
}
impl LPLine {
    pub fn to_line(&self) -> Line {
        let tags = self
            .labels
            .0
            .iter()
            .map(|(k, v)| (*k, v.as_ref()))
            .collect();

        Line::new(
            self.measure,
            tags,
            vec![("value", self.value.clone())],
            chrono::Utc::now().timestamp_nanos(),
        )
    }
}

#[derive(Debug)]
pub struct LPReporter<'a> {
    db: &'a str,
    current_measure: Option<(Vec<LPLine>, &'static str, MetricType)>,
    points_buffer: &'a mut Vec<Vec<u8>>,
}

impl<'a> LPReporter<'a> {
    pub fn new(db: &'a str, points_buffer: &'a mut Vec<Vec<u8>>) -> Self {
        Self {
            db,
            current_measure: None,
            points_buffer,
        }
    }
}

impl<'a> Reporter for LPReporter<'a> {
    fn start(&mut self, name: &'static str, _description: &'static str, metrics_type: MetricType) {
        if metrics_type.eq(&MetricType::UnTyped) {
            return;
        }
        self.current_measure = Some((vec![], name, metrics_type))
    }
    fn report(&mut self, label: &Labels, metrics_value: MetricValue) {
        let (lines, name, metric_type) = self.current_measure.as_mut().expect("start reporter");
        assert_eq!(
            metrics_value.metric_type(),
            *metric_type,
            "metric type error"
        );
        let field_value = match metrics_value {
            MetricValue::U64Counter(c) => FieldValue::U64(c),
            MetricValue::U64Gauge(g) => FieldValue::U64(g),
            MetricValue::DurationGauge(g) => FieldValue::F64(g.as_secs_f64()),
            MetricValue::DurationCounter(c) => FieldValue::F64(c.as_secs_f64()),
            MetricValue::U64Histogram(_h) => {
                todo!()
            }
            MetricValue::DurationHistogram(_h) => {
                todo!()
            }
            MetricValue::Null => return,
        };

        let line = LPLine {
            measure: name,
            labels: label.clone(),
            value: field_value,
        };
        lines.push(line);
    }

    fn stop(&mut self) {
        if let Some((lines, _name, _metrics_type)) = &self.current_measure {
            let lines = lines.iter().map(|l| l.to_line()).collect::<Vec<_>>();
            let points = parse_lines_to_points(self.db, &lines);
            self.points_buffer.push(points);
        }
    }
}
