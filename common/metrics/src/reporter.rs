use std::fmt::Debug;

use crate::label::Labels;
use crate::metric_type::MetricType;
use crate::metric_value::MetricValue;

pub trait Reporter: Debug {
    fn start(&mut self, name: &'static str, description: &'static str, metrics_type: MetricType);
    fn report(&mut self, label: &Labels, metrics_value: MetricValue);
    fn stop(&mut self);
}
