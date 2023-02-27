use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::metric_type::MetricType;
use crate::metric_value::MetricValue;
use crate::{CreateMetricRecorder, MetricRecorder};

#[derive(Debug, Clone, Default)]
pub struct U64Gauge {
    state: Arc<AtomicU64>,
}

impl U64Gauge {
    pub fn set(&self, value: u64) {
        self.state.store(value, Ordering::Relaxed);
    }

    pub fn inc(&self, delta: u64) {
        self.state.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn dec(&self, delta: u64) {
        self.state.fetch_sub(delta, Ordering::Relaxed);
    }

    pub fn fetch(&self) -> u64 {
        self.state.load(Ordering::Relaxed)
    }
}

impl MetricRecorder for U64Gauge {
    type Recorder = Self;

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn metric_type() -> MetricType {
        MetricType::U64Gauge
    }

    fn value(&self) -> MetricValue {
        MetricValue::U64Gauge(self.fetch())
    }
}

pub trait Gauge {
    fn fetch(&self) -> u64;
}

impl Debug for GaugeWrap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "gauge closure")
    }
}

impl CreateMetricRecorder for GaugeWrap {
    type Options = ();

    fn create(_: &Self::Options) -> Self {
        panic!("unsupported")
    }
}

impl MetricRecorder for GaugeWrap {
    type Recorder = Self;
    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }
    fn metric_type() -> MetricType {
        MetricType::U64Gauge
    }
    fn value(&self) -> MetricValue {
        match (self.gauge)() {
            Some(v) => MetricValue::U64Gauge(v),
            None => MetricValue::Null,
        }
    }
}

#[derive(Clone)]
pub struct GaugeWrap {
    gauge: Arc<dyn Fn() -> Option<u64>>,
}

impl GaugeWrap {
    pub fn new(gauge: Arc<dyn Fn() -> Option<u64>>) -> Self {
        Self { gauge }
    }

    pub fn gauge(&self) -> Option<u64> {
        (self.gauge)()
    }
}
