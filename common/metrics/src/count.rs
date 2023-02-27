use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::metric_type::MetricType;
use crate::metric_value::MetricValue;
use crate::MetricRecorder;

#[derive(Clone, Default, Debug)]
pub struct U64Counter {
    state: Arc<AtomicU64>,
}

impl U64Counter {
    pub fn inc(&self, count: u64) {
        self.state.fetch_add(count, Ordering::Relaxed);
    }

    pub fn inc_one(&self) {
        self.inc(1);
    }

    pub fn fetch(&self) -> u64 {
        self.state.load(Ordering::Relaxed)
    }
}
impl MetricRecorder for U64Counter {
    type Recorder = Self;

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn metric_type() -> MetricType {
        MetricType::U64Counter
    }

    fn value(&self) -> MetricValue {
        MetricValue::U64Counter(self.fetch())
    }
}
