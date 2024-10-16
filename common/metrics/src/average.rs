use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::metric_type::MetricType;
use crate::metric_value::MetricValue;
use crate::MetricRecorder;

#[derive(Clone, Default, Debug)]
pub struct U64Average {
    count: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
}

impl U64Average {
    pub fn add(&self, value: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(value, Ordering::Relaxed);
    }

    pub fn average(&self) -> u64 {
        let count = self.count.load(Ordering::Relaxed);
        let total = self.total.load(Ordering::Relaxed);

        self.clear();
        total / (count + 1)
    }

    fn clear(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.total.store(0, Ordering::Relaxed);
    }
}

impl MetricRecorder for U64Average {
    type Recorder = Self;

    fn recorder(&self) -> Self::Recorder {
        self.clone()
    }

    fn metric_type() -> MetricType {
        MetricType::U64Average
    }

    fn value(&self) -> MetricValue {
        MetricValue::U64Average(self.average())
    }
}
