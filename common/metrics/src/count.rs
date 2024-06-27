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

#[cfg(test)]
mod test {
    use std::thread::{spawn, JoinHandle};

    use super::*;

    #[test]
    fn test_u64_counter() {
        let counter = Arc::new(U64Counter::default());
        // 3 threads, each increment 10 times
        let join_handles: Vec<JoinHandle<()>> = (0..3)
            .map(|_| {
                let counter = counter.clone();
                spawn(move || {
                    for _ in 0..10 {
                        counter.inc(1);
                    }
                })
            })
            .collect();
        for jh in join_handles {
            jh.join().unwrap();
        }
        assert_eq!(counter.fetch(), 30); // 3 * 10
    }
}
