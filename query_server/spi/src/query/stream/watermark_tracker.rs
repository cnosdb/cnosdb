use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

pub type WatermarkTrackerRef = Arc<WatermarkTracker>;

#[derive(Default, Debug)]
pub struct WatermarkTracker {
    global_watermark_ns: AtomicI64,
    // _operator_to_watermark_map: RwLock<HashMap<usize, i64>>,
}

impl WatermarkTracker {
    pub fn current_watermark_ns(&self) -> i64 {
        self.global_watermark_ns.load(Ordering::Relaxed)
    }

    pub fn update_watermark(&self, event_time: i64) {
        // TODO
        self.global_watermark_ns
            .store(event_time, Ordering::Relaxed);
    }
}
