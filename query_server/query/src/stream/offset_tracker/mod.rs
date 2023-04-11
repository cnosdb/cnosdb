use std::cmp;
use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use spi::query::datasource::stream::Offset;

pub type OffsetTrackerRef = Arc<OffsetTracker>;

#[derive(Clone)]
pub struct OffsetTracker {
    processed_offsets: Arc<RwLock<HashMap<String, Offset>>>,
    available_offsets: Arc<RwLock<HashMap<String, Offset>>>,
}

impl OffsetTracker {
    pub fn new() -> Self {
        Self {
            processed_offsets: Default::default(),
            available_offsets: Default::default(),
        }
    }

    pub fn has_available_offsets(&self) -> bool {
        !self.available_offsets.read().is_empty()
    }

    pub fn update_available_offset(&self, topic: String, offset: i64) {
        let mut available_offsets = self.available_offsets.write();
        let current_offset = self
            .processed_offsets
            .read()
            .get(&topic)
            .cloned()
            .unwrap_or(i64::MIN);
        if offset > current_offset {
            available_offsets.insert(topic, offset);
        }
    }

    pub fn available_offsets(&self) -> HashMap<String, (Option<Offset>, Offset)> {
        let source_to_range = self
            .available_offsets
            .read()
            .iter()
            .map(|(id, offset)| {
                let start = self
                    .processed_offsets
                    .read()
                    .get(id)
                    .cloned()
                    // 防止处理重复数据所以+1
                    .map(|e| e + 1);
                (id.clone(), (start, *offset))
            })
            .collect::<HashMap<String, (Option<Offset>, Offset)>>();

        source_to_range
    }

    pub fn commit(&self, watermark_ns: i64) {
        // TODO 因为目前tskv表使用当前时间作为最新的可用offset，所以这里需要使用watermark_ns来保证不会丢失数据
        self.available_offsets
            .read()
            .iter()
            .for_each(|(id, offset)| {
                let offset = cmp::min(watermark_ns, *offset);
                self.processed_offsets.write().insert(id.clone(), offset);
            });
        // self.processed_offsets
        // .write()
        // .extend(self.available_offsets.read().clone());

        self.available_offsets.write().clear();
    }
}

impl Default for OffsetTracker {
    fn default() -> Self {
        Self::new()
    }
}
