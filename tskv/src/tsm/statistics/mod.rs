use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValueStatistics<T> {
    min: Option<T>,
    max: Option<T>,
    distinct_count: Option<u64>,
    null_count: u64,
}

impl<T> ValueStatistics<T> {
    pub fn new(
        min: Option<T>,
        max: Option<T>,
        distinct_count: Option<u64>,
        null_count: u64,
    ) -> Self {
        Self {
            min,
            max,
            distinct_count,
            null_count,
        }
    }

    pub fn min(&self) -> &Option<T> {
        &self.min
    }

    pub fn max(&self) -> &Option<T> {
        &self.max
    }

    pub fn distinct_count(&self) -> Option<u64> {
        self.distinct_count
    }

    pub fn null_count(&self) -> u64 {
        self.null_count
    }
}
