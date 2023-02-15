use config::CountBucketConfing;
use serde::{Deserialize, Serialize};

#[derive(Default, Copy, Clone, Deserialize, Serialize, Debug)]
pub struct CountBucketBuilder {
    count: i64,
    max_count: Option<i64>,
}

impl CountBucketBuilder {
    pub fn initial(&mut self, initial: i64) {
        self.count = initial;
    }
    pub fn max(&mut self, max: i64) {
        self.max_count = Some(max)
    }
    pub fn build(&self) -> CountBucket {
        CountBucket {
            count: self.count,
            max_count: self.max_count,
        }
    }
}

impl From<&CountBucketConfing> for CountBucket {
    fn from(value: &CountBucketConfing) -> Self {
        Self {
            count: value.initial,
            max_count: value.max,
        }
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CountBucket {
    count: i64,
    max_count: Option<i64>,
}

impl CountBucket {
    pub fn new(max_count: Option<i64>) -> Self {
        Self {
            count: 0,
            max_count,
        }
    }

    pub fn new_with_init(init: i64, max_count: Option<i64>) -> Self {
        Self {
            count: init,
            max_count,
        }
    }

    pub fn inc(&mut self, val: i64) {
        self.count += val;
    }

    pub fn dec(&mut self, val: i64) {
        self.count -= val;
    }

    #[allow(dead_code)]
    pub fn fetch(&self) -> i64 {
        self.count
    }

    #[allow(dead_code)]
    pub fn max(&self) -> Option<i64> {
        self.max_count
    }
}
