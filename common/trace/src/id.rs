use std::num::{NonZeroU128, NonZeroU64};

use rand::Rng;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TraceId(pub NonZeroU128);

impl TraceId {
    pub fn new(val: u128) -> Option<Self> {
        Some(Self(NonZeroU128::new(val)?))
    }

    pub fn get(self) -> u128 {
        self.0.get()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SpanId(pub NonZeroU64);

impl SpanId {
    pub fn new(val: u64) -> Option<Self> {
        Some(Self(NonZeroU64::new(val)?))
    }

    pub fn gen() -> Self {
        Self(rand::thread_rng().gen())
    }

    pub fn get(self) -> u64 {
        self.0.get()
    }
}
