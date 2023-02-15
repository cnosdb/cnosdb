use std::sync::atomic::AtomicU64;
use std::time::{SystemTime, UNIX_EPOCH};

const LOW_32BIT_MASK: u64 = (0x01 << 32) - 1;
const HIGH_32BIT_MASK: u64 = ((0x01 << 32) - 1) << 32;

pub fn split_id(id: u64) -> (u32, u32) {
    (
        ((id & HIGH_32BIT_MASK) >> 32) as u32,
        (id & LOW_32BIT_MASK) as u32,
    )
}

pub fn unite_id(hash_id: u32, incr_id: u32) -> u64 {
    let high = (hash_id as u64) << 32;
    let low = (incr_id & LOW_32BIT_MASK as u32) as u64;

    high | low
}

pub fn now_timestamp() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_nanos() as i64,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

pub fn to_str(arr: &[u8]) -> String {
    String::from_utf8(arr.to_vec()).unwrap()
}

pub fn min_num<T: std::cmp::PartialOrd>(a: T, b: T) -> T {
    if a < b {
        a
    } else {
        b
    }
}

pub fn max_num<T: std::cmp::PartialOrd>(a: T, b: T) -> T {
    if a > b {
        a
    } else {
        b
    }
}

#[derive(Default)]
pub struct SeqIdGenerator {
    next_id: AtomicU64,
}

impl SeqIdGenerator {
    pub fn new(start: u64) -> Self {
        Self {
            next_id: AtomicU64::new(start),
        }
    }

    pub fn next_id(&self) -> u64 {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}
