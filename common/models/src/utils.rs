use std::{
    cmp::Ordering,
    sync::atomic::AtomicU64,
    time::{SystemTime, UNIX_EPOCH},
};

const LOW_40BIT_MASK: u64 = (0x01 << 40) - 1;
const HIGH_24BIT_MASK: u64 = ((0x01 << 24) - 1) << 40;

pub fn split_id(id: u64) -> (u32, u64) {
    (((id & HIGH_24BIT_MASK) >> 40) as u32, id & LOW_40BIT_MASK)
}

pub fn unite_id(hash_id: u64, incr_id: u64) -> u64 {
    hash_id << 40 | (incr_id & LOW_40BIT_MASK)
}

pub fn and_u64(arr1: &[u64], arr2: &[u64]) -> Vec<u64> {
    let mut len = arr1.len();
    if len > arr2.len() {
        len = arr2.len();
    }

    let mut i = 0;
    let mut j = 0;
    let mut result = Vec::with_capacity(len);

    loop {
        if i >= arr1.len() || j >= arr2.len() {
            break;
        }

        match (arr1[i] & LOW_40BIT_MASK).cmp(&(arr2[j] & LOW_40BIT_MASK)) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                result.push(arr1[i]);
                i += 1;
                j += 1;
            }
        }
    }

    result
}

pub fn or_u64(arr1: &[u64], arr2: &[u64]) -> Vec<u64> {
    let mut i = 0;
    let mut j = 0;
    let mut result = Vec::with_capacity(arr1.len() + arr2.len());

    loop {
        if i >= arr1.len() || j >= arr2.len() {
            break;
        }

        match (arr1[i] & LOW_40BIT_MASK).cmp(&(arr2[j] & LOW_40BIT_MASK)) {
            Ordering::Less => {
                result.push(arr1[i]);
                i += 1;
            }
            Ordering::Greater => {
                result.push(arr2[j]);
                j += 1;
            }
            Ordering::Equal => {
                result.push(arr1[i]);
                i += 1;
                j += 1;
            }
        }
    }

    if i < arr1.len() {
        result.extend_from_slice(&arr1[i..]);
    }

    if j < arr2.len() {
        result.extend_from_slice(&arr2[j..]);
    }

    result
}

pub fn now_timestamp() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(n) => n.as_millis() as u64,
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
