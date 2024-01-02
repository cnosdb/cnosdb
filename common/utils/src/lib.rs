pub use bkdr_hash::BkdrHasher;
pub use bloom_filter::BloomFilter;
pub use dedup::{dedup_front_by, dedup_front_by_key};

pub mod backtrace;
pub mod bitset;
mod bkdr_hash;
mod bloom_filter;
mod dedup;

pub mod byte_utils;

#[cfg(unix)]
pub mod pprof_tools;

pub fn to_hex_string(data: &[u8]) -> String {
    format!("{:x?}", data)
}
