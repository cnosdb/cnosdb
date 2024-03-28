use serde::{Deserialize, Serialize};
use std::hash::Hasher;

use fnv::FnvHasher;
use siphasher::sip::SipHasher13;
use twox_hash::XxHash64;

use crate::bkdr_hash::BkdrHasher;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BloomFilter {
    b: Vec<u8>,
    mask: u64,
}

impl BloomFilter {
    /// Create a new instance of BloomFilter using m bits.
    /// The m should be a power of 2.
    pub fn new(m: u64) -> Self {
        let m = Self::pow2(m);
        let l = m as usize >> 3;
        let b: Vec<u8> = vec![0; l];
        Self { b, mask: m - 1 }
    }

    fn hashers() -> Vec<Box<dyn Hasher>> {
        vec![
            Box::<FnvHasher>::default(),
            Box::<XxHash64>::default(),
            Box::<SipHasher13>::default(),
            Box::<BkdrHasher>::default(),
        ]
    }

    /// Similar to `new()`
    pub fn with_data(data: &[u8]) -> Self {
        let mut b = data.to_vec();
        let l = Self::pow2(b.len() as u64);
        if l != b.len() as u64 {
            b.resize(l as usize, 0);
        }
        let m = l << 3;

        Self { b, mask: m - 1 }
    }

    pub fn insert(&mut self, data: &[u8]) {
        for hasher in Self::hashers().iter_mut() {
            hasher.write(data);
            let hash = hasher.finish();
            let loc = self.location(hash);
            self.b[loc >> 3] |= 1 << (loc & 7);
        }
    }

    pub fn maybe_contains(&self, data: &[u8]) -> bool {
        for hasher in Self::hashers().iter_mut() {
            hasher.write(data);
            let hash = hasher.finish();
            let loc = self.location(hash);
            if self.b[loc >> 3] & (1 << (loc & 7)) == 0 {
                return false;
            }
        }
        true
    }

    pub fn len(&self) -> usize {
        self.b.len()
    }

    pub fn is_empty(&self) -> bool {
        self.b.is_empty()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.b
    }

    fn location(&self, hash: u64) -> usize {
        (hash & self.mask) as usize
    }

    /// Returns the number that is the next highest power of 2.
    fn pow2(num: u64) -> u64 {
        let mut i = 8;
        while i < 1 << 62 {
            if i >= num {
                return i;
            }
            i *= 2;
        }
        panic!("next highest power of 2 is unreachable")
    }
}

impl Default for BloomFilter {
    fn default() -> Self {
        Self::new(512)
    }
}

#[cfg(test)]
mod test {
    use super::BloomFilter;

    #[test]
    fn test_bloom_filter() {
        let mut bloom_filter = BloomFilter::new(64);
        assert_eq!(bloom_filter.bytes().len(), 64 / 8);

        let v1 = 10_u64.to_be_bytes();
        bloom_filter.insert(&v1);
        assert!(bloom_filter.maybe_contains(&v1));

        let v2 = 11_u64.to_be_bytes();
        assert!(!bloom_filter.maybe_contains(&v2));
    }

    #[test]
    fn test_bloom_filter_series() {
        let mut bloom_filter = BloomFilter::new(512);
        for i in 1..1158_u32 {
            bloom_filter.insert(&i.to_be_bytes());
        }
        bloom_filter.insert(1159_u32.to_be_bytes().as_slice());
        bloom_filter.insert(1162_u32.to_be_bytes().as_slice());
        bloom_filter.insert(1163_u32.to_be_bytes().as_slice());
        bloom_filter.insert(1164_u32.to_be_bytes().as_slice());
        bloom_filter.insert(1165_u32.to_be_bytes().as_slice());

        assert!(bloom_filter.maybe_contains(1161_u32.to_be_bytes().as_slice()))
    }

    #[test]
    fn test_insert_contains() {
        let mut bloom_filter = BloomFilter::new(100);

        bloom_filter.insert(b"apple");
        bloom_filter.insert(b"banana");
        bloom_filter.insert(b"cherry");

        assert!(bloom_filter.contains(b"apple"));
        assert!(bloom_filter.contains(b"banana"));
        assert!(bloom_filter.contains(b"cherry"));
        assert!(!bloom_filter.contains(b"orange"));
    }

    #[test]
    fn test_contains_duplicates() {
        let mut bloom_filter = BloomFilter::new(100);

        bloom_filter.insert(b"apple");
        bloom_filter.insert(b"banana");
        bloom_filter.insert(b"cherry");
        bloom_filter.insert(b"apple"); // Inserting duplicate element

        assert!(bloom_filter.contains(b"apple")); // The element should still be considered present
        assert!(bloom_filter.contains(b"banana"));
        assert!(bloom_filter.contains(b"cherry"));
    }

    #[test]
    fn test_contains_large_data_set() {
        let mut bloom_filter = BloomFilter::new(100000);

        for i in 0..10_000 {
            bloom_filter.insert(i.to_string().as_bytes());
        }

        for i in 0..10_000 {
            assert!(bloom_filter.contains(i.to_string().as_bytes()));
        }

        assert!(!bloom_filter.contains(b"not_present"));
        assert!(bloom_filter.contains(b"999"));
        assert!(!bloom_filter.contains(b"10001"));
    }
}
