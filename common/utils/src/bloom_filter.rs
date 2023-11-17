use serde::{Deserialize, Serialize};

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
        let hash = Self::hash(data);
        let loc = self.location(hash);
        self.b[loc >> 3] |= 1 << (loc & 7);
    }

    pub fn contains(&self, data: &[u8]) -> bool {
        let hash = Self::hash(data);
        let loc = self.location(hash);
        self.b[loc >> 3] & (1 << (loc & 7)) != 0
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

    fn hash(data: &[u8]) -> u64 {
        BkdrHasher::new().hash_with(data).number()
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
        assert!(bloom_filter.contains(&v1));

        let v2 = 11_u64.to_be_bytes();
        assert!(!bloom_filter.contains(&v2));
    }
}
