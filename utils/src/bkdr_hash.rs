const SEED: u64 = 1313;

pub struct Hash(u64);

impl Hash {
    pub fn new() -> Self {
        Hash(0)
    }

    pub fn number(&self) -> u64 {
        self.0
    }
}

impl From<u64> for Hash {
    fn from(hash: u64) -> Self {
        Hash(hash)
    }
}

impl From<&Hash> for Hash {
    fn from(hash: &Hash) -> Self {
        Hash(hash.0)
    }
}

pub trait HashWith<T> {
    fn hash_with(&mut self, data: T) -> &Hash;
}

impl HashWith<&Vec<u8>> for Hash {
    fn hash_with(&mut self, data: &Vec<u8>) -> &Hash {
        for c in data {
            self.0 = self.0.wrapping_mul(SEED).wrapping_add(*c as u64);
        }
        self
    }
}

impl HashWith<&str> for Hash {
    fn hash_with(&mut self, data: &str) -> &Hash {
        for c in data.as_bytes() {
            self.0 = self.0.wrapping_mul(SEED).wrapping_add(*c as u64);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::bkdr_hash::{Hash, HashWith};

    #[test]
    fn test_hash() {
        let mut ha = Hash::new();
        let _ = ha.hash_with(&Vec::from("123")).number();
    }
}
