use std::hash::Hasher;

const SEED: u64 = 1313;

pub struct BkdrHasher(u64);

impl BkdrHasher {
    pub fn new() -> Self {
        BkdrHasher(0)
    }

    pub fn with_number(number: u64) -> Self {
        BkdrHasher(number)
    }

    pub fn hash_with(&mut self, bytes: &[u8]) -> &mut Self {
        self.write(bytes);
        self
    }

    pub fn number(&self) -> u64 {
        self.0
    }
}

impl Hasher for BkdrHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for c in bytes {
            self.0 = self.0.wrapping_mul(SEED).wrapping_add(*c as u64);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hasher;

    use crate::bkdr_hash::BkdrHasher;

    #[test]
    fn test_hash() {
        let a = {
            let mut h = BkdrHasher::new();
            h.write(b"Hello".as_slice());
            h.write(b"World".as_slice());
            h.finish()
        };
        let b = {
            let mut h = BkdrHasher::new();
            h.write(b"HelloWorld".as_slice());
            h.finish()
        };
        let c = {
            let mut h = BkdrHasher::new();
            h.write(b"World".as_slice());
            h.write(b"Hello".as_slice());
            h.number()
        };
        println!("{} == {} != {}", a, b, c);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
