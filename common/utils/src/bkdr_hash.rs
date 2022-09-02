use std::hash::Hasher;

const SEED: u64 = 1313;

pub struct BkdrHasher {
    prefix: Option<u64>,
    number: u64,
}

impl BkdrHasher {
    pub fn new() -> Self {
        Self {
            prefix: None,
            number: 0,
        }
    }

    pub fn with_prefix(prefix: u64) -> Self {
        Self {
            prefix: Some(prefix),
            number: 0,
        }
    }

    pub fn with_number(number: u64) -> Self {
        Self {
            prefix: None,
            number,
        }
    }

    pub fn hash_with(&mut self, bytes: &[u8]) -> &mut Self {
        self.write(bytes);
        self
    }

    pub fn number(&self) -> u64 {
        self.number
    }

    pub fn number_with_prefix(&self) -> u128 {
        if let Some(prefix) = self.prefix {
            (prefix as u128) << 64 | self.number as u128
        } else {
            self.number as u128
        }
    }
}

impl Hasher for BkdrHasher {
    fn finish(&self) -> u64 {
        self.number
    }

    fn write(&mut self, bytes: &[u8]) {
        for c in bytes {
            self.number = self.number.wrapping_mul(SEED).wrapping_add(*c as u64);
        }
    }
}

impl Default for BkdrHasher {
    fn default() -> Self {
        Self::new()
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
        let d = {
            let mut h = BkdrHasher::new();
            h.write(b"WorldHello".as_slice());
            h.number()
        };
        println!("{} == {} != {} == {}", a, b, c, d);
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_eq!(c, d);
    }

    #[test]
    fn test_hash_with_prefix() {
        let suffix = {
            let mut h = BkdrHasher::with_prefix(1);
            h.write(b"HelloWorld".as_slice());
            h.number()
        };
        let prefix_suffix = {
            let mut h = BkdrHasher::with_prefix(1);
            h.write(b"HelloWorld".as_slice());
            h.number_with_prefix()
        };

        assert_eq!(1, (prefix_suffix >> 64 & u64::MAX as u128) as u64);
        assert_eq!(suffix, (prefix_suffix & u64::MAX as u128) as u64);
    }
}
