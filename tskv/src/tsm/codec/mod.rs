mod boolean;
mod float;
mod instance;
mod integer;
mod simple8b;
mod string;
mod timestamp;
mod unsigned;

pub use instance::*;
use models::codec::Encoding;

/// Max number of bytes needed to store a varint-encoded 32-bit integer.
const MAX_VAR_INT_32: usize = 5;

/// Max number of bytes needed to store a varint-encoded 64-bit integer.
const MAX_VAR_INT_64: usize = 10;

/// Combined encoding ids with timestamp-block-encoding (high 4 bit)
/// and values-block-encoding (low 4 bit)
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct DataBlockEncoding {
    ts_encoding: Encoding,
    val_encoding: Encoding,
}

impl DataBlockEncoding {
    pub const fn new(ts_encoding: Encoding, val_encoding: Encoding) -> Self {
        Self {
            ts_encoding,
            val_encoding,
        }
    }
    pub fn split(&self) -> (Encoding, Encoding) {
        (self.ts_encoding, self.val_encoding)
    }
}
