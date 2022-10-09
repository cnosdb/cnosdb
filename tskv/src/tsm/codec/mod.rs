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
pub struct DataBlockEncoding(pub u8);

impl DataBlockEncoding {
    /// Combines timestamp-block-encoding (into high 4 bit)
    /// and values-block-encoding (into low 4 bit) into self.
    pub fn combine(ts_encoding: Encoding, val_encoding: Encoding) -> Self {
        Self((ts_encoding as u8) << 4 | (val_encoding as u8))
    }

    /// Combines timestamp-block-encoding (into high 4 bit)
    /// and values-block-encoding (into low 4 bit) into self.
    pub fn combine_raw(ts_encoding: u8, val_encoding: u8) -> Self {
        Self((ts_encoding) << 4 | (val_encoding))
    }

    /// Splits self into timestamp-block-encoding (at high 4 bit)
    /// and values-block-encoding (at low 4 bit)
    pub fn split(&self) -> (Encoding, Encoding) {
        (Encoding::from(self.0 >> 4), Encoding::from(self.0 & 15))
    }
}

#[test]
fn test_data_block_encoding() {
    let id_0 = DataBlockEncoding(0b0001_0011);
    let (code0, code1) = id_0.split();
    assert_eq!(code0 as u8, 1);
    assert_eq!(code1 as u8, 3);

    let id_1 = DataBlockEncoding(0b0101_1111);
    let (code2, code3) = id_1.split();
    assert_eq!(code2 as u8, 5);
    assert_eq!(code3 as u8, Encoding::Unknown as u8);
}

#[test]
fn test_combine_code_type_id() {
    let id_0 = 0b0000_0001;
    let id_1 = 0b0000_0001;

    let code = DataBlockEncoding::combine(Encoding::from(id_0), Encoding::from(id_1));
    assert_eq!(code.0, 0b00010001);

    let code = DataBlockEncoding::combine_raw(id_0, id_1);
    assert_eq!(code.0, 0b00010001);
}
