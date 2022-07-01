use models::{FieldID, SeriesID, ValueType};

use crate::{byte_utils::decode_be_u64, error::Result, tsm::FileBlock, Error};

/// ```text
/// +-------------+---------+
/// | field_id    | 8 bytes |
/// | field_type  | 1 bytes |
/// | block_count | 2 bytes |
/// | blocks      | -       |
/// +-------------+---------+
/// ```
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub block_type: ValueType,
    pub count: u16,
    pub block: FileBlock,
    pub curr_block: u16,
}

/// ```text
/// +-----------+---------+
/// | index_off | 8 bytes |
/// +-----------+---------+
/// ```
pub struct Footer {
    pub index_offset: u64,
}

impl IndexEntry {
    pub fn field_id(&self) -> FieldID {
        decode_be_u64(&self.key[..8])
    }
}
