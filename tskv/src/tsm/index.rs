use models::ValueType;

use crate::{compute::decode_be_u64, FileBlock};

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key: Vec<u8>,
    pub block_type: ValueType,
    pub count: u16,
    pub block: FileBlock,
    pub curr_block: u16,
}

pub struct Footer {
    pub index_offset: u64,
}
impl IndexEntry {
    pub fn filed_id(&self) -> u64 {
        decode_be_u64(&self.key[..8])
    }
}
