use std::io::SeekFrom;

use models::FieldId;

use super::{BLOCK_META_SIZE, FOOTER_SIZE, INDEX_META_SIZE};
use crate::{
    byte_utils::{decode_be_u16, decode_be_u64},
    direct_io::FileCursor,
    error::Result,
    Error,
};

#[derive(Debug, Clone)]
pub struct Index {
    /// In-memory index-block data
    ///
    /// ```text
    /// +-------------+---------+
    /// | field_id    | 8 bytes |
    /// | field_type  | 1 bytes |
    /// | block_count | 2 bytes |
    /// | blocks      | -       |
    /// +-------------+---------+
    /// ```
    data: Vec<u8>,
    /// Sorted FieldId
    field_ids: Vec<FieldId>,
    /// Sorted index-block offsets for each `FieldId` in `data`
    offsets: Vec<u64>,
}

impl Index {
    #[inline(always)]
    pub fn new(data: Vec<u8>, field_ids: Vec<FieldId>, offsets: Vec<u64>) -> Self {
        Self { data, field_ids, offsets }
    }

    #[inline(always)]
    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    #[inline(always)]
    pub fn field_ids(&self) -> &[FieldId] {
        self.field_ids.as_slice()
    }

    #[inline(always)]
    pub fn offsets(&self) -> &[u64] {
        self.offsets.as_slice()
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }
}
