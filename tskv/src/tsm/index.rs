use std::{io::SeekFrom, sync::Arc};

use models::{FieldId, Timestamp, ValueType};

use super::{BlockMetaIterator, BLOCK_META_SIZE, INDEX_META_SIZE};
use crate::byte_utils::{decode_be_i64, decode_be_u16, decode_be_u64};

pub trait IndexT {}

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
}

pub struct IndexMeta {
    index_ref: Arc<Index>,
    /// Array index in `Index::offsets`
    index_idx: usize,

    field_id: FieldId,
    field_type: ValueType,
    block_count: u16,
}

impl IndexMeta {
    pub fn iter(&self) -> BlockMetaIterator {
        let index_offset = self.index_ref.offsets()[self.index_idx] as usize;
        BlockMetaIterator::new(self.index_ref.clone(),
                               index_offset,
                               self.field_type,
                               self.block_count)
    }

    pub fn iter_opt(&self, min_ts: Timestamp, max_ts: Timestamp) -> BlockMetaIterator {
        let index_offset = self.index_ref.offsets()[self.index_idx] as usize;
        let mut iter = BlockMetaIterator::new(self.index_ref.clone(),
                                              index_offset,
                                              self.field_type,
                                              self.block_count);
        iter.filter_timerange(min_ts, max_ts);
        iter
    }

    #[inline(always)]
    pub fn field_id(&self) -> FieldId {
        self.field_id
    }

    #[inline(always)]
    pub fn field_type(&self) -> ValueType {
        self.field_type
    }

    #[inline(always)]
    pub fn block_count(&self) -> u16 {
        self.block_count
    }
}

#[derive(Debug, Clone)]
pub struct BlockMeta {
    index_ref: Arc<Index>,
    /// Array index in `Index::data` which current `BlockMeta` starts.
    block_offset: usize,
    field_type: ValueType,

    min_ts: Timestamp,
    max_ts: Timestamp,
}

impl BlockMeta {
    fn new(index: Arc<Index>, field_type: ValueType, block_offset: usize) -> Self {
        let min_ts = decode_be_i64(&index.data()[block_offset..block_offset + 8]);
        let max_ts = decode_be_i64(&&index.data()[block_offset + 8..block_offset + 16]);
        Self { index_ref: index, block_offset, field_type, min_ts, max_ts }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.index_ref.data()[self.block_offset..]
    }

    #[inline(always)]
    pub fn field_type(&self) -> ValueType {
        self.field_type
    }

    #[inline(always)]
    pub fn min_ts(&self) -> Timestamp {
        self.min_ts
    }

    #[inline(always)]
    pub fn max_ts(&self) -> Timestamp {
        self.max_ts
    }

    #[inline(always)]
    pub fn offset(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 16..self.block_offset + 24])
    }

    #[inline(always)]
    pub fn size(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 24..self.block_offset + 32])
    }

    #[inline(always)]
    pub fn val_off(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 32..self.block_offset + 40])
    }
}

pub(crate) fn get_index_meta_unchecked(index: Arc<Index>, idx: usize) -> IndexMeta {
    let off = index.offsets()[idx] as usize;

    let field_id = decode_be_u64(&index.data()[off..off + 8]);
    let block_type = ValueType::from(index.data()[off + 8]);
    let block_count = decode_be_u16(&index.data()[off + 9..off + 11]);

    IndexMeta { index_ref: index, index_idx: idx, field_id, field_type: block_type, block_count }
}

pub(crate) fn get_data_block_meta_unchecked(index: Arc<Index>,
                                            index_offset: usize,
                                            block_idx: usize,
                                            field_type: ValueType)
                                            -> BlockMeta {
    let base = index_offset + INDEX_META_SIZE + block_idx * BLOCK_META_SIZE;
    BlockMeta::new(index, field_type, base)
}
