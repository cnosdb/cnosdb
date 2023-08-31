use std::cmp;
use std::fmt::Display;
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::{FieldId, Timestamp, ValueType};
use utils::BloomFilter;

use crate::byte_utils::{decode_be_i64, decode_be_u16, decode_be_u32, decode_be_u64};
use crate::tsm::{
    BlockMetaIterator, DataBlock, WriteTsmError, WriteTsmResult, BLOCK_META_SIZE, INDEX_META_SIZE,
};

#[derive(Debug, Clone)]
pub struct Index {
    tsm_id: u64,
    bloom_filter: Arc<BloomFilter>,

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
    /// Sorted FieldId and it's offset if index-block
    field_id_offs: Vec<(FieldId, usize)>,
}

impl Index {
    #[inline(always)]
    pub fn new(
        tsm_id: u64,
        bloom_filter: Arc<BloomFilter>,
        data: Vec<u8>,
        field_id_offs: Vec<(FieldId, usize)>,
    ) -> Self {
        Self {
            tsm_id,
            bloom_filter,
            data,
            field_id_offs,
        }
    }

    pub fn bloom_filter(&self) -> Arc<BloomFilter> {
        self.bloom_filter.clone()
    }

    pub fn data(&self) -> &[u8] {
        self.data.as_slice()
    }

    pub fn field_id_offs(&self) -> &[(FieldId, usize)] {
        self.field_id_offs.as_slice()
    }
}

pub struct IndexMeta {
    index_ref: Arc<Index>,
    /// Array index in `Index::offsets`
    index_idx: usize,
    offset: usize,

    field_id: FieldId,
    field_type: ValueType,
    block_count: u16,
}

impl IndexMeta {
    pub fn block_iterator(&self) -> BlockMetaIterator {
        BlockMetaIterator::new(
            self.index_ref.clone(),
            self.offset,
            self.field_id,
            self.field_type,
            self.block_count,
        )
    }

    /// get block_meta_iterator filter by time_ranges
    pub fn block_iterator_opt(&self, time_ranges: Arc<TimeRanges>) -> BlockMetaIterator {
        let mut iter = BlockMetaIterator::new(
            self.index_ref.clone(),
            self.offset,
            self.field_id,
            self.field_type,
            self.block_count,
        );
        iter.filter_time_range(time_ranges);
        iter
    }

    pub fn offset(&self) -> usize {
        self.offset
    }

    pub fn field_id(&self) -> FieldId {
        self.field_id
    }

    pub fn field_type(&self) -> ValueType {
        self.field_type
    }

    pub fn block_count(&self) -> u16 {
        self.block_count
    }

    pub fn time_range(&self) -> TimeRange {
        if self.block_count == 0 {
            return TimeRange::new(Timestamp::MIN, Timestamp::MIN);
        }
        let first_blk_beg = self.index_ref.field_id_offs()[self.index_idx].1 + INDEX_META_SIZE;
        let min_ts = decode_be_i64(&self.index_ref.data[first_blk_beg..first_blk_beg + 8]);
        let last_blk_beg = first_blk_beg + BLOCK_META_SIZE * (self.block_count as usize - 1);
        let max_ts = decode_be_i64(&self.index_ref.data[last_blk_beg + 8..last_blk_beg + 16]);
        TimeRange::new(min_ts, max_ts)
    }
}

#[derive(Debug, Clone)]
pub struct BlockMeta {
    index_ref: Arc<Index>,
    /// Array index in `Index::data` which current `BlockMeta` starts.
    field_id: FieldId,
    block_offset: usize,
    field_type: ValueType,

    min_ts: Timestamp,
    max_ts: Timestamp,
    count: u32,
}

impl PartialEq for BlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.field_id == other.field_id
            && self.block_offset == other.block_offset
            && self.field_type == other.field_type
            && self.min_ts == other.min_ts
            && self.max_ts == other.max_ts
    }
}

impl Eq for BlockMeta {}

impl PartialOrd for BlockMeta {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BlockMeta {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        match self.field_id.cmp(&other.field_id) {
            cmp::Ordering::Equal => match self.min_ts.cmp(&other.min_ts) {
                cmp::Ordering::Equal => self.max_ts.cmp(&other.max_ts),
                other => other,
            },
            other => other,
        }
    }
}

impl BlockMeta {
    fn new(
        index: Arc<Index>,
        field_id: FieldId,
        field_type: ValueType,
        block_offset: usize,
    ) -> Self {
        let min_ts = decode_be_i64(&index.data()[block_offset..block_offset + 8]);
        let max_ts = decode_be_i64(&index.data()[block_offset + 8..block_offset + 16]);
        let count = decode_be_u32(&index.data()[block_offset + 16..block_offset + 20]);
        Self {
            index_ref: index,
            field_id,
            block_offset,
            field_type,
            min_ts,
            max_ts,
            count,
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        &self.index_ref.data()[self.block_offset..]
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
    pub fn min_ts(&self) -> Timestamp {
        self.min_ts
    }

    #[inline(always)]
    pub fn max_ts(&self) -> Timestamp {
        self.max_ts
    }

    #[inline(always)]
    pub fn time_range(&self) -> TimeRange {
        TimeRange {
            min_ts: self.min_ts,
            max_ts: self.max_ts,
        }
    }

    #[inline(always)]
    pub fn count(&self) -> u32 {
        self.count
    }

    #[inline(always)]
    pub fn offset(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 20..self.block_offset + 28])
    }

    #[inline(always)]
    pub fn size(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 28..self.block_offset + 36])
    }

    #[inline(always)]
    pub fn val_off(&self) -> u64 {
        decode_be_u64(&self.index_ref.data()[self.block_offset + 36..self.block_offset + 44])
    }
}

impl Display for BlockMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "BlockMeta: {{ field_id: {}, field_type: {:?}, min_ts: {}, max_ts: {}, count:{}, offset: {}, val_off: {} }}",
               self.field_id,
               self.field_type,
               self.min_ts,
               self.max_ts,
               self.count,
               self.offset(),
               self.val_off())
    }
}

pub(crate) fn get_index_meta_unchecked(index: Arc<Index>, idx: usize) -> IndexMeta {
    let (field_id, off) = unsafe { *index.field_id_offs.get_unchecked(idx) };
    let block_type = ValueType::from(index.data()[off + 8]);
    let block_count = decode_be_u16(&index.data()[off + 9..off + 11]);

    IndexMeta {
        index_ref: index,
        index_idx: idx,
        offset: off,
        field_id,
        field_type: block_type,
        block_count,
    }
}

pub(crate) fn get_data_block_meta_unchecked(
    index: Arc<Index>,
    index_offset: usize,
    block_idx: usize,
    field_id: FieldId,
    field_type: ValueType,
) -> BlockMeta {
    let base = index_offset + INDEX_META_SIZE + block_idx * BLOCK_META_SIZE;
    BlockMeta::new(index, field_id, field_type, base)
}

#[derive(Debug)]
pub(crate) struct IndexEntry {
    pub field_id: FieldId,
    pub field_type: ValueType,
    pub blocks: Vec<BlockEntry>,
}

impl IndexEntry {
    pub(crate) fn encode(&self, buf: &mut [u8]) -> WriteTsmResult<()> {
        assert!(buf.len() >= INDEX_META_SIZE);
        if buf.len() < INDEX_META_SIZE {
            return Err(WriteTsmError::Encode {
                source: "buffer too short".into(),
            });
        }

        buf[0..8].copy_from_slice(&self.field_id.to_be_bytes()[..]);
        buf[8] = self.field_type.into();
        buf[9..11].copy_from_slice(&(self.blocks.len() as u16).to_be_bytes()[..]);
        Ok(())
    }

    pub(crate) fn decode(data: &[u8]) -> (Self, u16) {
        assert!(data.len() >= INDEX_META_SIZE);
        (
            Self {
                field_id: decode_be_u64(&data[0..8]),
                field_type: data[8].into(),
                blocks: Vec::new(),
            },
            decode_be_u16(&data[9..11]),
        )
    }
}

#[derive(Debug)]
pub(crate) struct BlockEntry {
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
    pub count: u32,
    pub offset: u64,
    pub size: u64,
    pub val_offset: u64,
}

impl BlockEntry {
    pub fn with_block_meta(block_meta: &BlockMeta, offset: u64, size: u64) -> Self {
        let ts_len = block_meta.val_off() - block_meta.offset();
        Self {
            min_ts: block_meta.min_ts,
            max_ts: block_meta.max_ts,
            count: block_meta.count,
            offset,
            size,
            val_offset: offset + ts_len,
        }
    }

    pub fn with_block(
        data_block: &DataBlock,
        offset: u64,
        size: u64,
        encoded_ts_size: u64,
    ) -> Option<Self> {
        if data_block.is_empty() {
            return None;
        }
        let ts_sli = data_block.ts();
        Some(Self {
            min_ts: ts_sli[0],
            max_ts: ts_sli[ts_sli.len() - 1],
            count: ts_sli.len() as u32,
            offset,
            size,
            // Encoded timestamps block need a 4-bytes crc checksum together.
            val_offset: offset + encoded_ts_size + 4,
        })
    }

    pub fn encode(&self, buf: &mut [u8]) {
        assert!(buf.len() >= BLOCK_META_SIZE);
        buf[0..8].copy_from_slice(&self.min_ts.to_be_bytes()[..]);
        buf[8..16].copy_from_slice(&self.max_ts.to_be_bytes()[..]);
        buf[16..20].copy_from_slice(&self.count.to_be_bytes()[..]);
        buf[20..28].copy_from_slice(&self.offset.to_be_bytes()[..]);
        buf[28..36].copy_from_slice(&self.size.to_be_bytes()[..]);
        buf[36..44].copy_from_slice(&self.val_offset.to_be_bytes()[..]);
    }

    pub fn decode(data: &[u8]) -> Self {
        assert!(data.len() >= BLOCK_META_SIZE);
        Self {
            min_ts: decode_be_i64(&data[0..8]),
            max_ts: decode_be_i64(&data[8..16]),
            count: decode_be_u32(&data[16..20]),
            offset: decode_be_u64(&data[20..28]),
            size: decode_be_u64(&data[28..36]),
            val_offset: decode_be_u64(&data[36..44]),
        }
    }
}
