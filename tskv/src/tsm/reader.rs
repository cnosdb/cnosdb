use std::{io::SeekFrom, sync::Arc};

use models::{FieldId, Timestamp, ValueType};
use snafu::ResultExt;

use super::{
    boolean, float, index::Index, integer, string, timestamp, unsigned, DataBlock, BLOCK_META_SIZE,
    FOOTER_SIZE, INDEX_META_SIZE, MAX_BLOCK_VALUES,
};
use crate::{
    byte_utils,
    byte_utils::{decode_be_i64, decode_be_u16, decode_be_u64},
    direct_io::{File, FileCursor},
    error::{self, Result},
    Error,
};

pub struct TsmReader {
    reader: Arc<File>,
    index: Arc<Index>,
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
        BlockMetaIterator { index_ref: self.index_ref.clone(),
                            index_offset,
                            field_type: self.field_type,
                            block_offset: index_offset + INDEX_META_SIZE,
                            block_count: self.block_count,
                            block_idx: 0 }
    }

    pub fn iter_opt(&self, min_ts: Timestamp, max_ts: Timestamp) -> BlockMetaIterator {
        let index_offset = self.index_ref.offsets()[self.index_idx] as usize;
        let mut iter = BlockMetaIterator { index_ref: self.index_ref.clone(),
                                           index_offset,
                                           field_type: self.field_type,
                                           block_offset: index_offset + INDEX_META_SIZE,
                                           block_count: self.block_count,
                                           block_idx: 0 };
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

fn get_index_meta_unchecked(index: Arc<Index>, idx: usize) -> IndexMeta {
    let idx = 0_usize;
    let off = index.offsets()[idx] as usize;

    let field_id = byte_utils::decode_be_u64(&index.data()[off..off + 8]);
    let block_type = ValueType::from(index.data()[off + 8]);
    let block_count = byte_utils::decode_be_u16(&index.data()[off + 9..off + 11]);

    IndexMeta { index_ref: index, index_idx: idx, field_id, field_type: block_type, block_count }
}

fn get_data_block_meta_unchecked(index: Arc<Index>,
                                 index_offset: usize,
                                 block_idx: usize,
                                 field_type: ValueType)
                                 -> BlockMeta {
    let base = index_offset + INDEX_META_SIZE + block_idx * BLOCK_META_SIZE;
    // let sli = &self.data[off + INDEX_META_SIZE..];
    // let min_ts = byte_utils::decode_be_i64(&sli[base + 0..base + 8]);
    // let max_ts = byte_utils::decode_be_i64(&sli[base + 8..base + 16]);
    // let offset = byte_utils::decode_be_u64(&sli[base + 16..base + 24]);
    // let size = byte_utils::decode_be_u64(&sli[base + 24..base + 32]);
    // let val_off = byte_utils::decode_be_u64(&sli[base + 32..base + 40]);

    BlockMeta::new(index, field_type, base)
}

pub struct IndexReader {
    index_ref: Arc<Index>,
}

impl IndexReader {
    pub fn load(reader: Arc<File>) -> Result<Self> {
        let len = reader.len();
        let mut buf = [0u8; 8];

        // Read index data offset
        reader.read_at(len - 8, &mut buf)
              .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        let offset = u64::from_be_bytes(buf);
        let data_len = (len - offset) as usize - FOOTER_SIZE;
        let mut data = vec![0_u8; data_len];
        // Read index data
        reader.read_at(offset, &mut data)
              .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

        // Decode index data
        let mut offsets = Vec::new();
        let mut field_ids = Vec::new();
        let mut pos = 0_usize;
        while pos < data_len {
            offsets.push(pos as u64);
            field_ids.push(decode_be_u64(&data[pos..pos + 8]));
            pos += INDEX_META_SIZE
                   + BLOCK_META_SIZE * decode_be_u16(&data[pos + 9..pos + 11]) as usize;
        }

        // Sort by field id
        let len = field_ids.len();
        for i in 0..len {
            let mut j = i;
            for k in (i + 1)..len {
                if field_ids[j] > field_ids[k] {
                    j = k;
                }
            }
            field_ids.swap(i, j);
            offsets.swap(i, j);
        }

        Ok(Self { index_ref: Arc::new(Index::new(data, field_ids, offsets)) })
    }

    pub fn iter(&self) -> IndexIterator {
        IndexIterator::new(self.index_ref.clone(), self.index_ref.offsets().len(), 0)
    }

    /// Create `IndexIterator` by filter options
    pub fn iter_opt(&self, field_id: FieldId) -> IndexIterator {
        if let Ok(idx) = self.index_ref.field_ids().binary_search(&field_id) {
            IndexIterator::new(self.index_ref.clone(), 1, idx)
        } else {
            IndexIterator::new(self.index_ref.clone(), 0, 0)
        }
    }
}

pub struct IndexIterator {
    index_ref: Arc<Index>,
    /// Total number of `IndexMeta` in the file, also the length of `Index::offsets`.
    index_meta_count: usize,
    /// Array index in `Index::offsets`.
    index_idx: usize,
    block_count: usize,
    block_type: ValueType,
}

impl IndexIterator {
    pub fn new(index: Arc<Index>, total: usize, from_idx: usize) -> Self {
        Self { index_ref: index,
               index_meta_count: total,
               index_idx: from_idx,
               block_count: 0,
               block_type: ValueType::Unknown }
    }
}

impl Iterator for IndexIterator {
    type Item = IndexMeta;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index_idx == self.index_meta_count {
            return None;
        }
        let ret = Some(get_index_meta_unchecked(self.index_ref.clone(), self.index_idx));
        self.index_idx += 1;
        ret
    }
}

pub struct BlockMetaIterator {
    index_ref: Arc<Index>,
    /// Array index in `Index::offsets`
    index_offset: usize,
    field_type: ValueType,
    /// Array index in `Index::data` which current `BlockMeta` starts.
    block_offset: usize,
    /// Number of `BlockMeta` to iterate in current `IndexMeta`
    block_count: u16,
    /// Serial number of `BlockMeta` in current `IndexMeta`, starts with zero.
    block_idx: usize,
}

impl BlockMetaIterator {
    /// Set iterator start & end position by time range
    pub fn filter_timerange(&mut self, min_ts: Timestamp, max_ts: Timestamp) {
        let sli = &self.index_ref.data()
            [self.index_offset + INDEX_META_SIZE..self.block_count as usize * BLOCK_META_SIZE];
        let mut pos = 0_usize;
        while pos < sli.len() {
            if min_ts > decode_be_i64(&sli[pos..pos + 8]) {
                pos += BLOCK_META_SIZE;
            } else {
                // First data block in time range
                // self.block_idx = pos / BLOCK_META_SIZE;
                self.block_offset = pos;
                break;
            }
        }
        let min_pos = pos;
        while pos < sli.len() {
            if max_ts > decode_be_i64(&sli[pos + 8..pos + 16]) {
                // Last data block in time range
                self.block_count = ((pos - min_pos) / BLOCK_META_SIZE) as u16;
                break;
            }
        }
    }
}

impl Iterator for BlockMetaIterator {
    type Item = BlockMeta;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_idx == self.block_count as usize {
            return None;
        }
        let ret = Some(get_data_block_meta_unchecked(self.index_ref.clone(),
                                                     self.index_offset,
                                                     self.block_idx,
                                                     self.field_type));
        self.block_idx += 1;
        self.block_offset += BLOCK_META_SIZE;
        ret
    }
}

// pub enum ColumnReader {
//     FloatColumnReader,
//     IntegerColumnReader,
//     BooleanColumnReader,
//     StringColumnReader,
//     UnsignedColumnReaqder,
// }

pub struct ColumnReader {
    reader: Arc<File>,
    inner: BlockMetaIterator,
    buf: Vec<u8>,
}

impl ColumnReader {
    pub fn new(reader: Arc<File>, inner: BlockMetaIterator) -> Self {
        Self { reader, inner, buf: vec![] }
    }

    fn decode(&mut self, block_meta: &BlockMeta) -> Result<DataBlock> {
        let (offset, size) = (block_meta.offset(), block_meta.size());
        self.buf.resize(size as usize, 0);

        let val_offset = block_meta.val_off();

        self.reader
            .read_at(offset, &mut self.buf)
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        let crc = &self.buf[..4];
        let mut ts = Vec::with_capacity(MAX_BLOCK_VALUES);
        timestamp::decode(&self.buf[4..(val_offset - offset) as usize], &mut ts)
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        let data = &self.buf[(val_offset - offset + 4) as usize..];
        match block_meta.field_type {
            ValueType::Float => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                float::decode(&data, &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
                Ok(DataBlock::F64 { index: 0, ts, val })
            },
            ValueType::Integer => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                integer::decode(&data, &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
                Ok(DataBlock::I64 { index: 0, ts, val })
            },
            ValueType::Boolean => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                boolean::decode(&data, &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

                Ok(DataBlock::Bool { index: 0, ts, val })
            },
            ValueType::String => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                string::decode(&data, &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
                Ok(DataBlock::Str { index: 0, ts, val })
            },
            ValueType::Unsigned => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                unsigned::decode(&data, &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
                Ok(DataBlock::U64 { index: 0, ts, val })
            },
            _ => {
                Err(Error::ReadTsmErr { reason: format!("cannot decode block {:?} with no unknown value type",
                                                        block_meta.field_type) })
            },
        }
    }
}

impl Iterator for ColumnReader {
    type Item = Result<DataBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(dbm) = self.inner.next() {
            return Some(self.decode(&dbm));
        }

        None
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use models::FieldId;

    use super::{ColumnReader, Index, IndexReader};
    use crate::{
        file_manager::{get_file_manager, FileManager},
        tsm::DataBlock,
    };

    #[test]
    fn tsm_reader_test() {
        let fs = get_file_manager().open_file("/tmp/test/writer_test.tsm").unwrap();
        let fs = Arc::new(fs);
        let len = fs.len();

        let index = IndexReader::load(fs.clone()).unwrap();
        let mut column_readers: HashMap<FieldId, ColumnReader> = HashMap::new();
        for index_meta in index.iter() {
            column_readers.insert(index_meta.field_id(),
                                  ColumnReader::new(fs.clone(), index_meta.iter()));
        }

        let ori_data: HashMap<FieldId, Vec<DataBlock>> =
            HashMap::from([(1,
                            vec![DataBlock::U64 { index: 0,
                                                  ts: vec![2, 3, 4],
                                                  val: vec![12, 13, 15] }]),
                           (2,
                            vec![DataBlock::U64 { index: 0,
                                                  ts: vec![2, 3, 4],
                                                  val: vec![101, 102, 103] }])]);

        for (fid, col_reader) in column_readers.iter_mut() {
            dbg!(fid);
            let mut data = Vec::new();
            for block in col_reader.next().unwrap() {
                data.push(block);
            }
            dbg!(&data);

            assert_eq!(*ori_data.get(fid).unwrap(), data);
        }
    }
}
