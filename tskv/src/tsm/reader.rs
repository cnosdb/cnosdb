use std::{io::SeekFrom, sync::Arc};

use models::{FieldId, Timestamp, ValueType};
use snafu::ResultExt;

use super::{
    block, boolean, float, get_data_block_meta_unchecked, get_index_meta_unchecked,
    index::{self, Index},
    integer, string, timestamp, unsigned, BlockMeta, DataBlock, IndexMeta, Tombstone, TsmTombstone,
    BLOCK_META_SIZE, FOOTER_SIZE, INDEX_META_SIZE, MAX_BLOCK_VALUES,
};
use crate::{
    byte_utils,
    byte_utils::{decode_be_i64, decode_be_u16, decode_be_u64},
    direct_io::{File, FileCursor},
    error::{self, Result},
    Error,
};

/// Disk-based index reader
pub struct IndexFile {
    reader: Arc<File>,
    buf: [u8; 8],

    index_offset: u64,
    pos: u64,
    end_pos: u64,
    index_block_idx: usize,
    index_block_count: usize,
}

impl IndexFile {
    pub fn open(reader: Arc<File>) -> Result<Self> {
        let file_len = reader.len();
        let mut buf = [0_u8; 8];
        reader.read_at(file_len - 8, &mut buf)
              .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        let index_offset = u64::from_be_bytes(buf);
        Ok(Self { reader,
                  buf,
                  index_offset,
                  pos: index_offset,
                  end_pos: file_len - FOOTER_SIZE as u64,
                  index_block_idx: 0,
                  index_block_count: 0 })
    }

    // TODO: not implemented
    pub fn next_index_entry(&mut self) -> Result<Option<()>> {
        if self.pos >= self.end_pos {
            return Ok(None);
        }
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        let field_id = u64::from_be_bytes(self.buf);
        self.pos += 8;
        self.reader
            .read_at(self.pos, &mut self.buf[..3])
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.pos += 3;
        let field_type = ValueType::from(self.buf[0]);
        let block_count = decode_be_u16(&self.buf[1..3]);
        self.index_block_idx = 0;
        self.index_block_count = block_count as usize;

        Ok(Some(()))
    }

    // TODO: not implemented
    pub fn next_block_entry(&mut self) -> Result<Option<()>> {
        if self.index_block_idx >= self.index_block_count {
            return Ok(None);
        }
        // read min time on block entry
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.pos += 8;
        let min_ts = i64::from_be_bytes(self.buf);

        // read max time on block entry
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.pos += 8;
        let max_ts = i64::from_be_bytes(self.buf);

        // read block data offset
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.pos += 8;
        let offset = u64::from_be_bytes(self.buf);

        // read block size
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.pos += 8;
        let size = u64::from_be_bytes(self.buf);

        // read value offset
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.pos += 8;
        let val_off = u64::from_be_bytes(self.buf);

        Ok(Some(()))
    }
}

pub fn load_index(reader: Arc<File>) -> Result<Index> {
    let len = reader.len();
    let mut buf = [0u8; 8];

    // Read index data offset
    reader.read_at(len - 8, &mut buf).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
    let offset = u64::from_be_bytes(buf);
    let data_len = (len - offset) as usize - FOOTER_SIZE;
    let mut data = vec![0_u8; data_len];
    // Read index data
    reader.read_at(offset, &mut data).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

    // Decode index data
    let mut offsets = Vec::new();
    let mut field_ids = Vec::new();
    let mut pos = 0_usize;
    while pos < data_len {
        offsets.push(pos as u64);
        field_ids.push(decode_be_u64(&data[pos..pos + 8]));
        pos += INDEX_META_SIZE + BLOCK_META_SIZE * decode_be_u16(&data[pos + 9..pos + 11]) as usize;
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

    Ok(Index::new(data, field_ids, offsets))
}

/// Memory-based index reader
pub struct IndexReader {
    index_ref: Arc<Index>,
}

impl IndexReader {
    pub fn open(reader: Arc<File>) -> Result<Self> {
        let idx = load_index(reader)?;

        Ok(Self { index_ref: Arc::new(idx) })
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
    field_id: FieldId,
    field_type: ValueType,
    /// Array index in `Index::data` which current `BlockMeta` starts.
    block_offset: usize,
    /// Number of `BlockMeta` to iterate in current `IndexMeta`
    block_count: u16,
    /// Serial number of `BlockMeta` in current `IndexMeta`, starts with zero.
    block_idx: usize,
}

impl BlockMetaIterator {
    pub fn new(index: Arc<Index>,
               index_offset: usize,
               field_id: FieldId,
               field_type: ValueType,
               block_count: u16)
               -> Self {
        Self { index_ref: index,
               index_offset,
               field_id,
               field_type,
               block_offset: index_offset + INDEX_META_SIZE,
               block_count,
               block_idx: 0 }
    }

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
                                                     self.field_id,
                                                     self.field_type));
        self.block_idx += 1;
        self.block_offset += BLOCK_META_SIZE;
        ret
    }
}

pub struct TsmReader {
    reader: Arc<File>,
    index_reader: Arc<IndexReader>,
    tombstones: Vec<Tombstone>,
}

impl TsmReader {
    pub fn open(reader: Arc<File>, tombstone: Arc<File>) -> Result<Self> {
        let idx = IndexReader::open(reader.clone())?;
        let tombstones = TsmTombstone::load_all(reader.as_ref())?;
        Ok(Self { reader, index_reader: Arc::new(idx), tombstones })
    }

    pub fn index_iterator(&self) -> IndexIterator {
        self.index_reader.iter()
    }

    pub fn get_data_block(&self, block_meta: &BlockMeta) -> Result<DataBlock> {
        let mut buf = vec![0_u8; block_meta.size() as usize];
        let tombsotne: Vec<Tombstone> = self.tombstones
                                            .iter()
                                            .filter(|t| t.field_id == block_meta.field_id())
                                            .map(|t| *t)
                                            .collect();
        decode_data_block(self.reader.clone(),
                          &mut buf,
                          block_meta.field_type(),
                          block_meta.offset(),
                          block_meta.size(),
                          block_meta.val_off())
    }

    pub fn copy_to(&self, block_meta: &BlockMeta, writer: &mut FileCursor) -> Result<usize> {
        let mut buf = vec![0_u8; block_meta.size() as usize];
        self.reader
            .read_at(block_meta.offset(), &mut buf)
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        writer.write(&buf[..]).map_err(|e| Error::WriteTsmErr { reason: e.to_string() })
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
        decode_data_block(self.reader.clone(),
                          &mut self.buf,
                          block_meta.field_type(),
                          block_meta.offset(),
                          block_meta.size(),
                          block_meta.val_off())
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

pub fn decode_data_block(reader: Arc<File>,
                         buf: &mut [u8],
                         field_type: ValueType,
                         offset: u64,
                         size: u64,
                         val_off: u64)
                         -> Result<DataBlock> {
    assert!(buf.len() >= size as usize);

    reader.read_at(offset, buf).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
    // let crc_ts = &self.buf[..4];
    let mut ts = Vec::with_capacity(MAX_BLOCK_VALUES);
    timestamp::decode(&buf[4..(val_off - offset) as usize], &mut ts)
    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
    // let crc_data = &self.buf[(val_offset - offset) as usize..4];
    let data = &buf[(val_off - offset + 4) as usize..];
    match field_type {
        ValueType::Float => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            float::decode(&data, &mut val).map_err(|e| Error::ReadTsmErr { reason:
                                                                               e.to_string() })?;
            Ok(DataBlock::F64 { index: 0, ts, val })
        },
        ValueType::Integer => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            integer::decode(&data, &mut val).map_err(|e| Error::ReadTsmErr { reason:
                                                                                 e.to_string() })?;
            Ok(DataBlock::I64 { index: 0, ts, val })
        },
        ValueType::Boolean => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            boolean::decode(&data, &mut val).map_err(|e| Error::ReadTsmErr { reason:
                                                                                 e.to_string() })?;

            Ok(DataBlock::Bool { index: 0, ts, val })
        },
        ValueType::String => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            string::decode(&data, &mut val).map_err(|e| Error::ReadTsmErr { reason:
                                                                                e.to_string() })?;
            Ok(DataBlock::Str { index: 0, ts, val })
        },
        ValueType::Unsigned => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            unsigned::decode(&data, &mut val).map_err(|e| Error::ReadTsmErr { reason:
                                                                                  e.to_string() })?;
            Ok(DataBlock::U64 { index: 0, ts, val })
        },
        _ => {
            Err(Error::ReadTsmErr { reason: format!("cannot decode block {:?} with no unknown value type",
                                                    field_type) })
        },
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

        let index = IndexReader::open(fs.clone()).unwrap();
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
