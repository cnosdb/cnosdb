use std::{f32::consts::E, io::SeekFrom, path::Path, sync::Arc};

use models::{FieldId, Timestamp, ValueType};
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};

use super::{
    block, boolean, float, get_data_block_meta_unchecked, get_index_meta_unchecked,
    index::{self, Index},
    integer, string, timestamp, unsigned, BlockMeta, DataBlock, IndexMeta, Tombstone, TsmTombstone,
    BLOCK_META_SIZE, FOOTER_SIZE, INDEX_META_SIZE, MAX_BLOCK_VALUES,
};
use crate::{
    byte_utils,
    byte_utils::{decode_be_i64, decode_be_u16, decode_be_u32, decode_be_u64},
    direct_io::{File, FileCursor},
    error::{self, Error, Result},
    file_manager, file_utils,
};

pub type ReadTsmResult<T, E = ReadTsmError> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum ReadTsmError {
    #[snafu(display("IO error: {}", source))]
    IO { source: std::io::Error },

    #[snafu(display("Decode error: {}", source))]
    Decode { source: Box<dyn std::error::Error + Send + Sync> },

    #[snafu(display("TSM file is invalid: {}", reason))]
    Invalid { reason: String },
}

impl Into<Error> for ReadTsmError {
    fn into(self) -> Error {
        Error::ReadTsm { source: self }
    }
}

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
    pub fn open(reader: Arc<File>) -> ReadTsmResult<Self> {
        let file_len = reader.len();
        let mut buf = [0_u8; 8];
        reader.read_at(file_len - 8, &mut buf).context(IOSnafu)?;
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
    pub fn next_index_entry(&mut self) -> ReadTsmResult<Option<()>> {
        if self.pos >= self.end_pos {
            return Ok(None);
        }
        self.reader.read_at(self.pos, &mut self.buf[..]).context(IOSnafu)?;
        let field_id = u64::from_be_bytes(self.buf);
        self.pos += 8;
        self.reader.read_at(self.pos, &mut self.buf[..3]).context(IOSnafu)?;
        self.pos += 3;
        let field_type = ValueType::from(self.buf[0]);
        let block_count = decode_be_u16(&self.buf[1..3]);
        self.index_block_idx = 0;
        self.index_block_count = block_count as usize;

        Ok(Some(()))
    }

    // TODO: not implemented
    pub fn next_block_entry(&mut self) -> ReadTsmResult<Option<()>> {
        if self.index_block_idx >= self.index_block_count {
            return Ok(None);
        }
        // read min time on block entry
        self.reader.read_at(self.pos, &mut self.buf[..]).context(IOSnafu)?;
        self.pos += 8;
        let min_ts = i64::from_be_bytes(self.buf);

        // read max time on block entry
        self.reader.read_at(self.pos, &mut self.buf[..]).context(IOSnafu)?;
        self.pos += 8;
        let max_ts = i64::from_be_bytes(self.buf);

        // read count on block entry
        self.reader.read_at(self.pos, &mut self.buf[..4]).context(IOSnafu)?;
        self.pos += 4;
        let count = decode_be_u32(&self.buf[..4]);

        // read block data offset
        self.reader.read_at(self.pos, &mut self.buf[..]).context(IOSnafu)?;
        self.pos += 8;
        let offset = u64::from_be_bytes(self.buf);

        // read block size
        self.reader.read_at(self.pos, &mut self.buf[..]).context(IOSnafu)?;
        self.pos += 8;
        let size = u64::from_be_bytes(self.buf);

        // read value offset
        self.reader.read_at(self.pos, &mut self.buf[..]).context(IOSnafu)?;
        self.pos += 8;
        let val_off = u64::from_be_bytes(self.buf);

        Ok(Some(()))
    }
}

pub fn print_tsm_statistics(path: impl AsRef<Path>) {
    let reader = TsmReader::open(&path).unwrap();
    for idx in reader.index_iterator() {
        let tr = idx.timerange();
        println!("============================================================");
        println!("Field | FieldId: {}, FieldType: {:?}, BlockCount: {}, MinTime: {}, MaxTime: {}",
                 idx.field_id(),
                 idx.field_type(),
                 idx.block_count(),
                 tr.0,
                 tr.1);
        println!("------------------------------------------------------------");
        for blk in idx.block_iterator() {
            println!("Block | FieldId: {}, MinTime: {}, MaxTime: {}, Count: {}, Offset: {}, ValOffset: {}",
                     blk.field_id(),
                     blk.min_ts(),
                     blk.max_ts(),
                     blk.count(),
                     blk.offset(),
                     blk.val_off())
        }
    }
    println!("============================================================");
}

pub fn load_index(reader: Arc<File>) -> ReadTsmResult<Index> {
    let len = reader.len();
    if len < FOOTER_SIZE as u64 {
        return Err(ReadTsmError::Invalid { reason: format!("TSM file size less than FOOTER_SIZE({})",
                                                           FOOTER_SIZE) });
    }
    let mut buf = [0u8; 8];

    // Read index data offset
    reader.read_at(len - 8, &mut buf).context(IOSnafu)?;
    let offset = u64::from_be_bytes(buf);
    if offset > len - FOOTER_SIZE as u64 {
        return Err(ReadTsmError::Invalid { reason: format!("TSM file size less than index offset({})",
                                                           offset) });
    }
    let data_len = (len - offset - FOOTER_SIZE as u64) as usize;
    let mut data = vec![0_u8; data_len];
    // Read index data
    reader.read_at(offset, &mut data).context(IOSnafu)?;

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
        let idx = load_index(reader).context(error::ReadTsmSnafu)?;

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
        if max_ts == min_ts {
            self.block_count = 1;
            return;
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
    tombstone: Option<Arc<Mutex<TsmTombstone>>>,
}

/// Returns if r1 (min_ts, max_ts) overlaps r2 (min_ts, max_ts)
pub fn overlaps_tombstone(r1: (i64, i64), r2: (i64, i64)) -> bool {
    r1.0 <= r2.1 && r1.1 >= r2.0
}

impl TsmReader {
    pub fn open(tsm_path: impl AsRef<Path>) -> Result<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let tsm_id = file_utils::get_tsm_file_id_by_path(&path)?;
        let tsm = Arc::new(file_manager::open_file(tsm_path)?);
        let tsm_idx = IndexReader::open(tsm.clone())?;
        let tsm_tomb = match TsmTombstone::with_tsm_file_id(&path, tsm_id) {
            Ok(tomb) => Some(Arc::new(Mutex::new(tomb))),
            Err(e) => None,
        };
        Ok(Self { reader: tsm, index_reader: Arc::new(tsm_idx), tombstone: tsm_tomb })
    }

    pub fn index_iterator(&self) -> IndexIterator {
        self.index_reader.iter()
    }

    /// Returns a DataBlock without tombstoe
    pub fn get_data_block(&self, block_meta: &BlockMeta) -> ReadTsmResult<DataBlock> {
        let blk_range = (block_meta.min_ts(), block_meta.max_ts());
        let mut buf = vec![0_u8; block_meta.size() as usize];
        let mut blk = read_data_block(self.reader.clone(),
                                      &mut buf,
                                      block_meta.field_type(),
                                      block_meta.offset(),
                                      block_meta.size(),
                                      block_meta.val_off())?;
        // TODO This costs too much
        if let Some(tomb_ref) = &self.tombstone {
            let tomb = tomb_ref.lock();
            tomb.tombstones()
                .iter()
                .filter(|t| {
                    t.field_id == block_meta.field_id()
                    && overlaps_tombstone((t.min_ts, t.max_ts), blk_range)
                })
                .for_each(|t| {
                    blk.exclude(t.min_ts, t.max_ts);
                });
        }

        Ok(blk)
    }

    // Reads raw data from file and returns the read data size.
    pub fn get_raw_data(&self, block_meta: &BlockMeta, dst: &mut Vec<u8>) -> ReadTsmResult<usize> {
        let data_len = block_meta.size() as usize;
        if dst.len() < data_len {
            dst.resize(data_len, 0);
        }
        self.reader.read_at(block_meta.offset(), &mut dst[..data_len]).context(IOSnafu)?;
        Ok(data_len)
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

    fn decode(&mut self, block_meta: &BlockMeta) -> ReadTsmResult<DataBlock> {
        let (offset, size) = (block_meta.offset(), block_meta.size());
        self.buf.resize(size as usize, 0);
        read_data_block(self.reader.clone(),
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
            return Some(self.decode(&dbm).context(error::ReadTsmSnafu));
        }

        None
    }
}

fn read_data_block(reader: Arc<File>,
                   buf: &mut [u8],
                   field_type: ValueType,
                   offset: u64,
                   size: u64,
                   val_off: u64)
                   -> ReadTsmResult<DataBlock> {
    assert!(buf.len() >= size as usize);

    reader.read_at(offset, buf).context(IOSnafu)?;
    decode_data_block(buf, field_type, size, val_off - offset)
}

pub fn decode_data_block(buf: &[u8],
                         field_type: ValueType,
                         size: u64,
                         val_off: u64)
                         -> ReadTsmResult<DataBlock> {
    assert!(buf.len() >= size as usize);

    // let crc_ts = &self.buf[..4];
    let mut ts = Vec::with_capacity(MAX_BLOCK_VALUES);
    timestamp::decode(&buf[4..val_off as usize], &mut ts).context(DecodeSnafu)?;
    // let crc_data = &self.buf[(val_offset - offset) as usize..4];
    let data = &buf[(val_off + 4) as usize..];
    match field_type {
        ValueType::Float => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            float::decode(&data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::F64 { ts, val })
        },
        ValueType::Integer => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            integer::decode(&data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::I64 { ts, val })
        },
        ValueType::Boolean => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            boolean::decode(&data, &mut val).context(DecodeSnafu)?;

            Ok(DataBlock::Bool { ts, val })
        },
        ValueType::String => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            string::decode(&data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::Str { ts, val })
        },
        ValueType::Unsigned => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            unsigned::decode(&data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::U64 { ts, val })
        },
        _ => {
            Err(ReadTsmError::Decode { source: From::from(format!("cannot decode block {:?} with no unknown value type",
                                                                  field_type)) })
        },
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use models::FieldId;
    use parking_lot::Mutex;

    use super::print_tsm_statistics;
    use crate::{
        file_manager::{self, get_file_manager},
        file_utils,
        tsm::{DataBlock, TsmReader, TsmTombstone, TsmWriter},
    };

    fn prepare() -> (PathBuf, PathBuf) {
        let dir = PathBuf::from("/tmp/test/tsm_reader".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let tsm_file = file_utils::make_tsm_file_name(&dir, 1);
        let tombstone_file = file_utils::make_tsm_tombstone_file_name(&dir, 1);
        println!("Writing file: {}, {}",
                 tsm_file.to_str().unwrap(),
                 tombstone_file.to_str().unwrap());

        let file_write = get_file_manager().create_file(&tsm_file).unwrap();

        let ori_data: HashMap<FieldId, Vec<DataBlock>> =
            HashMap::from([(1, vec![DataBlock::U64 { ts: vec![2, 3, 4], val: vec![12, 13, 15] }]),
                           (2,
                            vec![DataBlock::U64 { ts: vec![2, 3, 4], val: vec![101, 102, 103] }])]);
        let mut writer = TsmWriter::open(file_write.into_cursor(), 1, false, 0).unwrap();
        for (fid, blks) in ori_data.iter() {
            for blk in blks.iter() {
                writer.write_block(*fid, blk).unwrap();
            }
        }
        writer.write_index().unwrap();
        writer.flush().unwrap();

        let mut tombstone = TsmTombstone::with_path(&tombstone_file).unwrap();
        tombstone.add_range(&[1], 2, 4).unwrap();
        tombstone.flush().unwrap();

        (tsm_file, tombstone_file)
    }

    #[test]
    fn test_tsm_reader() {
        let (tsm_file, tombstone_file) = prepare();
        println!("Reading file: {}, {}",
                 tsm_file.to_str().unwrap(),
                 tombstone_file.to_str().unwrap());
        print_tsm_statistics(&tsm_file);

        let reader = TsmReader::open(&tsm_file).unwrap();
        for idx in reader.index_iterator() {
            for blk in idx.block_iterator() {
                let data_blk = reader.get_data_block(&blk).unwrap();
                dbg!(&data_blk);
            }
        }
    }
}
