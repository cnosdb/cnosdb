use std::{collections::HashMap, path::Path};

use models::{FieldId, Timestamp, ValueType};
use snafu::{ResultExt, Snafu};
use utils::{BkdrHasher, BloomFilter};

use super::{
    block, index::Index, BlockMetaIterator, BLOCK_META_SIZE, BLOOM_FILTER_BITS, INDEX_META_SIZE,
    MAX_BLOCK_VALUES,
};
use crate::{
    direct_io::{File, FileCursor, FileSync},
    error::{self, Error, Result},
    file_manager, file_utils,
    tsm::{BlockMeta, DataBlock},
};

// A TSM file is composed for four sections: header, blocks, index and the footer.
//
// ┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
// │ Header │               Blocks               │    Index    │    Footer    │
// │5 bytes │              N bytes               │   N bytes   │   8 bytes    │
// └────────┴────────────────────────────────────┴─────────────┴──────────────┘
//
// ┌───────────────────┐
// │      Header       │
// ├─────────┬─────────┤
// │  Magic  │ Version │
// │ 4 bytes │ 1 byte  │
// └─────────┴─────────┘
//
// ┌───────────────────────────────────────┐
// │               Blocks                  │
// ├───────────────────┬───────────────────┤
// │                Block                  │
// ├─────────┬─────────┼─────────┬─────────┼
// │  CRC    │ ts      │  CRC    │  value  │
// │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
// └─────────┴─────────┴─────────┴─────────┴
//
// ┌──────────────────────────────────────────────────────────────────────┐
// │                               Index                                  │
// ├─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───────┤
// │ fieldId │ Type │ Count │Min Time │Max Time │ Offset │  Size  │Valoff │
// │ 8 bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │8 bytes │8 bytes│
// └─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───────┘
//
// ┌─────────────────────────┐
// │ Footer                  │
// ├───────────────┬─────────┤
// │ Bloom Filter  │Index Ofs│
// │ 8 bytes       │ 8 bytes │
// └───────────────┴─────────┘

const HEADER_LEN: u64 = 5;
const TSM_MAGIC: u32 = 0x01346613;
const VERSION: u8 = 1;

pub type WriteTsmResult<T, E = WriteTsmError> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct MaxFileSizeExceedError {
    max_file_size: u64,
    block_index: usize,
}

impl std::fmt::Display for MaxFileSizeExceedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "max_file_size: {}, block_index: {}", self.max_file_size, self.block_index)
    }
}

impl std::error::Error for MaxFileSizeExceedError {}

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum WriteTsmError {
    #[snafu(display("IO error: {}", source))]
    IO { source: std::io::Error },

    #[snafu(display("Encode error: {}", source))]
    Encode { source: Box<dyn std::error::Error + Send + Sync> },

    #[snafu(display("Max file size exceed: {}", source))]
    MaxFileSizeExceed { source: MaxFileSizeExceedError },
}

impl Into<Error> for WriteTsmError {
    fn into(self) -> Error {
        Error::WriteTsm { source: self }
    }
}

struct IndexBuf {
    index_offset: u64,
    index_meta: Vec<u8>,
    last_block_meta_offset: usize,
    block_meta_offsets: Vec<usize>,
    block_meta: Vec<u8>,

    bloom_filter: BloomFilter,
}

impl IndexBuf {
    pub fn new() -> Self {
        Self { index_offset: 0,
               index_meta: Vec::new(),
               last_block_meta_offset: 0,
               block_meta_offsets: Vec::new(),
               block_meta: Vec::new(),
               bloom_filter: BloomFilter::new(BLOOM_FILTER_BITS) }
    }

    pub fn set_index_offset(&mut self, index_offset: u64) {
        self.index_offset = index_offset;
    }

    pub fn insert_index_meta(&mut self,
                             field_id: FieldId,
                             block_type: ValueType,
                             block_count: u16) {
        self.index_meta.extend_from_slice(&field_id.to_be_bytes()[..]);
        self.index_meta.extend_from_slice(&[u8::from(block_type)][..]);
        self.index_meta.extend_from_slice(&block_count.to_be_bytes()[..]);
        self.block_meta_offsets.push(self.last_block_meta_offset);
        self.last_block_meta_offset = self.block_meta.len();
    }

    pub fn insert_block_meta(&mut self,
                             min_ts: i64,
                             max_ts: i64,
                             offset: u64,
                             size: u64,
                             val_off: u64) {
        self.block_meta.extend_from_slice(&min_ts.to_be_bytes()[..]);
        self.block_meta.extend_from_slice(&max_ts.to_be_bytes()[..]);
        self.block_meta.extend_from_slice(&offset.to_be_bytes()[..]);
        self.block_meta.extend_from_slice(&size.to_be_bytes()[..]);
        self.block_meta.extend_from_slice(&val_off.to_be_bytes()[..]);
    }

    pub fn write_to(&self, writer: &mut FileCursor) -> WriteTsmResult<usize> {
        let mut size = 0_usize;
        let mut index_pos = 0_usize;
        let mut index_idx = 0_usize;
        while index_pos < self.index_meta.len() {
            writer.write(&self.index_meta[index_pos..index_pos + INDEX_META_SIZE])
                  .map(|s| size += s)
                  .context(IOSnafu)?;
            index_pos += INDEX_META_SIZE;
            let blocks_sli = match self.block_meta_offsets.get(index_idx + 1) {
                Some(nbp) => &self.block_meta[self.block_meta_offsets[index_idx]..*nbp],
                None => &self.block_meta[self.block_meta_offsets[index_idx]..],
            };
            writer.write(&blocks_sli).map(|s| size += s).context(IOSnafu)?;
            index_idx += 1;
        }

        Ok(size)
    }
}

/// TSM file writer.
///
/// # Examples
/// ```rust
/// let path = "/tmp/tsm_writer/test_write.tsm";
/// let file = file_manager::get_file_manager().create_file(path);
/// // Create a new TSM file, write header.
/// let mut writer = TsmWriter::open(file).unwrap();
/// // Write blocks.
/// writer.write_block(1, &DataBlock::I64{ ts: vec![1], val: vec![1] }).unwrap();
/// // Write index and footer.
/// writer.write_index().unwrap();
/// // Sync to disk.
/// writer.flush().unwrap();
/// ```
pub struct TsmWriter {
    writer: FileCursor,
    /// Store tsm sequence for debug
    sequence: u64,
    /// Store is_delta for debug
    is_delta: bool,
    size: u64,
    max_size: u64,
    index_buf: IndexBuf,
}

impl TsmWriter {
    pub fn open(writer: FileCursor, sequence: u64, is_delta: bool, max_size: u64) -> Result<Self> {
        let mut w =
            Self { writer, sequence, is_delta, size: 0, max_size, index_buf: IndexBuf::new() };
        write_header_to(&mut w.writer).context(error::WriteTsmSnafu).map(|s| w.size = s as u64)?;
        Ok(w)
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn is_delta(&self) -> bool {
        self.is_delta
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    fn set_size(&mut self, size: u64) {
        self.size = size;
    }

    pub fn write_block(&mut self, field_id: FieldId, block: &DataBlock) -> WriteTsmResult<usize> {
        let mut write_pos = self.writer.pos();
        let ret =
            write_block_to(&mut self.writer, &mut write_pos, &mut self.index_buf, field_id, block);
        if let Ok(s) = ret {
            self.size += s as u64
        }
        ret
    }

    pub fn write_index(&mut self) -> WriteTsmResult<usize> {
        println!("[DEBUG] TsmWRiter::write_index()");
        let mut size = 0_usize;

        self.index_buf.set_index_offset(self.writer.pos());
        self.index_buf.write_to(&mut self.writer).map(|s| size += s)?;
        write_footer_to(&mut self.writer,
                        &self.index_buf.bloom_filter,
                        self.index_buf.index_offset).map(|s| size += s)?;

        Ok(size)
    }

    pub fn flush(&self) -> WriteTsmResult<()> {
        println!("[DEBUG] TsmWriter::flush()");
        self.writer.sync_all(FileSync::Hard).context(IOSnafu)
    }
}

pub fn new_tsm_writer(dir: &str,
                      tsm_sequence: u64,
                      is_delta: bool,
                      max_size: u64)
                      -> Result<TsmWriter> {
    let tsm_path = file_utils::make_tsm_file_name(dir, tsm_sequence);
    let tsm_cursor = file_manager::create_file(&tsm_path)?.into_cursor();
    TsmWriter::open(tsm_cursor, tsm_sequence, is_delta, max_size)
}

pub fn write_header_to(writer: &mut FileCursor) -> WriteTsmResult<usize> {
    let start = writer.pos();
    writer.write(&TSM_MAGIC.to_be_bytes().as_ref())
          .and_then(|_| writer.write(&VERSION.to_be_bytes()[..]))
          .context(IOSnafu)?;

    Ok((writer.pos() - start) as usize)
}

fn write_block_to(writer: &mut FileCursor,
                  write_pos: &mut u64,
                  index_buf: &mut IndexBuf,
                  field_id: FieldId,
                  block: &DataBlock)
                  -> WriteTsmResult<usize> {
    let point_cnt = block.len();
    let block_count = ((point_cnt - 1) / MAX_BLOCK_VALUES + 1) as u16;
    let idx_meta_beg = writer.pos();
    let block_type = block.field_type();
    let mut min_ts: i64;
    let mut max_ts: i64;
    let mut offset: u64;
    let mut val_off: u64;

    let ts_sli = block.ts();

    let field_type = block.field_type();
    let mut i = 0_usize;
    let mut last_index = 0_usize;
    let mut total_size = 0_usize;
    let mut blk_size: usize;

    while i < block_count as usize {
        blk_size = 0_usize;
        let start = last_index;
        // let end = point_cnt % MAX_BLOCK_VALUES + i * MAX_BLOCK_VALUES;
        let mut end = start + MAX_BLOCK_VALUES;
        if end > point_cnt {
            end = point_cnt;
        }
        last_index = end;

        min_ts = ts_sli[start];
        max_ts = ts_sli[end - 1];
        offset = writer.pos();

        // TODO Make encoding result streamable
        let (ts_buf, data_buf) = block.encode(start, end).context(EncodeSnafu)?;
        // Write u32 hash for timestamps
        writer.write(&crc32fast::hash(&ts_buf).to_be_bytes()[..])
              .map(|s| {
                  blk_size += s;
              })
              .context(IOSnafu)?;
        // Write timestamp blocks
        writer.write(&ts_buf)
              .map(|s| {
                  blk_size += s;
              })
              .context(IOSnafu)?;

        val_off = writer.pos();

        // Write u32 hash for value blocks
        writer.write(&crc32fast::hash(&data_buf).to_be_bytes()[..])
              .map(|s| {
                  blk_size += s;
              })
              .context(IOSnafu)?;
        // Write value blocks
        writer.write(&data_buf)
              .map(|s| {
                  blk_size += s;
              })
              .context(IOSnafu)?;

        total_size += blk_size;

        index_buf.insert_block_meta(min_ts, max_ts, offset, blk_size as u64, val_off);

        i += 1;
    }
    index_buf.insert_index_meta(field_id, block_type, block_count);

    Ok(total_size)
}

fn write_footer_to(writer: &mut FileCursor,
                   bloom_filter: &BloomFilter,
                   index_offset: u64)
                   -> WriteTsmResult<usize> {
    let start = writer.pos();
    writer.write(&bloom_filter.bytes())
          .and_then(|_| writer.write(&index_offset.to_be_bytes()[..]))
          .context(IOSnafu)?;

    Ok((writer.pos() - start) as usize)
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, path::Path, sync::Arc};

    use models::{FieldId, ValueType};

    use super::new_tsm_writer;
    use crate::{
        direct_io::FileSync,
        file_manager::{self, get_file_manager, FileManager},
        memcache::{BoolCell, DataType, F64Cell, I64Cell, StrCell, U64Cell},
        tsm::{coders, ColumnReader, DataBlock, IndexReader, TsmWriter},
    };

    #[test]
    fn test_str_encode() {
        // let block = DataBlock::new(10, crate::DataType::Str(StrCell{ts:1, val: vec![]}));
        // block.insert(crate::DataType::Str(StrCell{ts:1, val: vec![1]}));
        // block.insert(crate::DataType::Str(StrCell{ts:2, val: vec![2]}));
        // block.insert(crate::DataType::Str(StrCell{ts:3, val: vec![3]}));
        let mut data = vec![];
        let str = vec![vec![1_u8]];
        let tmp: Vec<&[u8]> = str.iter().map(|x| &x[..]).collect();
        let _ = coders::string::encode(&tmp, &mut data);
    }

    #[test]
    fn test_tsm_write_fast() {
        let dir = Path::new("/tmp/test/tsm_writer");
        if !file_manager::try_exists(dir) {
            std::fs::create_dir_all(dir).unwrap();
        }
        println!("Writing file: {}/tsm_write_fast.tsm", dir.to_str().unwrap());
        let file =
            get_file_manager().create_file(dir.join("tsm_write_fast.tsm")).unwrap().into_cursor();

        let data: HashMap<FieldId, DataBlock> =
            HashMap::from([(1, DataBlock::U64 { ts: vec![2, 3, 4], val: vec![12, 13, 15] }),
                           (2, DataBlock::U64 { ts: vec![2, 3, 4], val: vec![101, 102, 103] })]);

        let mut writer = TsmWriter::open(file, 0, false, 0).unwrap();
        for (fid, blk) in data.iter() {
            writer.write_block(*fid, blk).unwrap();
        }
        writer.write_index().unwrap();
        writer.flush().unwrap();

        println!("File write finish: {}/tsm_write_fast.tsm", dir.to_str().unwrap());
        test_tsm_read_fast();
    }

    fn test_tsm_read_fast() {
        let dir = Path::new("/tmp/test/tsm_writer");
        if !file_manager::try_exists(dir) {
            std::fs::create_dir_all(dir).unwrap();
        }
        println!("Reading file: {}/tsm_write_fast.tsm", dir.to_str().unwrap());
        let file = Arc::new(get_file_manager().open_file(dir.join("tsm_write_fast.tsm")).unwrap());
        let len = file.len();

        let index = IndexReader::open(file.clone()).unwrap();
        let mut column_readers: HashMap<FieldId, ColumnReader> = HashMap::new();
        for index_meta in index.iter() {
            column_readers.insert(index_meta.field_id(),
                                  ColumnReader::new(file.clone(), index_meta.block_iterator()));
        }

        let ori_data: HashMap<FieldId, Vec<DataBlock>> =
            HashMap::from([(1, vec![DataBlock::U64 { ts: vec![2, 3, 4], val: vec![12, 13, 15] }]),
                           (2,
                            vec![DataBlock::U64 { ts: vec![2, 3, 4], val: vec![101, 102, 103] }])]);

        for (fid, col_reader) in column_readers.iter_mut() {
            dbg!(fid);
            let mut data = Vec::new();
            for block in col_reader.next().unwrap() {
                data.push(block);
            }
            dbg!(&data);

            assert_eq!(*ori_data.get(fid).unwrap(), data);
        }
        println!("File read finish: {}/tsm_write_fast.tsm", dir.to_str().unwrap());
    }

    #[test]
    fn test_tsm_write_slow() {
        let dir = "/tmp/test/tsm_writer";
        if !file_manager::try_exists(dir) {
            std::fs::create_dir_all(dir).unwrap();
        }
        // Write 3 files.
        for file_seq in 1..4 {
            let mut cache_data: HashMap<FieldId, DataBlock> = HashMap::new();
            let fid_start = (file_seq - 1) * 100 + 1;
            let fid_end = (file_seq) * 100 + 1;
            for i in fid_start..fid_end {
                let fid = i as u64;
                // Use i%5 as ValueType
                let vtyp = ValueType::from((i % 5) as u8);
                cache_data.insert(fid, DataBlock::new(10000, vtyp));
                let blk_ref = cache_data.get_mut(&fid).unwrap();
                // Produce many ts-val pair, ts is from 1 to 10000, val is randomly generated
                for j in 1..10001 {
                    let val = match vtyp {
                        ValueType::Unknown => panic!("value type is unknown"),
                        ValueType::Float => {
                            DataType::F64(F64Cell { ts: j, val: rand::random::<f64>() })
                        },
                        ValueType::Integer => {
                            DataType::I64(I64Cell { ts: j, val: rand::random::<i64>() })
                        },
                        ValueType::Unsigned => {
                            DataType::U64(U64Cell { ts: j, val: rand::random::<u64>() })
                        },
                        ValueType::Boolean => {
                            DataType::Bool(BoolCell { ts: j, val: rand::random::<bool>() })
                        },
                        ValueType::String => {
                            DataType::Str(StrCell { ts: j, val: b"hello world".to_vec() })
                        },
                    };
                    blk_ref.insert(&val);
                }
            }

            // Write to tsm
            let mut writer = new_tsm_writer(&dir, file_seq, false, 0).unwrap();
            for (fid, blk) in cache_data.iter() {
                println!("{}", blk);
                writer.write_block(*fid, blk).unwrap();
            }
            writer.write_index().unwrap();
            writer.flush().unwrap();
        }
    }
}
