use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
};

use models::utils::split_code_type_id;
use models::{FieldId, Timestamp, ValueType};
use protos::kv_service::FieldType;
use snafu::{ResultExt, Snafu};
use utils::{BkdrHasher, BloomFilter};

use super::{
    block, index::Index, BlockEntry, BlockMetaIterator, IndexEntry, IndexMeta, BLOCK_META_SIZE,
    BLOOM_FILTER_BITS, INDEX_META_SIZE, MAX_BLOCK_VALUES,
};
use crate::tsm::coder_instence::CodeType;
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
// ┌───────────────────────────────────────────────────────────────────────────────┐
// │                               Index                                           │
// ├─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬────────┬───────┤
// │ fieldId │ Type │ Count │Min Time │Max Time │ count  │ Offset │  Size  │Valoff │
// │ 8 bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │4 bytes │8 bytes │8 bytes │8 bytes│
// └─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴────────┴───────┘
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
        write!(
            f,
            "max_file_size: {}, block_index: {}",
            self.max_file_size, self.block_index
        )
    }
}

impl std::error::Error for MaxFileSizeExceedError {}

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum WriteTsmError {
    #[snafu(display("IO error: {}", source))]
    IO { source: std::io::Error },

    #[snafu(display("Encode error: {}", source))]
    Encode {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Max file size exceed: {}", source))]
    MaxFileSizeExceed { source: MaxFileSizeExceedError },
}

impl From<WriteTsmError> for Error {
    fn from(wtr: WriteTsmError) -> Self {
        Error::WriteTsm { source: wtr }
    }
}

struct IndexBuf {
    index_offset: u64,
    buf: BTreeMap<FieldId, IndexEntry>,
    bloom_filter: BloomFilter,
}

impl IndexBuf {
    pub fn new() -> Self {
        Self {
            index_offset: 0,
            buf: BTreeMap::new(),
            bloom_filter: BloomFilter::new(BLOOM_FILTER_BITS),
        }
    }

    pub fn set_index_offset(&mut self, index_offset: u64) {
        self.index_offset = index_offset;
    }

    pub fn insert_block_meta(&mut self, ie: IndexEntry, be: BlockEntry) {
        let fid = ie.field_id;
        let idx = self.buf.entry(fid).or_insert(ie);
        idx.blocks.push(be);
        self.bloom_filter.insert(&fid.to_be_bytes()[..]);
    }

    pub fn write_to(&self, writer: &mut FileCursor) -> WriteTsmResult<usize> {
        let mut size = 0_usize;

        let mut buf = vec![0_u8; BLOCK_META_SIZE];
        for (_, idx) in self.buf.iter() {
            idx.encode(&mut buf[..INDEX_META_SIZE])?;
            writer.write(&buf[..INDEX_META_SIZE]).context(IOSnafu)?;
            size += 11;
            for blk in idx.blocks.iter() {
                blk.encode(&mut buf);
                writer.write(&buf[..]).context(IOSnafu)?;
                size += 44;
            }
        }

        Ok(size)
    }
}

/// TSM file writer.
///
/// # Examples
/// ```ignore
/// let path = "/tmp/tsm_writer/test_write.tsm";
/// let file = file_manager::create_file(path);
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
    path: PathBuf,
    writer: FileCursor,
    /// Store tsm sequence for debug
    sequence: u64,
    /// Store is_delta for debug
    is_delta: bool,
    /// Store min_ts for version edit
    min_ts: Timestamp,
    /// Store max_ts for version edit
    max_ts: Timestamp,
    size: u64,
    max_size: u64,
    index_buf: IndexBuf,
}

impl TsmWriter {
    pub fn open(
        path: impl AsRef<Path>,
        sequence: u64,
        is_delta: bool,
        max_size: u64,
    ) -> Result<Self> {
        let writer = file_manager::create_file(&path)?.into_cursor();
        let mut w = Self {
            path: path.as_ref().into(),
            writer,
            sequence,
            is_delta,
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
            size: 0,
            max_size,
            index_buf: IndexBuf::new(),
        };
        write_header_to(&mut w.writer)
            .context(error::WriteTsmSnafu)
            .map(|s| w.size = s as u64)?;
        Ok(w)
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn is_delta(&self) -> bool {
        self.is_delta
    }

    pub fn min_ts(&self) -> Timestamp {
        self.min_ts
    }

    pub fn max_ts(&self) -> Timestamp {
        self.max_ts
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn write_block(&mut self, field_id: FieldId, block: &DataBlock) -> WriteTsmResult<usize> {
        let mut write_pos = self.writer.pos();
        if let Some(rg) = block.time_range() {
            self.min_ts = self.min_ts.min(rg.0);
            self.max_ts = self.max_ts.max(rg.1);
        }
        let ret = write_block_to(
            &mut self.writer,
            &mut write_pos,
            &mut self.index_buf,
            field_id,
            block,
        );
        if let Ok(s) = ret {
            self.size += s as u64
        }
        ret
    }

    pub fn write_raw(&mut self, block_meta: &BlockMeta, block: &[u8]) -> WriteTsmResult<usize> {
        let mut write_pos = self.writer.pos();
        self.min_ts = self.min_ts.min(block_meta.min_ts());
        self.max_ts = self.max_ts.max(block_meta.max_ts());
        let ret = write_raw_data_to(
            &mut self.writer,
            &mut write_pos,
            &mut self.index_buf,
            block_meta,
            block,
        );
        if let Ok(s) = ret {
            self.size += s as u64
        }
        ret
    }

    pub fn write_index(&mut self) -> WriteTsmResult<usize> {
        let mut size = 0_usize;

        self.index_buf.set_index_offset(self.writer.pos());
        self.index_buf
            .write_to(&mut self.writer)
            .map(|s| size += s)?;
        write_footer_to(
            &mut self.writer,
            &self.index_buf.bloom_filter,
            self.index_buf.index_offset,
        )
        .map(|s| size += s)?;

        Ok(size)
    }

    pub fn flush(&self) -> WriteTsmResult<()> {
        self.writer.sync_all(FileSync::Hard).context(IOSnafu)
    }
}

pub fn new_tsm_writer(
    dir: impl AsRef<Path>,
    tsm_sequence: u64,
    is_delta: bool,
    max_size: u64,
) -> Result<TsmWriter> {
    let tsm_path = if is_delta {
        file_utils::make_delta_file_name(dir, tsm_sequence)
    } else {
        file_utils::make_tsm_file_name(dir, tsm_sequence)
    };
    TsmWriter::open(tsm_path, tsm_sequence, is_delta, max_size)
}

pub fn write_header_to(writer: &mut FileCursor) -> WriteTsmResult<usize> {
    let start = writer.pos();
    writer
        .write(TSM_MAGIC.to_be_bytes().as_ref())
        .and_then(|_| writer.write(&VERSION.to_be_bytes()[..]))
        .context(IOSnafu)?;

    Ok((writer.pos() - start) as usize)
}

fn write_raw_data_to(
    writer: &mut FileCursor,
    write_pos: &mut u64,
    index_buf: &mut IndexBuf,
    block_meta: &BlockMeta,
    block: &[u8],
) -> WriteTsmResult<usize> {
    let mut size = 0_usize;
    let offset = writer.pos();
    writer
        .write(block)
        .map(|s| {
            size += s;
        })
        .context(IOSnafu)?;
    let ts_block_len = block_meta.val_off() - block_meta.offset();

    index_buf.insert_block_meta(
        IndexEntry {
            field_id: block_meta.field_id(),
            field_type: block_meta.field_type(),
            blocks: vec![],
        },
        BlockEntry {
            min_ts: block_meta.min_ts(),
            max_ts: block_meta.max_ts(),
            count: block_meta.count(),
            offset,
            size: block.len() as u64,
            val_offset: offset + ts_block_len,
        },
    );

    Ok(size)
}

fn write_block_to(
    writer: &mut FileCursor,
    write_pos: &mut u64,
    index_buf: &mut IndexBuf,
    field_id: FieldId,
    block: &DataBlock,
) -> WriteTsmResult<usize> {
    if block.is_empty() {
        return Ok(0);
    }

    let offset = writer.pos();
    let mut size = 0;

    let (ts_code_type, val_code_type) = split_code_type_id(block.code_type_id());
    // TODO Make encoding result streamable
    let (ts_buf, data_buf) = block
        .encode(
            0,
            block.len(),
            CodeType::try_from(ts_code_type).unwrap(),
            CodeType::try_from(val_code_type).unwrap(),
        )
        .context(EncodeSnafu)?;
    // Write u32 hash for timestamps
    writer
        .write(&crc32fast::hash(&ts_buf).to_be_bytes()[..])
        .map(|s| {
            size += s;
        })
        .context(IOSnafu)?;
    // Write timestamp blocks
    writer
        .write(&ts_buf)
        .map(|s| {
            size += s;
        })
        .context(IOSnafu)?;

    let val_off = writer.pos();

    // Write u32 hash for value blocks
    writer
        .write(&crc32fast::hash(&data_buf).to_be_bytes()[..])
        .map(|s| {
            size += s;
        })
        .context(IOSnafu)?;
    // Write value blocks
    writer
        .write(&data_buf)
        .map(|s| {
            size += s;
        })
        .context(IOSnafu)?;

    index_buf.insert_block_meta(
        IndexEntry {
            field_id,
            field_type: block.field_type(),
            blocks: vec![],
        },
        BlockEntry {
            min_ts: block.ts()[0],
            max_ts: block.ts()[block.len() - 1],
            count: block.len() as u32,
            offset,
            size: size as u64,
            val_offset: val_off,
        },
    );

    Ok(size)
}

fn write_footer_to(
    writer: &mut FileCursor,
    bloom_filter: &BloomFilter,
    index_offset: u64,
) -> WriteTsmResult<usize> {
    let start = writer.pos();
    writer
        .write(bloom_filter.bytes())
        .and_then(|_| writer.write(&index_offset.to_be_bytes()[..]))
        .context(IOSnafu)?;

    Ok((writer.pos() - start) as usize)
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use models::{FieldId, ValueType};

    use super::new_tsm_writer;
    use crate::tsm::coder_instence::CodeType;
    use crate::{
        direct_io::FileSync,
        file_manager::{self, get_file_manager, FileManager},
        tsm::{coders, ColumnReader, DataBlock, IndexReader, TsmReader, TsmWriter},
    };

    const TEST_PATH: &str = "/tmp/test/tsm_writer";

    #[test]
    fn test_str_encode() {
        // let block = DataBlock::new(10, crate::DataType::Str(StrCell{ts:1, val: vec![]}));
        // block.insert(crate::DataType::Str(StrCell{ts:1, val: vec![1]}));
        // block.insert(crate::DataType::Str(StrCell{ts:2, val: vec![2]}));
        // block.insert(crate::DataType::Str(StrCell{ts:3, val: vec![3]}));
        let mut data = vec![];
        let str = vec![vec![1_u8]];
        let tmp: Vec<&[u8]> = str.iter().map(|x| &x[..]).collect();
        let _ = coders::string::str_snappy_encode(&tmp, &mut data);
    }

    fn write_to_tsm(dir: impl AsRef<Path>, file_name: &str, data: &HashMap<FieldId, DataBlock>) {
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let tsm_path = dir.as_ref().join(file_name);
        let mut writer = TsmWriter::open(tsm_path, 0, false, 0).unwrap();
        for (fid, blk) in data.iter() {
            writer.write_block(*fid, blk).unwrap();
        }
        writer.write_index().unwrap();
        writer.flush().unwrap();
    }

    fn read_from_tsm(path: impl AsRef<Path>) -> HashMap<FieldId, DataBlock> {
        let file = Arc::new(file_manager::open_file(&path).unwrap());
        let len = file.len();

        let index = IndexReader::open(file.clone()).unwrap();
        let mut data: HashMap<FieldId, DataBlock> = HashMap::new();
        for idx_meta in index.iter() {
            let cr = ColumnReader::new(file.clone(), idx_meta.block_iterator());
            for blk_ret in cr {
                let blk = blk_ret.unwrap();
                data.insert(idx_meta.field_id(), blk);
            }
        }

        data
    }

    fn check_tsm(path: impl AsRef<Path>, data: &HashMap<FieldId, DataBlock>) {
        let data = read_from_tsm(path);
        for (k, v) in data.iter() {
            assert_eq!(v, data.get(k).unwrap());
        }
    }

    #[test]
    fn test_tsm_write_fast() {
        let dir = Path::new(TEST_PATH);
        #[rustfmt::skip]
        let data: HashMap<FieldId, DataBlock> = HashMap::from([
            (1, DataBlock::U64 { ts: vec![2, 3, 4], val: vec![12, 13, 15], code_type_id: 0 }),
            (2, DataBlock::U64 { ts: vec![2, 3, 4], val: vec![101, 102, 103], code_type_id: 0 }),
        ]);

        write_to_tsm(&dir, "tsm_write_fast.tsm", &data);
        check_tsm(dir.join("tsm_write_fast.tsm"), &data);
    }

    #[test]
    fn test_tsm_write_1() {
        let mut ts_1: Vec<i64> = Vec::new();
        let mut val_1: Vec<i64> = Vec::new();
        for i in 1..1001 {
            ts_1.push(i as i64);
            val_1.push(i as i64);
        }
        let mut ts_2: Vec<i64> = Vec::new();
        let mut val_2: Vec<i64> = Vec::new();
        for i in 1001..2001 {
            ts_2.push(i as i64);
            val_2.push(i as i64);
        }
        let ts = ts_1.iter().chain(ts_2.iter()).copied().collect();
        let val = val_1.iter().chain(val_2.iter()).copied().collect();

        let dir = Path::new(TEST_PATH).join("1");
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let tsm_path = dir.join("test_tsm_write_1.tsm");
        let mut writer = TsmWriter::open(tsm_path, 0, false, 0).unwrap();

        #[rustfmt::skip]
        let data = vec![
            (1, DataBlock::I64 { ts: ts_1, val: val_1, code_type_id: 0 }),
            (1, DataBlock::I64 { ts: ts_2, val: val_2, code_type_id: 0 }),
        ];
        for (fid, blk) in data.iter() {
            writer.write_block(*fid, blk).unwrap();
        }
        writer.write_index().unwrap();
        writer.flush().unwrap();

        check_tsm(
            dir.join("test_tsm_write_1.tsm"),
            &HashMap::from([(
                1,
                DataBlock::I64 {
                    ts,
                    val,
                    code_type_id: 0,
                },
            )]),
        );
    }
}
