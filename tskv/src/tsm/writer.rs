use std::{
    collections::{BTreeMap, HashMap},
    io::IoSlice,
    path::{Path, PathBuf},
};

use models::{FieldId, Timestamp, ValueType};
use protos::kv_service::FieldType;
use snafu::{ResultExt, Snafu};
use utils::{BkdrHasher, BloomFilter};

use crate::{
    error::{self, Error, Result},
    file_system::{file_manager, FileCursor, IFile},
    file_utils,
    tsm::{
        BlockEntry, BlockMeta, BlockMetaIterator, DataBlock, Index, IndexEntry, IndexMeta,
        BLOCK_META_SIZE, BLOOM_FILTER_BITS, INDEX_META_SIZE, MAX_BLOCK_VALUES,
    },
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
const TSM_MAGIC: [u8; 4] = 0x01346613_u32.to_be_bytes();
const VERSION: [u8; 1] = [1];

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

    #[snafu(display("Tsm writer has been finished: {}", path.display()))]
    Finished { path: PathBuf },
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

    pub async fn write_to(&self, writer: &mut FileCursor) -> WriteTsmResult<usize> {
        let mut size = 0_usize;

        let mut buf = vec![0_u8; BLOCK_META_SIZE];
        for (_, idx) in self.buf.iter() {
            idx.encode(&mut buf[..INDEX_META_SIZE])?;
            writer
                .write(&buf[..INDEX_META_SIZE])
                .await
                .context(IOSnafu)?;
            size += INDEX_META_SIZE;
            for blk in idx.blocks.iter() {
                blk.encode(&mut buf);
                writer.write(&buf[..]).await.context(IOSnafu)?;
                size += BLOCK_META_SIZE;
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
    tmp_path: PathBuf,
    final_path: PathBuf,
    finished: bool,

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
    pub async fn open(
        path: impl AsRef<Path>,
        sequence: u64,
        is_delta: bool,
        max_size: u64,
    ) -> Result<Self> {
        let final_path: PathBuf = path.as_ref().into();
        let mut tmp_path_str = final_path.as_os_str().to_os_string();
        tmp_path_str.push(".tmp");
        let tmp_path = PathBuf::from(tmp_path_str);

        let writer = file_manager::create_file(&tmp_path).await?.into();
        let mut w = Self {
            tmp_path,
            final_path,
            finished: false,
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
            .await
            .context(error::WriteTsmSnafu)
            .map(|s| w.size = s as u64)?;
        Ok(w)
    }

    pub fn finished(&self) -> bool {
        self.finished
    }

    pub fn path(&self) -> PathBuf {
        if self.finished {
            self.final_path.clone()
        } else {
            self.tmp_path.clone()
        }
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

    pub async fn write_block(
        &mut self,
        field_id: FieldId,
        block: &DataBlock,
    ) -> WriteTsmResult<usize> {
        if self.finished {
            return Err(WriteTsmError::Finished {
                path: self.final_path.clone(),
            });
        }
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
        )
        .await;
        if let Ok(s) = ret {
            self.size += s as u64
        }
        ret
    }

    pub async fn write_raw(
        &mut self,
        block_meta: &BlockMeta,
        block: &[u8],
    ) -> WriteTsmResult<usize> {
        if self.finished {
            return Err(WriteTsmError::Finished {
                path: self.final_path.clone(),
            });
        }
        let mut write_pos = self.writer.pos();
        self.min_ts = self.min_ts.min(block_meta.min_ts());
        self.max_ts = self.max_ts.max(block_meta.max_ts());
        let ret = write_raw_data_to(
            &mut self.writer,
            &mut write_pos,
            &mut self.index_buf,
            block_meta,
            block,
        )
        .await;
        if let Ok(s) = ret {
            self.size += s as u64
        }
        ret
    }

    pub async fn write_index(&mut self) -> WriteTsmResult<usize> {
        if self.finished {
            return Err(WriteTsmError::Finished {
                path: self.final_path.clone(),
            });
        }

        self.index_buf.set_index_offset(self.writer.pos());
        let len1 = self.index_buf.write_to(&mut self.writer).await?;
        let len2 = write_footer_to(
            &mut self.writer,
            &self.index_buf.bloom_filter,
            self.index_buf.index_offset,
        )
        .await?;

        Ok(len1 + len2)
    }

    pub async fn finish(&mut self) -> WriteTsmResult<()> {
        if self.finished {
            return Err(WriteTsmError::Finished {
                path: self.final_path.clone(),
            });
        }
        self.writer.sync_data().await.context(IOSnafu)?;
        std::fs::rename(&self.tmp_path, &self.final_path).context(IOSnafu)?;
        self.finished = true;
        Ok(())
    }
}

pub async fn new_tsm_writer(
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
    let res = TsmWriter::open(tsm_path, tsm_sequence, is_delta, max_size).await;
    res
}

pub async fn write_header_to(writer: &mut FileCursor) -> WriteTsmResult<usize> {
    let size = writer
        .write_vec(
            [
                IoSlice::new(TSM_MAGIC.as_slice()),
                IoSlice::new(VERSION.as_slice()),
            ]
            .as_mut_slice(),
        )
        .await
        .context(IOSnafu)?;

    Ok(size)
}

async fn write_raw_data_to(
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
        .await
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

async fn write_block_to(
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

    // TODO Make encoding result streamable
    let (ts_buf, data_buf) = block
        .encode(0, block.len(), block.encodings())
        .context(EncodeSnafu)?;

    let size = writer
        .write_vec(
            [
                IoSlice::new(crc32fast::hash(&ts_buf).to_be_bytes().as_slice()),
                IoSlice::new(&ts_buf),
                IoSlice::new(crc32fast::hash(&data_buf).to_be_bytes().as_slice()),
                IoSlice::new(&data_buf),
            ]
            .as_mut_slice(),
        )
        .await
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
            val_offset: offset + ts_buf.len() as u64 + 4, // CRC32 is 4 bytes
        },
    );

    Ok(size)
}

async fn write_footer_to(
    writer: &mut FileCursor,
    bloom_filter: &BloomFilter,
    index_offset: u64,
) -> WriteTsmResult<usize> {
    let size = writer
        .write_vec(
            [
                IoSlice::new(bloom_filter.bytes()),
                IoSlice::new(index_offset.to_be_bytes().as_slice()),
            ]
            .as_mut_slice(),
        )
        .await
        .context(IOSnafu)?;
    Ok(size)
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use models::{FieldId, ValueType};

    use crate::file_system::file_manager::{self, get_file_manager, FileManager};
    use crate::file_system::IFile;
    use crate::{
        memcache::FieldVal,
        tsm::{
            codec::DataBlockEncoding, new_tsm_writer, ColumnReader, DataBlock, IndexReader,
            TsmReader, TsmWriter,
        },
    };

    const TEST_PATH: &str = "/tmp/test/tsm_writer";

    async fn write_to_tsm(
        dir: impl AsRef<Path>,
        file_name: &str,
        data: &HashMap<FieldId, Vec<DataBlock>>,
    ) {
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let tsm_path = dir.as_ref().join(file_name);
        let mut writer = TsmWriter::open(tsm_path, 0, false, 0).await.unwrap();
        for (fid, blks) in data.iter() {
            for blk in blks.iter() {
                writer.write_block(*fid, blk).await.unwrap();
            }
        }
        writer.write_index().await.unwrap();
        writer.finish().await.unwrap();
    }

    async fn read_from_tsm(path: impl AsRef<Path>) -> HashMap<FieldId, Vec<DataBlock>> {
        let file = Arc::new(file_manager::open_file(&path).await.unwrap());
        let len = file.len();

        let index = IndexReader::open(file.clone()).await.unwrap();
        let mut data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx_meta in index.iter() {
            let mut cr = ColumnReader::new(file.clone(), idx_meta.block_iterator());
            loop {
                match cr.next().await {
                    None => break,
                    Some(blk_ret) => {
                        let blk = blk_ret.unwrap();
                        data.entry(idx_meta.field_id())
                            .or_insert_with(Vec::new)
                            .push(blk);
                    }
                }
            }
        }
        data
    }

    async fn check_tsm(path: impl AsRef<Path>, data: &HashMap<FieldId, Vec<DataBlock>>) {
        let tsm_data = read_from_tsm(path).await;
        for (k, v) in tsm_data.iter() {
            assert_eq!(v, data.get(k).unwrap());
        }
    }

    #[tokio::test]
    async fn test_tsm_write_fast() {
        let dir = Path::new(TEST_PATH);
        #[rustfmt::skip]
        let data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from([
            (1, vec![DataBlock::U64 { ts: vec![2, 3, 4], val: vec![12, 13, 15], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::U64 { ts: vec![2, 3, 4], val: vec![101, 102, 103], enc: DataBlockEncoding::default() }]),
        ]);

        let file_name = "_000001.tsm";
        write_to_tsm(&dir, file_name, &data).await;
        check_tsm(dir.join(file_name), &data).await;
    }

    #[tokio::test]
    async fn test_tsm_write_1() {
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

        #[rustfmt::skip]
        let data = HashMap::from([
            (1, vec![
                DataBlock::I64 { ts: ts_1, val: val_1, enc: DataBlockEncoding::default() },
                DataBlock::I64 { ts: ts_2, val: val_2, enc: DataBlockEncoding::default() },
            ]),
        ]);

        let dir = Path::new(TEST_PATH).join("1");
        let file_name = "_000001.tsm";
        write_to_tsm(&dir, file_name, &data).await;
        check_tsm(dir.join(file_name), &data).await;
    }
}
