use minivec::MiniVec;
use std::{
    borrow::Borrow,
    path::{Path, PathBuf},
    sync::Arc,
};

use models::{utils as model_utils, FieldId, Timestamp, ValueType};
use parking_lot::RwLock;
use snafu::{ResultExt, Snafu};

use crate::file_system::{file_manager, AsyncFile, IFile};
use crate::{
    byte_utils::{decode_be_i64, decode_be_u16, decode_be_u32, decode_be_u64},
    error::{self, Error, Result},
    file_utils,
    tseries_family::TimeRange,
    tsm::{
        codec::{
            get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec,
            get_ts_codec, get_u64_codec, DataBlockEncoding,
        },
        get_data_block_meta_unchecked, get_index_meta_unchecked,
        tombstone::TsmTombstone,
        BlockMeta, DataBlock, Index, IndexMeta, BLOCK_META_SIZE, FOOTER_SIZE, INDEX_META_SIZE,
        MAX_BLOCK_VALUES,
    },
};

pub type ReadTsmResult<T, E = ReadTsmError> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum ReadTsmError {
    #[snafu(display("IO error: {}", source))]
    IO { source: std::io::Error },

    #[snafu(display("Decode error: {}", source))]
    Decode {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("TSM file is invalid: {}", reason))]
    Invalid { reason: String },
}

impl From<ReadTsmError> for Error {
    fn from(rte: ReadTsmError) -> Self {
        Error::ReadTsm { source: rte }
    }
}

/// Disk-based index reader
pub struct IndexFile {
    reader: Arc<AsyncFile>,
    buf: [u8; 8],

    index_offset: u64,
    pos: u64,
    end_pos: u64,
    index_block_idx: usize,
    index_block_count: usize,
}

impl IndexFile {
    pub async fn open(reader: Arc<AsyncFile>) -> ReadTsmResult<Self> {
        let file_len = reader.len();
        let mut buf = [0_u8; 8];
        reader
            .read_at(file_len - 8, &mut buf)
            .await
            .context(IOSnafu)?;
        let index_offset = u64::from_be_bytes(buf);
        Ok(Self {
            reader,
            buf,
            index_offset,
            pos: index_offset,
            end_pos: file_len - FOOTER_SIZE as u64,
            index_block_idx: 0,
            index_block_count: 0,
        })
    }

    // TODO: not implemented
    pub async fn next_index_entry(&mut self) -> ReadTsmResult<Option<()>> {
        if self.pos >= self.end_pos {
            return Ok(None);
        }
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .await
            .context(IOSnafu)?;
        let field_id = u64::from_be_bytes(self.buf);
        self.pos += 8;
        self.reader
            .read_at(self.pos, &mut self.buf[..3])
            .await
            .context(IOSnafu)?;
        self.pos += 3;
        let field_type = ValueType::from(self.buf[0]);
        let block_count = decode_be_u16(&self.buf[1..3]);
        self.index_block_idx = 0;
        self.index_block_count = block_count as usize;

        Ok(Some(()))
    }

    // TODO: not implemented
    pub async fn next_block_entry(&mut self) -> ReadTsmResult<Option<()>> {
        if self.index_block_idx >= self.index_block_count {
            return Ok(None);
        }
        // read min time on block entry
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .await
            .context(IOSnafu)?;
        self.pos += 8;
        let min_ts = i64::from_be_bytes(self.buf);

        // read max time on block entry
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .await
            .context(IOSnafu)?;
        self.pos += 8;
        let max_ts = i64::from_be_bytes(self.buf);

        // read count on block entry
        self.reader
            .read_at(self.pos, &mut self.buf[..4])
            .await
            .context(IOSnafu)?;
        self.pos += 4;
        let count = decode_be_u32(&self.buf[..4]);

        // read block data offset
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .await
            .context(IOSnafu)?;
        self.pos += 8;
        let offset = u64::from_be_bytes(self.buf);

        // read block size
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .await
            .context(IOSnafu)?;
        self.pos += 8;
        let size = u64::from_be_bytes(self.buf);

        // read value offset
        self.reader
            .read_at(self.pos, &mut self.buf[..])
            .await
            .context(IOSnafu)?;
        self.pos += 8;
        let val_off = u64::from_be_bytes(self.buf);

        Ok(Some(()))
    }
}

pub async fn print_tsm_statistics(path: impl AsRef<Path>, show_tombstone: bool) {
    let reader = TsmReader::open(path).await.unwrap();
    let mut points_cnt = 0_usize;
    println!("============================================================");
    for idx in reader.index_iterator() {
        let tr = idx.time_range();
        let mut buffer = String::with_capacity(1024);
        let mut idx_points_cnt = 0_usize;
        for blk in idx.block_iterator() {
            buffer.push_str(
                format!(
                    "\tBlock | FieldId: {}, MinTime: {}, MaxTime: {}, Count: {}, Offset: {}, Size: {}, ValOffset: {}\n",
                    blk.field_id(), blk.min_ts(), blk.max_ts(), blk.count(), blk.offset(), blk.size(), blk.val_off()
                ).as_str()
            );
            points_cnt += blk.count() as usize;
            idx_points_cnt += blk.count() as usize;
        }
        if !buffer.is_empty() {
            buffer.truncate(buffer.len() - 1);
        }
        println!("============================================================");
        println!("Field | FieldId: {}, FieldType: {:?}, BlockCount: {}, MinTime: {}, MaxTime: {}, PointsCount: {}",
                 idx.field_id(),
                 idx.field_type(),
                 idx.block_count(),
                 tr.0,
                 tr.1,
                 idx_points_cnt);
        println!("------------------------------------------------------------");
        println!("{}", buffer);
        if show_tombstone {
            println!("------------------------------------------------------------");
            print!("Tombstone | ");
            if let Some(time_ranges) = reader.get_cloned_tombstone_time_ranges(idx.field_id()) {
                if time_ranges.is_empty() {
                    println!("None");
                } else {
                    for (i, tr) in time_ranges.iter().enumerate() {
                        if i == time_ranges.len() - 1 {
                            println!("({}, {})", tr.min_ts, tr.max_ts);
                        } else {
                            print!("({}, {}), ", tr.min_ts, tr.max_ts);
                        }
                    }
                }
            } else {
                println!("None");
            }
        }
    }
    println!("============================================================");
    println!("============================================================");
    println!("PointsCount: {}", points_cnt);
}

pub async fn load_index(reader: Arc<AsyncFile>) -> ReadTsmResult<Index> {
    let len = reader.len();
    if len < FOOTER_SIZE as u64 {
        return Err(ReadTsmError::Invalid {
            reason: format!("TSM file size less than FOOTER_SIZE({})", FOOTER_SIZE),
        });
    }
    let mut buf = [0u8; 8];

    // Read index data offset
    reader.read_at(len - 8, &mut buf).await.context(IOSnafu)?;
    let offset = u64::from_be_bytes(buf);
    if offset > len - FOOTER_SIZE as u64 {
        return Err(ReadTsmError::Invalid {
            reason: format!("TSM file size less than index offset({})", offset),
        });
    }
    let data_len = (len - offset - FOOTER_SIZE as u64) as usize;
    let mut data = vec![0_u8; data_len];
    // Read index data
    reader.read_at(offset, &mut data).await.context(IOSnafu)?;

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
    pub async fn open(reader: Arc<AsyncFile>) -> Result<Self> {
        let idx = load_index(reader).await.context(error::ReadTsmSnafu)?;

        Ok(Self {
            index_ref: Arc::new(idx),
        })
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
    /// Array index in `Index::offsets`.
    index_idx: usize,
    block_count: usize,
    block_type: ValueType,

    /// The max number of iterations
    index_meta_limit: usize,
    /// The current iteration number.
    index_meta_idx: usize,
}

impl IndexIterator {
    pub fn new(index: Arc<Index>, total: usize, from_idx: usize) -> Self {
        Self {
            index_ref: index,
            index_idx: from_idx,
            block_count: 0,
            block_type: ValueType::Unknown,
            index_meta_limit: total,
            index_meta_idx: 0,
        }
    }
}

impl Iterator for IndexIterator {
    type Item = IndexMeta;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index_meta_idx >= self.index_meta_limit {
            return None;
        }
        self.index_meta_idx += 1;

        let ret = Some(get_index_meta_unchecked(
            self.index_ref.clone(),
            self.index_idx,
        ));
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
    /// Number of `BlockMeta` in current `IndexMeta`
    block_count: u16,

    /// The current iteration number.
    block_meta_idx: usize,
    /// The max number of iterations
    block_meta_idx_end: usize,
}

impl BlockMetaIterator {
    pub fn new(
        index: Arc<Index>,
        index_offset: usize,
        field_id: FieldId,
        field_type: ValueType,
        block_count: u16,
    ) -> Self {
        Self {
            index_ref: index,
            index_offset,
            field_id,
            field_type,
            block_offset: index_offset + INDEX_META_SIZE,
            block_count,
            block_meta_idx_end: block_count as usize - 1,
            block_meta_idx: 0,
        }
    }

    /// Set iterator start & end position by time range
    pub(crate) fn filter_time_range(&mut self, time_range: &TimeRange) {
        let TimeRange { min_ts, max_ts } = *time_range;
        let base = self.index_offset + INDEX_META_SIZE;
        let sli = &self.index_ref.data()[base..base + self.block_count as usize * BLOCK_META_SIZE];
        let mut pos = 0_usize;
        let mut idx = 0_usize;
        while pos < sli.len() {
            if min_ts > decode_be_i64(&sli[pos + 8..pos + 16]) {
                pos += BLOCK_META_SIZE;
                idx += 1;
            } else {
                // First data block in time range
                self.block_offset = pos;
                break;
            }
        }
        if max_ts == min_ts {
            self.block_meta_idx_end = 1;
            return;
        }
        self.block_meta_idx = idx;
        self.block_meta_idx_end = idx;
        pos += BLOCK_META_SIZE;
        while pos < sli.len() {
            if max_ts < decode_be_i64(&sli[pos..pos + 8]) {
                return;
            } else if max_ts < decode_be_i64(&sli[pos + 8..pos + 16]) {
                self.block_meta_idx_end += 1;
                return;
            } else {
                self.block_meta_idx_end += 1;
                pos += BLOCK_META_SIZE;
            }
        }
    }
}

impl Iterator for BlockMetaIterator {
    type Item = BlockMeta;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_meta_idx > self.block_meta_idx_end as usize {
            return None;
        }
        let ret = Some(get_data_block_meta_unchecked(
            self.index_ref.clone(),
            self.index_offset,
            self.block_meta_idx,
            self.field_id,
            self.field_type,
        ));
        self.block_meta_idx += 1;
        self.block_offset += BLOCK_META_SIZE;
        ret
    }
}

#[derive(Clone)]
pub struct TsmReader {
    reader: Arc<AsyncFile>,
    index_reader: Arc<IndexReader>,
    tombstone: Arc<RwLock<TsmTombstone>>,
}

impl TsmReader {
    pub async fn open(tsm_path: impl AsRef<Path>) -> Result<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let tsm_id = file_utils::get_tsm_file_id_by_path(&path)?;
        let tsm = Arc::new(file_manager::open_file(tsm_path).await?);
        let tsm_idx = IndexReader::open(tsm.clone()).await?;
        let tombstone_path = path.parent().unwrap_or_else(|| Path::new("/"));
        let tombstone = TsmTombstone::open(tombstone_path, tsm_id).await?;
        Ok(Self {
            reader: tsm,
            index_reader: Arc::new(tsm_idx),
            tombstone: Arc::new(RwLock::new(tombstone)),
        })
    }

    pub fn index_iterator(&self) -> IndexIterator {
        self.index_reader.iter()
    }

    pub fn index_iterator_opt(&self, field_id: FieldId) -> IndexIterator {
        self.index_reader.iter_opt(field_id)
    }

    /// Returns a DataBlock without tombstone
    pub async fn get_data_block(&self, block_meta: &BlockMeta) -> ReadTsmResult<DataBlock> {
        let blk_range = (block_meta.min_ts(), block_meta.max_ts());
        let mut buf = vec![0_u8; block_meta.size() as usize];
        let mut blk = read_data_block(
            self.reader.clone(),
            &mut buf,
            block_meta.field_type(),
            block_meta.offset(),
            block_meta.val_off(),
        )
        .await?;
        self.tombstone
            .read()
            .data_block_exclude_tombstones(block_meta.field_id(), &mut blk);
        Ok(blk)
    }

    // Reads raw data from file and returns the read data size.
    pub async fn get_raw_data(
        &self,
        block_meta: &BlockMeta,
        dst: &mut Vec<u8>,
    ) -> ReadTsmResult<usize> {
        let data_len = block_meta.size() as usize;
        if dst.len() < data_len {
            dst.resize(data_len, 0);
        }
        self.reader
            .read_at(block_meta.offset(), &mut dst[..data_len])
            .await
            .context(IOSnafu)?;
        Ok(data_len)
    }

    pub fn has_tombstone(&self) -> bool {
        !self.tombstone.read().is_empty()
    }

    /// Returns all tombstone `TimeRange`s for a `BlockMeta`.
    /// Returns None if there is nothing to return, or `TimeRange`s is empty.
    pub fn get_block_tombstone_time_ranges(
        &self,
        block_meta: &BlockMeta,
    ) -> Option<Vec<TimeRange>> {
        return self.tombstone.read().get_overlapped_time_ranges(
            block_meta.field_id(),
            &TimeRange::from((block_meta.min_ts(), block_meta.max_ts())),
        );
    }

    /// Returns all TimeRanges for a FieldId cloned from TsmTombstone.
    pub(crate) fn get_cloned_tombstone_time_ranges(
        &self,
        field_id: FieldId,
    ) -> Option<Vec<TimeRange>> {
        self.tombstone.read().get_cloned_time_ranges(field_id)
    }
}

pub struct ColumnReader {
    reader: Arc<AsyncFile>,
    inner: BlockMetaIterator,
    buf: Vec<u8>,
}

impl ColumnReader {
    pub fn new(reader: Arc<AsyncFile>, inner: BlockMetaIterator) -> Self {
        Self {
            reader,
            inner,
            buf: vec![],
        }
    }

    async fn decode(&mut self, block_meta: &BlockMeta) -> ReadTsmResult<DataBlock> {
        let (offset, size) = (block_meta.offset(), block_meta.size());
        self.buf.resize(size as usize, 0);
        read_data_block(
            self.reader.clone(),
            &mut self.buf,
            block_meta.field_type(),
            block_meta.offset(),
            block_meta.val_off(),
        )
        .await
    }
}

impl ColumnReader {
    pub async fn next(&mut self) -> Option<Result<DataBlock>> {
        if let Some(dbm) = self.inner.next() {
            let res = self.decode(&dbm).await.context(error::ReadTsmSnafu);
            return Some(res);
        }
        None
    }
}

async fn read_data_block(
    reader: Arc<AsyncFile>,
    buf: &mut [u8],
    field_type: ValueType,
    offset: u64,
    val_off: u64,
) -> ReadTsmResult<DataBlock> {
    reader.read_at(offset, buf).await.context(IOSnafu)?;
    decode_data_block(buf, field_type, val_off - offset)
}

pub fn decode_data_block(
    buf: &[u8],
    field_type: ValueType,
    val_off: u64,
) -> ReadTsmResult<DataBlock> {
    debug_assert!(buf.len() >= 8);
    if buf.len() < 8 {
        return Err(ReadTsmError::Decode {
            source: "buffer too short".into(),
        });
    }

    // let crc_ts = &self.buf[..4];
    let mut ts = Vec::with_capacity(MAX_BLOCK_VALUES as usize);
    let ts_encoding = get_encoding(&buf[4..val_off as usize]);
    let ts_codec = get_ts_codec(ts_encoding);
    ts_codec
        .decode(&buf[4..val_off as usize], &mut ts)
        .context(DecodeSnafu)?;

    // let crc_data = &self.buf[(val_offset - offset) as usize..4];
    let data = &buf[(val_off + 4) as usize..];
    match field_type {
        ValueType::Float => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            let val_encoding = get_encoding(data);
            let val_codec = get_f64_codec(val_encoding);
            val_codec.decode(data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::F64 {
                ts,
                val,
                enc: DataBlockEncoding::combine(ts_encoding, val_encoding),
            })
        }
        ValueType::Integer => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            let val_encoding = get_encoding(data);
            let val_codec = get_i64_codec(val_encoding);
            val_codec.decode(data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::I64 {
                ts,
                val,
                enc: DataBlockEncoding::combine(ts_encoding, val_encoding),
            })
        }
        ValueType::Boolean => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            let val_encoding = get_encoding(data);
            let val_codec = get_bool_codec(val_encoding);
            val_codec.decode(data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::Bool {
                ts,
                val,
                enc: DataBlockEncoding::combine(ts_encoding, val_encoding),
            })
        }
        ValueType::String => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            let val_encoding = get_encoding(data);
            let val_codec = get_str_codec(val_encoding);
            val_codec.decode(data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::Str {
                ts,
                val,
                enc: DataBlockEncoding::combine(ts_encoding, val_encoding),
            })
        }
        ValueType::Unsigned => {
            // values will be same length as time-stamps.
            let mut val = Vec::with_capacity(ts.len());
            let val_encoding = get_encoding(data);
            let val_codec = get_u64_codec(val_encoding);
            val_codec.decode(data, &mut val).context(DecodeSnafu)?;
            Ok(DataBlock::U64 {
                ts,
                val,
                enc: DataBlockEncoding::combine(ts_encoding, val_encoding),
            })
        }
        _ => Err(ReadTsmError::Decode {
            source: From::from(format!(
                "cannot decode block {:?} with no unknown value type",
                field_type
            )),
        }),
    }
}

#[cfg(test)]
pub mod tsm_reader_tests {
    use core::panic;
    use std::{
        collections::HashMap,
        path::{Path, PathBuf},
        sync::Arc,
    };

    use models::{FieldId, Timestamp};
    use parking_lot::Mutex;

    use super::print_tsm_statistics;
    use crate::file_system::file_manager::{self, get_file_manager};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::{
        file_utils,
        tseries_family::TimeRange,
        tsm::{DataBlock, TsmReader, TsmTombstone, TsmWriter},
    };

    async fn prepare(path: impl AsRef<Path>) -> (PathBuf, PathBuf) {
        if !file_manager::try_exists(&path) {
            std::fs::create_dir_all(&path).unwrap();
        }
        let tsm_file = file_utils::make_tsm_file_name(&path, 1);
        let tombstone_file = file_utils::make_tsm_tombstone_file_name(&path, 1);
        println!(
            "Writing file: {}, {}",
            tsm_file.to_str().unwrap(),
            tombstone_file.to_str().unwrap()
        );

        #[rustfmt::skip]
        let ori_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from([
            (1, vec![DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![11, 12, 13, 15], enc: DataBlockEncoding::default() }]
            ),
            (2, vec![
                DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![101, 102, 103, 104], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![9, 10, 11, 12], val: vec![109, 110, 111, 112], enc: DataBlockEncoding::default() },
            ]),
        ]);
        let mut writer = TsmWriter::open(&tsm_file, 1, false, 0).await.unwrap();
        for (fid, blks) in ori_data.iter() {
            for blk in blks.iter() {
                writer.write_block(*fid, blk).await.unwrap();
            }
        }
        writer.write_index().await.unwrap();
        writer.finish().await.unwrap();

        let mut tombstone = TsmTombstone::with_path(&tombstone_file).await.unwrap();
        tombstone
            .add_range(&[1], &TimeRange::new(2, 4))
            .await
            .unwrap();
        tombstone.flush().await.unwrap();

        (tsm_file, tombstone_file)
    }

    pub(crate) async fn read_and_check(
        reader: &TsmReader,
        expected_data: HashMap<FieldId, Vec<DataBlock>>,
    ) {
        let mut read_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx in reader.index_iterator() {
            for blk in idx.block_iterator() {
                let data_blk = reader.get_data_block(&blk).await.unwrap();
                read_data
                    .entry(idx.field_id())
                    .or_insert(Vec::new())
                    .push(data_blk);
            }
        }
        assert_eq!(expected_data.len(), read_data.len());
        for (field_id, data_blks) in read_data.iter() {
            let expected_data_blks = expected_data.get(field_id);
            if expected_data_blks.is_none() {
                panic!("Expected DataBlocks for field_id: '{}' is None", field_id);
            }
            assert_eq!(data_blks, expected_data_blks.unwrap());
        }
    }

    #[tokio::test]
    async fn test_tsm_reader_1() {
        let (tsm_file, tombstone_file) = prepare("/tmp/test/tsm_reader/1").await;
        println!(
            "Reading file: {}, {}",
            tsm_file.to_str().unwrap(),
            tombstone_file.to_str().unwrap()
        );
        print_tsm_statistics(&tsm_file, true).await;

        let reader = TsmReader::open(&tsm_file).await.unwrap();

        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from([
            (1, vec![DataBlock::U64 { ts: vec![1], val: vec![11], enc: DataBlockEncoding::default() }]
            ),
            (2, vec![
                DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![101, 102, 103, 104], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![9, 10, 11, 12], val: vec![109, 110, 111, 112], enc: DataBlockEncoding::default() },
            ]),
        ]);
        read_and_check(&reader, expected_data).await;
    }

    pub(crate) async fn read_opt_and_check(
        reader: &TsmReader,
        field_id: FieldId,
        time_range: (Timestamp, Timestamp),
        expected_data: HashMap<FieldId, Vec<DataBlock>>,
    ) {
        let mut read_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx in reader.index_iterator_opt(2) {
            for blk in idx.block_iterator_opt(&TimeRange::from(time_range)) {
                let data_blk = reader.get_data_block(&blk).await.unwrap();
                read_data
                    .entry(idx.field_id())
                    .or_insert(Vec::new())
                    .push(data_blk);
            }
        }
        assert_eq!(expected_data.len(), read_data.len());
        for (field_id, data_blks) in read_data.iter() {
            let expected_data_blks = expected_data.get(field_id).unwrap();
            assert_eq!(data_blks, expected_data_blks);
        }
    }

    #[tokio::test]
    async fn test_tsm_reader_2() {
        let (tsm_file, tombstone_file) = prepare("/tmp/test/tsm_reader/2").await;
        println!(
            "Reading file: {}, {}",
            tsm_file.to_str().unwrap(),
            tombstone_file.to_str().unwrap()
        );
        print_tsm_statistics(&tsm_file, true).await;

        let reader = TsmReader::open(&tsm_file).await.unwrap();

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (2, vec![
                    DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![101, 102, 103, 104], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 2, (2, 3), expected_data).await;
        }

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (2, vec![
                    DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 2, (5, 8), expected_data).await;
        }

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (2, vec![
                    DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                    DataBlock::U64 { ts: vec![9, 10, 11, 12], val: vec![109, 110, 111, 112], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 2, (6, 10), expected_data).await;
        }

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (2, vec![
                    DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                    DataBlock::U64 { ts: vec![9, 10, 11, 12], val: vec![109, 110, 111, 112], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 2, (8, 9), expected_data).await;
        }

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (2, vec![
                    DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                    DataBlock::U64 { ts: vec![9, 10, 11, 12], val: vec![109, 110, 111, 112], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 2, (5, 12), expected_data).await;
        }
    }
}
