use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::{FieldId, ValueType};
use snafu::{ResultExt, Snafu};
use utils::BloomFilter;

use crate::byte_utils::{self, decode_be_i64, decode_be_u16, decode_be_u64};
use crate::error::{self, Error, Result};
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::file_utils;
use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_ts_codec,
    get_u64_codec, DataBlockEncoding,
};
use crate::tsm::tombstone::TsmTombstone;
use crate::tsm::{
    get_data_block_meta_unchecked, get_index_meta_unchecked, BlockEntry, BlockMeta, DataBlock,
    Index, IndexEntry, IndexMeta, TsmVersion, BLOCK_META_SIZE, BLOOM_FILTER_BITS,
    BLOOM_FILTER_SIZE, FOOTER_MAGIC_LEN, FOOTER_MAGIC_V1, FOOTER_SIZE, INDEX_META_SIZE,
    MAX_BLOCK_VALUES, OLD_BLOOM_FILTER_SIZE, OLD_FOOTER_SIZE,
};

pub type ReadTsmResult<T, E = ReadTsmError> = std::result::Result<T, E>;

#[derive(Snafu, Debug)]
#[snafu(visibility(pub))]
pub enum ReadTsmError {
    #[snafu(display("IO error: {}", source))]
    ReadIO { source: std::io::Error },

    #[snafu(display("Decode error: {}", source))]
    Decode {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "Datablock crc32 check failed, expected: {crc}, calculated: {crc_calculated}"
    ))]
    CrcCheck { crc: u32, crc_calculated: u32 },

    #[snafu(display("TSM file is lost: {}", reason))]
    FileNotFound { reason: String },

    #[snafu(display("TSM file is invalid: {}", reason))]
    Invalid { reason: String },
}

impl From<ReadTsmError> for Error {
    fn from(rte: ReadTsmError) -> Self {
        match rte {
            ReadTsmError::CrcCheck { .. }
            | ReadTsmError::FileNotFound { reason: _ }
            | ReadTsmError::Invalid { reason: _ } => Error::TsmFileBroken { source: rte },

            _ => Error::ReadTsm { source: rte },
        }
    }
}

/// Disk-based index reader
pub struct IndexFile {
    reader: Arc<AsyncFile>,
    bloom_filter: BloomFilter,
    idx_meta_buf: Vec<u8>, // INDEX_META_SIZE
    blk_meta_buf: Vec<u8>, // BLOCK_META_SIZE
    index_offset: u64,
    pos: u64,
    end_pos: u64,
    index_block_idx: usize,
    index_block_count: usize,
}

impl IndexFile {
    pub(crate) async fn open(reader: Arc<AsyncFile>) -> ReadTsmResult<Self> {
        let file_len = reader.len();
        let footer = reader
            .read_at(file_len - FOOTER_SIZE as u64, FOOTER_SIZE)
            .await
            .context(ReadIOSnafu)?;
        let bloom_filter = BloomFilter::with_data(&footer[..BLOOM_FILTER_SIZE]);
        let index_offset = decode_be_u64(&footer[BLOOM_FILTER_SIZE..]);
        Ok(Self {
            reader,
            bloom_filter,
            idx_meta_buf: Vec::new(),
            blk_meta_buf: Vec::new(),
            index_offset,
            pos: index_offset,
            end_pos: file_len - FOOTER_SIZE as u64,
            index_block_idx: 0,
            index_block_count: 0,
        })
    }

    // TODO: not implemented
    pub(crate) async fn next_index_entry(&mut self) -> ReadTsmResult<Option<IndexEntry>> {
        if self.pos >= self.end_pos {
            return Ok(None);
        }
        self.idx_meta_buf = self
            .reader
            .read_at(self.pos, INDEX_META_SIZE)
            .await
            .context(ReadIOSnafu)?;
        self.pos += INDEX_META_SIZE as u64;
        let (entry, blk_count) = IndexEntry::decode(&self.idx_meta_buf);
        self.index_block_idx = 0;
        self.index_block_count = blk_count as usize;

        Ok(Some(entry))
    }

    // TODO: not implemented
    pub(crate) async fn next_block_entry(&mut self) -> ReadTsmResult<Option<BlockEntry>> {
        if self.index_block_idx >= self.index_block_count {
            return Ok(None);
        }
        self.blk_meta_buf = self
            .reader
            .read_at(self.pos, BLOCK_META_SIZE)
            .await
            .context(ReadIOSnafu)?;
        self.pos += BLOCK_META_SIZE as u64;
        let entry = BlockEntry::decode(&self.blk_meta_buf);
        self.index_block_idx += 1;

        Ok(Some(entry))
    }
}

pub async fn print_tsm_statistics(path: impl AsRef<Path>, show_tombstone: bool) {
    let reader = TsmReader::open(path).await.unwrap();
    let tomb_fields_excluded = reader.tombstone.fields_excluded_cloned();
    let tomb_all_excluded = reader.tombstone.all_excluded_cloned();
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
        println!("Offset: {} | Field | FieldId: {}, FieldType: {:?}, BlockCount: {}, MinTime: {}, MaxTime: {}, PointsCount: {}",
                 idx.offset(),
                 idx.field_id(),
                 idx.field_type(),
                 idx.block_count(),
                 tr.min_ts,
                 tr.max_ts,
                 idx_points_cnt);
        println!("------------------------------------------------------------");
        println!("{}", buffer);
        if show_tombstone {
            println!("------------------------------------------------------------");
            print!("Tombstone | all_excluded: {tomb_all_excluded} | fields_excluded: ");
            if let Some(time_ranges) = tomb_fields_excluded.get(&idx.field_id()) {
                if time_ranges.is_empty() {
                    println!("None");
                } else {
                    for (i, tr) in time_ranges.time_ranges().enumerate() {
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

pub async fn load_index(tsm_id: u64, reader: Arc<AsyncFile>) -> ReadTsmResult<Index> {
    let len = reader.len();
    if len < FOOTER_MAGIC_LEN {
        return Err(ReadTsmError::Invalid {
            reason: format!(
                "TSM file ({}) size less than FOOTER_MAGIC_LEN({})",
                tsm_id, FOOTER_SIZE
            ),
        });
    }

    let buf = reader
        .read_at(len - FOOTER_MAGIC_LEN, FOOTER_MAGIC_LEN as usize)
        .await
        .context(ReadIOSnafu)?;
    if buf == FOOTER_MAGIC_V1.as_bytes() {
        load_index_v2(tsm_id, reader).await
    } else {
        load_index_v1(tsm_id, reader).await
    }
}

pub async fn load_index_v1(tsm_id: u64, reader: Arc<AsyncFile>) -> ReadTsmResult<Index> {
    let len = reader.len();
    if len < OLD_FOOTER_SIZE as u64 {
        return Err(ReadTsmError::Invalid {
            reason: format!(
                "TSM file ({}) size less than OLD_FOOTER_SIZE({})",
                tsm_id, OLD_FOOTER_SIZE
            ),
        });
    }

    // Read index data offset
    let buf = reader
        .read_at(len - OLD_FOOTER_SIZE as u64, OLD_FOOTER_SIZE)
        .await
        .context(ReadIOSnafu)?;

    let offset = decode_be_u64(&buf[OLD_BLOOM_FILTER_SIZE..]);
    if offset > len - OLD_FOOTER_SIZE as u64 {
        return Err(ReadTsmError::Invalid {
            reason: format!(
                "TSM file ({}) size less than index offset({})",
                tsm_id, offset
            ),
        });
    }
    let data_len = (len - offset - OLD_FOOTER_SIZE as u64) as usize;
    // TODO if data_len is too big, read data part in parts and do not store it.
    // Read index data
    let data = reader
        .read_at(offset, data_len)
        .await
        .context(ReadIOSnafu)?;

    // Decode index data
    let assumed_field_count = (data_len / (INDEX_META_SIZE + BLOCK_META_SIZE)) + 1;
    let mut field_id_idx_offs: Vec<(FieldId, usize)> = Vec::with_capacity(assumed_field_count);
    let mut pos = 0_usize;
    while pos < data_len {
        // Push (FieldId, offset_of_index_block)
        field_id_idx_offs.push((decode_be_u64(&data[pos..pos + 8]), pos));
        pos += INDEX_META_SIZE + BLOCK_META_SIZE * decode_be_u16(&data[pos + 9..pos + 11]) as usize;
    }
    // Sort by field id
    // NOTICE that there must be no two equal field_ids.
    field_id_idx_offs.sort_unstable_by_key(|e| e.0);
    let mut bloom_filter = BloomFilter::new(BLOOM_FILTER_BITS);
    for (field_id, _) in &field_id_idx_offs {
        bloom_filter.insert(&field_id.to_be_bytes());
    }
    Ok(Index::new(
        tsm_id,
        Arc::new(bloom_filter),
        data,
        field_id_idx_offs,
    ))
}

pub async fn load_index_v2(tsm_id: u64, reader: Arc<AsyncFile>) -> ReadTsmResult<Index> {
    let len = reader.len();
    if len < FOOTER_SIZE as u64 {
        return Err(ReadTsmError::Invalid {
            reason: format!(
                "TSM file ({}) size less than FOOTER_SIZE({})",
                tsm_id, FOOTER_SIZE
            ),
        });
    }

    // Read index data offset
    let buf = reader
        .read_at(len - FOOTER_SIZE as u64, FOOTER_SIZE)
        .await
        .context(ReadIOSnafu)?;
    let bloom_filter = BloomFilter::with_data(&buf[..BLOOM_FILTER_SIZE]);
    let offset = decode_be_u64(&buf[BLOOM_FILTER_SIZE..BLOOM_FILTER_SIZE + 8]);
    if offset > len - FOOTER_SIZE as u64 {
        return Err(ReadTsmError::Invalid {
            reason: format!(
                "TSM file ({}) size less than index offset({})",
                tsm_id, offset
            ),
        });
    }
    let data_len = (len - offset - FOOTER_SIZE as u64) as usize;
    // TODO if data_len is too big, read data part in parts and do not store it.
    // Read index data
    let data = reader
        .read_at(offset, data_len)
        .await
        .context(ReadIOSnafu)?;

    // Decode index data
    let assumed_field_count = (data_len / (INDEX_META_SIZE + BLOCK_META_SIZE)) + 1;
    let mut field_id_idx_offs: Vec<(FieldId, usize)> = Vec::with_capacity(assumed_field_count);
    let mut pos = 0_usize;
    while pos < data_len {
        // Push (FieldId, offset_of_index_block)
        field_id_idx_offs.push((decode_be_u64(&data[pos..pos + 8]), pos));
        pos += INDEX_META_SIZE + BLOCK_META_SIZE * decode_be_u16(&data[pos + 9..pos + 11]) as usize;
    }
    // Sort by field id
    // NOTICE that there must be no two equal field_ids.
    field_id_idx_offs.sort_unstable_by_key(|e| e.0);

    Ok(Index::new(
        tsm_id,
        Arc::new(bloom_filter),
        data,
        field_id_idx_offs,
    ))
}

/// Memory-based index reader
pub struct IndexReader {
    index_ref: Arc<Index>,
}

impl IndexReader {
    pub async fn open(tsm_id: u64, reader: Arc<AsyncFile>) -> Result<Self> {
        let idx = load_index(tsm_id, reader)
            .await
            .context(error::ReadTsmSnafu)?;

        Ok(Self {
            index_ref: Arc::new(idx),
        })
    }

    pub fn iter(&self) -> IndexIterator {
        IndexIterator::new(
            self.index_ref.clone(),
            self.index_ref.field_id_offs().len(),
            0,
        )
    }

    /// Create `IndexIterator` by filter options
    pub fn iter_opt(&self, field_id: FieldId) -> IndexIterator {
        if let Ok(idx) = self
            .index_ref
            .field_id_offs()
            .binary_search_by_key(&field_id, |f| f.0)
        {
            IndexIterator::new(self.index_ref.clone(), 1, idx)
        } else {
            IndexIterator::new(self.index_ref.clone(), 0, 0)
        }
    }
}

#[derive(Clone)]
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

/// Iterates `BlockMeta`s in Index of a file.
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
    time_ranges: Option<Arc<TimeRanges>>,

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
            time_ranges: None,
            block_meta_idx_end: block_count as usize,
            block_meta_idx: 0,
        }
    }

    /// Set iterator start & end position by time range
    pub(crate) fn filter_time_range(&mut self, time_ranges: Arc<TimeRanges>) {
        if time_ranges.is_boundless() {
            self.time_ranges = Some(time_ranges);
            return;
        }
        let min_ts = time_ranges.min_ts();
        let max_ts = time_ranges.max_ts();
        debug_assert!(min_ts <= max_ts, "time_ranges invalid: {:#?}", time_ranges);

        self.time_ranges = Some(time_ranges);
        let base = self.index_offset + INDEX_META_SIZE;
        let sli = &self.index_ref.data()[base..base + self.block_count as usize * BLOCK_META_SIZE];
        let mut pos = 0_usize;
        let mut idx = 0_usize;
        // Find `idx` of index blocks that time_range.min_ts <= block.max_ts .
        while pos < sli.len() {
            if min_ts > decode_be_i64(&sli[pos + 8..pos + 16]) {
                // If time_range.min_ts > block.max_ts, go on to check next block.
                pos += BLOCK_META_SIZE;
                idx += 1;
            } else {
                // If time_range.min_ts <= block.max_ts, This block may be the start block.
                self.block_offset = pos;
                break;
            }
        }
        self.block_meta_idx = idx;
        self.block_meta_idx_end = idx;
        if idx >= self.block_count as usize {
            // If there are no blocks that time_range.min_ts <= block.max_ts .
            return;
        }

        // Find idx of index blocks that block.min_ts <= time_range.max_ts >= block.max_ts .
        while pos < sli.len() {
            if max_ts < decode_be_i64(&sli[pos..pos + 8]) {
                // If time_range.max_ts < block.min_ts, previous block is the end block.
                return;
            } else if max_ts < decode_be_i64(&sli[pos + 8..pos + 16]) {
                // If time_range.max_ts < block.max_ts, this block is the end block.
                self.block_meta_idx_end += 1;
                return;
            } else {
                // If time_range.max_ts >= block.max_ts, go on to check next block.
                self.block_meta_idx_end += 1;
                pos += BLOCK_META_SIZE;
            }
        }
    }
}

impl Iterator for BlockMetaIterator {
    type Item = BlockMeta;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_meta_idx >= self.block_meta_idx_end {
            return None;
        }
        let mut ret = None;
        if let Some(time_ranges) = self.time_ranges.as_ref() {
            for _ in self.block_meta_idx..self.block_meta_idx_end {
                let block_meta = get_data_block_meta_unchecked(
                    self.index_ref.clone(),
                    self.index_offset,
                    self.block_meta_idx,
                    self.field_id,
                    self.field_type,
                );
                self.block_meta_idx += 1;
                self.block_offset += BLOCK_META_SIZE;
                if time_ranges.overlaps(&(block_meta.min_ts(), block_meta.max_ts()).into()) {
                    ret = Some(block_meta);
                    break;
                }
            }
        } else {
            let block_meta = get_data_block_meta_unchecked(
                self.index_ref.clone(),
                self.index_offset,
                self.block_meta_idx,
                self.field_id,
                self.field_type,
            );
            self.block_meta_idx += 1;
            self.block_offset += BLOCK_META_SIZE;
            ret = Some(block_meta);
        }

        ret
    }
}

#[derive(Clone)]
pub struct TsmReader {
    file_id: u64,
    reader: Arc<AsyncFile>,
    version: TsmVersion,
    index_reader: Arc<IndexReader>,
    tombstone: Arc<TsmTombstone>,
}

impl TsmReader {
    pub async fn open(tsm_path: impl AsRef<Path>) -> Result<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let file_id = file_utils::get_tsm_file_id_by_path(&path)?;
        let tsm = Arc::new(file_manager::open_file(tsm_path).await?);
        let version = tsm.read_at(4, 1).await?;
        let tsm_idx = IndexReader::open(file_id, tsm.clone()).await?;
        let tombstone_path = path.parent().unwrap_or_else(|| Path::new("/"));
        let tombstone = TsmTombstone::open(tombstone_path, file_id).await?;
        Ok(Self {
            file_id,
            reader: tsm,
            version: TsmVersion::from(version[0]),
            index_reader: Arc::new(tsm_idx),
            tombstone: Arc::new(tombstone),
        })
    }

    pub fn version(&self) -> TsmVersion {
        self.version
    }

    pub fn index_iterator(&self) -> IndexIterator {
        self.index_reader.iter()
    }

    pub fn index_iterator_opt(&self, field_id: FieldId) -> IndexIterator {
        self.index_reader.iter_opt(field_id)
    }

    /// Returns a DataBlock without tombstone
    pub async fn get_data_block(&self, block_meta: &BlockMeta) -> ReadTsmResult<Option<DataBlock>> {
        let _blk_range = (block_meta.min_ts(), block_meta.max_ts());
        let mut blk = read_data_block(
            self.reader.clone(),
            block_meta.size() as usize,
            block_meta.field_type(),
            block_meta.offset(),
            block_meta.val_off(),
        )
        .await?;
        if self
            .tombstone
            .is_data_block_all_excluded_by_tombstones(block_meta.field_id(), &blk)
        {
            return Ok(None);
        }
        self.tombstone
            .data_block_exclude_tombstones(block_meta.field_id(), &mut blk);
        Ok(Some(blk))
    }

    /// Reads raw data from file.
    pub async fn get_raw_data(&self, pos: u64, len: usize) -> ReadTsmResult<Vec<u8>> {
        let buf = self.reader.read_at(pos, len).await.context(ReadIOSnafu)?;
        Ok(buf)
    }

    pub fn has_tombstone(&self) -> bool {
        !self.tombstone.is_empty()
    }

    pub(crate) fn tombstone(&self) -> &TsmTombstone {
        self.tombstone.as_ref()
    }

    pub async fn add_tombstone(&self, field_ids: &[FieldId], time_range: TimeRange) -> Result<()> {
        self.tombstone
            .add_range(field_ids, time_range, Some(self.bloom_filter()))
            .await?;
        self.tombstone.flush().await
    }

    /// Add a time range to `all_excluded` and then compact tombstones,
    /// return compacted `all_excluded`.
    pub async fn add_tombstone_and_compact_to_tmp(
        &self,
        time_range: TimeRange,
    ) -> Result<TimeRanges> {
        self.tombstone
            .add_range_and_compact_to_tmp(time_range)
            .await
    }

    /// Replace current tombstone file with compact_tmp tombstone file.
    pub async fn replace_tombstone_with_compact_tmp(&self) -> Result<()> {
        self.tombstone.replace_with_compact_tmp().await
    }

    pub(crate) fn file_id(&self) -> u64 {
        self.file_id
    }

    pub(crate) fn bloom_filter(&self) -> Arc<BloomFilter> {
        self.index_reader.index_ref.bloom_filter()
    }
}

impl Debug for TsmReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TsmReader")
            .field("id", &self.file_id)
            .field("fd", &self.reader.fd())
            .finish()
    }
}

async fn read_data_block(
    reader: Arc<AsyncFile>,
    val_len: usize,
    field_type: ValueType,
    offset: u64,
    val_off: u64,
) -> ReadTsmResult<DataBlock> {
    let buf = reader.read_at(offset, val_len).await.context(ReadIOSnafu)?;
    decode_data_block(&buf, field_type, val_off - offset)
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

    let crc = byte_utils::decode_be_u32(&buf[..4]);
    let crc_calculated = crc32fast::hash(&buf[4..val_off as usize]);
    if crc != crc_calculated {
        return Err(ReadTsmError::CrcCheck {
            crc,
            crc_calculated,
        });
    }
    let mut ts = Vec::with_capacity(MAX_BLOCK_VALUES as usize);
    let ts_encoding = get_encoding(&buf[4..val_off as usize]);
    let ts_codec = get_ts_codec(ts_encoding);
    ts_codec
        .decode(&buf[4..val_off as usize], &mut ts)
        .context(DecodeSnafu)?;

    let crc: u32 = byte_utils::decode_be_u32(&buf[val_off as usize..]);
    let crc_calculated = crc32fast::hash(&buf[(val_off + 4) as usize..]);
    if crc != crc_calculated {
        return Err(ReadTsmError::CrcCheck {
            crc,
            crc_calculated,
        });
    }
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
                enc: DataBlockEncoding::new(ts_encoding, val_encoding),
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
                enc: DataBlockEncoding::new(ts_encoding, val_encoding),
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
                enc: DataBlockEncoding::new(ts_encoding, val_encoding),
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
                enc: DataBlockEncoding::new(ts_encoding, val_encoding),
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
                enc: DataBlockEncoding::new(ts_encoding, val_encoding),
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
pub mod test {
    use core::panic;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use models::predicate::domain::{TimeRange, TimeRanges};
    use models::{FieldId, Timestamp};
    use snafu::ResultExt;

    use crate::error::{self, Result};
    use crate::file_system::file::IFile;
    use crate::file_system::file_manager::{self};
    use crate::file_utils;
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::reader::IndexReader;
    use crate::tsm::test::write_to_tsm;
    use crate::tsm::{BlockEntry, DataBlock, IndexEntry, IndexFile, TsmReader, TsmTombstone};

    /// Write tsm data and tombstone data to dir/_000001.tsm, return expected data(tsm data - tombstoen data).
    async fn prepare(
        dir: impl AsRef<Path>,
        data: Option<Vec<(FieldId, Vec<DataBlock>)>>,
        tombstone: Option<HashMap<FieldId, Vec<TimeRange>>>,
    ) -> Result<(PathBuf, PathBuf, Vec<(FieldId, Vec<DataBlock>)>)> {
        if file_manager::try_exists(&dir) {
            let _ = std::fs::remove_dir_all(&dir);
        }
        std::fs::create_dir_all(&dir).context(error::IOSnafu)?;

        let tsm_file = file_utils::make_tsm_file_name(&dir, 1);
        let tombstone_file = file_utils::make_tsm_tombstone_file_name(&dir, 1);
        println!(
            "Writing file: {}, {}",
            tsm_file.display(),
            tombstone_file.display(),
        );

        #[rustfmt::skip]
        let mut ori_data: Vec<(FieldId, Vec<DataBlock>)> = data.unwrap_or_else(|| vec![
            (1, vec![DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![11, 12, 13, 15], enc: DataBlockEncoding::default() }]),
            (2, vec![
                DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![101, 102, 103, 104], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![9, 10, 11, 12], val: vec![109, 110, 111, 112], enc: DataBlockEncoding::default() },
            ]),
            (3, vec![
                DataBlock::U64 { ts: vec![5], val: vec![105], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![9], val: vec![109], enc: DataBlockEncoding::default() },
            ]),
        ]);
        write_to_tsm(&tsm_file, &ori_data, false).await?;

        ori_data.sort_by_key(|a| a.0);

        let tomb_data =
            tombstone.unwrap_or_else(|| HashMap::from([(1, vec![TimeRange::from((2, 4))])]));
        let tombstone = TsmTombstone::with_path(&tombstone_file).await?;
        for (field_id, tomb_trs) in tomb_data {
            let ori_data_blocks_idx = ori_data.binary_search_by_key(&field_id, |(fid, _)| *fid);
            for tr in tomb_trs {
                tombstone.add_range(&[1], tr, None).await?;
                if let Ok(i) = ori_data_blocks_idx {
                    let (_, blocks) = &mut ori_data[i];
                    for blk in blocks.iter_mut() {
                        blk.exclude(&tr);
                    }
                }
            }
        }
        tombstone.flush().await?;

        println!(
            "Generated tsm file: '{}', and tombstone: '{}'",
            tsm_file.display(),
            tombstone_file.display(),
        );

        let mut expected_data = Vec::with_capacity(ori_data.len());
        for (field_id, blocks) in ori_data {
            let mut new_blocks = Vec::with_capacity(blocks.len());
            for blk in blocks {
                if !blk.is_empty() {
                    new_blocks.push(blk);
                }
            }
            if !new_blocks.is_empty() {
                expected_data.push((field_id, new_blocks));
            }
        }
        Ok((tsm_file, tombstone_file, expected_data))
    }

    /// Read data blocks from reader and check with expected data.
    pub async fn read_and_check(reader: &TsmReader, expected_data: &[(FieldId, Vec<DataBlock>)]) {
        let mut last_idx_i = 0_usize;
        for (idx_i, idx) in reader.index_iterator().enumerate() {
            last_idx_i = idx_i;
            if idx_i > expected_data.len() {
                panic!("IndexMeta count is more than expected");
            }
            let (exp_field_id, exp_data_blocks) = &expected_data[idx_i];
            assert_eq!(
                idx.field_id(),
                *exp_field_id,
                "IndexMeta[{idx_i}].field_id is {} but expected {exp_field_id}",
                idx.field_id(),
            );

            let mut last_blk_i = 0_usize;
            for (blk_i, blk) in idx.block_iterator().enumerate() {
                last_blk_i = blk_i;
                if blk_i > exp_data_blocks.len() {
                    panic!("BlockMeta count is more than expected");
                }
                let exp_blk = &exp_data_blocks[blk_i];
                let blk_field_id = blk.field_id();
                assert_eq!(blk_field_id, *exp_field_id, "DataBlock[{exp_field_id}][{blk_i}].field_id is {blk_field_id} but expected {exp_field_id}");
                // Because of tombstones, although the BlockMeta.count > 0, the expected block may be empty.
                let data_block = reader.get_data_block(&blk).await.unwrap().unwrap();
                assert_eq!(
                    data_block, *exp_blk,
                    "DataBlock[{exp_field_id}][{blk_i}] is:\n{data_block}\nbut expected\n{exp_blk}",
                );
            }
            if last_blk_i + 1 < exp_data_blocks.len() {
                panic!("BlockMeta count is less than expected");
            }
        }
        if last_idx_i + 1 < expected_data.len() {
            panic!(
                "IndexMeta count {} is less than expected {}",
                last_idx_i + 1,
                expected_data.len()
            );
        }
    }

    #[tokio::test]
    async fn test_tsm_reader() {
        let (tsm_file, _, expected_data) =
            prepare("/tmp/test/tsm_reader/test_tsm_reader", None, None)
                .await
                .unwrap();
        let reader = TsmReader::open(&tsm_file).await.unwrap();
        read_and_check(&reader, &expected_data).await;
    }

    async fn read_opt_and_check(
        reader: &TsmReader,
        field_id: FieldId,
        time_range: (Timestamp, Timestamp),
        expected_data: &HashMap<FieldId, Vec<DataBlock>>,
    ) {
        let time_ranges = Arc::new(TimeRanges::with_inclusive_bounds(
            time_range.0,
            time_range.1,
        ));
        let mut read_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx in reader.index_iterator_opt(field_id) {
            for blk in idx.block_iterator_opt(time_ranges.clone()) {
                let data_blk = reader.get_data_block(&blk).await.unwrap().unwrap();
                read_data.entry(idx.field_id()).or_default().push(data_blk);
            }
        }
        assert_eq!(expected_data.len(), read_data.len());
        for (field_id, data_blks) in read_data.iter() {
            let expected_data_blks = expected_data.get(field_id).unwrap();
            assert_eq!(data_blks, expected_data_blks);
        }
    }

    #[tokio::test]
    async fn test_tsm_reader_read_with_options() {
        let (tsm_file, _, _) = prepare("/tmp/test/tsm_reader/read_with_options", None, None)
            .await
            .unwrap();
        let reader = TsmReader::open(&tsm_file).await.unwrap();

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (2, vec![
                    DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![101, 102, 103, 104], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 2, (0, 4), &expected_data).await;
            read_opt_and_check(&reader, 2, (1, 1), &expected_data).await;
            read_opt_and_check(&reader, 2, (1, 4), &expected_data).await;
            read_opt_and_check(&reader, 2, (2, 3), &expected_data).await;
            read_opt_and_check(&reader, 2, (2, 2), &expected_data).await;
            read_opt_and_check(&reader, 2, (4, 4), &expected_data).await;
        }

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (2, vec![
                    DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 2, (5, 5), &expected_data).await;
            read_opt_and_check(&reader, 2, (5, 8), &expected_data).await;
            read_opt_and_check(&reader, 2, (8, 8), &expected_data).await;
        }

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (2, vec![
                    DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                    DataBlock::U64 { ts: vec![9, 10, 11, 12], val: vec![109, 110, 111, 112], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 2, (5, 9), &expected_data).await;
            read_opt_and_check(&reader, 2, (5, 12), &expected_data).await;
            read_opt_and_check(&reader, 2, (5, 13), &expected_data).await;
            read_opt_and_check(&reader, 2, (6, 10), &expected_data).await;
            read_opt_and_check(&reader, 2, (8, 9), &expected_data).await;
            read_opt_and_check(&reader, 2, (8, 12), &expected_data).await;
            read_opt_and_check(&reader, 2, (8, 13), &expected_data).await;
        }

        {
            let expected_data = HashMap::new();
            read_opt_and_check(&reader, 3, (1, 1), &expected_data).await;
            read_opt_and_check(&reader, 3, (1, 4), &expected_data).await;
            read_opt_and_check(&reader, 3, (3, 4), &expected_data).await;
            read_opt_and_check(&reader, 3, (4, 4), &expected_data).await;
            read_opt_and_check(&reader, 3, (6, 8), &expected_data).await;
            read_opt_and_check(&reader, 3, (8, 8), &expected_data).await;
            read_opt_and_check(&reader, 3, (10, 10), &expected_data).await;
            read_opt_and_check(&reader, 3, (10, 11), &expected_data).await;
        }

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (3, vec![DataBlock::U64 { ts: vec![5], val: vec![105], enc: DataBlockEncoding::default() }])
            ]);
            read_opt_and_check(&reader, 3, (4, 6), &expected_data).await;
            read_opt_and_check(&reader, 3, (4, 5), &expected_data).await;
            read_opt_and_check(&reader, 3, (5, 5), &expected_data).await;
            read_opt_and_check(&reader, 3, (5, 6), &expected_data).await;
        }

        {
            #[rustfmt::skip]
            let expected_data = HashMap::from([
                (3, vec![
                    DataBlock::U64 { ts: vec![5], val: vec![105], enc: DataBlockEncoding::default() },
                    DataBlock::U64 { ts: vec![9], val: vec![109], enc: DataBlockEncoding::default() },
                ])
            ]);
            read_opt_and_check(&reader, 3, (4, 10), &expected_data).await;
            read_opt_and_check(&reader, 3, (5, 9), &expected_data).await;
            read_opt_and_check(&reader, 3, (5, 10), &expected_data).await;
        }
    }

    #[tokio::test]
    async fn test_index_file() {
        let (tsm_file, _, _) = prepare("/tmp/test/tsm_reader/index_file", None, None)
            .await
            .unwrap();

        let file = Arc::new(file_manager::open_file(&tsm_file).await.unwrap());
        let mut idx_file = IndexFile::open(file).await.unwrap();
        let mut idx_metas: Vec<IndexEntry> = Vec::new();
        let mut blk_metas: Vec<BlockEntry> = Vec::new();

        loop {
            match idx_file.next_index_entry().await {
                Ok(None) => break,
                Ok(Some(idx_entry)) => {
                    idx_metas.push(idx_entry);
                    loop {
                        match idx_file.next_block_entry().await {
                            Ok(None) => break,
                            Ok(Some(blk_entry)) => {
                                blk_metas.push(blk_entry);
                            }
                            Err(e) => panic!("Error reading block entry: {:?}", e),
                        }
                    }
                }
                Err(e) => panic!("Error reading index entry: {:?}", e),
            }
        }

        assert_eq!(idx_metas[0].field_id, 1);
        assert_eq!(idx_metas[1].field_id, 2);

        assert_eq!(blk_metas[0].min_ts, 1);
        assert_eq!(blk_metas[0].max_ts, 4);
        assert_eq!(blk_metas[0].count, 4);
        assert_eq!(blk_metas[1].min_ts, 1);
        assert_eq!(blk_metas[1].max_ts, 4);
        assert_eq!(blk_metas[1].count, 4);
        assert_eq!(blk_metas[2].min_ts, 5);
        assert_eq!(blk_metas[2].max_ts, 8);
        assert_eq!(blk_metas[2].count, 4);
        assert_eq!(blk_metas[3].min_ts, 9);
        assert_eq!(blk_metas[3].max_ts, 12);
        assert_eq!(blk_metas[3].count, 4);
    }

    /// Test index reader for tsm v2.
    #[tokio::test]
    async fn test_index_reader_well_ordered() {
        let (tsm_file, _, _) = prepare("/tmp/test/tsm_reader/index_reader_1", None, None)
            .await
            .unwrap();

        let file = Arc::new(file_manager::open_file(&tsm_file).await.unwrap());
        let file_version = file.read_at(4, 1).await.unwrap();
        let idx_reader = IndexReader::open(1, file).await.unwrap();
        assert_eq!(file_version[0], 2);

        let mut index_meta_field_ids: Vec<FieldId> = Vec::new();
        let mut blk_metas: Vec<(TimeRange, u32)> = Vec::new();
        for idx_meta in idx_reader.iter() {
            index_meta_field_ids.push(idx_meta.field_id());
            for blk_meta in idx_meta.block_iterator() {
                blk_metas.push((blk_meta.time_range(), blk_meta.count()));
            }
        }
        assert_eq!(index_meta_field_ids, vec![1, 2, 3]);
        assert_eq!(
            blk_metas,
            vec![
                ((1, 4).into(), 4),
                ((1, 4).into(), 4),
                ((5, 8).into(), 4),
                ((9, 12).into(), 4),
                ((5, 5).into(), 1),
                ((9, 9).into(), 1),
            ]
        );
    }

    /// Test index reader for tsm v1.
    #[tokio::test]
    async fn test_index_reader_disordered() {
        #[rustfmt::skip]
        let ori_data = vec![
            (2, vec![
                DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![101, 102, 103, 104], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![5, 6, 7, 8], val: vec![105, 106, 107, 108], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![9, 10, 11, 12], val: vec![109, 110, 111, 112], enc: DataBlockEncoding::default() },
            ]),
            (3, vec![
                DataBlock::U64 { ts: vec![5], val: vec![105], enc: DataBlockEncoding::default() },
                DataBlock::U64 { ts: vec![9], val: vec![109], enc: DataBlockEncoding::default() },
            ]),
            (1, vec![DataBlock::U64 { ts: vec![1, 2, 3, 4], val: vec![11, 12, 13, 15], enc: DataBlockEncoding::default() }]),
        ];
        let (tsm_file, _, _) = prepare("/tmp/test/tsm_reader/index_reader_2", Some(ori_data), None)
            .await
            .unwrap();

        let file = Arc::new(file_manager::open_file(&tsm_file).await.unwrap());
        let idx_reader = IndexReader::open(1, file).await.unwrap();

        let mut index_meta_field_ids: Vec<FieldId> = Vec::new();
        let mut blk_metas: Vec<(TimeRange, u32)> = Vec::new();
        for idx_meta in idx_reader.iter() {
            index_meta_field_ids.push(idx_meta.field_id());
            for blk_meta in idx_meta.block_iterator() {
                blk_metas.push((blk_meta.time_range(), blk_meta.count()));
            }
        }
        assert_eq!(index_meta_field_ids, vec![1, 2, 3]);
        assert_eq!(
            blk_metas,
            vec![
                ((1, 4).into(), 4),
                ((1, 4).into(), 4),
                ((5, 8).into(), 4),
                ((9, 12).into(), 4),
                ((5, 5).into(), 1),
                ((9, 9).into(), 1),
            ]
        );
    }
}
