use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;

use models::predicate::domain::TimeRange;
use models::schema::TskvTableSchemaRef;
use models::{SeriesId, SeriesKey};
use trace::{info, trace};
use utils::BloomFilter;

use crate::compaction::CompactReq;
use crate::context::GlobalContext;
use crate::error::Result;
use crate::summary::{CompactMeta, VersionEdit};
use crate::tseries_family::TseriesFamily;
use crate::tsm2::page::{Chunk, ColumnGroup, Page};
use crate::tsm2::reader::{decode_pages, decode_pages_buf, TSM2MetaData, TSM2Reader};
use crate::tsm2::writer::{DataBlock2, Tsm2Writer};
use crate::tsm2::ColumnGroupID;
use crate::{ColumnFileId, Error, LevelId, TseriesFamilyId};

/// Temporary compacting data block meta
#[derive(Clone)]
pub(crate) struct CompactingBlockMeta {
    reader_idx: usize,
    reader: Arc<TSM2Reader>,
    meta: Arc<Chunk>,
    column_group_id: ColumnGroupID,
}

impl PartialEq for CompactingBlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.reader.file_id() == other.reader.file_id()
            && self.meta.series_id() == other.meta.series_id()
            && self.column_group_id == other.column_group_id
    }
}

impl Eq for CompactingBlockMeta {}

impl PartialOrd for CompactingBlockMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactingBlockMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let res = self.meta.series_id().cmp(&other.meta.series_id());
        if res != std::cmp::Ordering::Equal {
            res
        } else {
            match (
                self.meta.column_group().get(&self.column_group_id),
                other.meta.column_group().get(&other.column_group_id),
            ) {
                (Some(cg1), Some(cg2)) => cg1.time_range().cmp(cg2.time_range()),
                (None, Some(_)) => std::cmp::Ordering::Less,
                (Some(_), None) => std::cmp::Ordering::Greater,
                _ => std::cmp::Ordering::Equal,
            }
        }
    }
}

impl CompactingBlockMeta {
    pub fn new(
        tsm_reader_idx: usize,
        tsm_reader: Arc<TSM2Reader>,
        chunk: Arc<Chunk>,
        column_group_id: ColumnGroupID,
    ) -> Self {
        Self {
            reader_idx: tsm_reader_idx,
            reader: tsm_reader,
            meta: chunk,
            column_group_id,
        }
    }

    pub fn time_range(&self) -> Result<TimeRange> {
        let column_group =
            self.meta
                .column_group()
                .get(&self.column_group_id)
                .ok_or(Error::CommonError {
                    reason: format!(
                        "column group {} not found in chunk {:?}",
                        self.column_group_id, self.meta
                    ),
                })?;
        Ok(*column_group.time_range())
    }

    pub async fn get_data_block_filter_by_tomb(&self) -> Result<DataBlock2> {
        let sid = self.meta.series_id();
        let mut data_block = self
            .reader
            .read_datablock(sid, self.column_group_id)
            .await?;
        if self.reader.has_tombstone() {
            let tomb_filter = self.reader.tombstone();
            data_block.filter_by_tomb(tomb_filter, sid)?;
        }
        Ok(data_block)
    }

    pub async fn get_raw_data(&self) -> Result<Vec<u8>> {
        self.reader
            .read_datablock_raw(self.meta.series_id(), self.column_group_id)
            .await
    }

    pub fn tsm_meta(&self) -> Arc<TSM2MetaData> {
        self.reader.tsm_meta_data()
    }

    pub fn column_group(&self) -> Result<Arc<ColumnGroup>> {
        self.meta
            .column_group()
            .get(&self.column_group_id)
            .cloned()
            .ok_or(Error::CommonError {
                reason: format!(
                    "column group {} not found in chunk {:?}",
                    self.column_group_id, self.meta
                ),
            })
    }

    pub fn table_schema(&self) -> Option<TskvTableSchemaRef> {
        self.reader.table_schema(self.meta.table_name())
    }

    pub fn has_tombstone(&self) -> bool {
        self.reader.has_tombstone()
    }
}

#[derive(Clone)]
pub(crate) struct CompactingBlockMetaGroup {
    series_id: SeriesId,
    chunk: Arc<Chunk>,
    blk_metas: Vec<CompactingBlockMeta>,
    time_range: TimeRange,
}
impl CompactingBlockMetaGroup {
    pub fn new(series_id: SeriesId, blk_meta: CompactingBlockMeta) -> Result<Self> {
        let time_range = blk_meta.time_range()?;
        Ok(Self {
            series_id,
            chunk: blk_meta.meta.clone(),
            blk_metas: vec![blk_meta],
            time_range,
        })
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.time_range.overlaps(&other.time_range)
    }

    pub fn append(&mut self, other: &mut CompactingBlockMetaGroup) {
        self.blk_metas.append(&mut other.blk_metas);
        self.time_range.merge(&other.time_range);
    }

    pub async fn merge(
        mut self,
        previous_block: Option<CompactingBlock>,
        max_block_size: usize,
    ) -> Result<Vec<CompactingBlock>> {
        if self.blk_metas.is_empty() {
            return Ok(vec![]);
        }
        self.blk_metas
            .sort_by(|a, b| a.reader_idx.cmp(&b.reader_idx).reverse());

        let merged_block;
        if self.blk_metas.len() == 1 && !self.blk_metas[0].has_tombstone() {
            // Only one compacting block and has no tombstone, write as raw block.
            trace!("only one compacting block, write as raw block");
            let meta_0 = &self.blk_metas[0].meta;
            let column_group_id = self.blk_metas[0].column_group_id;
            let column_group = self.blk_metas[0].column_group()?;
            let buf_0 = self.blk_metas[0].get_raw_data().await?;

            if column_group.row_len() >= max_block_size {
                // Raw data block is full, so do not merge with the previous, directly return.
                let mut merged_blks = Vec::new();
                if let Some(blk) = previous_block {
                    merged_blks.push(blk);
                }
                let table_schema = self.blk_metas[0].table_schema().ok_or(Error::CommonError {
                    reason: format!("table schema not found for table {}", meta_0.table_name()),
                })?;
                merged_blks.push(CompactingBlock::raw(
                    self.blk_metas[0].reader_idx,
                    meta_0.clone(),
                    table_schema,
                    column_group_id,
                    buf_0,
                ));

                return Ok(merged_blks);
            } else if let Some(compacting_block) = previous_block {
                // Raw block is not full, so decode and merge with compacting_block.
                let meta = self.blk_metas[0].tsm_meta();
                let chunk = self.blk_metas[0].meta.clone();
                let column_group_id = self.blk_metas[0].column_group_id;
                let schema = meta
                    .table_schema(chunk.table_name())
                    .ok_or(Error::CommonError {
                        reason: format!("table schema not found for table {}", chunk.table_name()),
                    })?;
                let decoded_raw_block = decode_pages_buf(&buf_0, chunk, column_group_id, schema)?;
                let mut data_block = compacting_block.decode()?;
                let data_block = data_block.merge(decoded_raw_block)?;

                merged_block = data_block;
            } else {
                // Raw block is not full, but nothing to merge with, directly return.
                let table_schema = self.blk_metas[0].table_schema().ok_or(Error::CommonError {
                    reason: format!("table schema not found for table {}", meta_0.table_name()),
                })?;
                return Ok(vec![CompactingBlock::raw(
                    self.blk_metas[0].reader_idx,
                    meta_0.clone(),
                    table_schema,
                    column_group_id,
                    buf_0,
                )]);
            }
        } else {
            // One block with tombstone or multi compacting blocks, decode and merge these data block.
            trace!(
                "there are {} compacting blocks, need to decode and merge",
                self.blk_metas.len()
            );
            let head = &mut self.blk_metas[0];
            let mut head_block = head.get_data_block_filter_by_tomb().await?;

            if let Some(compacting_block) = previous_block {
                let mut data_block = compacting_block.decode()?;
                let data_block = data_block.merge(head_block)?;
                head_block = data_block;
            }

            for blk_meta in self.blk_metas[1..].iter_mut() {
                // Merge decoded data block.
                let blk_block = blk_meta.get_data_block_filter_by_tomb().await?;
                head_block = head_block.merge(blk_block)?;
            }
            merged_block = head_block;
        }

        self.chunk_merged_block(merged_block, max_block_size)
    }

    fn chunk_merged_block(
        &self,
        data_block: DataBlock2,
        max_block_size: usize,
    ) -> Result<Vec<CompactingBlock>> {
        let mut merged_blks = Vec::new();
        if max_block_size == 0 || data_block.len() < max_block_size {
            // Data block elements less than max_block_size, do not encode it.
            // Try to merge with the next CompactingBlockMetaGroup.
            merged_blks.push(CompactingBlock::decoded(
                0,
                self.series_id,
                self.chunk.series_key().clone(),
                data_block,
            ));
        } else {
            let len = data_block.len();
            let mut start = 0;
            while start + max_block_size < len {
                let data_block_merge = data_block.chunk(start, start + max_block_size)?;
                let time_range = data_block_merge.time_range()?;
                let table_schema = data_block_merge.schema();
                let data_block_merge_pages = data_block_merge.block_to_page()?;
                // Encode decoded data blocks into chunks.
                merged_blks.push(CompactingBlock::encoded(
                    0,
                    table_schema,
                    self.series_id,
                    self.chunk.series_key().clone(),
                    time_range,
                    data_block_merge_pages,
                ));

                start += max_block_size;
            }
            if start < len {
                // Encode the remaining decoded data blocks.
                let data_block_merge = data_block.chunk(start, len)?;
                let time_range = data_block_merge.time_range()?;
                let table_schema = data_block_merge.schema();
                let data_block_merge_pages = data_block_merge.block_to_page()?;
                // Encode decoded data blocks into chunks.
                merged_blks.push(CompactingBlock::encoded(
                    0,
                    table_schema,
                    self.series_id,
                    self.chunk.series_key().clone(),
                    time_range,
                    data_block_merge_pages,
                ));
            }
        }

        Ok(merged_blks)
    }

    pub fn is_empty(&self) -> bool {
        self.blk_metas.is_empty()
    }

    pub fn len(&self) -> usize {
        self.blk_metas.len()
    }
}

/// Temporary compacting data block.
/// - priority: When merging two (timestamp, value) pair with the same
/// timestamp from two data blocks, pair from data block with lower
/// priority will be discarded.
#[derive(Debug)]
pub enum CompactingBlock {
    Decoded {
        priority: usize,
        series_id: SeriesId,
        series_key: SeriesKey,
        data_block: DataBlock2,
    },
    Encoded {
        priority: usize,
        table_schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: SeriesKey,
        time_range: TimeRange,
        data_block: Vec<Page>,
    },
    Raw {
        priority: usize,
        table_schema: TskvTableSchemaRef,
        meta: Arc<Chunk>,
        column_group_id: ColumnGroupID,
        raw: Vec<u8>,
    },
}

impl CompactingBlock {
    pub fn decoded(
        priority: usize,
        series_id: SeriesId,
        series_key: SeriesKey,
        data_block: DataBlock2,
    ) -> CompactingBlock {
        Self::Decoded {
            priority,
            series_id,
            series_key,
            data_block,
        }
    }

    pub fn encoded(
        priority: usize,
        table_schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: SeriesKey,
        time_range: TimeRange,
        data_block: Vec<Page>,
    ) -> CompactingBlock {
        Self::Encoded {
            priority,
            series_id,
            series_key,
            table_schema,
            time_range,
            data_block,
        }
    }

    pub fn raw(
        priority: usize,
        chunk: Arc<Chunk>,
        table_schema: TskvTableSchemaRef,
        column_group_id: ColumnGroupID,
        raw: Vec<u8>,
    ) -> CompactingBlock {
        CompactingBlock::Raw {
            priority,
            meta: chunk,
            table_schema,
            column_group_id,
            raw,
        }
    }

    pub fn decode(self) -> Result<DataBlock2> {
        match self {
            CompactingBlock::Decoded { data_block, .. } => Ok(data_block),
            CompactingBlock::Encoded {
                data_block,
                table_schema,
                ..
            } => decode_pages(data_block, table_schema),
            CompactingBlock::Raw {
                raw,
                meta,
                table_schema,
                column_group_id,
                ..
            } => decode_pages_buf(&raw, meta, column_group_id, table_schema),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CompactingBlock::Decoded { data_block, .. } => data_block.len(),
            CompactingBlock::Encoded { data_block, .. } => data_block[0].meta.num_values as usize,
            CompactingBlock::Raw {
                meta,
                column_group_id,
                ..
            } => {
                meta.column_group()[column_group_id].pages()[0]
                    .meta
                    .num_values as usize
            }
        }
    }

    pub fn time_range(&self) -> Result<TimeRange> {
        match self {
            CompactingBlock::Decoded { data_block, .. } => data_block.time_range(),
            CompactingBlock::Encoded { time_range, .. } => Ok(*time_range),
            CompactingBlock::Raw {
                meta,
                column_group_id,
                ..
            } => Ok(*meta.column_group()[column_group_id].time_range()),
        }
    }
}

struct CompactingFile {
    i: usize,
    tsm_reader: Arc<TSM2Reader>,
    series_idx: usize,
    series_ids: Vec<SeriesId>,
}

impl CompactingFile {
    fn new(i: usize, tsm_reader: Arc<TSM2Reader>) -> Self {
        let mut series_ids = {
            let chunks = tsm_reader.chunk_group();
            chunks
                .iter()
                .flat_map(|(_, chunk)| chunk.chunks.iter().map(|chunk_meta| chunk_meta.series_id))
                .collect::<Vec<_>>()
        };
        series_ids.sort();

        Self {
            i,
            tsm_reader,
            series_idx: 0,
            series_ids,
        }
    }

    fn next(&mut self) {
        self.series_idx += 1;
    }

    fn series_id(&self) -> Option<SeriesId> {
        self.series_ids.get(self.series_idx).copied()
    }

    fn chunk(&self) -> Option<Arc<Chunk>> {
        let sid = self.series_id()?;
        self.tsm_reader.chunk().get(&sid).cloned()
    }
}

impl Eq for CompactingFile {}

impl PartialEq for CompactingFile {
    fn eq(&self, other: &Self) -> bool {
        self.tsm_reader.file_id() == other.tsm_reader.file_id()
            && self.series_id() == other.series_id()
    }
}

impl Ord for CompactingFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let res = match (self.series_id(), other.series_id()) {
            (Some(sid1), Some(sid2)) => sid1.cmp(&sid2),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        res.reverse()
    }
}

impl PartialOrd for CompactingFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct CompactIterator {
    tsm_readers: Vec<Arc<TSM2Reader>>,
    compacting_files: BinaryHeap<Pin<Box<CompactingFile>>>,
    /// Maximum values in generated CompactingBlock
    max_data_block_size: usize,
    // /// The time range of data to be merged of level-0 data blocks.
    // /// The level-0 data that out of the thime range will write back to level-0.
    // level_time_range: TimeRange,
    /// Decode a data block even though it doesn't need to merge with others,
    /// return CompactingBlock::DataBlock rather than CompactingBlock::Raw .
    decode_non_overlap_blocks: bool,

    tmp_tsm_blk_meta_iters: Vec<(Arc<Chunk>, ColumnGroupID, usize)>,
    /// Index to mark `Peekable<BlockMetaIterator>` in witch `TsmReader`,
    /// tmp_tsm_blks[i] is in self.tsm_readers[ tmp_tsm_blk_tsm_reader_idx[i] ]
    tmp_tsm_blk_tsm_reader_idx: Vec<usize>,
    /// When a TSM file at index i is ended, finished_idxes[i] is set to true.
    finished_readers: Vec<bool>,
    /// How many finished_idxes is set to true.
    finished_reader_cnt: usize,
    curr_sid: Option<SeriesId>,

    merging_blk_meta_groups: VecDeque<CompactingBlockMetaGroup>,
}

/// To reduce construction code
impl Default for CompactIterator {
    fn default() -> Self {
        Self {
            tsm_readers: Default::default(),
            compacting_files: Default::default(),
            max_data_block_size: 0,
            decode_non_overlap_blocks: false,
            tmp_tsm_blk_meta_iters: Default::default(),
            tmp_tsm_blk_tsm_reader_idx: Default::default(),
            finished_readers: Default::default(),
            finished_reader_cnt: Default::default(),
            curr_sid: Default::default(),
            merging_blk_meta_groups: Default::default(),
        }
    }
}

impl CompactIterator {
    pub(crate) fn new(
        tsm_readers: Vec<Arc<TSM2Reader>>,
        max_data_block_size: usize,
        decode_non_overlap_blocks: bool,
    ) -> Self {
        let compacting_files: BinaryHeap<Pin<Box<CompactingFile>>> = tsm_readers
            .iter()
            .enumerate()
            .map(|(i, r)| Box::pin(CompactingFile::new(i, r.clone())))
            .collect();
        let compacting_files_cnt = compacting_files.len();

        Self {
            tsm_readers,
            compacting_files,
            max_data_block_size,
            decode_non_overlap_blocks,
            finished_readers: vec![false; compacting_files_cnt],
            ..Default::default()
        }
    }

    /// Update tmp_tsm_blks and tmp_tsm_blk_tsm_reader_idx for field id in next iteration.
    fn next_series_id(&mut self) -> Result<()> {
        self.curr_sid = None;
        self.tmp_tsm_blk_tsm_reader_idx.clear();
        self.tmp_tsm_blk_meta_iters.clear();

        if let Some(f) = self.compacting_files.peek() {
            if self.curr_sid.is_none() {
                trace!(
                    "selected new field {:?} from file {} as current field id",
                    f.series_id(),
                    f.tsm_reader.file_id()
                );
                self.curr_sid = f.series_id()
            }
        } else {
            // TODO finished
            trace!("no file to select, mark finished");
            self.finished_reader_cnt += 1;
        }
        self.tmp_tsm_blk_meta_iters.clear();
        while let Some(mut f) = self.compacting_files.pop() {
            let loop_series_id = f.series_id();
            let loop_file_i = f.i;
            if self.curr_sid == loop_series_id {
                if let Some(sid) = loop_series_id {
                    self.tmp_tsm_blk_tsm_reader_idx.push(loop_file_i);
                    let meta =
                        f.tsm_reader
                            .chunk()
                            .get(&sid)
                            .cloned()
                            .ok_or(Error::CommonError {
                                reason: format!(
                                    "series id {} not found in file {}",
                                    sid, loop_file_i
                                ),
                            })?;
                    let column_groups_id = meta.column_group().keys().cloned().collect::<Vec<_>>();
                    column_groups_id.iter().for_each(|&column_group_id| {
                        self.tmp_tsm_blk_meta_iters.push((
                            meta.clone(),
                            column_group_id,
                            self.tmp_tsm_blk_tsm_reader_idx.len() - 1,
                        ));
                    });
                    f.next();
                    self.compacting_files.push(f);
                } else {
                    // This tsm-file has been finished
                    trace!("file {} is finished.", loop_file_i);
                    self.finished_readers[loop_file_i] = true;
                    self.finished_reader_cnt += 1;
                }
            } else {
                self.compacting_files.push(f);
                break;
            }
        }
        Ok(())
    }

    /// Collect merging `DataBlock`s.
    async fn fetch_merging_block_meta_groups(&mut self) -> Result<bool> {
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            return Ok(false);
        }
        let series_id = match self.curr_sid {
            Some(sid) => sid,
            None => return Ok(false),
        };

        let mut blk_metas: Vec<CompactingBlockMeta> =
            Vec::with_capacity(self.tmp_tsm_blk_meta_iters.len());
        // Get all block_meta, and check if it's tsm file has a related tombstone file.
        for (chunk, column_grouop_id, index) in self.tmp_tsm_blk_meta_iters.iter() {
            let tsm_reader_idx = self.tmp_tsm_blk_tsm_reader_idx[*index];
            let tsm_reader_ptr = self.tsm_readers[tsm_reader_idx].clone();
            blk_metas.push(CompactingBlockMeta::new(
                tsm_reader_idx,
                tsm_reader_ptr,
                chunk.clone(),
                *column_grouop_id,
            ));
        }
        // Sort by field_id, min_ts and max_ts.
        blk_metas.sort();

        let mut blk_meta_groups: Vec<CompactingBlockMetaGroup> =
            Vec::with_capacity(blk_metas.len());
        for blk_meta in blk_metas {
            blk_meta_groups.push(CompactingBlockMetaGroup::new(series_id, blk_meta)?);
        }
        // Compact blk_meta_groups.
        let mut i = 0;
        loop {
            let mut head_idx = i;
            // Find the first non-empty as head.
            for (off, bmg) in blk_meta_groups[i..].iter().enumerate() {
                if !bmg.is_empty() {
                    head_idx += off;
                    break;
                }
            }
            if head_idx >= blk_meta_groups.len() - 1 {
                // There no other blk_meta_group to merge with the last one.
                break;
            }
            let mut head = blk_meta_groups[head_idx].clone();
            i = head_idx + 1;
            for bmg in blk_meta_groups[i..].iter_mut() {
                if bmg.is_empty() {
                    continue;
                }
                if head.overlaps(bmg) {
                    head.append(bmg);
                }
            }
            blk_meta_groups[head_idx] = head;
        }
        let blk_meta_groups: VecDeque<CompactingBlockMetaGroup> = blk_meta_groups
            .into_iter()
            .filter(|l| !l.is_empty())
            .collect();

        self.merging_blk_meta_groups = blk_meta_groups;

        Ok(true)
    }
}

impl CompactIterator {
    pub(crate) async fn next(&mut self) -> Result<Option<CompactingBlockMetaGroup>> {
        if let Some(g) = self.merging_blk_meta_groups.pop_front() {
            return Ok(Some(g));
        }

        // For each tsm-file, get next index reader for current iteration field id
        self.next_series_id()?;

        trace!(
            "selected {} blocks meta iterators",
            self.tmp_tsm_blk_meta_iters.len()
        );
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            trace!("iteration field_id {:?} is finished", self.curr_sid);
            self.curr_sid = None;
            return Ok(None);
        }

        // Get all of block_metas of this field id, and merge these blocks
        self.fetch_merging_block_meta_groups().await?;

        if let Some(g) = self.merging_blk_meta_groups.pop_front() {
            return Ok(Some(g));
        }
        Ok(None)
    }
}

/// Returns if r1 (min_ts, max_ts) overlaps r2 (min_ts, max_ts)
fn overlaps_tuples(r1: (i64, i64), r2: (i64, i64)) -> bool {
    r1.0 <= r2.1 && r1.1 >= r2.0
}

pub async fn run_compaction_job(
    request: CompactReq,
    kernel: Arc<GlobalContext>,
) -> Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    info!(
        "Compaction: Running compaction job on ts_family: {} and files: [ {} ]",
        request.ts_family_id,
        request
            .files
            .iter()
            .map(|f| {
                format!(
                    "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    f.level(),
                    f.file_id(),
                    f.time_range().min_ts,
                    f.time_range().max_ts
                )
            })
            .collect::<Vec<String>>()
            .join(", ")
    );

    if request.files.is_empty() {
        // Nothing to compact
        return Ok(None);
    }

    // Buffers all tsm-files and it's indexes for this compaction
    let tsf_id = request.ts_family_id;
    let mut tsm_readers = Vec::new();
    for col_file in request.files.iter() {
        let tsm_reader = request
            .version
            .get_tsm_reader2(col_file.file_path())
            .await?;
        tsm_readers.push(tsm_reader);
    }

    let max_block_size = TseriesFamily::MAX_DATA_BLOCK_SIZE as usize;
    let mut iter = CompactIterator::new(tsm_readers, max_block_size, false);
    let tsm_dir = request.storage_opt.tsm_dir(&request.database, tsf_id);
    let max_file_size = request.storage_opt.level_max_file_size(request.out_level);
    let mut tsm_writer =
        Tsm2Writer::open(&tsm_dir, kernel.file_id_next(), max_file_size, false).await?;
    // let mut tsm_writer = tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0).await?;
    info!(
        "Compaction: File: {} been created (level: {}).",
        tsm_writer.file_id(),
        request.out_level
    );
    let mut version_edit = VersionEdit::new(tsf_id);
    let mut file_metas: HashMap<ColumnFileId, Arc<BloomFilter>> = HashMap::new();

    let mut previous_merged_block: Option<CompactingBlock> = None;
    let mut sid = iter.curr_sid;
    while let Some(blk_meta_group) = iter.next().await? {
        trace!("===============================");
        if sid.is_some() && sid != iter.curr_sid {
            // Iteration of next field id, write previous merged block.
            if let Some(blk) = previous_merged_block.take() {
                tsm_writer.write_compacting_block(blk).await?;
                if handle_finish_write_tsm_meta(
                    &mut tsm_writer,
                    &mut file_metas,
                    &mut version_edit,
                    &request,
                )
                .await?
                {
                    tsm_writer =
                        Tsm2Writer::open(&tsm_dir, kernel.file_id_next(), max_file_size, false)
                            .await?;
                }
            }
        }

        sid = iter.curr_sid;
        let mut compacting_blks = blk_meta_group
            .merge(previous_merged_block.take(), max_block_size)
            .await?;
        if compacting_blks.len() == 1 && compacting_blks[0].len() < max_block_size {
            // The only one data block too small, try to extend the next compacting blocks.
            previous_merged_block = Some(compacting_blks.remove(0));
            continue;
        }

        let last_blk_idx = compacting_blks.len() - 1;
        for (i, blk) in compacting_blks.into_iter().enumerate() {
            if i == last_blk_idx && blk.len() < max_block_size {
                // The last data block too small, try to extend to
                // the next compacting blocks (current field id).
                previous_merged_block = Some(blk);
                break;
            }
            tsm_writer.write_compacting_block(blk).await?;
            if handle_finish_write_tsm_meta(
                &mut tsm_writer,
                &mut file_metas,
                &mut version_edit,
                &request,
            )
            .await?
            {
                tsm_writer =
                    Tsm2Writer::open(&tsm_dir, kernel.file_id_next(), max_file_size, false).await?;
            }
        }
    }
    if let Some(blk) = previous_merged_block {
        tsm_writer.write_compacting_block(blk).await?;
        handle_finish_write_tsm_meta(
            &mut tsm_writer,
            &mut file_metas,
            &mut version_edit,
            &request,
        )
        .await?;
    }

    if !tsm_writer.is_finished() {
        tsm_writer.finish().await?;
        handle_finish_write_tsm_meta(
            &mut tsm_writer,
            &mut file_metas,
            &mut version_edit,
            &request,
        )
        .await?;
    }

    for file in request.files {
        version_edit.del_file(file.level(), file.file_id(), file.is_delta());
    }

    info!(
        "Compaction: Compact finished, version edits: {:?}",
        version_edit
    );
    Ok(Some((version_edit, file_metas)))
}

async fn handle_finish_write_tsm_meta(
    tsm_writer: &mut Tsm2Writer,
    file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    version_edit: &mut VersionEdit,
    request: &CompactReq,
) -> Result<bool> {
    if !tsm_writer.is_finished() {
        return Ok(false);
    }

    let max_level_ts = request.version.max_level_ts();
    file_metas.insert(
        tsm_writer.file_id(),
        Arc::new(tsm_writer.series_bloom_filter().clone()),
    );
    info!(
        "Compaction: File: {} write finished (level: {}, {} B).",
        tsm_writer.file_id(),
        request.out_level,
        tsm_writer.size()
    );

    let cm = new_compact_meta(tsm_writer, request.ts_family_id, request.out_level);
    version_edit.add_file(cm, max_level_ts);

    Ok(true)
}

fn new_compact_meta(
    tsm_writer: &Tsm2Writer,
    tsf_id: TseriesFamilyId,
    level: LevelId,
) -> CompactMeta {
    CompactMeta {
        file_id: tsm_writer.file_id(),
        file_size: tsm_writer.size() as u64,
        tsf_id,
        level,
        min_ts: tsm_writer.min_ts(),
        max_ts: tsm_writer.max_ts(),
        high_seq: 0,
        low_seq: 0,
        is_delta: false,
    }
}

#[cfg(test)]
pub mod test {
    use core::panic;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use arrow::datatypes::TimeUnit;
    use cache::ShardedAsyncCache;
    use models::codec::Encoding;
    use models::field_value::FieldVal;
    use models::predicate::domain::TimeRange;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesId, SeriesKey, ValueType};

    use crate::compaction::{run_compaction_job, CompactReq};
    use crate::context::GlobalContext;
    use crate::file_system::async_filesystem::LocalFileSystem;
    use crate::file_system::FileSystem;
    use crate::file_utils;
    use crate::kv_option::Options;
    use crate::summary::VersionEdit;
    use crate::tseries_family::{ColumnFile, LevelInfo, Version};
    use crate::tsm::TsmTombstone;
    use crate::tsm2::reader::TSM2Reader;
    use crate::tsm2::writer::{Column, DataBlock2, Tsm2Writer};

    pub(crate) async fn write_data_blocks_to_column_file(
        dir: impl AsRef<Path>,
        data: Vec<HashMap<SeriesId, DataBlock2>>,
    ) -> (u64, Vec<Arc<ColumnFile>>) {
        if !LocalFileSystem::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut cfs = Vec::new();
        let mut file_seq = 0;
        for (i, d) in data.iter().enumerate() {
            file_seq = i as u64 + 1;
            let mut writer = Tsm2Writer::open(&dir, file_seq, 0, false).await.unwrap();
            for (sid, data_blks) in d.iter() {
                writer
                    .write_datablock(*sid, SeriesKey::default(), data_blks.clone())
                    .await
                    .unwrap();
            }
            writer.finish().await.unwrap();
            let mut cf = ColumnFile::new(
                file_seq,
                2,
                TimeRange::new(writer.min_ts(), writer.max_ts()),
                writer.size() as u64,
                false,
                writer.path(),
            );
            cf.set_field_id_filter(Arc::new(writer.series_bloom_filter().clone()));
            cfs.push(Arc::new(cf));
        }
        (file_seq + 1, cfs)
    }

    async fn read_data_blocks_from_column_file(
        path: impl AsRef<Path>,
    ) -> HashMap<SeriesId, Vec<DataBlock2>> {
        let tsm_reader = TSM2Reader::open(&path).await.unwrap();
        let mut data = HashMap::new();
        for (sid, chunk) in tsm_reader.chunk() {
            let mut blks = vec![];
            for column_group_id in chunk.column_group().keys() {
                let blk = tsm_reader
                    .read_datablock(*sid, *column_group_id)
                    .await
                    .unwrap();
                blks.push(blk)
            }
            data.insert(*sid, blks);
        }
        data
    }

    fn i64_column(data: Vec<i64>) -> Column {
        let mut col =
            Column::empty_with_cap(ColumnType::Field(ValueType::Integer), data.len()).unwrap();
        for datum in data {
            col.push(Some(FieldVal::Integer(datum)))
        }
        col
    }

    fn ts_column(data: Vec<i64>) -> Column {
        let mut col =
            Column::empty_with_cap(ColumnType::Time(TimeUnit::Nanosecond), data.len()).unwrap();
        for datum in data {
            col.push(Some(FieldVal::Integer(datum)))
        }
        col
    }

    fn i64_some_column(data: Vec<Option<i64>>) -> Column {
        let mut col =
            Column::empty_with_cap(ColumnType::Field(ValueType::Integer), data.len()).unwrap();
        for datum in data {
            col.push(datum.map(FieldVal::Integer))
        }
        col
    }

    fn u64_some_column(data: Vec<Option<u64>>) -> Column {
        let mut col =
            Column::empty_with_cap(ColumnType::Field(ValueType::Unsigned), data.len()).unwrap();
        for datum in data {
            col.push(datum.map(FieldVal::Unsigned))
        }
        col
    }

    fn f64_some_column(data: Vec<Option<f64>>) -> Column {
        let mut col =
            Column::empty_with_cap(ColumnType::Field(ValueType::Float), data.len()).unwrap();
        for datum in data {
            col.push(datum.map(FieldVal::Float))
        }
        col
    }

    fn bool_some_column(data: Vec<Option<bool>>) -> Column {
        let mut col =
            Column::empty_with_cap(ColumnType::Field(ValueType::Boolean), data.len()).unwrap();
        for datum in data {
            col.push(datum.map(FieldVal::Boolean))
        }
        col
    }

    fn get_result_file_path(dir: impl AsRef<Path>, version_edit: VersionEdit) -> PathBuf {
        if version_edit.has_file_id && !version_edit.add_files.is_empty() {
            let file_id = version_edit.add_files.first().unwrap().file_id;
            return file_utils::make_tsm_file(dir, file_id);
        }

        panic!("VersionEdit doesn't contain any add_files.");
    }

    /// Compare DataBlocks in path with the expected_Data using assert_eq.
    async fn check_column_file(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        expected_data: HashMap<SeriesId, Vec<DataBlock2>>,
    ) {
        let path = get_result_file_path(dir, version_edit);
        let mut data = read_data_blocks_from_column_file(path).await;
        let mut data_series_ids = data.keys().copied().collect::<Vec<_>>();
        data_series_ids.sort_unstable();
        let mut expected_data_series_ids = expected_data.keys().copied().collect::<Vec<_>>();
        expected_data_series_ids.sort_unstable();
        assert_eq!(data_series_ids, expected_data_series_ids);

        for (k, v) in expected_data.into_iter() {
            let data_blks = data.remove(&k).unwrap();
            println!("v.len(): {}", v.len());
            println!("data_blks.len(): {}", data_blks.len());
            for v in v.iter().enumerate() {
                println!("v[{}]: {}", v.0, v.1.len());
            }
            for data_blk in data_blks.iter().enumerate() {
                println!("data_blk[{}]: {}", data_blk.0, data_blk.1.len());
            }
            for (v, data_blc) in v.iter().zip(data_blks.iter()) {
                assert_eq!(v, data_blc);
            }
        }
    }

    pub(crate) fn create_options(base_dir: String) -> Arc<Options> {
        let mut config = config::get_config_for_test();
        config.storage.path = base_dir;
        let opt = Options::from(&config);
        Arc::new(opt)
    }

    fn prepare_compact_req_and_kernel(
        database: Arc<String>,
        opt: Arc<Options>,
        next_file_id: u64,
        files: Vec<Arc<ColumnFile>>,
    ) -> (CompactReq, Arc<GlobalContext>) {
        let version = Arc::new(Version::new(
            1,
            database.clone(),
            opt.storage.clone(),
            1,
            LevelInfo::init_levels(database.clone(), 0, opt.storage.clone()),
            1000,
            Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
        ));
        let compact_req = CompactReq {
            ts_family_id: 1,
            database,
            storage_opt: opt.storage.clone(),
            files,
            version,
            out_level: 2,
        };
        let kernel = Arc::new(GlobalContext::new());
        kernel.set_file_id(next_file_id);

        (compact_req, kernel)
    }

    #[tokio::test]
    async fn test_compaction_fast() {
        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let data1 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![1, 2, 3]),
            schema.time_column(),
            vec![
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let data2 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![4, 5, 6]),
            schema.time_column(),
            vec![
                i64_column(vec![4, 5, 6]),
                i64_column(vec![4, 5, 6]),
                i64_column(vec![4, 5, 6]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let data3 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![7, 8, 9]),
            schema.time_column(),
            vec![
                i64_column(vec![7, 8, 9]),
                i64_column(vec![7, 8, 9]),
                i64_column(vec![7, 8, 9]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let expected_data = DataBlock2::new(
            schema.clone(),
            ts_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            schema.time_column(),
            vec![
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let data = vec![
            HashMap::from([(1, data1)]),
            HashMap::from([(1, data2)]),
            HashMap::from([(1, data3)]),
        ];

        let expected_data = HashMap::from([(1 as SeriesId, vec![expected_data])]);

        let dir = "/tmp/test/compaction/fast";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_1() {
        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let data1 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![4, 5, 6]),
            schema.time_column(),
            vec![
                i64_column(vec![4, 5, 6]),
                i64_column(vec![4, 5, 6]),
                i64_column(vec![4, 5, 6]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let data2 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![1, 2, 3]),
            schema.time_column(),
            vec![
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let data3 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![7, 8, 9]),
            schema.time_column(),
            vec![
                i64_column(vec![7, 8, 9]),
                i64_column(vec![7, 8, 9]),
                i64_column(vec![7, 8, 9]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let expected_data = DataBlock2::new(
            schema.clone(),
            ts_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            schema.time_column(),
            vec![
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let data = vec![
            HashMap::from([(1, data1)]),
            HashMap::from([(1, data2)]),
            HashMap::from([(1, data3)]),
        ];

        let expected_data = HashMap::from([(1 as SeriesId, vec![expected_data])]);

        let dir = "/tmp/test/compaction/1";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_2() {
        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    4,
                    "f4".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let data1 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![1, 2, 3, 4]),
            schema.time_column(),
            vec![
                i64_some_column(vec![Some(1), Some(2), Some(3), Some(5)]),
                i64_some_column(vec![Some(1), Some(2), Some(3), Some(5)]),
                i64_some_column(vec![Some(1), Some(2), Some(3), None]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
                schema.column("f4").cloned().unwrap(),
            ],
        );

        let data2 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![4, 5, 6, 7]),
            schema.time_column(),
            vec![
                i64_some_column(vec![Some(4), Some(5), Some(6), None]),
                i64_some_column(vec![Some(4), Some(5), Some(6), None]),
                i64_some_column(vec![Some(4), Some(5), Some(6), Some(8)]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let data3 = DataBlock2::new(
            schema.clone(),
            ts_column(vec![7, 8, 9]),
            schema.time_column(),
            vec![
                i64_column(vec![7, 8, 9]),
                i64_column(vec![7, 8, 9]),
                i64_column(vec![7, 8, 9]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
            ],
        );

        let expected_data = DataBlock2::new(
            schema.clone(),
            ts_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
            schema.time_column(),
            vec![
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
                i64_some_column(vec![
                    None,
                    None,
                    None,
                    Some(4),
                    Some(5),
                    Some(6),
                    Some(7),
                    Some(8),
                    Some(9),
                ]),
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]),
                i64_some_column(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                ]),
            ],
            vec![
                schema.column("f1").cloned().unwrap(),
                schema.column("f2").cloned().unwrap(),
                schema.column("f3").cloned().unwrap(),
                schema.column("f4").cloned().unwrap(),
            ],
        );

        let data = vec![
            HashMap::from([(1, data1)]),
            HashMap::from([(1, data2)]),
            HashMap::from([(1, data3)]),
        ];

        let expected_data = HashMap::from([(1 as SeriesId, vec![expected_data])]);

        let dir = "/tmp/test/compaction/2";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    fn generate_column_ts(min_ts: i64, max_ts: i64) -> Column {
        let mut col = Column::empty_with_cap(
            ColumnType::Time(TimeUnit::Nanosecond),
            (max_ts - min_ts + 1) as usize,
        )
        .unwrap();
        for i in min_ts..max_ts + 1 {
            col.push(Some(FieldVal::Integer(i)));
        }
        col
    }

    fn generate_column_i64(len: usize, none_range: Vec<(usize, usize)>) -> Column {
        let mut col = Column::empty_with_cap(ColumnType::Field(ValueType::Integer), len).unwrap();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                col.push(None);
            } else {
                col.push(Some(FieldVal::Integer(i as i64)));
            }
        }
        col
    }

    fn generate_column_u64(len: usize, none_range: Vec<(usize, usize)>) -> Column {
        let mut col = Column::empty_with_cap(ColumnType::Field(ValueType::Unsigned), len).unwrap();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                col.push(None);
            } else {
                col.push(Some(FieldVal::Unsigned(i as u64)));
            }
        }
        col
    }

    fn generate_column_f64(len: usize, none_range: Vec<(usize, usize)>) -> Column {
        let mut col = Column::empty_with_cap(ColumnType::Field(ValueType::Float), len).unwrap();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                col.push(None);
            } else {
                col.push(Some(FieldVal::Float(i as f64)));
            }
        }
        col
    }

    fn generate_column_bool(len: usize, none_range: Vec<(usize, usize)>) -> Column {
        let mut col = Column::empty_with_cap(ColumnType::Field(ValueType::Boolean), len).unwrap();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                col.push(None);
            } else {
                col.push(Some(FieldVal::Boolean(true)));
            }
        }
        col
    }

    #[tokio::test]
    async fn test_compaction_3() {
        let schema1 = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Unsigned),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Boolean),
                    Encoding::default(),
                ),
            ],
        );
        let mut schema2 = schema1.clone();
        schema2.add_column(TableColumn::new(
            4,
            "f4".to_string(),
            ColumnType::Field(ValueType::Float),
            Encoding::default(),
        ));
        schema2.schema_id += 1;

        let schema1 = Arc::new(schema1);
        let schema2 = Arc::new(schema2);
        let data_desc = [
            // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, Timestamp_end) ] )]
            (
                1_u64,
                vec![
                    DataBlock2::new(
                        schema1.clone(),
                        generate_column_ts(1, 1000),
                        schema1.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![]),
                        ],
                        schema1.fields(),
                    ),
                    DataBlock2::new(
                        schema1.clone(),
                        generate_column_ts(1001, 2000),
                        schema1.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(500, 999)]),
                            generate_column_bool(1000, vec![(500, 999)]),
                        ],
                        schema1.fields(),
                    ),
                    DataBlock2::new(
                        schema1.clone(),
                        generate_column_ts(2001, 2500),
                        schema1.time_column(),
                        vec![
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                        ],
                        schema1.fields(),
                    ),
                ],
            ),
            (
                2,
                vec![
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(1, 1000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(1001, 2000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![]),
                            generate_column_f64(1000, vec![(500, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(2001, 3000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(500, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(3001, 4000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(4001, 4500),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                            generate_column_f64(500, vec![(0, 499)]),
                        ],
                        schema2.fields(),
                    ),
                ],
            ),
            (
                3,
                vec![
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(1001, 2000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(2001, 3000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![]),
                            generate_column_f64(1000, vec![(500, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(3001, 4000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(500, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(4001, 5000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(5001, 6000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(6001, 6500),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                            generate_column_f64(500, vec![(0, 499)]),
                        ],
                        schema2.fields(),
                    ),
                ],
            ),
        ];
        let expected_data: Vec<DataBlock2> = vec![
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(1, 1000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![]),
                    generate_column_f64(1000, vec![]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(1001, 2000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![]),
                    generate_column_f64(1000, vec![]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(2001, 3000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![]),
                    generate_column_f64(1000, vec![(500, 999)]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(3001, 4000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![(500, 999)]),
                    generate_column_f64(1000, vec![(0, 999)]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(4001, 5000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![(0, 999)]),
                    generate_column_f64(1000, vec![(0, 999)]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(5001, 6000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![(0, 999)]),
                    generate_column_bool(1000, vec![(0, 999)]),
                    generate_column_f64(1000, vec![(0, 999)]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(6001, 6500),
                schema2.time_column(),
                vec![
                    generate_column_u64(500, vec![]),
                    generate_column_i64(500, vec![(0, 499)]),
                    generate_column_bool(500, vec![(0, 499)]),
                    generate_column_f64(500, vec![(0, 499)]),
                ],
                schema2.fields(),
            ),
        ];
        let expected_data = HashMap::from([(1 as SeriesId, expected_data)]);

        let dir = "/tmp/test/compaction/3";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !LocalFileSystem::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut column_files = Vec::new();
        for (tsm_sequence, args) in data_desc.into_iter() {
            let mut tsm_writer = Tsm2Writer::open(&dir, tsm_sequence, 0, false)
                .await
                .unwrap();
            for arg in args.into_iter() {
                tsm_writer
                    .write_datablock(1, SeriesKey::default(), arg)
                    .await
                    .unwrap();
            }
            tsm_writer.finish().await.unwrap();
            column_files.push(Arc::new(ColumnFile::new(
                tsm_sequence,
                2,
                TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
                tsm_writer.size() as u64,
                false,
                tsm_writer.path(),
            )));
        }

        let next_file_id = 4_u64;

        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, column_files);

        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_4() {
        let schema1 = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Unsigned),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Boolean),
                    Encoding::default(),
                ),
            ],
        );
        let mut schema2 = schema1.clone();
        schema2.add_column(TableColumn::new(
            4,
            "f4".to_string(),
            ColumnType::Field(ValueType::Float),
            Encoding::default(),
        ));
        schema2.schema_id += 1;

        let schema1 = Arc::new(schema1);
        let schema2 = Arc::new(schema2);
        let data_desc = [
            // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, Timestamp_end) ] )]
            (
                1_u64,
                vec![
                    DataBlock2::new(
                        schema1.clone(),
                        generate_column_ts(1, 1000),
                        schema1.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![]),
                        ],
                        schema1.fields(),
                    ),
                    DataBlock2::new(
                        schema1.clone(),
                        generate_column_ts(1001, 2000),
                        schema1.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(500, 999)]),
                            generate_column_bool(1000, vec![(500, 999)]),
                        ],
                        schema1.fields(),
                    ),
                    DataBlock2::new(
                        schema1.clone(),
                        generate_column_ts(2001, 2500),
                        schema1.time_column(),
                        vec![
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                        ],
                        schema1.fields(),
                    ),
                ],
            ),
            (
                2,
                vec![
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(1, 1000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(1001, 2000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![]),
                            generate_column_f64(1000, vec![(500, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(2001, 3000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(500, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(3001, 4000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(4001, 4500),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                            generate_column_f64(500, vec![(0, 499)]),
                        ],
                        schema2.fields(),
                    ),
                ],
            ),
            (
                3,
                vec![
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(1001, 2000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(2001, 3000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![]),
                            generate_column_f64(1000, vec![(500, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(3001, 4000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![(0, 999)]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(500, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(4001, 5000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(5001, 6000),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(1000, vec![]),
                            generate_column_i64(1000, vec![(0, 999)]),
                            generate_column_bool(1000, vec![(0, 999)]),
                            generate_column_f64(1000, vec![(0, 999)]),
                        ],
                        schema2.fields(),
                    ),
                    DataBlock2::new(
                        schema2.clone(),
                        generate_column_ts(6001, 6500),
                        schema2.time_column(),
                        vec![
                            generate_column_u64(500, vec![]),
                            generate_column_i64(500, vec![(0, 499)]),
                            generate_column_bool(500, vec![(0, 499)]),
                            generate_column_f64(500, vec![(0, 499)]),
                        ],
                        schema2.fields(),
                    ),
                ],
            ),
        ];
        let expected_data: Vec<DataBlock2> = vec![
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(1, 1000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![(0, 499)]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![]),
                    generate_column_f64(1000, vec![]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(1001, 2000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![]),
                    generate_column_f64(1000, vec![]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(2001, 3000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![(0, 699)]),
                    generate_column_f64(1000, vec![(500, 999)]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(3001, 4000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![(500, 999)]),
                    generate_column_f64(1000, vec![(0, 999)]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(4001, 5000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![]),
                    generate_column_bool(1000, vec![(0, 999)]),
                    generate_column_f64(1000, vec![(0, 999)]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(5001, 6000),
                schema2.time_column(),
                vec![
                    generate_column_u64(1000, vec![]),
                    generate_column_i64(1000, vec![(0, 999)]),
                    generate_column_bool(1000, vec![(0, 999)]),
                    generate_column_f64(1000, vec![(0, 999)]),
                ],
                schema2.fields(),
            ),
            DataBlock2::new(
                schema2.clone(),
                generate_column_ts(6001, 6500),
                schema2.time_column(),
                vec![
                    generate_column_u64(500, vec![]),
                    generate_column_i64(500, vec![(0, 499)]),
                    generate_column_bool(500, vec![(0, 499)]),
                    generate_column_f64(500, vec![(0, 499)]),
                ],
                schema2.fields(),
            ),
        ];
        let expected_data = HashMap::from([(1 as SeriesId, expected_data)]);

        let dir = "/tmp/test/compaction/4";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !LocalFileSystem::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut column_files = Vec::new();
        for (tsm_sequence, args) in data_desc.into_iter() {
            let mut tsm_writer = Tsm2Writer::open(&dir, tsm_sequence, 0, false)
                .await
                .unwrap();
            for arg in args.into_iter() {
                tsm_writer
                    .write_datablock(1, SeriesKey::default(), arg)
                    .await
                    .unwrap();
            }
            tsm_writer.finish().await.unwrap();
            let mut tsm_tombstone = TsmTombstone::open(&dir, tsm_sequence).await.unwrap();
            tsm_tombstone
                .add_range(&[(1, 1)], &TimeRange::new(0, 500))
                .await
                .unwrap();

            tsm_tombstone
                .add_range(&[(1, 3)], &TimeRange::new(2001, 2700))
                .await
                .unwrap();

            tsm_tombstone.flush().await.unwrap();
            column_files.push(Arc::new(ColumnFile::new(
                tsm_sequence,
                2,
                TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
                tsm_writer.size() as u64,
                false,
                tsm_writer.path(),
            )));
        }

        let next_file_id = 4_u64;

        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, column_files);

        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data).await;
    }
}
