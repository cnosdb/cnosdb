use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fmt::{Display, Formatter};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use models::predicate::domain::TimeRange;
use models::{FieldId, Timestamp};
use snafu::ResultExt;
use trace::{error, info, trace};
use utils::BloomFilter;

use super::iterator::BufferedIterator;
use crate::compaction::CompactReq;
use crate::context::GlobalContext;
use crate::error::{self, Result};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tseries_family::TseriesFamily;
use crate::tsm::{
    self, BlockMeta, BlockMetaIterator, DataBlock, EncodedDataBlock, IndexIterator, IndexMeta,
    TsmReader, TsmWriter, WriteTsmError, WriteTsmResult,
};
use crate::{ColumnFileId, Error, LevelId, TseriesFamilyId};

/// Temporary compacting data block meta
#[derive(Clone)]
pub(crate) struct CompactingBlockMeta {
    reader_idx: usize,
    reader: Arc<TsmReader>,
    meta: BlockMeta,
}

impl PartialEq for CompactingBlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.reader.file_id() == other.reader.file_id() && self.meta == other.meta
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
        self.meta.cmp(&other.meta)
    }
}

impl Display for CompactingBlockMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {{ len: {}, min_ts: {}, max_ts: {} }}",
            self.meta.field_type(),
            self.meta.count(),
            self.meta.min_ts(),
            self.meta.max_ts(),
        )
    }
}

impl CompactingBlockMeta {
    pub fn new(tsm_reader_idx: usize, tsm_reader: Arc<TsmReader>, block_meta: BlockMeta) -> Self {
        Self {
            reader_idx: tsm_reader_idx,
            reader: tsm_reader,
            meta: block_meta,
        }
    }

    pub fn time_range(&self) -> TimeRange {
        self.meta.time_range()
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.meta.min_ts() <= other.meta.max_ts() && self.meta.max_ts() >= other.meta.min_ts()
    }

    pub fn overlaps_time_range(&self, time_range: &TimeRange) -> bool {
        self.meta.min_ts() <= time_range.max_ts && self.meta.max_ts() >= time_range.min_ts
    }

    pub async fn get_data_block(&self) -> Result<DataBlock> {
        self.reader
            .get_data_block(&self.meta)
            .await
            .context(error::ReadTsmSnafu)
    }

    pub async fn get_raw_data(&self, dst: &mut Vec<u8>) -> Result<usize> {
        self.reader
            .get_raw_data(&self.meta, dst)
            .await
            .context(error::ReadTsmSnafu)
    }

    pub fn has_tombstone(&self) -> bool {
        self.reader.has_tombstone()
    }
}

#[derive(Clone)]
pub(crate) struct CompactingBlockMetaGroup {
    field_id: FieldId,
    blk_metas: Vec<CompactingBlockMeta>,
    time_range: TimeRange,
}
impl CompactingBlockMetaGroup {
    pub fn new(field_id: FieldId, blk_meta: CompactingBlockMeta) -> Self {
        let time_range = blk_meta.time_range();
        Self {
            field_id,
            blk_metas: vec![blk_meta],
            time_range,
        }
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
            let mut buf_0 = Vec::with_capacity(meta_0.size() as usize);
            let data_len_0 = self.blk_metas[0].get_raw_data(&mut buf_0).await?;
            buf_0.truncate(data_len_0);

            if meta_0.size() >= max_block_size as u64 {
                // Raw data block is full, so do not merge with the previous, directly return.
                let mut merged_blks = Vec::new();
                if let Some(blk) = previous_block {
                    merged_blks.push(blk);
                }
                merged_blks.push(CompactingBlock::raw(
                    self.blk_metas[0].reader_idx,
                    meta_0.clone(),
                    buf_0,
                ));

                return Ok(merged_blks);
            } else if let Some(compacting_block) = previous_block {
                // Raw block is not full, so decode and merge with compacting_block.
                let decoded_raw_block = tsm::decode_data_block(
                    &buf_0,
                    meta_0.field_type(),
                    meta_0.val_off() - meta_0.offset(),
                )
                .context(error::ReadTsmSnafu)?;
                let mut data_block = compacting_block.decode()?;
                data_block.extend(decoded_raw_block);

                merged_block = data_block;
            } else {
                // Raw block is not full, but nothing to merge with, directly return.

                return Ok(vec![CompactingBlock::raw(
                    self.blk_metas[0].reader_idx,
                    meta_0.clone(),
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
            let mut head_block = head.get_data_block().await?;

            if let Some(compacting_block) = previous_block {
                let mut data_block = compacting_block.decode()?;
                data_block.extend(head_block);
                head_block = data_block;
            }

            for blk_meta in self.blk_metas[1..].iter_mut() {
                // Merge decoded data block.
                let blk_block = blk_meta.get_data_block().await?;
                head_block = head_block.merge(blk_block);
            }
            merged_block = head_block;
        }

        self.chunk_merged_block(merged_block, max_block_size)
    }

    fn chunk_merged_block(
        &self,
        data_block: DataBlock,
        max_block_size: usize,
    ) -> Result<Vec<CompactingBlock>> {
        let mut merged_blks = Vec::new();
        if max_block_size == 0 || data_block.len() < max_block_size {
            // Data block elements less than max_block_size, do not encode it.
            // Try to merge with the next CompactingBlockMetaGroup.
            merged_blks.push(CompactingBlock::decoded(0, self.field_id, data_block));
        } else {
            let len = data_block.len();
            let mut start = 0;
            let mut end = len.min(max_block_size);
            while start + end < len {
                // Encode decoded data blocks into chunks.
                let encoded_blk =
                    EncodedDataBlock::encode(&data_block, start, end).map_err(|e| {
                        Error::WriteTsm {
                            source: tsm::WriteTsmError::Encode { source: e },
                        }
                    })?;
                merged_blks.push(CompactingBlock::encoded(0, self.field_id, encoded_blk));

                start += end;
                end = len.min(start + max_block_size);
            }
            if start < len {
                // Encode the remaining decoded data blocks.
                let encoded_blk =
                    EncodedDataBlock::encode(&data_block, start, len).map_err(|e| {
                        Error::WriteTsm {
                            source: tsm::WriteTsmError::Encode { source: e },
                        }
                    })?;
                merged_blks.push(CompactingBlock::encoded(0, self.field_id, encoded_blk));
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
pub(crate) enum CompactingBlock {
    Decoded {
        priority: usize,
        field_id: FieldId,
        data_block: DataBlock,
    },
    Encoded {
        priority: usize,
        field_id: FieldId,
        data_block: EncodedDataBlock,
    },
    Raw {
        priority: usize,
        meta: BlockMeta,
        raw: Vec<u8>,
    },
}

impl Display for CompactingBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompactingBlock::Decoded {
                priority,
                field_id,
                data_block,
            } => {
                write!(f, "p: {priority}, f: {field_id}, block: {data_block}")
            }
            CompactingBlock::Encoded {
                priority,
                field_id,
                data_block,
            } => {
                write!(f, "p: {priority}, f: {field_id}, block: {data_block}")
            }
            CompactingBlock::Raw { priority, meta, .. } => {
                write!(
                    f,
                    "p: {priority}, f: {}, block: {}: {{ len: {}, min_ts: {}, max_ts: {} }}",
                    meta.field_id(),
                    meta.field_type(),
                    meta.count(),
                    meta.min_ts(),
                    meta.max_ts()
                )
            }
        }
    }
}

impl CompactingBlock {
    pub fn decoded(priority: usize, field_id: FieldId, data_block: DataBlock) -> CompactingBlock {
        Self::Decoded {
            priority,
            field_id,
            data_block,
        }
    }

    pub fn encoded(
        priority: usize,
        field_id: FieldId,
        data_block: EncodedDataBlock,
    ) -> CompactingBlock {
        Self::Encoded {
            priority,
            field_id,
            data_block,
        }
    }

    pub fn raw(priority: usize, meta: BlockMeta, raw: Vec<u8>) -> CompactingBlock {
        CompactingBlock::Raw {
            priority,
            meta,
            raw,
        }
    }

    pub fn decode(self) -> Result<DataBlock> {
        match self {
            CompactingBlock::Decoded { data_block, .. } => Ok(data_block),
            CompactingBlock::Encoded { data_block, .. } => {
                data_block.decode().context(error::DecodeSnafu)
            }
            CompactingBlock::Raw { raw, meta, .. } => {
                tsm::decode_data_block(&raw, meta.field_type(), meta.val_off() - meta.offset())
                    .context(error::ReadTsmSnafu)
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            CompactingBlock::Decoded { data_block, .. } => data_block.len(),
            CompactingBlock::Encoded { data_block, .. } => data_block.count as usize,
            CompactingBlock::Raw { meta, .. } => meta.count() as usize,
        }
    }
}

struct CompactingFile {
    i: usize,
    tsm_reader: Arc<TsmReader>,
    index_iter: BufferedIterator<IndexIterator>,
    field_id: Option<FieldId>,
}

impl CompactingFile {
    fn new(i: usize, tsm_reader: Arc<TsmReader>) -> Self {
        let mut index_iter = BufferedIterator::new(tsm_reader.index_iterator());
        let first_field_id = index_iter.peek().map(|i| i.field_id());
        Self {
            i,
            tsm_reader,
            index_iter,
            field_id: first_field_id,
        }
    }

    fn next(&mut self) -> Option<&IndexMeta> {
        let idx_meta = self.index_iter.next();
        idx_meta.map(|i| self.field_id.replace(i.field_id()));
        idx_meta
    }

    fn peek(&mut self) -> Option<&IndexMeta> {
        self.index_iter.peek()
    }
}

impl Eq for CompactingFile {}

impl PartialEq for CompactingFile {
    fn eq(&self, other: &Self) -> bool {
        self.tsm_reader.file_id() == other.tsm_reader.file_id() && self.field_id == other.field_id
    }
}

impl Ord for CompactingFile {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.field_id.cmp(&other.field_id).reverse()
    }
}

impl PartialOrd for CompactingFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct CompactIterator {
    tsm_readers: Vec<Arc<TsmReader>>,
    compacting_files: BinaryHeap<Pin<Box<CompactingFile>>>,
    /// Maximum values in generated CompactingBlock
    max_data_block_size: usize,
    /// Decode a data block even though it doesn't need to merge with others,
    /// return CompactingBlock::DataBlock rather than CompactingBlock::Raw .
    decode_non_overlap_blocks: bool,
    // /// Whether to enable `out_level_time_range`.
    // delta_compaction: bool,
    // /// The time range of data to be merged of level-0 data blocks.
    // /// The level-0 data that after the time range will write back to level-0.
    // out_level_time_range: TimeRange,
    tmp_tsm_blk_meta_iters: Vec<BlockMetaIterator>,
    /// Index to mark `Peekable<BlockMetaIterator>` in witch `TsmReader`,
    /// tmp_tsm_blks[i] is in self.tsm_readers[ tmp_tsm_blk_tsm_reader_idx[i] ]
    tmp_tsm_blk_tsm_reader_idx: Vec<usize>,
    /// When a TSM file at index i is ended, finished_idxes[i] is set to true.
    finished_readers: Vec<bool>,
    /// How many finished_idxes is set to true.
    finished_reader_cnt: usize,
    curr_fid: Option<FieldId>,

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
            curr_fid: Default::default(),
            merging_blk_meta_groups: Default::default(),
        }
    }
}

impl CompactIterator {
    pub(crate) fn new(
        tsm_readers: Vec<Arc<TsmReader>>,
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
    fn next_field_id(&mut self) {
        self.curr_fid = None;

        if let Some(f) = self.compacting_files.peek() {
            if self.curr_fid.is_none() {
                trace!(
                    "selected new field {:?} from file {} as current field id",
                    f.field_id,
                    f.tsm_reader.file_id()
                );
                self.curr_fid = f.field_id
            }
        } else {
            // TODO finished
            trace!("no file to select, mark finished");
            self.finished_reader_cnt += 1;
        }
        while let Some(mut f) = self.compacting_files.pop() {
            let loop_field_id = f.field_id;
            let loop_file_i = f.i;
            if self.curr_fid == loop_field_id {
                if let Some(idx_meta) = f.peek() {
                    self.tmp_tsm_blk_meta_iters.push(idx_meta.block_iterator());
                    self.tmp_tsm_blk_tsm_reader_idx.push(loop_file_i);
                    trace!("merging idx_meta: field_id: {}, field_type: {:?}, block_count: {}, time_range: {:?}",
                        idx_meta.field_id(),
                        idx_meta.field_type(),
                        idx_meta.block_count(),
                        idx_meta.time_range()
                    );
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
    }

    /// Collect merging `DataBlock`s.
    async fn fetch_merging_block_meta_groups(&mut self) -> bool {
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            return false;
        }
        let field_id = match self.curr_fid {
            Some(fid) => fid,
            None => return false,
        };

        let mut blk_metas: Vec<CompactingBlockMeta> =
            Vec::with_capacity(self.tmp_tsm_blk_meta_iters.len());
        // Get all block_meta, and check if it's tsm file has a related tombstone file.
        for (i, blk_iter) in self.tmp_tsm_blk_meta_iters.iter_mut().enumerate() {
            for blk_meta in blk_iter.by_ref() {
                let tsm_reader_idx = self.tmp_tsm_blk_tsm_reader_idx[i];
                let tsm_reader_ptr = self.tsm_readers[tsm_reader_idx].clone();
                blk_metas.push(CompactingBlockMeta::new(
                    tsm_reader_idx,
                    tsm_reader_ptr,
                    blk_meta,
                ));
            }
        }
        // Sort by field_id, min_ts and max_ts.
        blk_metas.sort();

        let mut blk_meta_groups: Vec<CompactingBlockMetaGroup> =
            Vec::with_capacity(blk_metas.len());
        for blk_meta in blk_metas {
            blk_meta_groups.push(CompactingBlockMetaGroup::new(field_id, blk_meta));
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
        trace!(
            "selected merging meta groups: {}",
            blk_meta_groups
                .iter()
                .map(|g| format!(
                    "[{}]",
                    g.blk_metas
                        .iter()
                        .map(|b| format!("{}", b))
                        .collect::<Vec<String>>()
                        .join(", ")
                ))
                .collect::<Vec<String>>()
                .join(", ")
        );

        self.merging_blk_meta_groups = blk_meta_groups;

        true
    }
}

impl CompactIterator {
    pub(crate) async fn next(&mut self) -> Option<CompactingBlockMetaGroup> {
        if let Some(g) = self.merging_blk_meta_groups.pop_front() {
            return Some(g);
        }

        // For each tsm-file, get next index reader for current iteration field id
        self.next_field_id();

        trace!(
            "selected {} blocks meta iterators",
            self.tmp_tsm_blk_meta_iters.len()
        );
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            trace!("iteration field_id {:?} is finished", self.curr_fid);
            self.curr_fid = None;
            return None;
        }

        // Get all of block_metas of this field id, and merge these blocks
        self.fetch_merging_block_meta_groups().await;

        if let Some(g) = self.merging_blk_meta_groups.pop_front() {
            return Some(g);
        }
        None
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
    let mut tsm_readers = Vec::new();
    for col_file in request.files.iter() {
        let tsm_reader = request.version.get_tsm_reader(col_file.file_path()).await?;
        tsm_readers.push(tsm_reader);
    }

    let max_block_size = TseriesFamily::MAX_DATA_BLOCK_SIZE as usize;
    let mut iter = CompactIterator::new(tsm_readers, max_block_size, false);
    let mut writer_wrapper = WriterWrapper::new(&request, kernel.clone());

    let mut previous_merged_block: Option<CompactingBlock> = None;
    let mut fid = iter.curr_fid;
    while let Some(blk_meta_group) = iter.next().await {
        trace!("===============================");
        if fid.is_some() && fid != iter.curr_fid {
            // Iteration of next field id, write previous merged block.
            if let Some(blk) = previous_merged_block.take() {
                // Write the small previous merged block.
                writer_wrapper.write(blk).await?;
            }
        }

        fid = iter.curr_fid;
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
            writer_wrapper.write(blk).await?;
        }
    }
    if let Some(blk) = previous_merged_block {
        writer_wrapper.write(blk).await?;
    }

    let (mut version_edit, file_metas) = writer_wrapper.close().await?;
    for file in request.files {
        version_edit.del_file(file.level(), file.file_id(), file.is_delta());
    }

    info!(
        "Compaction: Compact finished, version edits: {:?}",
        version_edit
    );
    Ok(Some((version_edit, file_metas)))
}

pub(crate) struct WriterWrapper {
    // Init values.
    delta_compaction: bool,
    ts_family_id: TseriesFamilyId,
    out_level: LevelId,
    out_level_max_ts: Timestamp,
    max_level_ts: Timestamp,
    max_file_size: u64,
    tsm_dir: PathBuf,
    delta_dir: PathBuf,
    context: Arc<GlobalContext>,

    // Temporary values.
    tsm_writer_full: bool,
    tsm_writer: Option<TsmWriter>,
    delta_writer_full: bool,
    delta_writer: Option<TsmWriter>,

    // Result values.
    version_edit: VersionEdit,
    file_metas: HashMap<ColumnFileId, Arc<BloomFilter>>,
}

impl WriterWrapper {
    pub fn new(request: &CompactReq, context: Arc<GlobalContext>) -> Self {
        Self {
            delta_compaction: request.in_level == 0,
            ts_family_id: request.ts_family_id,
            out_level: request.out_level,
            out_level_max_ts: request.time_range.max_ts,
            max_level_ts: request.version.max_level_ts,
            max_file_size: request
                .version
                .storage_opt
                .level_max_file_size(request.out_level),
            tsm_dir: request
                .storage_opt
                .tsm_dir(&request.database, request.ts_family_id),
            delta_dir: request
                .storage_opt
                .delta_dir(&request.database, request.ts_family_id),
            context,

            tsm_writer_full: false,
            tsm_writer: None,
            delta_writer_full: false,
            delta_writer: None,

            version_edit: VersionEdit::new(request.ts_family_id),
            file_metas: HashMap::new(),
        }
    }

    pub async fn close(mut self) -> Result<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)> {
        if let Some(ref mut w) = self.delta_writer {
            Self::close_writer(
                w,
                &mut self.file_metas,
                &mut self.version_edit,
                0,
                self.ts_family_id,
                self.max_level_ts,
            )
            .await?;
        }
        if let Some(ref mut w) = self.tsm_writer {
            Self::close_writer(
                w,
                &mut self.file_metas,
                &mut self.version_edit,
                self.out_level,
                self.ts_family_id,
                self.max_level_ts,
            )
            .await?;
        }

        Ok((self.version_edit, self.file_metas))
    }

    /// Write CompactingBlock to TsmWriter, fill file_metas and version_edit.
    pub async fn write(&mut self, blk: CompactingBlock) -> Result<()> {
        match blk {
            CompactingBlock::Decoded {
                priority: _priority,
                field_id,
                data_block,
            } => {
                if self.delta_compaction {
                    if let Some(tr) = data_block.time_range() {
                        if tr.0 < self.out_level_max_ts && tr.1 > self.out_level_max_ts {
                            // Split block.
                            let (tsm_blk, delta_blk) = data_block.split(self.out_level_max_ts);
                            if !tsm_blk.is_empty() {
                                self.write_tsm_data_block(field_id, &tsm_blk).await?;
                            }
                            if !delta_blk.is_empty() {
                                self.write_delta_data_block(field_id, &delta_blk).await?;
                            }
                        } else if tr.0 > self.out_level_max_ts {
                            self.write_delta_data_block(field_id, &data_block).await?;
                        } else {
                            self.write_tsm_data_block(field_id, &data_block).await?;
                        }
                    }
                } else {
                    self.write_tsm_data_block(field_id, &data_block).await?;
                }
            }
            CompactingBlock::Encoded {
                priority,
                field_id,
                data_block,
            } => {
                if self.delta_compaction {
                    if let Some(tr) = data_block.time_range {
                        if tr.min_ts < self.out_level_max_ts && tr.max_ts > self.out_level_max_ts {
                            // Split block.
                            let decoded_blk =
                                CompactingBlock::encoded(priority, field_id, data_block)
                                    .decode()?;
                            let (tsm_blk, delta_blk) = decoded_blk.split(self.out_level_max_ts);
                            if !tsm_blk.is_empty() {
                                self.write_tsm_data_block(field_id, &tsm_blk).await?;
                            }
                            if !delta_blk.is_empty() {
                                self.write_delta_data_block(field_id, &delta_blk).await?;
                            }
                        } else if tr.min_ts > self.out_level_max_ts {
                            self.write_delta_encoded_data_block(field_id, &data_block)
                                .await?;
                        } else {
                            self.write_tsm_encoded_data_block(field_id, &data_block)
                                .await?;
                        }
                    }
                } else {
                    self.write_tsm_encoded_data_block(field_id, &data_block)
                        .await?;
                }
            }
            CompactingBlock::Raw {
                priority,
                meta,
                raw,
            } => {
                if self.delta_compaction {
                    let tr = meta.time_range();
                    if tr.min_ts < self.out_level_max_ts && tr.max_ts > self.out_level_max_ts {
                        // Split block.
                        let field_id = meta.field_id();
                        let decoded_blk = CompactingBlock::raw(priority, meta, raw).decode()?;
                        let (tsm_blk, delta_blk) = decoded_blk.split(self.out_level_max_ts);
                        if !tsm_blk.is_empty() {
                            self.write_tsm_data_block(field_id, &tsm_blk).await?;
                        }
                        if !delta_blk.is_empty() {
                            self.write_delta_data_block(field_id, &delta_blk).await?;
                        }
                    } else if tr.min_ts > self.out_level_max_ts {
                        self.write_delta_raw_data_block(&meta, &raw).await?;
                    } else {
                        self.write_tsm_raw_data_block(&meta, &raw).await?;
                    }
                } else {
                    self.write_tsm_raw_data_block(&meta, &raw).await?;
                }
            }
        }

        Ok(())
    }

    pub async fn write_delta_data_block(
        &mut self,
        field_id: FieldId,
        data_block: &DataBlock,
    ) -> Result<usize> {
        self.write_block_inner(field_id, data_block, true).await
    }

    pub async fn write_tsm_data_block(
        &mut self,
        field_id: FieldId,
        data_block: &DataBlock,
    ) -> Result<usize> {
        self.write_block_inner(field_id, data_block, false).await
    }

    async fn write_block_inner(
        &mut self,
        field_id: FieldId,
        data_block: &DataBlock,
        is_delta: bool,
    ) -> Result<usize> {
        let write_ret = if is_delta {
            let write_ret = self
                .delta_writer_mut()
                .await?
                .write_block(field_id, data_block)
                .await;
            if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
                self.delta_writer_full = true;
                return Ok(write_size);
            }
            write_ret
        } else {
            let write_ret = self
                .tsm_writer_mut()
                .await?
                .write_block(field_id, data_block)
                .await;
            if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
                self.tsm_writer_full = true;
                return Ok(write_size);
            }
            write_ret
        };
        Self::warp_write_tsm_result(write_ret)
    }

    pub async fn write_delta_encoded_data_block(
        &mut self,
        field_id: FieldId,
        data_block: &EncodedDataBlock,
    ) -> Result<usize> {
        self.write_encoded_block_inner(field_id, data_block, true)
            .await
    }

    pub async fn write_tsm_encoded_data_block(
        &mut self,
        field_id: FieldId,
        data_block: &EncodedDataBlock,
    ) -> Result<usize> {
        self.write_encoded_block_inner(field_id, data_block, false)
            .await
    }

    async fn write_encoded_block_inner(
        &mut self,
        field_id: FieldId,
        data_block: &EncodedDataBlock,
        is_delta: bool,
    ) -> Result<usize> {
        let write_ret = if is_delta {
            let write_ret = self
                .delta_writer_mut()
                .await?
                .write_encoded_block(field_id, data_block)
                .await;
            if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
                self.delta_writer_full = true;
                return Ok(write_size);
            }
            write_ret
        } else {
            let write_ret = self
                .tsm_writer_mut()
                .await?
                .write_encoded_block(field_id, data_block)
                .await;
            if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
                self.tsm_writer_full = true;
                return Ok(write_size);
            }
            write_ret
        };
        Self::warp_write_tsm_result(write_ret)
    }

    pub async fn write_delta_raw_data_block(
        &mut self,
        block_meta: &BlockMeta,
        data_block: &[u8],
    ) -> Result<usize> {
        self.write_raw_inner(block_meta, data_block, true).await
    }

    pub async fn write_tsm_raw_data_block(
        &mut self,
        block_meta: &BlockMeta,
        data_block: &[u8],
    ) -> Result<usize> {
        self.write_raw_inner(block_meta, data_block, false).await
    }

    async fn write_raw_inner(
        &mut self,
        block_meta: &BlockMeta,
        data_block: &[u8],
        is_delta: bool,
    ) -> Result<usize> {
        let write_ret = if is_delta {
            let write_ret = self
                .delta_writer_mut()
                .await?
                .write_raw(block_meta, data_block)
                .await;
            if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
                self.delta_writer_full = true;
                return Ok(write_size);
            }
            write_ret
        } else {
            let write_ret = self
                .tsm_writer_mut()
                .await?
                .write_raw(block_meta, data_block)
                .await;
            if let Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) = write_ret {
                self.tsm_writer_full = true;
                return Ok(write_size);
            }
            write_ret
        };
        Self::warp_write_tsm_result(write_ret)
    }

    async fn tsm_writer_mut(&mut self) -> Result<&mut TsmWriter> {
        if self.tsm_writer_full {
            if let Some(ref mut w) = self.tsm_writer {
                Self::close_writer(
                    w,
                    &mut self.file_metas,
                    &mut self.version_edit,
                    self.out_level,
                    self.ts_family_id,
                    self.out_level_max_ts,
                )
                .await?;
            }
            self.new_writer(false).await
        } else {
            match self.tsm_writer {
                Some(ref mut w) => Ok(w),
                None => self.new_writer(false).await,
            }
        }
    }

    async fn delta_writer_mut(&mut self) -> Result<&mut TsmWriter> {
        if self.delta_writer_full {
            if let Some(ref mut w) = self.tsm_writer {
                Self::close_writer(
                    w,
                    &mut self.file_metas,
                    &mut self.version_edit,
                    0,
                    self.ts_family_id,
                    self.out_level_max_ts,
                )
                .await?;
            }
            self.new_writer(true).await
        } else {
            match self.delta_writer {
                Some(ref mut w) => Ok(w),
                None => self.new_writer(true).await,
            }
        }
    }

    async fn new_writer(&mut self, is_delta: bool) -> Result<&mut TsmWriter> {
        let writer_path = if is_delta {
            &self.delta_dir
        } else {
            &self.tsm_dir
        };
        let writer = tsm::new_tsm_writer(
            writer_path,
            self.context.file_id_next(),
            is_delta,
            self.max_file_size,
        )
        .await?;
        info!(
            "Compaction: File: {} been created (level: {}).",
            writer.sequence(),
            if is_delta { 0 } else { self.out_level }
        );

        if is_delta {
            self.delta_writer_full = false;
            Ok(self.delta_writer.insert(writer))
        } else {
            self.tsm_writer_full = false;
            Ok(self.tsm_writer.insert(writer))
        }
    }

    async fn close_writer(
        tsm_writer: &mut TsmWriter,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
        version_edit: &mut VersionEdit,
        out_level: LevelId,
        ts_family_id: TseriesFamilyId,
        max_level_ts: Timestamp,
    ) -> Result<()> {
        tsm_writer
            .write_index()
            .await
            .context(error::WriteTsmSnafu)?;
        tsm_writer.finish().await.context(error::WriteTsmSnafu)?;
        file_metas.insert(
            tsm_writer.sequence(),
            Arc::new(tsm_writer.bloom_filter_cloned()),
        );
        info!(
            "Compaction: File: {} write finished (level: {}, {} B).",
            tsm_writer.sequence(),
            out_level,
            tsm_writer.size()
        );

        let cm = new_compact_meta(tsm_writer, ts_family_id, out_level);
        version_edit.add_file(cm, max_level_ts);

        Ok(())
    }

    fn warp_write_tsm_result<T: Default>(write_result: WriteTsmResult<T>) -> Result<T> {
        match write_result {
            Ok(size) => Ok(size),
            Err(tsm::WriteTsmError::WriteIO { source }) => {
                // TODO try re-run compaction on other time.
                error!("Failed compaction: IO error when write tsm: {:?}", source);
                Err(Error::IO { source })
            }
            Err(tsm::WriteTsmError::Encode { source }) => {
                // TODO try re-run compaction on other time.
                error!(
                    "Failed compaction: encoding error when write tsm: {:?}",
                    source
                );
                Err(Error::Encode { source })
            }
            Err(tsm::WriteTsmError::Finished { path }) => {
                error!(
                    "Failed compaction: Trying write already finished tsm file: '{}'",
                    path.display()
                );
                Err(Error::WriteTsm {
                    source: tsm::WriteTsmError::Finished { path },
                })
            }
            Err(tsm::WriteTsmError::MaxFileSizeExceed { .. }) => {
                // This error should be already handled before, ignore.
                error!("WriteTsmError::MaxFileSizeExceed should be handled before.");
                Ok(T::default())
            }
        }
    }
}

fn new_compact_meta(
    tsm_writer: &TsmWriter,
    tsf_id: TseriesFamilyId,
    level: LevelId,
) -> CompactMeta {
    CompactMeta {
        file_id: tsm_writer.sequence(),
        file_size: tsm_writer.size(),
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

    use lru_cache::asynchronous::ShardedCache;
    use minivec::MiniVec;
    use models::predicate::domain::TimeRange;
    use models::{FieldId, Timestamp, ValueType};

    use crate::compaction::{run_compaction_job, CompactReq};
    use crate::context::GlobalContext;
    use crate::file_system::file_manager;
    use crate::kv_option::Options;
    use crate::summary::VersionEdit;
    use crate::tseries_family::{ColumnFile, LevelInfo, Version};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::{self, DataBlock, TsmReader, TsmTombstone};
    use crate::{file_utils, ColumnFileId, LevelId};

    pub(crate) async fn write_data_blocks_to_column_file(
        dir: impl AsRef<Path>,
        data: Vec<HashMap<FieldId, Vec<DataBlock>>>,
    ) -> (u64, Vec<Arc<ColumnFile>>) {
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut cfs = Vec::new();
        let mut file_seq = 0;
        for (i, d) in data.iter().enumerate() {
            file_seq = i as u64 + 1;
            let mut writer = tsm::new_tsm_writer(&dir, file_seq, false, 0).await.unwrap();
            for (fid, data_blks) in d.iter() {
                for blk in data_blks.iter() {
                    writer.write_block(*fid, blk).await.unwrap();
                }
            }
            writer.write_index().await.unwrap();
            writer.finish().await.unwrap();
            let mut cf = ColumnFile::new(
                file_seq,
                2,
                TimeRange::new(writer.min_ts(), writer.max_ts()),
                writer.size(),
                false,
                writer.path(),
            );
            cf.set_field_id_filter(Arc::new(writer.bloom_filter_cloned()));
            cfs.push(Arc::new(cf));
        }
        (file_seq + 1, cfs)
    }

    async fn read_data_blocks_from_column_file(
        path: impl AsRef<Path>,
    ) -> HashMap<FieldId, Vec<DataBlock>> {
        let tsm_reader = TsmReader::open(path).await.unwrap();
        let mut data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx in tsm_reader.index_iterator() {
            let field_id = idx.field_id();
            for blk_meta in idx.block_iterator() {
                let blk = tsm_reader.get_data_block(&blk_meta).await.unwrap();
                data.entry(field_id).or_default().push(blk);
            }
        }
        data
    }

    fn get_result_file_path(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        level: LevelId,
    ) -> PathBuf {
        if version_edit.has_file_id && !version_edit.add_files.is_empty() {
            if let Some(f) = version_edit
                .add_files
                .into_iter()
                .find(|f| f.level == level)
            {
                if level == 0 {
                    return file_utils::make_delta_file_name(dir, f.file_id);
                } else {
                    return file_utils::make_tsm_file_name(dir, f.file_id);
                }
            }
            panic!("VersionEdit::add_files doesn't contain any file matches level-{level}.");
        }
        panic!("VersionEdit::add_files is empty, no file to read.");
    }

    /// Compare DataBlocks in path with the expected_Data using assert_eq.
    async fn check_column_file(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        expected_data: HashMap<FieldId, Vec<DataBlock>>,
        expected_data_level: LevelId,
    ) {
        let path = get_result_file_path(dir, version_edit, expected_data_level);
        let data = read_data_blocks_from_column_file(path).await;
        let mut data_field_ids = data.keys().copied().collect::<Vec<_>>();
        data_field_ids.sort_unstable();
        let mut expected_data_field_ids = expected_data.keys().copied().collect::<Vec<_>>();
        expected_data_field_ids.sort_unstable();
        assert_eq!(data_field_ids, expected_data_field_ids);

        for (k, v) in expected_data.iter() {
            let data_blks = data.get(k).unwrap();
            if v.len() != data_blks.len() {
                let v_str = format_data_blocks(v.as_slice());
                let data_blks_str = format_data_blocks(data_blks.as_slice());
                panic!("fid={k}, v.len != data_blks.len: v={v_str}, data_blks={data_blks_str}")
            }
            assert_eq!(v.len(), data_blks.len());
            for (v_idx, v_blk) in v.iter().enumerate() {
                assert_eq!(data_blks.get(v_idx).unwrap(), v_blk);
            }
        }
    }

    pub(crate) fn create_options(base_dir: String) -> Arc<Options> {
        let mut config = config::get_config_for_test();
        config.storage.path = base_dir.clone();
        config.log.path = base_dir;
        Arc::new(Options::from(&config))
    }

    fn prepare_compaction(
        database: Arc<String>,
        opt: Arc<Options>,
        next_file_id: ColumnFileId,
        files: Vec<Arc<ColumnFile>>,
        max_level_ts: Timestamp,
    ) -> (CompactReq, Arc<GlobalContext>) {
        let version = Arc::new(Version::new(
            1,
            database.clone(),
            opt.storage.clone(),
            1,
            LevelInfo::init_levels(database.clone(), 0, opt.storage.clone()),
            max_level_ts,
            Arc::new(ShardedCache::with_capacity(1)),
        ));
        let compact_req = CompactReq {
            ts_family_id: 1,
            database,
            storage_opt: opt.storage.clone(),
            files,
            version,
            in_level: 1,
            out_level: 2,
            time_range: TimeRange::all(),
        };
        let context = Arc::new(GlobalContext::new());
        context.set_file_id(next_file_id);

        (compact_req, context)
    }

    fn format_data_blocks(data_blocks: &[DataBlock]) -> String {
        format!(
            "[{}]",
            data_blocks
                .iter()
                .map(|b| format!("{}", b))
                .collect::<Vec<String>>()
                .join(", ")
        )
    }

    /// Test compaction with ordered data.
    #[tokio::test]
    async fn test_compaction_fast() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction/0";
        let _ = std::fs::remove_dir_all(dir);
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        let max_level_ts = 9;

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compaction(database, opt, next_file_id, files, max_level_ts);
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    #[tokio::test]
    async fn test_compaction_1() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction/1";
        let _ = std::fs::remove_dir_all(dir);
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        let max_level_ts = 9;

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compaction(database, opt, next_file_id, files, max_level_ts);
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    /// Test compact with duplicate timestamp.
    #[tokio::test]
    async fn test_compaction_2() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 2, 3, 5], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 2, 3, 5], enc: DataBlockEncoding::default() }]),
                (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7], val: vec![4, 5, 6, 8], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7, 8, 9], val: vec![4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction/2";
        let _ = std::fs::remove_dir_all(dir);
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        let max_level_ts = 9;

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data).await;
        let (compact_req, kernel) =
            prepare_compaction(database, opt, next_file_id, files, max_level_ts);
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    /// Returns a generated `DataBlock` with default value and specified size, `DataBlock::ts`
    /// is all the time-ranges in data_descriptors.
    ///
    /// The default value is different for each ValueType:
    /// - Unsigned: 1
    /// - Integer: 1
    /// - String: "1"
    /// - Float: 1.0
    /// - Boolean: true
    /// - Unknown: will create a panic
    fn generate_data_block(value_type: ValueType, data_descriptors: Vec<(i64, i64)>) -> DataBlock {
        match value_type {
            ValueType::Unsigned => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(1000);
                let mut val_vec: Vec<u64> = Vec::with_capacity(1000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1_u64);
                    }
                }
                DataBlock::U64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Integer => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(1000);
                let mut val_vec: Vec<i64> = Vec::with_capacity(1000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1_i64);
                    }
                }
                DataBlock::I64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::String => {
                let word = MiniVec::from(&b"1"[..]);
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<MiniVec<u8>> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(word.clone());
                    }
                }
                DataBlock::Str {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Float => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<f64> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1.0);
                    }
                }
                DataBlock::F64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Boolean => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<bool> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(true);
                    }
                }
                DataBlock::Bool {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Unknown => {
                panic!("value type is Unknown")
            }
        }
    }

    type DataDesc = (
        ColumnFileId,
        Vec<(ValueType, FieldId, Timestamp, Timestamp)>,
        Vec<(FieldId, Timestamp, Timestamp)>,
    );

    #[allow(clippy::type_complexity)]
    async fn write_data_block_desc(
        dir: impl AsRef<Path>,
        data_desc: &[DataDesc],
    ) -> Vec<Arc<ColumnFile>> {
        let mut column_files = Vec::new();
        for (tsm_sequence, tsm_desc, tombstone_desc) in data_desc.iter() {
            let mut tsm_writer = tsm::new_tsm_writer(&dir, *tsm_sequence, false, 0)
                .await
                .unwrap();
            for &(val_type, fid, min_ts, max_ts) in tsm_desc.iter() {
                tsm_writer
                    .write_block(fid, &generate_data_block(val_type, vec![(min_ts, max_ts)]))
                    .await
                    .unwrap();
            }
            tsm_writer.write_index().await.unwrap();
            tsm_writer.finish().await.unwrap();
            let mut tsm_tombstone = TsmTombstone::open(&dir, *tsm_sequence).await.unwrap();
            for (fid, min_ts, max_ts) in tombstone_desc.iter() {
                tsm_tombstone
                    .add_range(&[*fid][..], &TimeRange::new(*min_ts, *max_ts))
                    .await
                    .unwrap();
            }

            tsm_tombstone.flush().await.unwrap();
            column_files.push(Arc::new(ColumnFile::new(
                *tsm_sequence,
                2,
                TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
                tsm_writer.size(),
                false,
                tsm_writer.path(),
            )));
        }

        column_files
    }

    /// Test compaction without tombstones.
    #[tokio::test]
    async fn test_compaction_3() {
        #[rustfmt::skip]
        let data_desc: [DataDesc; 3] = [
            // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, Timestamp_end) ] )]
            (1_u64, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1_u64, 1_i64, 1000_i64),
                (ValueType::Unsigned, 1, 1001, 2000),
                (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000),
                (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000),
                (ValueType::Boolean, 3, 1001, 1500),
            ], vec![]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000),
                (ValueType::Unsigned, 1, 3001, 4000),
                (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000),
                (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000),
                (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000),
                (ValueType::Float, 4, 1001, 1500),
            ], vec![]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 5000),
                (ValueType::Unsigned, 1, 5001, 6000),
                (ValueType::Unsigned, 1, 6001, 6500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 4000),
                (ValueType::Integer, 2, 4001, 5000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 3000),
                (ValueType::Boolean, 3, 3001, 3500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 2000),
                (ValueType::Float, 4, 2001, 2500),
            ], vec![]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(1, 1000)]),
                    generate_data_block(ValueType::Unsigned, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Unsigned, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Unsigned, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Unsigned, vec![(4001, 5000)]),
                    generate_data_block(ValueType::Unsigned, vec![(5001, 6000)]),
                    generate_data_block(ValueType::Unsigned, vec![(6001, 6500)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                    generate_data_block(ValueType::Integer, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Integer, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Integer, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Integer, vec![(4001, 5000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                    generate_data_block(ValueType::Boolean, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Boolean, vec![(3001, 3500)]),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, vec![(1, 1000)]),
                    generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Float, vec![(2001, 2500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/3";
        let _ = std::fs::remove_dir_all(dir);
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let max_level_ts = 6500;

        let column_files = write_data_block_desc(&dir, &data_desc).await;
        let next_file_id = 4_u64;

        let (compact_req, kernel) =
            prepare_compaction(database, opt, next_file_id, column_files, max_level_ts);
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    /// Test compaction with tombstones
    #[tokio::test]
    async fn test_compaction_4() {
        #[rustfmt::skip]
        let data_desc: [DataDesc; 3] = [
            // [( tsm_data:  tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)],
            //    tombstone: vec![(FieldId, MinTimestamp, MaxTimestamp)]
            // )]
            (1, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1, 1, 1000), (ValueType::Unsigned, 1, 1001, 2000), (ValueType::Unsigned, 1, 2001, 2500),
            ], vec![(1, 1, 2), (1, 2001, 2100)]),
            (2, vec![
                // 1, 2001~4500
                // 2101~3100, 3101~4100, 4101~4499
                (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
            ], vec![(1, 2001, 2100), (1, 4500, 4501)]),
            (3, vec![
                // 1, 4001~6500
                // 4001~4499, 4502~5501, 5502~6500
                (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
            ], vec![(1, 4500, 4501)]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(3, 1002)]),
                    generate_data_block(ValueType::Unsigned, vec![(1003, 2000), (2101, 2102)]),
                    generate_data_block(ValueType::Unsigned, vec![(2103, 3102)]),
                    generate_data_block(ValueType::Unsigned, vec![(3103, 4102)]),
                    generate_data_block(ValueType::Unsigned, vec![(4103, 4499), (4502, 5104)]),
                    generate_data_block(ValueType::Unsigned, vec![(5105, 6104)]),
                    generate_data_block(ValueType::Unsigned, vec![(6105, 6500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/4";
        let _ = std::fs::remove_dir_all(dir);
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let max_level_ts = 6500;

        let column_files = write_data_block_desc(&dir, &data_desc).await;
        let next_file_id = 4_u64;
        let (compact_req, kernel) =
            prepare_compaction(database, opt, next_file_id, column_files, max_level_ts);
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    /// Test compaction with multi-field and tombstones.
    #[tokio::test]
    async fn test_compaction_5() {
        #[rustfmt::skip]
        let data_desc: [DataDesc; 3] = [
            // [( tsm_data:  tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)],
            //    tombstone: vec![(FieldId, MinTimestamp, MaxTimestamp)]
            // )]
            (1, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1, 1, 1000), (ValueType::Unsigned, 1, 1001, 2000),  (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000), (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000), (ValueType::Boolean, 3, 1001, 1500),
            ], vec![
                (1, 1, 2), (1, 2001, 2100),
                (2, 1001, 1002),
                (3, 1499, 1500),
            ]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000), (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000), (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000), (ValueType::Float, 4, 1001, 1500),
            ], vec![
                (1, 2001, 2100), (1, 4500, 4501),
                (2, 1001, 1002), (2, 2501, 2502),
                (3, 1499, 1500),
            ]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 4000), (ValueType::Integer, 2, 4001, 5000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 3000), (ValueType::Boolean, 3, 3001, 3500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 2000), (ValueType::Float, 4, 2001, 2500),
            ], vec![
                (1, 4500, 4501),
                (2, 4001, 4002),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(3, 1002)]),
                    generate_data_block(ValueType::Unsigned, vec![(1003, 2000), (2101, 2102)]),
                    generate_data_block(ValueType::Unsigned, vec![(2103, 3102)]),
                    generate_data_block(ValueType::Unsigned, vec![(3103, 4102)]),
                    generate_data_block(ValueType::Unsigned, vec![(4103, 4499), (4502, 5104)]),
                    generate_data_block(ValueType::Unsigned, vec![(5105, 6104)]),
                    generate_data_block(ValueType::Unsigned, vec![(6105, 6500)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                    generate_data_block(ValueType::Integer, vec![(1003, 2002)]),
                    generate_data_block(ValueType::Integer, vec![(2003, 2500), (2503, 3004)]),
                    generate_data_block(ValueType::Integer, vec![(3005, 4000), (4003, 4006)]),
                    generate_data_block(ValueType::Integer, vec![(4007, 5000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                    generate_data_block(ValueType::Boolean, vec![(1001, 1498), (1501, 2002)]),
                    generate_data_block(ValueType::Boolean, vec![(2003, 3002)]),
                    generate_data_block(ValueType::Boolean, vec![(3003, 3500)]),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, vec![(1, 1000)]),
                    generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Float, vec![(2001, 2500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/5";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let max_level_ts = 6500;

        let column_files = write_data_block_desc(&dir, &data_desc).await;
        let next_file_id = 4_u64;
        let (compact_req, kernel) =
            prepare_compaction(database, opt, next_file_id, column_files, max_level_ts);
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    /// Test compaction on level-0 (delta compaction) with multi-field.
    #[tokio::test]
    async fn test_compaction_6() {
        #[rustfmt::skip]
        let data_desc: [DataDesc; 3] = [
            // [( tsm_data:  tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)],
            //    tombstone: vec![(FieldId, MinTimestamp, MaxTimestamp)]
            // )]
            (1, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1, 1, 1000), (ValueType::Unsigned, 1, 1001, 2000),  (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000), (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000), (ValueType::Boolean, 3, 1001, 1500),
            ], vec![]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000), (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000), (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000), (ValueType::Float, 4, 1001, 1500),
            ], vec![]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 5000), (ValueType::Unsigned, 1, 5001, 6000), (ValueType::Unsigned, 1, 6001, 6500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 4000), (ValueType::Integer, 2, 4001, 5000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 3000), (ValueType::Boolean, 3, 3001, 3500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 2000), (ValueType::Float, 4, 2001, 2500),
            ], vec![]),
        ];
        #[rustfmt::skip]
        let expected_data_target_level: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(1, 1000)]),
                    generate_data_block(ValueType::Unsigned, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Unsigned, vec![(2001, 3000)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                    generate_data_block(ValueType::Integer, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Integer, vec![(2001, 3000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                    generate_data_block(ValueType::Boolean, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, vec![(1, 1000)]),
                    generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Float, vec![(2001, 2500)]),
                ]),
            ]
        );
        #[rustfmt::skip]
        let expected_data_delta_level: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Unsigned, vec![(4001, 5000)]),
                    generate_data_block(ValueType::Unsigned, vec![(5001, 6000)]),
                    generate_data_block(ValueType::Unsigned, vec![(6001, 6500)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Integer, vec![(4001, 5000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(3001, 3500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/6";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let tsm_dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&tsm_dir) {
            std::fs::create_dir_all(&tsm_dir).unwrap();
        }
        let delta_dir = opt.storage.delta_dir(&database, 1);
        if !file_manager::try_exists(&delta_dir) {
            std::fs::create_dir_all(&delta_dir).unwrap();
        }
        let max_level_ts = 9000;

        let column_files = write_data_block_desc(&delta_dir, &data_desc).await;
        let next_file_id = 4_u64;
        let (mut compact_req, kernel) =
            prepare_compaction(database, opt, next_file_id, column_files, max_level_ts);
        compact_req.in_level = 0;
        compact_req.out_level = 2;
        compact_req.time_range = TimeRange::new(1, 3000);

        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(
            &delta_dir,
            version_edit.clone(),
            expected_data_delta_level,
            0,
        )
        .await;
        check_column_file(tsm_dir, version_edit, expected_data_target_level, 2).await;
    }
}
