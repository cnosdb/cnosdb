use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::FieldId;
use snafu::ResultExt;
use utils::BloomFilter;

use super::iterator::BufferedIterator;
use super::CompactTask;
use crate::compaction::metric::{self, CompactMetrics, FakeMetricStore, MetricStore};
use crate::compaction::CompactReq;
use crate::context::GlobalContext;
use crate::error::{self, Result};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tseries_family::TseriesFamily;
use crate::tsm::{
    self, BlockMeta, BlockMetaIterator, DataBlock, EncodedDataBlock, IndexIterator, IndexMeta,
    ReadTsmResult, TsmReader, TsmWriter, WriteTsmError, WriteTsmResult,
};
use crate::{ColumnFileId, Error, LevelId};

pub async fn run_compaction_job(
    request: CompactReq,
    ctx: Arc<GlobalContext>,
) -> Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    trace::info!(
        "Compaction({}): Running compaction job on {request}",
        request.compact_task
    );
    let compact_task = request.compact_task;

    let (delta_files, level_file) = request.split_delta_and_level_files();
    if delta_files.is_empty() {
        return Ok(None);
    }
    let out_time_range = request.out_time_range;

    // Collect l0-files that can be deleted after compaction.
    let mut l0_file_metas_will_delete: Vec<CompactMeta> = Vec::new();
    // Collect l0-files that partly deleted after compaction.
    let mut l0_file_metas_will_partly_delete: Vec<CompactMeta> = Vec::new();
    // Open l0-files and tsm-file to run compaction.
    let mut tsm_readers = match &level_file {
        None => Vec::with_capacity(delta_files.len()),
        Some(f) => {
            let mut tsm_readers = Vec::with_capacity(1 + delta_files.len());
            tsm_readers.push(request.version.get_tsm_reader(f.file_path()).await?);
            tsm_readers
        }
    };
    for file in delta_files {
        let l0_file_reader = request.version.get_tsm_reader(file.file_path()).await?;
        let compacted_all_excluded_time_range = l0_file_reader
            .add_tombstone_and_compact_to_tmp(out_time_range)
            .await?;
        if compacted_all_excluded_time_range.includes(file.time_range()) {
            // The tombstone includes all of the l0-file, so delete it.
            l0_file_metas_will_delete.push(CompactMeta::from(file.as_ref()));
        } else {
            // The tombstone includes partly of the l0_file and the excluded range close to the edge.
            l0_file_metas_will_partly_delete.push(CompactMeta::new_del_file_part(
                0,
                file.file_id(),
                out_time_range.min_ts,
                out_time_range.max_ts,
            ));
        }
        tsm_readers.push(l0_file_reader);
    }

    let max_block_size = TseriesFamily::MAX_DATA_BLOCK_SIZE as usize;
    let mut state = CompactState::new(tsm_readers, out_time_range, max_block_size);
    let mut writer_wrapper = WriterWrapper::new(&request, ctx.clone()).await?;

    let mut previous_merged_block = Option::<CompactingBlock>::None;
    let mut merging_blk_meta_groups = Vec::with_capacity(32);
    let mut merged_blks = Vec::with_capacity(32);

    let mut curr_fid: Option<FieldId> = None;

    let mut compact_metrics: Box<dyn MetricStore> = if request
        .version
        .borrowed_storage_opt()
        .collect_compaction_metrics
    {
        Box::new(CompactMetrics::default(compact_task))
    } else {
        Box::new(FakeMetricStore)
    };

    compact_metrics.begin_all();
    loop {
        compact_metrics.begin(metric::NEXT_FIELD);
        let fid = match state.next(&mut merging_blk_meta_groups).await {
            Some(fid) => fid,
            None => break,
        };
        compact_metrics.finish(metric::NEXT_FIELD);

        compact_metrics.begin(metric::MERGE_FIELD);

        for blk_meta_group in merging_blk_meta_groups.drain(..) {
            trace::trace!("merging meta group: {blk_meta_group}");
            if let Some(c_fid) = curr_fid {
                if c_fid != fid {
                    // Iteration of next field id, write previous merged block.
                    if let Some(blk) = previous_merged_block.take() {
                        // Write the small previous merged block.
                        trace::trace!(
                            "write the previous compacting block (fid={curr_fid:?}): {blk}"
                        );
                        compact_metrics.begin(metric::WRITE_BLOCK);
                        writer_wrapper.write(blk).await?;
                        compact_metrics.finish(metric::WRITE_BLOCK);
                    }
                }
            }
            curr_fid = Some(fid);

            compact_metrics.begin(metric::MERGE_BLOCK);
            blk_meta_group
                .merge_with_previous_block(
                    previous_merged_block.take(),
                    max_block_size,
                    &out_time_range,
                    &mut merged_blks,
                    &mut state.compacting_files,
                    &mut compact_metrics,
                )
                .await?;
            compact_metrics.finish(metric::MERGE_BLOCK);
            if merged_blks.is_empty() {
                continue;
            }
            if merged_blks.len() == 1 && merged_blks[0].len() < max_block_size {
                // The only one data block too small, try to extend the next compacting blocks.
                previous_merged_block = Some(merged_blks.remove(0));
                continue;
            }
            let last_blk_idx = merged_blks.len() - 1;
            for (i, blk) in merged_blks.drain(..).enumerate() {
                if i == last_blk_idx && blk.len() < max_block_size {
                    // The last data block too small, try to extend to
                    // the next compacting blocks (current field id).
                    trace::trace!("compacting block (fid={fid}) {blk} is too small, try to merge with the next compacting blocks");
                    previous_merged_block = Some(blk);
                    break;
                }
                trace::trace!("write compacting block(fid={fid}): {blk}");
                compact_metrics.begin(metric::WRITE_BLOCK);
                writer_wrapper.write(blk).await?;
                compact_metrics.finish(metric::WRITE_BLOCK);
            }
        }

        compact_metrics.finish(metric::MERGE_FIELD);
    }
    if let Some(blk) = previous_merged_block {
        trace::trace!("write the final compacting block(fid={curr_fid:?}): {blk}");
        compact_metrics.begin(metric::WRITE_BLOCK);
        writer_wrapper.write(blk).await?;
        compact_metrics.finish(metric::WRITE_BLOCK);
    }

    let (mut version_edit, file_metas) = writer_wrapper.close().await?;

    compact_metrics.finish_all();

    // Level 0 files that can be deleted after compaction.
    version_edit.del_files = l0_file_metas_will_delete;
    if let Some(f) = level_file {
        // Lvel 1-4 file that can be deleted after compaction.
        version_edit.del_files.push(f.as_ref().into());
    }
    // Level 0 files that partly deleted after compaction.
    version_edit.partly_del_files = l0_file_metas_will_partly_delete;

    trace::info!("Compaction({compact_task}): Compact finished, version edits: {version_edit:?}");
    Ok(Some((version_edit, file_metas)))
}

pub struct CompactState {
    tsm_readers: Vec<Arc<TsmReader>>,
    /// The TimeRange for delta files to partly compact with other files.
    out_time_range: TimeRange,
    /// The TimeRanges for delta files to partly compact with other files.
    out_time_ranges: Arc<TimeRanges>,
    /// Maximum values in generated CompactingBlock
    max_data_block_size: usize,

    compacting_files: Vec<CompactingFile>,
    tsm_reader_to_compacting_file_map: Vec<Option<usize>>,
    /// Temporarily stored index of `TsmReader` in self.tsm_readers,
    /// and `BlockMetaIterator` of current field_id.
    tmp_tsm_blk_meta_iters: Vec<(usize, BlockMetaIterator)>,
}

impl CompactState {
    pub fn new(
        tsm_readers: Vec<Arc<TsmReader>>,
        out_time_range: TimeRange,
        max_data_block_size: usize,
    ) -> Self {
        let out_time_ranges = Arc::new(TimeRanges::new(vec![out_time_range]));
        let tsm_readers_len = tsm_readers.len();
        let mut compacting_tsm_readers = Vec::with_capacity(tsm_readers_len);
        let mut compacting_files = Vec::with_capacity(tsm_readers_len);
        let mut compacting_tsm_file_idx = 0_usize;
        for tsm_reader in tsm_readers {
            if let Some(f) = CompactingFile::new(
                compacting_tsm_file_idx,
                tsm_reader.clone(),
                out_time_range,
                out_time_ranges.clone(),
            ) {
                compacting_tsm_readers.push(tsm_reader);
                compacting_files.push(f);
                compacting_tsm_file_idx += 1;
            }
        }

        let mut tsm_reader_to_compacting_file_map = vec![None::<_>; tsm_readers_len];
        for (i, f) in compacting_files.iter().enumerate() {
            tsm_reader_to_compacting_file_map[f.tsm_reader_index] = Some(i);
        }

        Self {
            tsm_readers: compacting_tsm_readers,
            out_time_range,
            out_time_ranges,
            max_data_block_size,

            compacting_files,
            tsm_reader_to_compacting_file_map,
            tmp_tsm_blk_meta_iters: Vec::with_capacity(compacting_tsm_file_idx),
        }
    }

    pub async fn next(
        &mut self,
        compacting_blk_meta_groups: &mut Vec<CompactingBlockMetaGroup>,
    ) -> Option<FieldId> {
        // For each tsm-file, get next index reader for current iteration field id
        if let Some(field_id) = self.next_field() {
            compacting_blk_meta_groups.clear();
            // Get all of block_metas of this field id, and group these block_metas.
            self.fill_compacting_block_meta_groups(field_id, compacting_blk_meta_groups);

            Some(field_id)
        } else {
            None
        }
    }
}

impl CompactState {
    /// Update tmp_tsm_blk_meta_iters for field id in next iteration.
    fn next_field(&mut self) -> Option<FieldId> {
        trace::trace!("===============================");

        self.tmp_tsm_blk_meta_iters.clear();
        let mut curr_fid: FieldId;

        loop {
            if let Some(f) = self.compacting_files.first() {
                if f.finished {
                    trace::trace!("all files are finished, mark finished",);
                    return None;
                }
                trace::trace!(
                    "selected new field {} from file {} as current field id",
                    f.field_id,
                    f.tsm_reader.file_id()
                );
                curr_fid = f.field_id
            } else {
                trace::trace!("no file to select, mark finished");
                return None;
            }

            let mut loop_tsm_reader_idx;
            for f in self.compacting_files.iter_mut() {
                loop_tsm_reader_idx = f.tsm_reader_index;
                if self.tsm_reader_to_compacting_file_map[f.tsm_reader_index].is_none() {
                    continue;
                }
                if curr_fid == f.field_id {
                    if let Some(idx_meta) = f.peek() {
                        trace::trace!(
                            "for tsm file @{loop_tsm_reader_idx}, got idx_meta((field_id: {}, field_type: {:?}, block_count: {}, time_range: {:?}), put the @{} block iterator with filter: {:?}",
                            idx_meta.field_id(),
                            idx_meta.field_type(),
                            idx_meta.block_count(),
                            idx_meta.time_range(),
                            self.tmp_tsm_blk_meta_iters.len(),
                            &self.out_time_range
                        );
                        self.tmp_tsm_blk_meta_iters.push((
                            loop_tsm_reader_idx,
                            idx_meta.block_iterator_opt(self.out_time_ranges.clone()),
                        ));
                        f.next();
                    } else {
                        // This tsm-file has been finished, do not push it back.
                        trace::trace!("file {loop_tsm_reader_idx} is finished.");
                        self.tsm_reader_to_compacting_file_map[loop_tsm_reader_idx] = None;
                    }
                } else {
                    // This tsm-file do not need to compact at this time, push it back.
                    break;
                }
            }
            self.compacting_files.sort();
            for (i, f) in self.compacting_files.iter().enumerate() {
                if self.tsm_reader_to_compacting_file_map[f.tsm_reader_index].is_some() {
                    self.tsm_reader_to_compacting_file_map[f.tsm_reader_index] = Some(i);
                }
            }

            if !self.tmp_tsm_blk_meta_iters.is_empty() {
                trace::trace!(
                    "selected {} blocks meta iterators",
                    self.tmp_tsm_blk_meta_iters.len()
                );
                break;
            } else {
                trace::trace!("iteration field_id {curr_fid} is finished, trying next field.");
                continue;
            }
        }

        Some(curr_fid)
    }

    /// Clear buffer vector and collect compacting `DataBlock`s into the buffer vector.
    fn fill_compacting_block_meta_groups(
        &mut self,
        field_id: FieldId,
        compacting_blk_meta_groups: &mut Vec<CompactingBlockMetaGroup>,
    ) {
        if self.tmp_tsm_blk_meta_iters.is_empty() {
            return;
        }

        let mut blk_metas: Vec<CompactingBlockMeta> =
            Vec::with_capacity(self.tmp_tsm_blk_meta_iters.len());
        // Get all block_meta, and check if it's tsm file has a related tombstone file.
        for (tsm_reader_idx, blk_iter) in self.tmp_tsm_blk_meta_iters.iter_mut() {
            if let Some(compacting_file_idx) =
                self.tsm_reader_to_compacting_file_map[*tsm_reader_idx]
            {
                for blk_meta in blk_iter.by_ref() {
                    blk_metas.push(CompactingBlockMeta::new(
                        *tsm_reader_idx,
                        compacting_file_idx,
                        blk_meta,
                    ));
                }
            }
        }
        if blk_metas.is_empty() {
            // Cannot load any data.
            return;
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

        for cbm_group in blk_meta_groups {
            if !cbm_group.is_empty() {
                compacting_blk_meta_groups.push(cbm_group);
            }
        }
        trace::trace!(
            "selected merging meta groups: {}",
            CompactingBlockMetaGroups(compacting_blk_meta_groups),
        );
    }
}

pub struct CompactingFile {
    pub tsm_reader_index: usize,
    pub tsm_reader: Arc<TsmReader>,
    out_time_range: TimeRange,
    out_time_ranges: Arc<TimeRanges>,
    pub index_iter: BufferedIterator<IndexIterator>,
    pub field_id: FieldId,
    pub finished: bool,

    cache_index_iter: BufferedIterator<IndexIterator>,
    max_cache_len: usize,
    cache_data: Vec<u8>,
    cached_tsm_off: u64,
}

impl CompactingFile {
    pub fn new(
        tsm_reader_index: usize,
        tsm_reader: Arc<TsmReader>,
        out_time_range: TimeRange,
        out_time_ranges: Arc<TimeRanges>,
    ) -> Option<Self> {
        let mut index_iter = BufferedIterator::new(tsm_reader.index_iterator());
        let cache_index_iter = index_iter.clone();
        index_iter
            .peek()
            .map(|idx_meta| idx_meta.field_id())
            .map(|field_id| Self {
                tsm_reader_index,
                tsm_reader,
                out_time_range,
                out_time_ranges,
                index_iter,
                field_id,
                finished: false,

                cache_index_iter,
                max_cache_len: 1024 * 1024 * 64,
                cache_data: Vec::with_capacity(1024 * 1024 * 64),
                cached_tsm_off: 0,
            })
    }

    /// Fetch the next index meta of tsm-file, field_id may be changed.
    fn next(&mut self) -> Option<&IndexMeta> {
        if let Some(idx_meta) = self.index_iter.next() {
            self.field_id = idx_meta.field_id();
            Some(idx_meta)
        } else {
            self.finished = true;
            None
        }
    }

    pub fn peek(&mut self) -> Option<&IndexMeta> {
        if let Some(idx_meta) = self.index_iter.peek() {
            Some(idx_meta)
        } else {
            self.finished = true;
            None
        }
    }

    pub async fn get_data_block(
        &mut self,
        block_meta: &BlockMeta,
    ) -> ReadTsmResult<Option<DataBlock>> {
        trace::trace!(
            "Getting data block from file:{}: off:{},len:{}, cache: off:{},len:{}",
            self.tsm_reader.file_id(),
            block_meta.offset(),
            block_meta.size(),
            self.cached_tsm_off,
            self.cache_data.len(),
        );
        let mut cached_data = self.read_cache(block_meta);
        if cached_data.is_none() {
            if !self.fill_cache(block_meta).await? {
                return self.tsm_reader.get_data_block(block_meta).await;
            }
            cached_data = self.read_cache(block_meta);
        }
        if let Some(data) = cached_data {
            return match tsm::decode_data_block(
                data,
                block_meta.field_type(),
                block_meta.val_off() - block_meta.offset(),
            ) {
                Ok(mut blk) => {
                    if self
                        .tsm_reader
                        .tombstone()
                        .is_data_block_all_excluded_by_tombstones(block_meta.field_id(), &blk)
                    {
                        return Ok(None);
                    }
                    self.tsm_reader
                        .tombstone()
                        .data_block_exclude_tombstones(block_meta.field_id(), &mut blk);
                    Ok(blk.intersection(&self.out_time_range))
                }
                Err(e) => Err(e),
            };
        } else {
            panic!(
                "Unexpected block_meta to read data_block: off:{},len:{} from cache: off:{},len:{}",
                block_meta.offset(),
                block_meta.size(),
                self.cached_tsm_off,
                self.cache_data.len(),
            );
        }
    }

    pub async fn get_raw_data(&mut self, block_meta: &BlockMeta) -> ReadTsmResult<Vec<u8>> {
        trace::trace!(
            "Getting raw block from file:{}: off:{},len:{}, cache: off:{},len:{}",
            self.tsm_reader.file_id(),
            block_meta.offset(),
            block_meta.size(),
            self.cached_tsm_off,
            self.cache_data.len(),
        );
        if let Some(data) = self.read_cache(block_meta) {
            return Ok(data.to_vec());
        }
        if self.fill_cache(block_meta).await? {
            if let Some(data) = self.read_cache(block_meta) {
                Ok(data.to_vec())
            } else {
                panic!(
                    "Unexpected block_meta to read data_block: off:{},len:{} from cache: off:{},len:{}",
                    block_meta.offset(),
                    block_meta.size(),
                    self.cached_tsm_off,
                    self.cache_data.len(),
                );
            }
        } else {
            self.tsm_reader
                .get_raw_data(block_meta.offset(), block_meta.size() as usize)
                .await
        }
    }

    pub fn has_tombstone(&self) -> bool {
        self.tsm_reader.has_tombstone()
    }

    async fn fill_cache(&mut self, block_meta: &BlockMeta) -> ReadTsmResult<bool> {
        trace::trace!(
            "Filling cache from file:{} for block_neta: fid:{},off:{},len:{}",
            self.tsm_reader.file_id(),
            block_meta.field_id(),
            block_meta.offset(),
            block_meta.size(),
        );
        let mut load_off_start = 0_u64;
        let mut load_len = 0_usize;

        let mut found_curr_field = false;
        'idx_iter: while let Some(idx) = self.cache_index_iter.peek() {
            let blk_meta_iter = idx.block_iterator_opt(self.out_time_ranges.clone());
            for blk in blk_meta_iter {
                if !found_curr_field {
                    if blk.field_id() == block_meta.field_id() {
                        found_curr_field = true;
                    } else {
                        // All of this IndexMeta has been already consumed.
                        self.cache_index_iter.next();
                        continue 'idx_iter;
                    }
                }

                if blk.offset() < block_meta.offset() {
                    continue;
                }

                let blk_off = blk.offset();
                let blk_size = blk.size();
                if blk_size > (self.max_cache_len - load_len) as u64 {
                    // Cache is full
                    break 'idx_iter;
                }

                if load_off_start == 0 {
                    load_off_start = blk_off;
                }
                load_len = (blk_off + blk_size - load_off_start) as usize;
            }

            self.cache_index_iter.next();
        }

        if load_len == 0 {
            return Ok(false);
        }
        self.cached_tsm_off = load_off_start;
        trace::trace!("Reading file: off:{},len:{}", load_off_start, load_len);
        self.cache_data = self
            .tsm_reader
            .get_raw_data(load_off_start, load_len)
            .await?;
        Ok(true)
    }

    fn read_cache(&self, block_meta: &BlockMeta) -> Option<&[u8]> {
        let blk_off = block_meta.offset();
        let blk_len = block_meta.size() as usize;
        if blk_off < self.cached_tsm_off
            || blk_off > (self.cached_tsm_off + self.cache_data.len() as u64)
            || blk_len > self.cache_data.len()
        {
            return None;
        }
        let cache_off = (blk_off - self.cached_tsm_off) as usize;
        Some(&self.cache_data[cache_off..cache_off + blk_len])
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
        // from: (false, 1), (true, 2), (false, 3), (true, 4)
        // to:   (false, 1), (false, 3), (true, 2), (true, 4)
        match self.finished.cmp(&other.finished) {
            std::cmp::Ordering::Equal => self.field_id.cmp(&other.field_id),
            others => others,
        }
    }
}

impl PartialOrd for CompactingFile {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Temporary compacting data block meta, holding the priority of reader,
/// the reader and the meta of data block.
#[derive(Clone)]
pub struct CompactingBlockMeta {
    pub tsm_reader_index: usize,
    pub compacting_file_index: usize,
    pub block_meta: BlockMeta,
}

impl CompactingBlockMeta {
    pub fn new(
        tsm_reader_index: usize,
        compacting_file_index: usize,
        block_meta: BlockMeta,
    ) -> Self {
        Self {
            tsm_reader_index,
            compacting_file_index,
            block_meta,
        }
    }

    pub fn time_range(&self) -> TimeRange {
        self.block_meta.time_range()
    }

    pub fn overlaps(&self, other: &Self) -> bool {
        self.block_meta.min_ts() <= other.block_meta.max_ts()
            && self.block_meta.max_ts() >= other.block_meta.min_ts()
    }

    pub fn overlaps_time_range(&self, time_range: &TimeRange) -> bool {
        self.block_meta.min_ts() <= time_range.max_ts
            && self.block_meta.max_ts() >= time_range.min_ts
    }

    pub fn included_in_time_range(&self, time_range: &TimeRange) -> bool {
        self.block_meta.min_ts() >= time_range.min_ts
            && self.block_meta.max_ts() <= time_range.max_ts
    }
}

impl Eq for CompactingBlockMeta {}

impl PartialEq for CompactingBlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.block_meta == other.block_meta
    }
}

impl PartialOrd for CompactingBlockMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompactingBlockMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.block_meta.cmp(&other.block_meta)
    }
}

impl std::fmt::Display for CompactingBlockMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {{ len: {}, min_ts: {}, max_ts: {} }}",
            self.block_meta.field_type(),
            self.block_meta.count(),
            self.block_meta.min_ts(),
            self.block_meta.max_ts(),
        )
    }
}

///
#[derive(Clone)]
pub struct CompactingBlockMetaGroup {
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

    pub async fn merge_with_previous_block(
        mut self,
        previous_block: Option<CompactingBlock>,
        max_block_size: usize,
        time_range: &TimeRange,
        compacting_blocks: &mut Vec<CompactingBlock>,
        compacting_files: &mut [CompactingFile],
        metrics: &mut Box<dyn MetricStore>,
    ) -> Result<()> {
        compacting_blocks.clear();
        if self.blk_metas.is_empty() {
            return Ok(());
        }
        self.blk_metas
            .sort_by(|a, b| a.tsm_reader_index.cmp(&b.tsm_reader_index));

        let mut merged_block = Option::<DataBlock>::None;
        if self.blk_metas.len() == 1
            && !compacting_files[self.blk_metas[0].compacting_file_index].has_tombstone()
            && self.blk_metas[0].included_in_time_range(time_range)
        {
            // Only one compacting block and has no tombstone, write as raw block.
            trace::trace!("only one compacting block without tombstone and time_range is entirely included by target level, handled as raw block");
            let head_meta = &self.blk_metas[0].block_meta;
            metrics.begin(metric::READ_BLOCK);
            let buf = compacting_files[self.blk_metas[0].compacting_file_index]
                .get_raw_data(&self.blk_metas[0].block_meta)
                .await?;
            metrics.finish(metric::READ_BLOCK);

            if head_meta.size() >= max_block_size as u64 {
                // Raw data block is full, so do not merge with the previous, directly return.
                if let Some(prev_compacting_block) = previous_block {
                    compacting_blocks.push(prev_compacting_block);
                }
                compacting_blocks.push(CompactingBlock::raw(
                    self.blk_metas[0].tsm_reader_index,
                    head_meta.clone(),
                    buf,
                ));

                return Ok(());
            }
            if let Some(prev_compacting_block) = previous_block {
                // Raw block is not full, so decode and merge with compacting_block.
                let decoded_raw_block = tsm::decode_data_block(
                    &buf,
                    head_meta.field_type(),
                    head_meta.val_off() - head_meta.offset(),
                )
                .context(error::ReadTsmSnafu)?;
                if let Some(mut data_block) = prev_compacting_block.decode_opt(time_range)? {
                    data_block.extend(decoded_raw_block);
                    merged_block = Some(data_block);
                }
            } else {
                // Raw block is not full, but nothing to merge with, directly return.
                compacting_blocks.push(CompactingBlock::raw(
                    self.blk_metas[0].tsm_reader_index,
                    head_meta.clone(),
                    buf,
                ));
                return Ok(());
            }
        } else {
            // One block with tombstone or multi compacting blocks, decode and merge these data block.
            trace::trace!(
                "there are {} compacting blocks, need to decode and merge",
                self.blk_metas.len()
            );

            let (mut head_block, mut head_i) = (Option::<DataBlock>::None, 0_usize);
            for (i, meta) in self.blk_metas.iter().enumerate() {
                let cf = &mut compacting_files[meta.compacting_file_index];
                metrics.begin(metric::READ_BLOCK);
                if let Some(blk) = cf.get_data_block(&meta.block_meta).await? {
                    metrics.finish(metric::READ_BLOCK);
                    head_block = Some(blk);
                    head_i = i;
                    break;
                }
            }
            if let Some(mut head_blk) = head_block.take() {
                // Merge with previous compacting block.
                if let Some(prev_compacting_block) = previous_block {
                    if let Some(mut data_block) = prev_compacting_block.decode_opt(time_range)? {
                        data_block.extend(head_blk);
                        head_blk = data_block;
                    }
                }

                trace::trace!("=== Resolving {} blocks", self.blk_metas.len() - head_i - 1);
                for blk_meta in self.blk_metas.iter_mut().skip(head_i + 1) {
                    // Merge decoded data block.
                    let cf = &mut compacting_files[blk_meta.compacting_file_index];
                    metrics.begin(metric::READ_BLOCK);
                    if let Some(blk) = cf.get_data_block(&blk_meta.block_meta).await? {
                        metrics.finish(metric::READ_BLOCK);
                        metrics.begin(metric::MERGE_BLOCK_BATCH);
                        head_blk = head_blk.merge(blk);
                        metrics.finish(metric::MERGE_BLOCK_BATCH);
                    }
                }
                trace::trace!("Compaction(delta): Finished task to merge data blocks");
                head_block = Some(head_blk);
            } else if let Some(prev_compacting_block) = previous_block {
                // Use the previous compacting block.
                if let Some(data_block) = prev_compacting_block.decode_opt(time_range)? {
                    head_block = Some(data_block);
                }
            }

            merged_block = head_block;
        }
        if let Some(blk) = merged_block {
            chunk_data_block_into_compacting_blocks(
                self.field_id,
                blk,
                max_block_size,
                compacting_blocks,
            )
        } else {
            Ok(())
        }
    }

    pub fn into_compacting_block_metas(self) -> Vec<CompactingBlockMeta> {
        self.blk_metas
    }

    pub fn is_empty(&self) -> bool {
        self.blk_metas.is_empty()
    }

    pub fn len(&self) -> usize {
        self.blk_metas.len()
    }
}

impl std::fmt::Display for CompactingBlockMetaGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{field_id: {}, blk_metas: [", self.field_id)?;
        if !self.blk_metas.is_empty() {
            write!(f, "{}", &self.blk_metas[0])?;
            for b in self.blk_metas.iter().skip(1) {
                write!(f, ", {}", b)?;
            }
        }
        write!(f, "]}}")
    }
}

struct CompactingBlockMetaGroups<'a>(&'a [CompactingBlockMetaGroup]);

impl<'a> std::fmt::Display for CompactingBlockMetaGroups<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.0.iter();
        if let Some(d) = iter.next() {
            write!(f, "{}", d)?;
            for d in iter {
                write!(f, ", {d}")?;
            }
        }
        Ok(())
    }
}

/// Temporary compacting data block.
/// - priority: When merging two (timestamp, value) pair with the same
/// timestamp from two data blocks, pair from data block with lower
/// priority will be discarded.
#[derive(Debug, PartialEq)]
pub enum CompactingBlock {
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

impl std::fmt::Display for CompactingBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            CompactingBlock::Raw {
                priority,
                meta,
                raw,
            } => {
                write!(
                    f,
                    "p: {priority}, f: {}, block: {}: {{ len: {}, min_ts: {}, max_ts: {}, raw_len: {} }}",
                    meta.field_id(),
                    meta.field_type(),
                    meta.count(),
                    meta.min_ts(),
                    meta.max_ts(),
                    raw.len(),
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

    /// Decode data block and return the intersected segment with out_time_range.
    pub fn decode_opt(self, out_time_range: &TimeRange) -> Result<Option<DataBlock>> {
        let data_block = match self {
            CompactingBlock::Decoded { data_block, .. } => data_block,
            CompactingBlock::Encoded { data_block, .. } => {
                data_block.decode().context(error::DecodeSnafu)?
            }
            CompactingBlock::Raw { raw, meta, .. } => {
                tsm::decode_data_block(&raw, meta.field_type(), meta.val_off() - meta.offset())
                    .context(error::ReadTsmSnafu)?
            }
        };
        match data_block.time_range() {
            Some((min_ts, _max_ts)) if min_ts < out_time_range.max_ts => {
                Ok(data_block.intersection(out_time_range))
            }
            _ => Ok(None),
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

pub struct CompactingBlocks<'a>(pub &'a [CompactingBlock]);

impl<'a> std::fmt::Display for CompactingBlocks<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut iter = self.0.iter();
        if let Some(d) = iter.next() {
            write!(f, "{d}")?;
            for d in iter {
                write!(f, ", {d}")?;
            }
        }
        Ok(())
    }
}

fn chunk_data_block_into_compacting_blocks(
    field_id: FieldId,
    data_block: DataBlock,
    max_block_size: usize,
    compacting_blocks: &mut Vec<CompactingBlock>,
) -> Result<()> {
    trace::trace!("Chunking data block {}", data_block);
    compacting_blocks.clear();
    if max_block_size == 0 || data_block.len() < max_block_size {
        // Data block elements less than max_block_size, do not encode it.
        // Try to merge with the next CompactingBlockMetaGroup.
        compacting_blocks.push(CompactingBlock::decoded(0, field_id, data_block));
    } else {
        // Data block is so big that split into multi CompactingBlock
        let len = data_block.len();
        let mut start = 0;
        let mut end = len.min(max_block_size);
        while start < len {
            // Encode decoded data blocks into chunks.
            let encoded_blk =
                EncodedDataBlock::encode(&data_block, start, end).map_err(|e| Error::WriteTsm {
                    source: WriteTsmError::Encode { source: e },
                })?;
            compacting_blocks.push(CompactingBlock::encoded(0, field_id, encoded_blk));

            start = end;
            end = len.min(start + max_block_size);
        }
    }
    trace::trace!(
        "Chunked compacting blocks: {}",
        CompactingBlocks(compacting_blocks)
    );

    Ok(())
}

struct WriterWrapper {
    // Init values.
    context: Arc<GlobalContext>,
    compact_task: CompactTask,
    out_level: LevelId,
    tsm_dir: PathBuf,

    // Temporary values.
    tsm_writer_full: bool,
    tsm_writer: Option<TsmWriter>,

    // Result values.
    version_edit: VersionEdit,
    file_metas: HashMap<ColumnFileId, Arc<BloomFilter>>,
}

impl WriterWrapper {
    pub async fn new(request: &CompactReq, context: Arc<GlobalContext>) -> Result<Self> {
        let ts_family_id = request.compact_task.ts_family_id();
        let storage_opt = request.version.borrowed_storage_opt();
        let tsm_dir = storage_opt.tsm_dir(request.version.borrowed_database(), ts_family_id);
        Ok(Self {
            context,
            compact_task: request.compact_task,
            out_level: request.out_level,
            tsm_dir,

            tsm_writer_full: false,
            tsm_writer: None,

            version_edit: VersionEdit::new(ts_family_id),
            file_metas: HashMap::new(),
        })
    }

    pub async fn close(mut self) -> Result<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)> {
        if let Some(mut tsm_writer) = self.tsm_writer {
            tsm_writer
                .write_index()
                .await
                .context(error::WriteTsmSnafu)?;
            tsm_writer.finish().await.context(error::WriteTsmSnafu)?;

            trace::info!(
                "Compaction({}): File: {} write finished (level: {}, {} B).",
                self.compact_task,
                tsm_writer.sequence(),
                self.out_level,
                tsm_writer.size()
            );

            let file_id = tsm_writer.sequence();
            let cm = CompactMeta {
                file_id,
                file_size: tsm_writer.size(),
                tsf_id: self.compact_task.ts_family_id(),
                level: self.out_level,
                min_ts: tsm_writer.min_ts(),
                max_ts: tsm_writer.max_ts(),
                high_seq: 0,
                low_seq: 0,
                is_delta: false,
            };
            self.version_edit.add_file(cm);
            let bloom_filter = tsm_writer.into_bloom_filter();
            self.file_metas.insert(file_id, Arc::new(bloom_filter));
        }

        Ok((self.version_edit, self.file_metas))
    }

    pub async fn writer(&mut self) -> Result<&mut TsmWriter> {
        if self.tsm_writer.is_none() {
            let file_id = self.context.file_id_next();
            let tsm_writer = tsm::new_tsm_writer(&self.tsm_dir, file_id, false, 0).await?;
            trace::info!(
                "Compaction({}): File: {file_id} been created (level: {}).",
                self.compact_task,
                self.out_level,
            );
            self.tsm_writer = Some(tsm_writer);
        }
        Ok(self.tsm_writer.as_mut().unwrap())
    }

    /// Write CompactingBlock to TsmWriter, fill file_metas and version_edit.
    pub async fn write(&mut self, blk: CompactingBlock) -> Result<usize> {
        let write_result: WriteTsmResult<usize> = match blk {
            CompactingBlock::Decoded {
                field_id,
                data_block,
                ..
            } => {
                if data_block.is_empty() {
                    return Ok(0);
                }
                self.writer()
                    .await?
                    .write_block(field_id, &data_block)
                    .await
            }
            CompactingBlock::Encoded {
                field_id,
                data_block,
                ..
            } => {
                if data_block.count == 0 {
                    return Ok(0);
                }
                self.writer()
                    .await?
                    .write_encoded_block(field_id, &data_block)
                    .await
            }
            CompactingBlock::Raw { meta, raw, .. } => {
                self.writer().await?.write_raw(&meta, &raw).await
            }
        };
        match write_result {
            Ok(size) => Ok(size),
            Err(WriteTsmError::WriteIO { source }) => {
                // TODO try re-run compaction on other time.
                trace::error!(
                    "Compaction({}): IO error when write tsm: {:?}",
                    self.compact_task,
                    source
                );
                Err(Error::IO { source })
            }
            Err(WriteTsmError::Encode { source }) => {
                // TODO try re-run compaction on other time.
                trace::error!(
                    "Compaction({}): Encoding error when write tsm: {:?}",
                    self.compact_task,
                    source
                );
                Err(Error::Encode { source })
            }
            Err(WriteTsmError::Finished { path }) => {
                trace::error!(
                    "Compaction({}): Trying write already finished tsm file: '{}'",
                    self.compact_task,
                    path.display()
                );
                Err(Error::WriteTsm {
                    source: WriteTsmError::Finished { path },
                })
            }
            Err(WriteTsmError::MaxFileSizeExceed { write_size, .. }) => {
                self.tsm_writer_full = true;
                Ok(write_size)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::path::Path;

    use cache::ShardedAsyncCache;
    use models::codec::Encoding;
    use models::{FieldId, Timestamp, ValueType};

    use super::*;
    use crate::compaction::test::{
        check_column_file, create_options, generate_data_block, write_data_block_desc,
        write_data_blocks_to_column_file, TsmSchema,
    };
    use crate::compaction::CompactTask;
    use crate::file_system::file_manager;
    use crate::tseries_family::{ColumnFile, LevelInfo, Version};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::test::write_to_tsm_tombstone_v2;
    use crate::tsm::TsmTombstoneCache;
    use crate::{file_utils, record_file, Options};

    #[tokio::test]
    #[ignore = "Manually test"]
    async fn test_compacting_file() {
        #[rustfmt::skip]
        let data = vec![
            vec![
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![114, 115, 116], enc: INT_BLOCK_ENCODING }]),
                (2, vec![DataBlock::U64 { ts: vec![4, 5, 6], val: vec![124, 125, 126], enc: INT_BLOCK_ENCODING }]),
            ],
        ];
        // #[rustfmt::skip]
        // let expected_data = HashMap::from([
        //     (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![211, 212, 213, 114, 115, 116, 317, 318, 319], enc: INT_BLOCK_ENCODING }]),
        // ]);

        let dir = "/tmp/test/delta_compaction/test_compacting_file";
        let (_, files) = write_data_blocks_to_column_file(&dir, data, 0).await;
        let tsm_reader = TsmReader::open(files[0].file_path()).await.unwrap();
        let tsm_reader = Arc::new(tsm_reader);
        let mut f =
            CompactingFile::new(1, tsm_reader, TimeRange::all(), Arc::new(TimeRanges::all()))
                .unwrap();
        let mut blk_meta_iter = f
            .peek()
            .unwrap()
            .block_iterator_opt(Arc::new(TimeRanges::all()));
        let blk = f
            .get_data_block(&blk_meta_iter.next().unwrap())
            .await
            .unwrap()
            .unwrap();
        println!("{blk}");

        let mut blk_meta_iter = f
            .next()
            .unwrap()
            .block_iterator_opt(Arc::new(TimeRanges::all()));
        let blk = f
            .get_data_block(&blk_meta_iter.next().unwrap())
            .await
            .unwrap()
            .unwrap();
        println!("{blk}");
    }

    #[test]
    fn test_chunk_merged_block() {
        let data_block = DataBlock::U64 {
            ts: vec![0, 1, 2, 10, 11, 12, 100, 101, 102, 1000, 1001, 1002],
            val: vec![0, 3, 6, 30, 33, 36, 300, 303, 306, 3000, 3003, 3006],
            enc: DataBlockEncoding::default(),
        };
        let field_id = 1;
        // Trying to chunk with no chunk size
        {
            let mut chunks = Vec::new();
            chunk_data_block_into_compacting_blocks(field_id, data_block.clone(), 0, &mut chunks)
                .unwrap();
            assert_eq!(chunks.len(), 1);
            assert_eq!(
                chunks[0],
                CompactingBlock::decoded(0, 1, data_block.clone())
            );
        }
        // Trying to chunk with too big chunk size
        {
            let mut chunks: Vec<_> = Vec::new();
            chunk_data_block_into_compacting_blocks(field_id, data_block.clone(), 100, &mut chunks)
                .unwrap();
            assert_eq!(chunks.len(), 1);
            assert_eq!(
                chunks[0],
                CompactingBlock::decoded(0, 1, data_block.clone())
            );
        }
        // Trying to chunk with chunk size that can divide data block exactly
        {
            let mut chunks = Vec::new();
            chunk_data_block_into_compacting_blocks(field_id, data_block.clone(), 4, &mut chunks)
                .unwrap();
            assert_eq!(chunks.len(), 3);
            assert_eq!(
                chunks[0],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 0, 4).unwrap()
                )
            );
            assert_eq!(
                chunks[1],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 4, 8).unwrap()
                )
            );
            assert_eq!(
                chunks[2],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 8, 12).unwrap()
                )
            );
        }
        // Trying to chunk with chunk size that cannot divide data block exactly
        {
            let mut chunks = Vec::new();
            chunk_data_block_into_compacting_blocks(field_id, data_block.clone(), 5, &mut chunks)
                .unwrap();
            assert_eq!(chunks.len(), 3);
            assert_eq!(
                chunks[0],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 0, 5).unwrap()
                )
            );
            assert_eq!(
                chunks[1],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 5, 10).unwrap()
                )
            );
            assert_eq!(
                chunks[2],
                CompactingBlock::encoded(
                    0,
                    field_id,
                    EncodedDataBlock::encode(&data_block, 10, 12).unwrap()
                )
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn prepare_delta_compaction(
        tenant_database: Arc<String>,
        opt: Arc<Options>,
        next_file_id: ColumnFileId,
        delta_files: Vec<Arc<ColumnFile>>,
        tsm_files: Vec<Arc<ColumnFile>>,
        out_time_range: TimeRange,
        out_level: LevelId,
        out_level_max_ts: Timestamp,
    ) -> (CompactReq, Arc<GlobalContext>) {
        let vnode_id = 1;
        let version = Arc::new(Version::new(
            vnode_id,
            tenant_database.clone(),
            opt.storage.clone(),
            1,
            LevelInfo::init_levels(tenant_database, 0, opt.storage.clone()),
            out_level_max_ts,
            Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
        ));
        let mut files = delta_files;
        files.extend_from_slice(&tsm_files);
        let compact_req = CompactReq {
            compact_task: CompactTask::Delta(vnode_id),
            version,
            files,
            in_level: 0,
            out_level,
            out_time_range,
        };
        let context = Arc::new(GlobalContext::new());
        context.set_file_id(next_file_id);

        (compact_req, context)
    }

    const INT_BLOCK_ENCODING: DataBlockEncoding =
        DataBlockEncoding::new(Encoding::Delta, Encoding::Delta);

    #[tokio::test]
    #[ignore = "Manually test"]
    async fn test_big_delta_compaction() {
        let dir = "/tmp/test/big_delta_compaction/1";
        let tenant_database = Arc::new("cnosdb.benchmark".to_string());
        let opt = create_options(dir.to_string(), 1);

        let delta_dir = opt.storage.delta_dir(&tenant_database, 1);
        #[rustfmt::skip]
        let (compact_req, kernel) = prepare_delta_compaction(
            tenant_database,
            opt,
            5,
            vec![
                Arc::new(ColumnFile::new(1187, 0, (1626052320072000000, 1626057359280000000).into(), 0, delta_dir.join("_001187.delta"))),
                Arc::new(ColumnFile::new(1199, 0, (1626057360072000000, 1626074639280000000).into(), 0, delta_dir.join("_001199.delta"))),
                Arc::new(ColumnFile::new(1210, 0, (1626069600072000000, 1626073919280000000).into(), 0, delta_dir.join("_001210.delta"))),
                Arc::new(ColumnFile::new(1225, 0, (1626074640072000000, 1626086159280000000).into(), 0, delta_dir.join("_001225.delta"))),
            ],
            vec![],
            (1626053759280000001, 1626086159280000000).into(),
            1,
            0,
        );
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        println!("{version_edit}");
    }

    #[tokio::test]
    async fn test_delta_compaction_1() {
        #[rustfmt::skip]
        let data = vec![
            vec![
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![114, 115, 116], enc: INT_BLOCK_ENCODING }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![124, 125, 126], enc: INT_BLOCK_ENCODING }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![134, 135, 136], enc: INT_BLOCK_ENCODING }]),
            ],
            vec![
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![211, 212, 213], enc: INT_BLOCK_ENCODING }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![221, 222, 223], enc: INT_BLOCK_ENCODING }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![231, 232, 233], enc: INT_BLOCK_ENCODING }]),
            ],
            vec![
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![317, 318, 319], enc: INT_BLOCK_ENCODING }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![327, 328, 329], enc: INT_BLOCK_ENCODING }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![337, 338, 339], enc: INT_BLOCK_ENCODING }]),
            ],
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![211, 212, 213, 114, 115, 116, 317, 318, 319], enc: INT_BLOCK_ENCODING }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![221, 222, 223, 124, 125, 126, 327, 328, 329], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![231, 232, 233, 134, 135, 136, 337, 338, 339], enc: INT_BLOCK_ENCODING }]),
        ]);

        let dir = "/tmp/test/delta_compaction/1";
        let _ = std::fs::remove_dir_all(dir);
        let tenant_database = Arc::new("cnosdb.dba".to_string());
        let opt = create_options(dir.to_string(), 1);
        let dir = opt.storage.tsm_dir(&tenant_database, 1);
        let max_level_ts = 9;

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, 0).await;
        let (compact_req, kernel) = prepare_delta_compaction(
            tenant_database,
            opt,
            next_file_id,
            files,
            vec![],
            (1, 9).into(),
            1,
            max_level_ts,
        );
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    /// Test compact with duplicate timestamp.
    #[tokio::test]
    async fn test_delta_compaction_2() {
        #[rustfmt::skip]
        let data = vec![
            vec![
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![111, 112, 113, 114], enc: INT_BLOCK_ENCODING }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![131, 132, 133, 134], enc: INT_BLOCK_ENCODING }]),
                (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![141, 142, 143], enc: INT_BLOCK_ENCODING }]),
            ],
            vec![
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![214, 215, 216], enc: INT_BLOCK_ENCODING }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![224, 225, 226], enc: INT_BLOCK_ENCODING }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7], val: vec![234, 235, 236, 237], enc: INT_BLOCK_ENCODING }]),
            ],
            vec![
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![317, 318, 319], enc: INT_BLOCK_ENCODING }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![327, 328, 329], enc: INT_BLOCK_ENCODING }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![337, 338, 339], enc: INT_BLOCK_ENCODING }]),
            ],
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![111, 112, 113, 214, 215, 216, 317, 318, 319], enc: INT_BLOCK_ENCODING }]),
            (2, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7, 8, 9], val: vec![224, 225, 226, 327, 328, 329], enc: INT_BLOCK_ENCODING }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![131, 132, 133, 234, 235, 236, 337, 338, 339], enc: INT_BLOCK_ENCODING }]),
            (4, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![141, 142, 143], enc: INT_BLOCK_ENCODING }]),
        ]);

        let dir = "/tmp/test/delta_compaction/2";
        let _ = std::fs::remove_dir_all(dir);
        let tenant_database = Arc::new("cnosdb.dba".to_string());
        let opt = create_options(dir.to_string(), 1);
        let dir = opt.storage.tsm_dir(&tenant_database, 1);
        let max_level_ts = 9;

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, 0).await;
        let (compact_req, kernel) = prepare_delta_compaction(
            tenant_database,
            opt,
            next_file_id,
            files,
            vec![],
            (1, 9).into(),
            1,
            max_level_ts,
        );
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    /// Test compact with tombstones.
    #[tokio::test]
    async fn test_delta_compaction_3() {
        #[rustfmt::skip]
        let data = vec![
            vec![
                (1, vec![DataBlock::I64 { ts: vec![1], val: vec![111], enc: INT_BLOCK_ENCODING }]),
            ],
            vec![
                (1, vec![DataBlock::I64 { ts: vec![2, 3, 4], val: vec![212, 213, 214], enc: INT_BLOCK_ENCODING }]),
            ],
            vec![
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![314, 315, 316], enc: INT_BLOCK_ENCODING }]),
            ],
            vec![
                (1, vec![DataBlock::I64 { ts: vec![8, 9], val: vec![418, 419], enc: INT_BLOCK_ENCODING }]),
            ],
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 8, 9], val: vec![111, 418, 419], enc: INT_BLOCK_ENCODING }]),
        ]);

        let dir = "/tmp/test/delta_compaction/3";
        let _ = std::fs::remove_dir_all(dir);
        let tenant_database = Arc::new("cnosdb.dba".to_string());
        let opt = create_options(dir.to_string(), 1);
        let dir = opt.storage.tsm_dir(&tenant_database, 1);
        let max_level_ts = 9;

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data, 0).await;
        for f in files.iter().take(2 + 1).skip(1) {
            let mut path = f.file_path().clone();
            path.set_extension("tombstone");
            write_to_tsm_tombstone_v2(path, &TsmTombstoneCache::with_all_excluded((2, 6).into()))
                .await;
        }
        let (compact_req, kernel) = prepare_delta_compaction(
            tenant_database,
            opt,
            next_file_id,
            files,
            vec![],
            (1, 9).into(),
            1,
            max_level_ts,
        );
        let out_level = compact_req.out_level;
        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data, out_level).await;
    }

    #[allow(clippy::too_many_arguments)]
    async fn test_delta_compaction(
        dir: &str,
        delta_files_desc: &[TsmSchema],
        tsm_files_desc: &[TsmSchema],
        out_time_range: TimeRange,
        max_ts: Timestamp,
        expected_data_desc: HashMap<FieldId, Vec<DataBlock>>,
        expected_data_level: LevelId,
        expected_delta_tombstone_all_excluded: HashMap<ColumnFileId, TimeRanges>,
    ) {
        let _ = std::fs::remove_dir_all(dir);
        let tenant_database = Arc::new("cnosdb.dba".to_string());
        let opt = create_options(dir.to_string(), 1);
        let tsm_dir = opt.storage.tsm_dir(&tenant_database, 1);
        if !file_manager::try_exists(&tsm_dir) {
            std::fs::create_dir_all(&tsm_dir).unwrap();
        }
        let delta_dir = opt.storage.delta_dir(&tenant_database, 1);
        if !file_manager::try_exists(&delta_dir) {
            std::fs::create_dir_all(&delta_dir).unwrap();
        }

        let delta_files = write_data_block_desc(&delta_dir, delta_files_desc, 0).await;
        let tsm_files = write_data_block_desc(&tsm_dir, tsm_files_desc, 2).await;
        let next_file_id = delta_files_desc
            .iter()
            .map(|(_file_id, blk_desc, _tomb_desc)| {
                blk_desc
                    .iter()
                    .map(|(_vtype, field_id, _min_ts, _max_ts)| *field_id)
                    .max()
                    .unwrap_or(1)
            })
            .max()
            .unwrap_or(1)
            + 1;
        let (mut compact_req, kernel) = prepare_delta_compaction(
            tenant_database,
            opt,
            next_file_id,
            delta_files,
            tsm_files,
            out_time_range,
            expected_data_level,
            max_ts,
        );
        compact_req.in_level = 0;
        compact_req.out_level = expected_data_level;

        let (version_edit, _) = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .expect("Delta compaction sucessfully generated some new files");

        check_column_file(
            tsm_dir,
            version_edit,
            expected_data_desc,
            expected_data_level,
        )
        .await;
        check_delta_file_tombstone(delta_dir, expected_delta_tombstone_all_excluded).await;
    }

    async fn check_delta_file_tombstone(
        dir: impl AsRef<Path>,
        all_excludes: HashMap<ColumnFileId, TimeRanges>,
    ) {
        for (file_id, time_ranges) in all_excludes {
            let tombstone_path = file_utils::make_tsm_tombstone_file_name(&dir, file_id);
            let tombstone_path = tsm::tombstone_compact_tmp_path(&tombstone_path).unwrap();
            let mut record_reader = record_file::Reader::open(tombstone_path).await.unwrap();
            let tombstone = TsmTombstoneCache::load_from(&mut record_reader, false)
                .await
                .unwrap();
            assert_eq!(tombstone.all_excluded(), &time_ranges);
        }
    }

    /// Test compaction on level-0 (delta compaction) with multi-field.
    #[tokio::test]
    async fn test_big_delta_compaction_1() {
        #[rustfmt::skip]
        let delta_files_desc: [TsmSchema; 3] = [
            // [( tsm_data:  tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)],
            //    tombstone: vec![(FieldId, MinTimestamp, MaxTimestamp)]
            // )]
            (2, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1, 1, 1000), (ValueType::Unsigned, 1, 1001, 2000),  (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000), (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000), (ValueType::Boolean, 3, 1001, 1500),
            ], vec![]),
            (3, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 3001, 4000), (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000), (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000), (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000), (ValueType::Float, 4, 1001, 1500),
            ], vec![]),
            (4, vec![
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
        // The target tsm file: [2001~5050]
        let max_level_ts = 5050;
        #[rustfmt::skip]
        let tsm_file_desc: TsmSchema = (1, vec![
            // 1, 2001~5050
            (ValueType::Unsigned, 1, 2001, 3000), (ValueType::Unsigned, 1, 4001, 5000),  (ValueType::Unsigned, 1, 5001, 5050),
            // 2, 2001~5000
            (ValueType::Integer, 2, 2001, 3000), (ValueType::Integer, 2, 4001, 5000),
            // 3, 3001~5000
            (ValueType::Boolean, 3, 3001, 4000), (ValueType::Boolean, 3, 4001, 5000),
            // 3, 2001~2500
            (ValueType::Float, 4, 2001, 2500),
        ], vec![]);

        let expected_data_target_level: HashMap<FieldId, Vec<DataBlock>> = HashMap::from([
            (
                // 1, 2001~5050
                1,
                vec![
                    generate_data_block(ValueType::Unsigned, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Unsigned, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Unsigned, vec![(4001, 5000)]),
                    generate_data_block(ValueType::Unsigned, vec![(5001, 5050)]),
                ],
            ),
            (
                // 2, 2001~5000
                2,
                vec![
                    generate_data_block(ValueType::Integer, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Integer, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Integer, vec![(4001, 5000)]),
                ],
            ),
            (
                // 3, 2001~3500
                3,
                vec![
                    generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Boolean, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Boolean, vec![(4001, 5000)]),
                ],
            ),
            (
                // 4, 2001~3500
                4,
                vec![generate_data_block(ValueType::Float, vec![(2001, 2500)])],
            ),
        ]);

        test_delta_compaction(
            "/tmp/test/delta_compaction/big_1",
            &delta_files_desc, // (1,2500), (1,4500), (1001,6500)
            &[tsm_file_desc],  // (2005,5050)
            (2001, 5050).into(),
            max_level_ts,
            expected_data_target_level,
            1,
            HashMap::from([
                (2, TimeRanges::new(vec![(2001, 5050).into()])),
                (3, TimeRanges::new(vec![(2001, 5050).into()])),
                (4, TimeRanges::new(vec![(2001, 5050).into()])),
            ]),
        )
        .await;
    }
}
