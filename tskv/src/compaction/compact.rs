#[cfg(test)]
mod compact_test;
#[cfg(test)]
mod delta_compact_test;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::FieldId;
use snafu::ResultExt;
use utils::BloomFilter;

use super::iterator::BufferedIterator;
use super::metric::MetricStore;
use super::CompactTask;
use crate::compaction::metric::{CompactMetrics, FakeMetricStore};
use crate::compaction::{metric, CompactReq};
use crate::context::GlobalContext;
use crate::error::{self, Result};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tseries_family::TseriesFamily;
use crate::tsm::{
    self, BlockMeta, BlockMetaIterator, DataBlock, EncodedDataBlock, IndexIterator, IndexMeta,
    ReadTsmResult, TsmReader, TsmVersion, TsmWriter, WriteTsmError, WriteTsmResult,
};
use crate::{ColumnFileId, Error, LevelId};

pub async fn run_compaction_job(
    request: CompactReq,
    ctx: Arc<GlobalContext>,
) -> Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    trace::info!(
        "Compaction: Running compaction job on {}",
        request.compact_task
    );

    if request.files.is_empty() {
        // Nothing to compact
        return Ok(None);
    }
    let compact_task = request.compact_task;

    // ALl tsm-files that can be deleted after compaction.
    let mut tsm_file_metas_will_delete: Vec<CompactMeta> = Vec::new();
    // Buffers all tsm-files and it's indexes for this compaction
    let mut tsm_readers = Vec::new();
    for file in request.files.iter() {
        tsm_file_metas_will_delete.push(CompactMeta::from(file.as_ref()));
        let tsm_reader = request.version.get_tsm_reader(file.file_path()).await?;
        tsm_readers.push(tsm_reader);
    }

    let (mut version_edit, file_metas) =
        compact_files(request, ctx, tsm_readers, TimeRange::all()).await?;

    // Level 0 files that can be deleted after compaction.
    version_edit.del_files = tsm_file_metas_will_delete;

    trace::info!("Compaction({compact_task}): Compact finished, version edits: {version_edit:?}");
    Ok(Some((version_edit, file_metas)))
}

pub async fn run_delta_compaction_job(
    request: CompactReq,
    ctx: Arc<GlobalContext>,
) -> Result<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    trace::info!(
        "Compaction({}): Running compaction job on {request}",
        request.compact_task
    );

    let (delta_files, level_file) = request.split_delta_and_level_files();
    if delta_files.is_empty() {
        return Ok(None);
    }
    let compact_task = request.compact_task;
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

    let (mut version_edit, file_metas) =
        compact_files(request, ctx, tsm_readers, out_time_range).await?;

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

async fn compact_files(
    request: CompactReq,
    ctx: Arc<GlobalContext>,
    tsm_readers: Vec<Arc<TsmReader>>,
    out_time_range: TimeRange,
) -> Result<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)> {
    let storage_opt = request.version.borrowed_storage_opt();
    let max_block_size = TseriesFamily::MAX_DATA_BLOCK_SIZE as usize;
    let mut state = CompactState::new(
        tsm_readers,
        out_time_range,
        storage_opt.compact_file_cache_size as usize,
        max_block_size,
        false,
    );
    let mut writer_wrapper = WriterWrapper::new(&request, ctx.clone()).await?;

    let mut previous_merged_block = Option::<CompactingBlock>::None;
    let mut merging_blk_meta_groups = Vec::with_capacity(32);
    let mut merged_blks = Vec::with_capacity(32);

    let mut curr_fid: Option<FieldId> = None;

    let mut compact_metrics: Box<dyn MetricStore> = if storage_opt.collect_compaction_metrics {
        Box::new(CompactMetrics::default(request.compact_task))
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

    let (version_edit, file_metas) = writer_wrapper.close().await?;

    compact_metrics.finish_all();

    Ok((version_edit, file_metas))
}

pub struct CompactState {
    tsm_readers: Vec<Arc<TsmReader>>,
    /// The TimeRange for delta files to partly compact with other files.
    out_time_range: Option<TimeRange>,
    /// The TimeRanges for delta files to partly compact with other files.
    out_time_ranges: Option<Arc<TimeRanges>>,
    /// Maximum values in generated CompactingBlock
    max_data_block_size: usize,
    /// Decode a data block even though it doesn't need to merge with others,
    /// return CompactingBlock::DataBlock rather than CompactingBlock::Raw .
    decode_non_overlap_blocks: bool,

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
        file_cache_size: usize,
        max_data_block_size: usize,
        decode_non_overlap_blocks: bool,
    ) -> Self {
        let out_time_ranges = if out_time_range.is_boundless() {
            None
        } else {
            Some(Arc::new(TimeRanges::new(vec![out_time_range])))
        };
        let out_time_range = if out_time_range.is_boundless() {
            None
        } else {
            Some(out_time_range)
        };
        let tsm_readers_len = tsm_readers.len();
        let mut compacting_tsm_readers = Vec::with_capacity(tsm_readers_len);
        let mut compacting_files = Vec::with_capacity(tsm_readers_len);
        let mut compacting_tsm_file_idx = 0_usize;
        for tsm_reader in tsm_readers {
            if let Some(f) = CompactingFile::new(
                compacting_tsm_file_idx,
                tsm_reader.clone(),
                file_cache_size,
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
            decode_non_overlap_blocks,

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
                            match &self.out_time_ranges {
                                Some(time_ranges) => {
                                    idx_meta.block_iterator_opt(time_ranges.clone())
                                }
                                None => idx_meta.block_iterator(),
                            },
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

/// Temporary compacting data block meta, holding the priority of reader,
/// the reader index in `CompactState` and the meta of data block.
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

impl PartialEq for CompactingBlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.block_meta == other.block_meta
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

pub struct TsmCache {
    index_iter: BufferedIterator<IndexIterator>,
    capacity: usize,
    data: Vec<u8>,
    tsm_off: u64,
}

impl TsmCache {
    pub fn new(index_iter: BufferedIterator<IndexIterator>, file_cache_size: usize) -> Self {
        Self {
            index_iter,
            capacity: file_cache_size,
            data: Vec::with_capacity(file_cache_size),
            tsm_off: 0,
        }
    }

    /// Read data from tsm_reader and fill the cache until the cache is full, then return a slice of cached data.
    /// 1. Advance the index_iter, until find the BlockMeta with the same offset of block_meta,
    ///    bind the offset value as load_off_start.
    /// 2. Advance the index_iter, set the load_len as the sum of the size of BlockMeta from load_off_start,
    ///    until the load_len + BlockMeta::size > capacity.
    /// 3. Read data from tsm_reader from load_off_start to load_off_start + load_len, and save data to self.data.
    /// 4. Return a slice of data.
    async fn fill_and_read(
        &mut self,
        tsm_reader: &TsmReader,
        block_meta: &BlockMeta,
        out_time_ranges: &Option<Arc<TimeRanges>>,
    ) -> ReadTsmResult<Option<&[u8]>> {
        trace::trace!(
            "Filling cache from file:{} for block_neta: field:{},off:{},len:{}, begin to find scale of cached data.",
            tsm_reader.file_id(),
            block_meta.field_id(),
            block_meta.offset(),
            block_meta.size(),
        );

        let block_meta_off = block_meta.offset();
        let block_meta_len = block_meta.size();

        let mut load_off_start = 0_u64;
        let mut load_len = 0_usize;
        let mut found_curr_field = false;
        'idx_iter: while let Some(idx) = self.index_iter.peek() {
            let blk_meta_iter = match out_time_ranges {
                Some(time_ranges) => idx.block_iterator_opt(time_ranges.clone()),
                None => idx.block_iterator(),
            };
            for blk in blk_meta_iter {
                if !found_curr_field {
                    if blk.field_id() == blk.field_id() {
                        found_curr_field = true;
                    } else {
                        // All of this IndexMeta has been already consumed.
                        self.index_iter.next();
                        continue 'idx_iter;
                    }
                }
                let blk_off = blk.offset();
                if blk_off < block_meta_off {
                    continue;
                }
                let blk_len = blk.size();
                if blk_len > (self.capacity - load_len) as u64 {
                    // Cache is full, stop loading next BlockMeta, nether next IndeMeta.
                    break 'idx_iter;
                }

                if load_off_start == 0 {
                    load_off_start = blk_off;
                }
                load_len = (blk_off + blk_len - load_off_start) as usize;
            }

            self.index_iter.next();
        }

        if load_len == 0 {
            return Ok(None);
        }
        self.tsm_off = load_off_start;
        trace::trace!(
            "Filling cache from file:{} for block_meta: field:{},off:{},len:{}, cache_scale: off:{},len:{}",
            tsm_reader.file_id(),
            block_meta.field_id(),
            block_meta.offset(),
            block_meta.size(),
            load_off_start,
            load_len
        );
        self.data = tsm_reader.get_raw_data(load_off_start, load_len).await?;

        let cache_off = (block_meta_off - self.tsm_off) as usize;
        Ok(Some(
            &self.data[cache_off..cache_off + block_meta_len as usize],
        ))
    }

    /// Read cached data of block_meta, if data of block_meta is not loaded, return None.
    fn read(&self, block_meta: &BlockMeta) -> Option<&[u8]> {
        let blk_off = block_meta.offset();
        let blk_len = block_meta.size() as usize;
        let cache_off = (blk_off - self.tsm_off) as usize;
        // The block_meta is not in the cache.
        if blk_off < self.tsm_off
            // Cached data is exceeded, need to fill the cache from the block_meta.
            || cache_off >= self.data.len()
            // This means the block_meta is not for the file.
            || cache_off + blk_len > self.data.len()
        {
            return None;
        }
        Some(&self.data[cache_off..cache_off + blk_len])
    }
}

pub struct CompactingFile {
    pub tsm_reader_index: usize,
    pub tsm_reader: Arc<TsmReader>,
    out_time_range: Option<TimeRange>,
    out_time_ranges: Option<Arc<TimeRanges>>,
    pub index_iter: BufferedIterator<IndexIterator>,
    pub field_id: FieldId,
    pub finished: bool,
    cache: Option<TsmCache>,
}

impl CompactingFile {
    pub fn new(
        tsm_reader_index: usize,
        tsm_reader: Arc<TsmReader>,
        file_cache_size: usize,
        out_time_range: Option<TimeRange>,
        out_time_ranges: Option<Arc<TimeRanges>>,
    ) -> Option<Self> {
        let mut index_iter = BufferedIterator::new(tsm_reader.index_iterator());
        let cache = if tsm_reader.version().meet_minimum_version(TsmVersion::V2) {
            trace::debug!(
                "Creating cache for compacting file: {}",
                tsm_reader.file_id()
            );
            Some(TsmCache::new(index_iter.clone(), file_cache_size))
        } else {
            None
        };
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
                cache,
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
        if let Some(cache) = &mut self.cache {
            trace::trace!(
                "Getting data block from file:{} for block_meta: field:{},off:{},len:{}, cache: off:{},len:{}",
                self.tsm_reader.file_id(),
                block_meta.field_id(),
                block_meta.offset(),
                block_meta.size(),
                cache.tsm_off,
                cache.data.len(),
            );
            let mut cached_data = cache.read(block_meta);
            if cached_data.is_none() {
                cached_data = cache
                    .fill_and_read(&self.tsm_reader, block_meta, &self.out_time_ranges)
                    .await?;
            }
            if let Some(data) = cached_data {
                let mut blk = tsm::decode_data_block(
                    data,
                    block_meta.field_type(),
                    block_meta.val_off() - block_meta.offset(),
                )?;
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
                return Ok(match &self.out_time_range {
                    Some(time_range) => blk.intersection(time_range),
                    None => Some(blk),
                });
            }
            trace::error!(
                "Unexpected block_meta to read data_block from file:{}: off:{},len:{} from cache: off:{},len:{}",
                block_meta.file_id(),
                block_meta.offset(),
                block_meta.size(),
                cache.tsm_off,
                cache.data.len(),
            );
        }
        self.tsm_reader.get_data_block(block_meta).await
    }

    pub async fn get_raw_data(&mut self, block_meta: &BlockMeta) -> ReadTsmResult<Vec<u8>> {
        if let Some(cache) = &mut self.cache {
            trace::trace!(
                "Getting raw block from file:{} for block_meta: field:{},off:{},len:{}, cache: off:{},len:{}",
                self.tsm_reader.file_id(),
                block_meta.field_id(),
                block_meta.offset(),
                block_meta.size(),
                cache.tsm_off,
                cache.data.len(),
            );

            if let Some(data) = cache.read(block_meta) {
                return Ok(data.to_vec());
            }
            if let Some(raw_data) = cache
                .fill_and_read(&self.tsm_reader, block_meta, &self.out_time_ranges)
                .await?
            {
                return Ok(raw_data.to_vec());
            }
            trace::error!(
                "Unexpected block_meta to read data_block from file:{}: off:{},len:{} from cache: off:{},len:{}",
                block_meta.file_id(),
                block_meta.offset(),
                block_meta.size(),
                cache.tsm_off,
                cache.data.len(),
            );
        }
        self.tsm_reader
            .get_raw_data(block_meta.offset(), block_meta.size() as usize)
            .await
    }

    pub fn has_tombstone(&self) -> bool {
        self.tsm_reader.has_tombstone()
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

pub struct WriterWrapper {
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
pub mod test {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use minivec::MiniVec;
    use models::codec::Encoding;
    use models::predicate::domain::TimeRange;
    use models::{FieldId, Timestamp, ValueType};

    use crate::compaction::compact::{
        chunk_data_block_into_compacting_blocks, CompactingBlock, TsmCache,
    };
    use crate::compaction::iterator::BufferedIterator;
    use crate::file_system::file_manager;
    use crate::tseries_family::ColumnFile;
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::{
        self, DataBlock, EncodedDataBlock, TsmReader, TsmTombstone, TsmVersion, TsmWriter,
    };
    use crate::{file_utils, ColumnFileId, LevelId, Options, VersionEdit};

    pub type TsmSchema = (
        ColumnFileId,                                    // tsm file id
        Vec<(ValueType, FieldId, Timestamp, Timestamp)>, // Data block definitions
        Vec<(FieldId, Timestamp, Timestamp)>,            // Tombstone definitions
    );

    pub const INT_BLOCK_ENCODING: DataBlockEncoding =
        DataBlockEncoding::new(Encoding::Delta, Encoding::Delta);

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
            let chunks = &mut Vec::new();
            chunk_data_block_into_compacting_blocks(field_id, data_block.clone(), 0, chunks)
                .unwrap();
            assert_eq!(chunks.len(), 1);
            assert_eq!(
                chunks[0],
                CompactingBlock::decoded(0, 1, data_block.clone())
            );
        }
        // Trying to chunk with too big chunk size
        {
            let chunks = &mut Vec::new();
            chunk_data_block_into_compacting_blocks(field_id, data_block.clone(), 100, chunks)
                .unwrap();
            assert_eq!(chunks.len(), 1);
            assert_eq!(
                chunks[0],
                CompactingBlock::decoded(0, 1, data_block.clone())
            );
        }
        // Trying to chunk with chunk size that can divide data block exactly
        {
            let chunks = &mut Vec::new();
            chunk_data_block_into_compacting_blocks(field_id, data_block.clone(), 4, chunks)
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
            let chunks = &mut Vec::new();
            chunk_data_block_into_compacting_blocks(field_id, data_block.clone(), 5, chunks)
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

    pub async fn write_data_blocks_to_column_file(
        dir: impl AsRef<Path>,
        data: Vec<Vec<(FieldId, Vec<DataBlock>)>>,
        level: LevelId,
        version: TsmVersion,
    ) -> (u64, Vec<Arc<ColumnFile>>) {
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut cfs = Vec::new();
        let mut file_seq = 0;
        for (i, d) in data.iter().enumerate() {
            file_seq = i as u64 + 1;

            let file_path = file_utils::make_tsm_file_name(&dir, file_seq);
            let mut writer = TsmWriter::open_with_version(file_path, file_seq, false, 0, version)
                .await
                .unwrap();
            for (fid, data_blks) in d.iter() {
                for blk in data_blks.iter() {
                    writer.write_block(*fid, blk).await.unwrap();
                }
            }
            writer.write_index().await.unwrap();
            writer.finish().await.unwrap();
            let mut cf = ColumnFile::new(
                file_seq,
                level,
                TimeRange::new(writer.min_ts(), writer.max_ts()),
                writer.size(),
                writer.path(),
            );
            cf.set_field_id_filter(tokio::sync::RwLock::new(Some(Arc::new(
                writer.into_bloom_filter(),
            ))));
            cfs.push(Arc::new(cf));
        }
        (file_seq + 1, cfs)
    }

    pub async fn read_data_blocks_from_column_file(
        path: impl AsRef<Path>,
    ) -> HashMap<FieldId, Vec<DataBlock>> {
        let tsm_reader = TsmReader::open(path).await.unwrap();
        let mut data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx in tsm_reader.index_iterator() {
            let field_id = idx.field_id();
            for blk_meta in idx.block_iterator() {
                if let Some(blk) = tsm_reader.get_data_block(&blk_meta).await.unwrap() {
                    data.entry(field_id).or_default().push(blk);
                }
            }
        }
        data
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
                }
                return file_utils::make_tsm_file_name(dir, f.file_id);
            }
            panic!("VersionEdit::add_files doesn't contain any file matches level-{level}.");
        }
        panic!("VersionEdit::add_files is empty, no file to read.");
    }

    /// Compare DataBlocks in path with the expected_Data using assert_eq.
    pub async fn check_column_file(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        expected_data: HashMap<FieldId, Vec<DataBlock>>,
        expected_data_level: LevelId,
    ) {
        let path = get_result_file_path(dir, version_edit, expected_data_level);
        let data = read_data_blocks_from_column_file(path).await;
        let mut data_field_ids = data.keys().copied().collect::<Vec<_>>();
        data_field_ids.sort();
        let mut expected_data_field_ids = expected_data.keys().copied().collect::<Vec<_>>();
        expected_data_field_ids.sort();
        assert_eq!(
            data_field_ids, expected_data_field_ids,
            "All Field IDs in the file are not as expected"
        );

        for (exp_field_id, exp_blks) in expected_data.iter() {
            let data_blks = data.get(exp_field_id).unwrap();
            if exp_blks.len() != data_blks.len() {
                let ev_str = format_data_blocks(exp_blks.as_slice());
                let data_blks_str = format_data_blocks(data_blks.as_slice());
                panic!("fid={exp_field_id}, v.len != data_blks.len:\n          v={ev_str}\n  data_blks={data_blks_str}")
            }
            assert_eq!(exp_blks.len(), data_blks.len());
            for (i, exp_blk) in exp_blks.iter().enumerate() {
                assert_eq!(
                    data_blks.get(i).unwrap(),
                    exp_blk,
                    "[fid:{exp_field_id}][blk:{i}] File data != Expected data"
                );
            }
        }
    }

    pub fn create_options(base_dir: String, compact_trigger_file_num: u32) -> Arc<Options> {
        let mut config = config::get_config_for_test();
        config.storage.path = base_dir.clone();
        config.storage.compact_trigger_file_num = compact_trigger_file_num;
        config.log.path = base_dir;
        Arc::new(Options::from(&config))
    }

    #[test]
    fn test_create_options() {
        let dir = "/tmp/test/compaction/test_create_options";
        let opt = create_options(dir.to_string(), 4);
        assert_eq!(opt.storage.path.to_string_lossy(), dir);
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
    pub fn generate_data_block(
        value_type: ValueType,
        data_descriptors: Vec<(i64, i64)>,
    ) -> DataBlock {
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
                    enc: DataBlockEncoding::new(Encoding::Delta, Encoding::Delta),
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
                    enc: DataBlockEncoding::new(Encoding::Delta, Encoding::Delta),
                }
            }
            ValueType::String => {
                let word = MiniVec::from(&b"hello_world"[..]);
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
                    enc: DataBlockEncoding::new(Encoding::Delta, Encoding::Snappy),
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
                    enc: DataBlockEncoding::new(Encoding::Delta, Encoding::Gorilla),
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
                    enc: DataBlockEncoding::new(Encoding::Delta, Encoding::BitPack),
                }
            }
            ValueType::Unknown => {
                panic!("value type is Unknown")
            }
        }
    }

    pub async fn write_data_block_desc(
        dir: impl AsRef<Path>,
        data_desc: &[TsmSchema],
        level: LevelId,
    ) -> Vec<Arc<ColumnFile>> {
        let mut column_files = Vec::new();
        for (tsm_sequence, tsm_desc, tombstone_desc) in data_desc.iter() {
            let mut tsm_writer = tsm::new_tsm_writer(&dir, *tsm_sequence, level == 0, 0)
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
            let tsm_tombstone = TsmTombstone::open(&dir, *tsm_sequence).await.unwrap();
            for (fid, min_ts, max_ts) in tombstone_desc.iter() {
                tsm_tombstone
                    .add_range(&[*fid][..], TimeRange::new(*min_ts, *max_ts), None)
                    .await
                    .unwrap();
            }

            tsm_tombstone.flush().await.unwrap();
            column_files.push(Arc::new(ColumnFile::new(
                *tsm_sequence,
                level,
                TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
                tsm_writer.size(),
                tsm_writer.path(),
            )));
        }

        column_files
    }

    #[tokio::test]
    async fn test_tsm_cache() {
        let dir = "/tmp/test/compaction/test_tsm_cache";
        let file_id = 1;
        let path: PathBuf = file_utils::make_tsm_file_name(dir, file_id);

        {
            let _ = std::fs::remove_dir_all(dir);
            std::fs::create_dir_all(dir).unwrap();
            // Generate 1.1M~1.5MB data
            let mut tsm_writer = tsm::new_tsm_writer(dir, 1, false, 0).await.unwrap();
            let ts_vec: Vec<Timestamp> = (1..=1000).collect();
            let mut val_vec = Vec::with_capacity(1000);
            for field_id in 0..100 {
                let field_blocks_num = if field_id % 2 == 0 { 2 } else { 1 };
                for _ in 0..field_blocks_num {
                    val_vec.clear();
                    for _ in 0..1000 {
                        val_vec.push(rand::random::<u64>());
                    }
                    let block = DataBlock::U64 {
                        ts: ts_vec.clone(),
                        val: val_vec.clone(),
                        enc: DataBlockEncoding::new(Encoding::Delta, Encoding::Delta),
                    };
                    tsm_writer.write_block(field_id, &block).await.unwrap();
                }
            }
            tsm_writer.write_index().await.unwrap();
            tsm_writer.finish().await.unwrap();
        }

        let tsm_reader = TsmReader::open(&path).await.unwrap();
        let index_reader = tsm_reader.index_iterator();
        let mut cache = TsmCache::new(BufferedIterator::new(index_reader.clone()), 1024 * 16);
        for idx_meta in index_reader {
            for blk_meta in idx_meta.block_iterator() {
                let fd_1 = tsm_reader.get_data_block(&blk_meta).await.unwrap();
                assert!(fd_1.is_some());
                let fd_2 = tsm_reader
                    .get_raw_data(blk_meta.offset(), blk_meta.size() as usize)
                    .await
                    .unwrap();
                let fd_2_block = tsm::decode_data_block(
                    &fd_2,
                    ValueType::Unsigned,
                    blk_meta.val_off() - blk_meta.offset(),
                )
                .unwrap();

                let cd_1 = match cache.read(&blk_meta) {
                    Some(cd) => cd.to_vec(),
                    None => {
                        let cd = cache
                            .fill_and_read(&tsm_reader, &blk_meta, &None)
                            .await
                            .unwrap();
                        assert!(cd.is_some());
                        assert_eq!(cd.unwrap().len(), blk_meta.size() as usize);
                        cd.unwrap().to_vec()
                    }
                };
                let cd_2 = cache.read(&blk_meta).unwrap();
                assert_eq!(&cd_1, cd_2);
                assert_eq!(cd_2, &fd_2);
                let cd_2_block = tsm::decode_data_block(
                    cd_2,
                    ValueType::Unsigned,
                    blk_meta.val_off() - blk_meta.offset(),
                )
                .unwrap();
                assert_eq!(cd_2_block, fd_2_block);
            }
        }
    }
}
