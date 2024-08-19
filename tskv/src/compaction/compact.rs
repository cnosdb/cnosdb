use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::StreamExt;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::{TableColumn, TskvTableSchema, TskvTableSchemaRef};
use models::schema::TIME_FIELD_NAME;
use models::{SeriesId, SeriesKey};
use snafu::{OptionExt, ResultExt};
use trace::{info, trace};
use utils::BloomFilter;

use super::metrics::VnodeCompactionMetrics;
use crate::compaction::metrics::DurationMetricRecorder;
use crate::compaction::CompactReq;
use crate::context::GlobalContext;
use crate::error::{ArrowSnafu, CommonSnafu, ModelSnafu, TskvResult};
use crate::reader::sort_merge::sort_merge;
use crate::reader::{SchemableMemoryBatchReaderStream, SendableSchemableTskvRecordBatchStream};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tsm::chunk::Chunk;
use crate::tsm::column_group::ColumnGroup;
use crate::tsm::page::Page;
use crate::tsm::reader::{decode_pages, decode_pages_buf, TsmReader};
use crate::tsm::writer::TsmWriter;
use crate::tsm::ColumnGroupID;
use crate::{ColumnFileId, LevelId, TseriesFamilyId};

/// Temporary compacting data block meta
#[derive(Clone)]
pub(crate) struct CompactingBlockMeta {
    reader_idx: usize,
    reader: Arc<TsmReader>,
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
        tsm_reader: Arc<TsmReader>,
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

    pub fn time_range(&self) -> TskvResult<TimeRange> {
        let column_group = self
            .meta
            .column_group()
            .get(&self.column_group_id)
            .context(CommonSnafu {
                reason: format!(
                    "column group {} not found in chunk {:?}",
                    self.column_group_id, self.meta
                ),
            })?;
        Ok(*column_group.time_range())
    }

    pub async fn get_record_batch(&self) -> TskvResult<RecordBatch> {
        let sid = self.meta.series_id();
        let record_batch = self
            .reader
            .read_record_batch(sid, self.column_group_id)
            .await?;
        Ok(record_batch)
    }

    pub async fn get_raw_data(&self) -> TskvResult<Vec<u8>> {
        self.reader
            .read_datablock_raw(self.meta.series_id(), self.column_group_id)
            .await
    }

    pub fn column_group(&self) -> TskvResult<Arc<ColumnGroup>> {
        self.meta
            .column_group()
            .get(&self.column_group_id)
            .cloned()
            .context(CommonSnafu {
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
    pub fn new(series_id: SeriesId, blk_meta: CompactingBlockMeta) -> TskvResult<Self> {
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
        metrics: &mut VnodeCompactionMetrics,
        previous_block: Option<CompactingBlock>,
        max_block_size: usize,
    ) -> TskvResult<Vec<CompactingBlock>> {
        if self.blk_metas.is_empty() {
            return Ok(vec![]);
        }
        self.blk_metas
            .sort_by(|a, b| a.reader_idx.cmp(&b.reader_idx));

        let table_schema = self.blk_metas[0].table_schema().context(CommonSnafu {
            reason: format!(
                "table schema not found for table {}",
                self.blk_metas[0].meta.table_name()
            ),
        })?;

        if self.blk_metas.len() == 1 && !self.blk_metas[0].has_tombstone() {
            // Only one compacting block and has no tombstone, write as raw block.
            trace!("only one compacting block, write as raw block");
            let meta_0 = &self.blk_metas[0].meta;
            let column_group_id = self.blk_metas[0].column_group_id;
            let column_group = self.blk_metas[0].column_group()?;
            metrics.read_begin();
            let buf_0 = self.blk_metas[0].get_raw_data().await?;
            metrics.read_end();

            if column_group.row_len() >= max_block_size {
                // Raw data block is full, so do not merge with the previous, directly return.
                let mut merged_blks = Vec::new();
                if let Some(blk) = previous_block {
                    merged_blks.push(blk);
                }
                merged_blks.push(CompactingBlock::raw(
                    meta_0.clone(),
                    table_schema,
                    column_group_id,
                    buf_0,
                ));

                Ok(merged_blks)
            } else if let Some(compacting_block) = previous_block {
                // Raw block is not full, so decode and merge with compacting_block.
                let chunk = self.blk_metas[0].meta.clone();
                let column_group_id = self.blk_metas[0].column_group_id;
                let decoded_raw_record_batch =
                    decode_pages_buf(&buf_0, chunk, column_group_id, table_schema.clone())?;
                let record_batch = compacting_block.decode()?;
                metrics.merge_begin();
                let record_batches = Self::merge_record_batches(
                    vec![decoded_raw_record_batch, record_batch],
                    max_block_size,
                )
                .await?;
                metrics.merge_end();
                let merged_blks = record_batches
                    .into_iter()
                    .map(|rb| {
                        CompactingBlock::decoded(
                            self.series_id,
                            self.chunk.series_key().clone(),
                            table_schema.clone(),
                            rb,
                        )
                    })
                    .collect::<Vec<_>>();
                return Ok(merged_blks);
            } else {
                // Raw block is not full, but nothing to merge with, directly return.
                return Ok(vec![CompactingBlock::raw(
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
            let record_batches = {
                metrics.read_begin();
                let mut record_batches =
                    Vec::with_capacity(self.blk_metas.len() + previous_block.is_some() as usize);
                for blk_meta in self.blk_metas.iter() {
                    let record_batch = blk_meta.get_record_batch().await?;
                    record_batches.push(record_batch);
                }
                if let Some(blk) = previous_block {
                    record_batches.push(blk.decode()?);
                }
                metrics.read_end();
                record_batches
            };
            let record_batches = {
                metrics.merge_begin();
                let record_batches =
                    Self::merge_record_batches(record_batches, max_block_size).await?;
                metrics.merge_end();
                record_batches
            };
            let merged_blks = record_batches
                .into_iter()
                .map(|rb| {
                    CompactingBlock::decoded(
                        self.series_id,
                        self.chunk.series_key().clone(),
                        table_schema.clone(),
                        rb,
                    )
                })
                .collect::<Vec<_>>();
            Ok(merged_blks)
        }
    }

    pub async fn merge_record_batches(
        record_batches: Vec<RecordBatch>,
        block_size: usize,
    ) -> TskvResult<Vec<RecordBatch>> {
        let (adapted_record_batches, target_schema) =
            Self::schema_adapt_record_batches(record_batches)?;
        let merge_streams = adapted_record_batches
            .into_iter()
            .map(|rb| {
                let stream = SchemableMemoryBatchReaderStream::new(target_schema.clone(), vec![rb]);
                Ok(Box::pin(stream) as SendableSchemableTskvRecordBatchStream)
            })
            .collect::<TskvResult<Vec<_>>>()?;
        let res_stream = sort_merge(
            merge_streams,
            target_schema,
            block_size,
            TIME_FIELD_NAME,
            &ExecutionPlanMetricsSet::new(),
        )?;
        let record_batches = res_stream
            .collect::<Vec<TskvResult<RecordBatch>>>()
            .await
            .into_iter()
            .collect::<TskvResult<Vec<RecordBatch>>>()?;
        Ok(record_batches)
    }

    pub fn schema_adapt_record_batches(
        record_batches: Vec<RecordBatch>,
    ) -> TskvResult<(Vec<RecordBatch>, SchemaRef)> {
        let mut potential_error = None;
        let target_schema = record_batches
            .iter()
            .map(|rb| {
                let schema = rb.schema();
                let schema_version =
                    match TskvTableSchema::schema_version_from_arrow_array_schema(schema.clone())
                        .context(ModelSnafu)
                    {
                        Ok(v) => v,
                        Err(e) => {
                            potential_error = Some(e);
                            0
                        }
                    };
                (schema_version, schema)
            })
            .collect::<Vec<(_, _)>>()
            .iter()
            .max_by_key(|(sv, _)| sv)
            .map(|(_, schema)| schema.clone())
            .ok_or_else(|| {
                CommonSnafu {
                    reason: "no schema found".to_string(),
                }
                .build()
            })?;
        if let Some(e) = potential_error {
            return Err(e);
        }
        let mut adapted_record_batches = Vec::with_capacity(record_batches.len());
        for record_batch in record_batches {
            let adapted_record_batch =
                Self::record_batch_project_schema(record_batch, target_schema.clone())?;
            adapted_record_batches.push(adapted_record_batch);
        }
        Ok((adapted_record_batches, target_schema))
    }

    pub fn record_batch_project_schema(
        record_batch: RecordBatch,
        target_schema: SchemaRef,
    ) -> TskvResult<RecordBatch> {
        let mut target_arrays = Vec::with_capacity(target_schema.fields().len());
        for field in target_schema.fields() {
            let column_id =
                TableColumn::column_id_from_arrow_field(field.clone()).context(ModelSnafu)?;
            let mut potential_error = None;
            let array = record_batch
                .schema()
                .fields
                .iter()
                .enumerate()
                .find(|(_, field)| {
                    let current_column_id =
                        TableColumn::column_id_from_arrow_field((*field).clone())
                            .context(ModelSnafu);
                    match current_column_id {
                        Ok(id) => id == column_id,
                        Err(e) => {
                            potential_error = Some(e);
                            false
                        }
                    }
                })
                .map(|(index, _)| record_batch.column(index).clone());
            if let Some(e) = potential_error {
                return Err(e);
            }
            if let Some(array) = array {
                target_arrays.push(array);
            } else {
                let empty_array =
                    arrow_array::new_null_array(field.data_type(), record_batch.num_rows());
                target_arrays.push(empty_array);
            }
        }
        let record_batch =
            RecordBatch::try_new(target_schema, target_arrays).context(ArrowSnafu)?;
        Ok(record_batch)
    }

    pub fn is_empty(&self) -> bool {
        self.blk_metas.is_empty()
    }
}

/// Temporary compacting data block.
/// - priority: When merging two (timestamp, value) pair with the same
/// timestamp from two data blocks, pair from data block with lower
/// priority will be discarded.
#[derive(Debug)]
pub enum CompactingBlock {
    Decoded {
        series_id: SeriesId,
        series_key: SeriesKey,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
    },
    Encoded {
        table_schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: SeriesKey,
        time_range: TimeRange,
        record_batch: Vec<Page>,
    },
    Raw {
        table_schema: TskvTableSchemaRef,
        meta: Arc<Chunk>,
        column_group_id: ColumnGroupID,
        raw: Vec<u8>,
    },
}

impl CompactingBlock {
    pub fn decoded(
        series_id: SeriesId,
        series_key: SeriesKey,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
    ) -> CompactingBlock {
        CompactingBlock::Decoded {
            series_id,
            series_key,
            table_schema,
            record_batch,
        }
    }

    pub fn encoded(
        table_schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: SeriesKey,
        time_range: TimeRange,
        record_batch: Vec<Page>,
    ) -> CompactingBlock {
        CompactingBlock::Encoded {
            table_schema,
            series_id,
            series_key,
            time_range,
            record_batch,
        }
    }

    pub fn raw(
        chunk: Arc<Chunk>,
        table_schema: TskvTableSchemaRef,
        column_group_id: ColumnGroupID,
        raw: Vec<u8>,
    ) -> CompactingBlock {
        CompactingBlock::Raw {
            meta: chunk,
            table_schema,
            column_group_id,
            raw,
        }
    }

    pub fn decode(self) -> TskvResult<RecordBatch> {
        match self {
            CompactingBlock::Decoded { record_batch, .. } => Ok(record_batch),
            CompactingBlock::Encoded {
                record_batch,
                table_schema,
                ..
            } => decode_pages(record_batch, table_schema, None),
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
            CompactingBlock::Decoded { record_batch, .. } => record_batch.num_rows(),
            CompactingBlock::Encoded { record_batch, .. } => {
                record_batch[0].meta.num_values as usize
            }
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
}

struct CompactingFile {
    i: usize,
    tsm_reader: Arc<TsmReader>,
    series_idx: usize,
    series_ids: Vec<SeriesId>,
}

impl CompactingFile {
    fn new(i: usize, tsm_reader: Arc<TsmReader>) -> Self {
        let mut series_ids = {
            let chunks = tsm_reader.chunk_group();
            chunks
                .iter()
                .flat_map(|(_, chunk)| {
                    chunk
                        .chunks()
                        .iter()
                        .map(|chunk_meta| chunk_meta.series_id())
                })
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
    tsm_readers: Vec<Arc<TsmReader>>,
    compacting_files: BinaryHeap<Pin<Box<CompactingFile>>>,
    // /// The time range of data to be merged of level-0 data blocks.
    // /// The level-0 data that out of the thime range will write back to level-0.
    // level_time_range: TimeRange,
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
    pub(crate) fn new(tsm_readers: Vec<Arc<TsmReader>>) -> Self {
        let compacting_files: BinaryHeap<Pin<Box<CompactingFile>>> = tsm_readers
            .iter()
            .enumerate()
            .map(|(i, r)| Box::pin(CompactingFile::new(i, r.clone())))
            .collect();
        let compacting_files_cnt = compacting_files.len();

        Self {
            tsm_readers,
            compacting_files,
            finished_readers: vec![false; compacting_files_cnt],
            ..Default::default()
        }
    }

    /// Update tmp_tsm_blks and tmp_tsm_blk_tsm_reader_idx for field id in next iteration.
    fn next_series_id(&mut self) -> TskvResult<()> {
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
                    let meta = f
                        .tsm_reader
                        .chunk()
                        .get(&sid)
                        .cloned()
                        .context(CommonSnafu {
                            reason: format!("series id {} not found in file {}", sid, loop_file_i),
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
    async fn fetch_merging_block_meta_groups(&mut self) -> TskvResult<bool> {
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
    pub(crate) async fn next(&mut self) -> TskvResult<Option<CompactingBlockMetaGroup>> {
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

pub async fn run_compaction_job(
    request: CompactReq,
    kernel: Arc<GlobalContext>,
    mut metrics: VnodeCompactionMetrics,
) -> TskvResult<Option<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)>> {
    metrics.begin();
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
        let tsm_reader = request.version.get_tsm_reader(col_file.file_path()).await?;
        tsm_readers.push(tsm_reader);
    }

    let max_block_size = request.storage_opt.max_datablock_size as usize;
    let mut iter = CompactIterator::new(tsm_readers);
    let tsm_dir = request.storage_opt.tsm_dir(&request.owner, tsf_id);
    let max_file_size = request.storage_opt.level_max_file_size(request.out_level);
    let mut tsm_writer =
        TsmWriter::open(&tsm_dir, kernel.file_id_next(), max_file_size, false).await?;
    // let mut tsm_writer = tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0).await?;
    info!(
        "Compaction: File: {} been created (level: {}).",
        tsm_writer.file_id(),
        request.out_level
    );

    let mut file_metas: HashMap<ColumnFileId, Arc<BloomFilter>> = HashMap::new();
    let mut version_edit = VersionEdit::new_update_vnode(
        tsf_id,
        request.owner.to_string(),
        request.version.last_seq(),
    );
    let mut previous_merged_block: Option<CompactingBlock> = None;
    let mut sid = iter.curr_sid;
    while let Some(blk_meta_group) = iter.next().await? {
        trace!("===============================");
        if sid.is_some() && sid != iter.curr_sid {
            // Iteration of next field id, write previous merged block.
            if let Some(blk) = previous_merged_block.take() {
                metrics.write_begin();
                tsm_writer.write_compacting_block(blk).await?;
                metrics.write_end();
                if handle_finish_write_tsm_meta(
                    &mut tsm_writer,
                    &mut file_metas,
                    &mut version_edit,
                    &request,
                )
                .await?
                {
                    tsm_writer =
                        TsmWriter::open(&tsm_dir, kernel.file_id_next(), max_file_size, false)
                            .await?;
                }
            }
        }

        sid = iter.curr_sid;
        let mut compacting_blks = blk_meta_group
            .merge(&mut metrics, previous_merged_block.take(), max_block_size)
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
            metrics.write_begin();
            tsm_writer.write_compacting_block(blk).await?;
            metrics.write_end();
            if handle_finish_write_tsm_meta(
                &mut tsm_writer,
                &mut file_metas,
                &mut version_edit,
                &request,
            )
            .await?
            {
                tsm_writer =
                    TsmWriter::open(&tsm_dir, kernel.file_id_next(), max_file_size, false).await?;
            }
        }
    }
    if let Some(blk) = previous_merged_block {
        metrics.write_begin();
        tsm_writer.write_compacting_block(blk).await?;
        metrics.write_end();
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
    tsm_writer: &mut TsmWriter,
    file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    version_edit: &mut VersionEdit,
    request: &CompactReq,
) -> TskvResult<bool> {
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
    tsm_writer: &TsmWriter,
    tsf_id: TseriesFamilyId,
    level: LevelId,
) -> CompactMeta {
    CompactMeta {
        file_id: tsm_writer.file_id(),
        file_size: tsm_writer.size(),
        tsf_id,
        level,
        min_ts: tsm_writer.min_ts(),
        max_ts: tsm_writer.max_ts(),
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
    use arrow_array::{ArrayRef, RecordBatch};
    use cache::ShardedAsyncCache;
    use models::codec::Encoding;
    use models::field_value::FieldVal;
    use models::predicate::domain::TimeRange;
    use models::schema::tskv_table_schema::{
        ColumnType, TableColumn, TskvTableSchema, TskvTableSchemaRef,
    };
    use models::{SeriesId, SeriesKey, ValueType};
    use tokio::sync::RwLock;

    use crate::compaction::metrics::VnodeCompactionMetrics;
    use crate::compaction::{run_compaction_job, CompactReq};
    use crate::context::GlobalContext;
    use crate::file_system::async_filesystem::LocalFileSystem;
    use crate::file_system::FileSystem;
    use crate::file_utils;
    use crate::kv_option::Options;
    use crate::summary::VersionEdit;
    use crate::tsfamily::column_file::ColumnFile;
    use crate::tsfamily::level_info::LevelInfo;
    use crate::tsfamily::version::Version;
    use crate::tsm::mutable_column::MutableColumn;
    use crate::tsm::reader::{decode_pages, TsmReader};
    use crate::tsm::writer::TsmWriter;
    use crate::tsm::TsmTombstone;

    pub(crate) async fn write_data_blocks_to_column_file(
        dir: impl AsRef<Path>,
        data: Vec<HashMap<SeriesId, RecordBatch>>,
        table_schema: TskvTableSchemaRef,
    ) -> (u64, Vec<Arc<ColumnFile>>) {
        if !LocalFileSystem::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut cfs = Vec::new();
        let mut file_seq = 0;
        for (i, d) in data.iter().enumerate() {
            file_seq = i as u64 + 1;
            let mut writer = TsmWriter::open(&dir, file_seq, 0, false).await.unwrap();
            for (sid, data_blks) in d.iter() {
                writer
                    .write_record_batch(
                        *sid,
                        SeriesKey::default(),
                        table_schema.clone(),
                        data_blks.clone(),
                    )
                    .await
                    .unwrap();
            }
            writer.finish().await.unwrap();
            let mut cf = ColumnFile::new(
                file_seq,
                2,
                TimeRange::new(writer.min_ts(), writer.max_ts()),
                writer.size(),
                false,
                writer.path(),
            );
            cf.set_series_id_filter(RwLock::new(Some(Arc::new(
                writer.series_bloom_filter().clone(),
            ))));
            cfs.push(Arc::new(cf));
        }
        (file_seq + 1, cfs)
    }

    async fn read_data_blocks_from_column_file(
        path: impl AsRef<Path>,
    ) -> HashMap<SeriesId, Vec<RecordBatch>> {
        let tsm_reader = TsmReader::open(&path).await.unwrap();
        let mut data = HashMap::new();
        for (sid, chunk) in tsm_reader.chunk() {
            let mut blks = vec![];
            for column_group_id in chunk.column_group().keys() {
                let blk = tsm_reader
                    .read_record_batch(*sid, *column_group_id)
                    .await
                    .unwrap();
                blks.push(blk)
            }
            data.insert(*sid, blks);
        }
        data
    }

    fn i64_column(data: Vec<i64>, col: TableColumn) -> ArrayRef {
        let mut col = MutableColumn::empty_with_cap(col, data.len()).unwrap();
        for datum in data {
            col.push(Some(FieldVal::Integer(datum))).unwrap()
        }
        col.to_arrow_array(None).unwrap()
    }

    fn i64_some_column(data: Vec<Option<i64>>, col: TableColumn) -> ArrayRef {
        let mut col = MutableColumn::empty_with_cap(col, data.len()).unwrap();
        for datum in data {
            col.push(datum.map(FieldVal::Integer)).unwrap()
        }
        col.to_arrow_array(None).unwrap()
    }

    fn get_result_file_path(dir: impl AsRef<Path>, version_edit: VersionEdit) -> PathBuf {
        if !version_edit.add_files.is_empty() {
            let file_id = version_edit.add_files.first().unwrap().file_id;
            return file_utils::make_tsm_file(dir, file_id);
        }

        panic!("VersionEdit doesn't contain any add_files.");
    }

    /// Compare DataBlocks in path with the expected_Data using assert_eq.
    async fn check_column_file(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        expected_data: HashMap<SeriesId, Vec<RecordBatch>>,
    ) {
        let path = get_result_file_path(dir, version_edit);
        let mut data = read_data_blocks_from_column_file(path).await;
        let mut data_series_ids = data.keys().copied().collect::<Vec<_>>();
        data_series_ids.sort_unstable();
        let mut expected_data_series_ids = expected_data.keys().copied().collect::<Vec<_>>();
        expected_data_series_ids.sort_unstable();
        assert_eq!(data_series_ids, expected_data_series_ids);

        for (k, v) in expected_data.into_iter() {
            let record_batches = data.remove(&k).unwrap();
            println!("v.len(): {}", v.len());
            println!("data_blks.len(): {}", record_batches.len());
            for (v, data_blc) in v.iter().zip(record_batches.iter()) {
                assert_eq!(v, data_blc);
            }
        }
    }

    pub(crate) fn create_options(base_dir: String) -> Arc<Options> {
        let mut config = config::tskv::get_config_for_test();
        config.storage.path = base_dir;
        config.storage.max_datablock_size = 1000;
        let opt = Options::from(&config);
        Arc::new(opt)
    }

    fn prepare_compact_req_and_kernel(
        owner: Arc<String>,
        opt: Arc<Options>,
        next_file_id: u64,
        files: Vec<Arc<ColumnFile>>,
    ) -> (CompactReq, Arc<GlobalContext>) {
        let version = Arc::new(Version::new(
            1,
            owner.clone(),
            opt.storage.clone(),
            1,
            LevelInfo::init_levels(owner.clone(), 0, opt.storage.clone()),
            1000,
            Arc::new(ShardedAsyncCache::create_lru_sharded_cache(1)),
        ));
        let compact_req = CompactReq {
            ts_family_id: 1,
            owner,
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
        let data1 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3], schema.time_column()),
                i64_column(vec![1, 2, 3], schema.column("f1").cloned().unwrap()),
                i64_column(vec![1, 2, 3], schema.column("f2").cloned().unwrap()),
                i64_column(vec![1, 2, 3], schema.column("f3").cloned().unwrap()),
            ],
        )
        .unwrap();

        let data2 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![4, 5, 6], schema.time_column()),
                i64_column(vec![4, 5, 6], schema.column("f1").cloned().unwrap()),
                i64_column(vec![4, 5, 6], schema.column("f2").cloned().unwrap()),
                i64_column(vec![4, 5, 6], schema.column("f3").cloned().unwrap()),
            ],
        )
        .unwrap();

        let data3 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![7, 8, 9], schema.time_column()),
                i64_column(vec![7, 8, 9], schema.column("f1").cloned().unwrap()),
                i64_column(vec![7, 8, 9], schema.column("f2").cloned().unwrap()),
                i64_column(vec![7, 8, 9], schema.column("f3").cloned().unwrap()),
            ],
        )
        .unwrap();

        let expected_data = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9], schema.time_column()),
                i64_column(
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_column(
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_column(
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                    schema.column("f3").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

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

        let (next_file_id, files) =
            write_data_blocks_to_column_file(&dir, data, schema.clone()).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) =
            run_compaction_job(compact_req, kernel, VnodeCompactionMetrics::fake())
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
        let data1 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![4, 5, 6], schema.time_column()),
                i64_column(vec![4, 5, 6], schema.column("f1").cloned().unwrap()),
                i64_column(vec![4, 5, 6], schema.column("f2").cloned().unwrap()),
                i64_column(vec![4, 5, 6], schema.column("f3").cloned().unwrap()),
            ],
        )
        .unwrap();

        let data2 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3], schema.time_column()),
                i64_column(vec![1, 2, 3], schema.column("f1").cloned().unwrap()),
                i64_column(vec![1, 2, 3], schema.column("f2").cloned().unwrap()),
                i64_column(vec![1, 2, 3], schema.column("f3").cloned().unwrap()),
            ],
        )
        .unwrap();

        let data3 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![7, 8, 9], schema.time_column()),
                i64_column(vec![7, 8, 9], schema.column("f1").cloned().unwrap()),
                i64_column(vec![7, 8, 9], schema.column("f2").cloned().unwrap()),
                i64_column(vec![7, 8, 9], schema.column("f3").cloned().unwrap()),
            ],
        )
        .unwrap();

        let expected_data = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9], schema.time_column()),
                i64_column(
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_column(
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_column(
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                    schema.column("f3").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

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

        let (next_file_id, files) =
            write_data_blocks_to_column_file(&dir, data, schema.clone()).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) =
            run_compaction_job(compact_req, kernel, VnodeCompactionMetrics::fake())
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
        let data1 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3, 4], schema.time_column()),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![None, None, None, None],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), None],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

        let data2 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![4, 5, 6, 7], schema.time_column()),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), None],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), None],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), Some(8)],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![None, None, None, None],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

        let data3 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![7, 8, 9], schema.time_column()),
                i64_column(vec![7, 8, 9], schema.column("f1").cloned().unwrap()),
                i64_column(vec![7, 8, 9], schema.column("f2").cloned().unwrap()),
                i64_column(vec![7, 8, 9], schema.column("f3").cloned().unwrap()),
                i64_some_column(
                    vec![None, None, None],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

        let expected_data = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3, 4, 5, 6, 7, 8, 9], schema.time_column()),
                i64_column(
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![
                        None,
                        None,
                        None,
                        Some(4),
                        Some(5),
                        Some(6),
                        Some(7),
                        Some(8),
                        Some(9),
                    ],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_column(
                    vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![
                        Some(1),
                        Some(2),
                        Some(3),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

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

        let (next_file_id, files) =
            write_data_blocks_to_column_file(&dir, data, schema.clone()).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let (version_edit, _) =
            run_compaction_job(compact_req, kernel, VnodeCompactionMetrics::fake())
                .await
                .unwrap()
                .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    fn generate_column_ts(min_ts: i64, max_ts: i64, col: TableColumn) -> ArrayRef {
        let mut col = MutableColumn::empty_with_cap(col, (max_ts - min_ts + 1) as usize).unwrap();
        for i in min_ts..max_ts + 1 {
            col.push(Some(FieldVal::Integer(i))).unwrap();
        }
        col.to_arrow_array(None).unwrap()
    }

    fn generate_column_i64(
        len: usize,
        none_range: Vec<(usize, usize)>,
        col: TableColumn,
    ) -> ArrayRef {
        let mut col = MutableColumn::empty_with_cap(col, len).unwrap();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                col.push(None).unwrap();
            } else {
                col.push(Some(FieldVal::Integer(i as i64))).unwrap();
            }
        }
        col.to_arrow_array(None).unwrap()
    }

    fn generate_column_u64(
        len: usize,
        none_range: Vec<(usize, usize)>,
        col: TableColumn,
    ) -> ArrayRef {
        let mut col = MutableColumn::empty_with_cap(col, len).unwrap();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                col.push(None).unwrap();
            } else {
                col.push(Some(FieldVal::Unsigned(i as u64))).unwrap();
            }
        }
        col.to_arrow_array(None).unwrap()
    }

    fn generate_column_f64(
        len: usize,
        none_range: Vec<(usize, usize)>,
        col: TableColumn,
    ) -> ArrayRef {
        let mut col = MutableColumn::empty_with_cap(col, len).unwrap();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                col.push(None).unwrap();
            } else {
                col.push(Some(FieldVal::Float(i as f64))).unwrap();
            }
        }
        col.to_arrow_array(None).unwrap()
    }

    fn generate_column_bool(
        len: usize,
        none_range: Vec<(usize, usize)>,
        col: TableColumn,
    ) -> ArrayRef {
        let mut col = MutableColumn::empty_with_cap(col, len).unwrap();
        for i in 0..len {
            if none_range.iter().any(|(min, max)| i >= *min && i <= *max) {
                col.push(None).unwrap();
            } else {
                col.push(Some(FieldVal::Boolean(true))).unwrap();
            }
        }
        col.to_arrow_array(None).unwrap()
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
        schema2.schema_version += 1;

        let schema1 = Arc::new(schema1);
        let schema2 = Arc::new(schema2);
        let data_desc = [
            (
                1_u64,
                vec![
                    (
                        RecordBatch::try_new(
                            schema1.to_record_data_schema(),
                            vec![
                                generate_column_ts(1, 1000, schema1.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema1.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema1.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![],
                                    schema1.column("f3").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema1.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema1.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(1001, 2000, schema1.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema1.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(500, 999)],
                                    schema1.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(500, 999)],
                                    schema1.column("f3").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema1.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema1.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(2001, 2500, schema1.time_column()),
                                generate_column_u64(
                                    500,
                                    vec![],
                                    schema1.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    500,
                                    vec![(0, 499)],
                                    schema1.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    500,
                                    vec![(0, 499)],
                                    schema1.column("f3").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema1.clone(),
                    ),
                ],
            ),
            (
                2,
                vec![
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(1, 1000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(1001, 2000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(500, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(2001, 3000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(500, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(3001, 4000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(4001, 4500, schema2.time_column()),
                                generate_column_u64(
                                    500,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                ],
            ),
            (
                3,
                vec![
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(1001, 2000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(2001, 3000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(500, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(3001, 4000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(500, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(4001, 5000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(5001, 6000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(6001, 6500, schema2.time_column()),
                                generate_column_u64(
                                    500,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                ],
            ),
        ];
        let expected_data: Vec<RecordBatch> = vec![
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(1, 1000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(1000, vec![], schema2.column("f3").cloned().unwrap()),
                    generate_column_f64(1000, vec![], schema2.column("f4").cloned().unwrap()),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(1001, 2000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(1000, vec![], schema2.column("f3").cloned().unwrap()),
                    generate_column_f64(1000, vec![], schema2.column("f4").cloned().unwrap()),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(2001, 3000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(1000, vec![], schema2.column("f3").cloned().unwrap()),
                    generate_column_f64(
                        1000,
                        vec![(500, 999)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(3001, 4000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(
                        1000,
                        vec![(500, 999)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(4001, 5000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(5001, 6000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f2").cloned().unwrap(),
                    ),
                    generate_column_bool(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(6001, 6500, schema2.time_column()),
                    generate_column_u64(500, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(
                        500,
                        vec![(0, 499)],
                        schema2.column("f2").cloned().unwrap(),
                    ),
                    generate_column_bool(
                        500,
                        vec![(0, 499)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        500,
                        vec![(0, 499)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
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
            let mut tsm_writer = TsmWriter::open(&dir, tsm_sequence, 0, false).await.unwrap();
            for (record_batch, schema) in args.into_iter() {
                tsm_writer
                    .write_record_batch(1, SeriesKey::default(), schema, record_batch)
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

        let (version_edit, _) =
            run_compaction_job(compact_req, kernel, VnodeCompactionMetrics::fake())
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
        schema2.schema_version += 1;

        let schema1 = Arc::new(schema1);
        let schema2 = Arc::new(schema2);
        let data_desc = [
            // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, Timestamp_end) ] )]
            (
                1_u64,
                vec![
                    (
                        RecordBatch::try_new(
                            schema1.to_record_data_schema(),
                            vec![
                                generate_column_ts(1, 1000, schema1.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema1.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema1.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![],
                                    schema1.column("f3").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema1.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema1.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(1001, 2000, schema1.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema1.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(500, 999)],
                                    schema1.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(500, 999)],
                                    schema1.column("f3").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema1.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema1.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(2001, 2500, schema1.time_column()),
                                generate_column_u64(
                                    500,
                                    vec![],
                                    schema1.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    500,
                                    vec![(0, 499)],
                                    schema1.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    500,
                                    vec![(0, 499)],
                                    schema1.column("f3").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema1.clone(),
                    ),
                ],
            ),
            (
                2,
                vec![
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(1, 1000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(1001, 2000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(500, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(2001, 3000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(500, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(3001, 4000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(4001, 4500, schema2.time_column()),
                                generate_column_u64(
                                    500,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                ],
            ),
            (
                3,
                vec![
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(1001, 2000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(2001, 3000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(500, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(3001, 4000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(500, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(4001, 5000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(5001, 6000, schema2.time_column()),
                                generate_column_u64(
                                    1000,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    1000,
                                    vec![(0, 999)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                    (
                        RecordBatch::try_new(
                            schema2.clone().to_record_data_schema(),
                            vec![
                                generate_column_ts(6001, 6500, schema2.time_column()),
                                generate_column_u64(
                                    500,
                                    vec![],
                                    schema2.column("f1").cloned().unwrap(),
                                ),
                                generate_column_i64(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f2").cloned().unwrap(),
                                ),
                                generate_column_bool(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f3").cloned().unwrap(),
                                ),
                                generate_column_f64(
                                    500,
                                    vec![(0, 499)],
                                    schema2.column("f4").cloned().unwrap(),
                                ),
                            ],
                        )
                        .unwrap(),
                        schema2.clone(),
                    ),
                ],
            ),
        ];
        let expected_data: Vec<RecordBatch> = vec![
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(1, 1000, schema2.time_column()),
                    generate_column_u64(
                        1000,
                        vec![(0, 499)],
                        schema2.column("f1").cloned().unwrap(),
                    ),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(1000, vec![], schema2.column("f3").cloned().unwrap()),
                    generate_column_f64(1000, vec![], schema2.column("f4").cloned().unwrap()),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(1001, 2000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(1000, vec![], schema2.column("f3").cloned().unwrap()),
                    generate_column_f64(1000, vec![], schema2.column("f4").cloned().unwrap()),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(2001, 3000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(
                        1000,
                        vec![(0, 699)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        1000,
                        vec![(500, 999)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(3001, 4000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(
                        1000,
                        vec![(500, 999)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(4001, 5000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(1000, vec![], schema2.column("f2").cloned().unwrap()),
                    generate_column_bool(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(5001, 6000, schema2.time_column()),
                    generate_column_u64(1000, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f2").cloned().unwrap(),
                    ),
                    generate_column_bool(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        1000,
                        vec![(0, 999)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema2.clone().to_record_data_schema(),
                vec![
                    generate_column_ts(6001, 6500, schema2.time_column()),
                    generate_column_u64(500, vec![], schema2.column("f1").cloned().unwrap()),
                    generate_column_i64(
                        500,
                        vec![(0, 499)],
                        schema2.column("f2").cloned().unwrap(),
                    ),
                    generate_column_bool(
                        500,
                        vec![(0, 499)],
                        schema2.column("f3").cloned().unwrap(),
                    ),
                    generate_column_f64(
                        500,
                        vec![(0, 499)],
                        schema2.column("f4").cloned().unwrap(),
                    ),
                ],
            )
            .unwrap(),
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
            let mut tsm_writer = TsmWriter::open(&dir, tsm_sequence, 0, false).await.unwrap();
            for (record_batch, schema) in args.into_iter() {
                tsm_writer
                    .write_record_batch(1, SeriesKey::default(), schema, record_batch)
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

        let (version_edit, _) =
            run_compaction_job(compact_req, kernel, VnodeCompactionMetrics::fake())
                .await
                .unwrap()
                .unwrap();

        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_5() {
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
        let data1 = RecordBatch::try_new(
            schema.clone().to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3, 4], schema.time_column()),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();
        let data2 = RecordBatch::try_new(
            schema.clone().to_record_data_schema(),
            vec![
                i64_column(vec![4, 5, 6, 7], schema.time_column()),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), None],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), None],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), Some(8)],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![None, None, None, None],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

        let expected_data1 = RecordBatch::try_new(
            schema.clone().to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3, 4], schema.time_column()),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(5)],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

        let expected_data2 = RecordBatch::try_new(
            schema.clone().to_record_data_schema(),
            vec![
                i64_column(vec![4, 5, 6, 7], schema.time_column()),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), None],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), None],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(4), Some(5), Some(6), Some(8)],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![None, None, None, None],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();

        let data = vec![
            HashMap::from([(1, data1)]),
            HashMap::from([(2, data2.clone())]),
        ];

        let expected_data = HashMap::from([
            (1 as SeriesId, vec![expected_data1]),
            (2 as SeriesId, vec![expected_data2]),
        ]);

        let dir = "/tmp/test/compaction/5";
        let database = Arc::new("dba".to_string());
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) =
            write_data_blocks_to_column_file(&dir, data, schema.clone()).await;
        // println!("files:{:?}", files);
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database.clone(), opt.clone(), next_file_id, files);
        let max_file_size = compact_req
            .storage_opt
            .level_max_file_size(compact_req.out_level);
        let (version_edit, _) =
            run_compaction_job(compact_req, kernel.clone(), VnodeCompactionMetrics::fake())
                .await
                .unwrap()
                .unwrap();
        check_column_file(dir.clone(), version_edit.clone(), expected_data.clone()).await;

        let file_id = version_edit.add_files.first().unwrap().file_id;
        // println!("version_edit: {:?}", version_edit);

        let data3 = RecordBatch::try_new(
            schema.clone().to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3, 4], schema.time_column()),
                i64_some_column(
                    vec![Some(1), None, None, Some(0)],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), None, None, Some(6)],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), None, None, Some(6)],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), None, None, Some(6)],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();
        let mut tsm_writer = TsmWriter::open(&dir, file_id + 1, max_file_size, false)
            .await
            .unwrap();
        tsm_writer
            .write_record_batch(1, SeriesKey::default(), schema.clone(), data3.clone())
            .await
            .unwrap();
        // println!("tsm_writter: {:?}", tsm_writer.file_id());
        tsm_writer.finish().await.unwrap();
        let data3_bol = tsm_writer.series_bloom_filter().clone();
        // println!("3:{:?}", tsm_writer.series_bloom_filter().clone());
        let tsm_reader = TsmReader::open(tsm_writer.path()).await.unwrap();
        let pages = tsm_reader.read_series_pages(1, 0).await.unwrap();
        let real_data = decode_pages(pages, schema.clone(), None).unwrap();
        assert_eq!(data3, real_data);

        let mut cfs = Vec::new();
        let writer = TsmWriter::open(&dir, 3, 1000, false).await.unwrap();
        let mut cf = ColumnFile::new(
            3,
            2,
            TimeRange::new(writer.min_ts(), writer.max_ts()),
            writer.size() as u64,
            false,
            writer.path(),
        );

        cf.set_series_id_filter(RwLock::new(Some(Arc::new(
            writer.series_bloom_filter().clone(),
        ))));
        cfs.push(Arc::new(cf));
        let writer = TsmWriter::open(&dir, 4, 1000, false).await.unwrap();
        let mut cf = ColumnFile::new(
            4,
            2,
            TimeRange::new(writer.min_ts(), writer.max_ts()),
            writer.size() as u64,
            false,
            writer.path(),
        );
        cf.set_series_id_filter(RwLock::new(Some(Arc::new(data3_bol))));
        cfs.push(Arc::new(cf));
        let (compact_req, kernel) = prepare_compact_req_and_kernel(database, opt, 5, cfs.clone());
        // println!("cfs:{:?}", cfs.clone());
        let (version_edit, _) =
            run_compaction_job(compact_req, kernel.clone(), VnodeCompactionMetrics::fake())
                .await
                .unwrap()
                .unwrap();
        // println!("{:?}", version_edit);
        let data4 = RecordBatch::try_new(
            schema.clone().to_record_data_schema(),
            vec![
                i64_column(vec![1, 2, 3, 4], schema.time_column()),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(0)],
                    schema.column("f1").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(6)],
                    schema.column("f2").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(6)],
                    schema.column("f3").cloned().unwrap(),
                ),
                i64_some_column(
                    vec![Some(1), Some(2), Some(3), Some(6)],
                    schema.column("f4").cloned().unwrap(),
                ),
            ],
        )
        .unwrap();
        let expected_data =
            HashMap::from([(1 as SeriesId, vec![data4]), (2 as SeriesId, vec![data2])]);
        check_column_file(dir, version_edit, expected_data).await;
    }
}
