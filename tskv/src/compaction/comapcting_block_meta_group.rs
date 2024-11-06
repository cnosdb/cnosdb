use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::StreamExt;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::{TableColumn, TskvTableSchema};
use models::schema::TIME_FIELD_NAME;
use models::SeriesId;
use snafu::{OptionExt, ResultExt};
use trace::trace;

use crate::compaction::compacting_block_meta::CompactingBlockMeta;
use crate::compaction::metrics::VnodeCompactionMetrics;
use crate::compaction::{CompactingBlock, CompactingFile};
use crate::error::{ArrowSnafu, CommonSnafu, ModelSnafu};
use crate::reader::sort_merge::sort_merge;
use crate::reader::{SchemableMemoryBatchReaderStream, SendableSchemableTskvRecordBatchStream};
use crate::tsm::chunk::Chunk;
use crate::tsm::reader::decode_pages_buf;
use crate::TskvResult;

/// Temporary compacting data block meta
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
            chunk: blk_meta.meta(),
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

    pub async fn merge_with_previous_block(
        mut self,
        previous_block: Option<CompactingBlock>,
        max_block_size: usize,
        time_range: &TimeRange,
        compacting_files: &mut [CompactingFile],
        metrics: &mut VnodeCompactionMetrics,
    ) -> TskvResult<Vec<CompactingBlock>> {
        if self.blk_metas.is_empty() {
            return Ok(vec![]);
        }
        self.blk_metas.sort_by_key(|a| a.reader_idx());

        let table_schema = self.blk_metas[0].table_schema().context(CommonSnafu {
            reason: format!(
                "table schema not found for table {}",
                self.blk_metas[0].meta().table_name()
            ),
        })?;

        if self.blk_metas.len() == 1
            && !compacting_files[self.blk_metas[0].compacting_file_index()].has_tombstone()
            && self.blk_metas[0].included_in_time_range(time_range)?
        {
            // Only one compacting block and has no tombstone, write as raw block.
            trace::trace!("only one compacting block without tombstone and time_range is entirely included by target level, handled as raw block");
            let meta_0 = &self.blk_metas[0].meta();
            let column_group_id = self.blk_metas[0].column_group_id();
            let column_group = self.blk_metas[0].column_group()?;
            metrics.read_begin();
            let buf_0 = compacting_files[self.blk_metas[0].compacting_file_index()]
                .get_raw_data(&self.blk_metas[0])
                .await?;
            metrics.read_end();

            if column_group.row_len() >= max_block_size {
                // Raw data block is full, so do not merge with the previous, directly return.
                let mut compacting_blocks = Vec::with_capacity(2);
                if let Some(blk) = previous_block {
                    compacting_blocks.push(blk);
                }
                compacting_blocks.push(CompactingBlock::raw(
                    meta_0.clone(),
                    table_schema,
                    column_group_id,
                    buf_0,
                ));

                return Ok(compacting_blocks);
            }
            if let Some(compacting_block) = previous_block {
                // Raw block is not full, so decode and merge with compacting_block.
                let chunk = self.blk_metas[0].meta();
                let column_group_id = self.blk_metas[0].column_group_id();
                let decoded_raw_record_batch =
                    decode_pages_buf(&buf_0, chunk, column_group_id, table_schema.clone())?;
                let record_batch = compacting_block.decode_opt(*time_range)?;
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
                Ok(merged_blks)
            } else {
                // Raw block is not full, but nothing to merge with, directly return.
                Ok(vec![CompactingBlock::raw(
                    meta_0.clone(),
                    table_schema,
                    column_group_id,
                    buf_0,
                )])
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
                    let cf = &mut compacting_files[blk_meta.compacting_file_index()];
                    if let Some(record_batch) = cf.get_record_batch(blk_meta).await? {
                        record_batches.push(record_batch);
                    }
                }
                if let Some(blk) = previous_block {
                    record_batches.push(blk.decode_opt(*time_range)?);
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
