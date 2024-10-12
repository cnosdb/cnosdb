use std::sync::Arc;

use arrow_array::RecordBatch;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use snafu::OptionExt;

use crate::error::CommonSnafu;
use crate::tsm::chunk::Chunk;
use crate::tsm::column_group::ColumnGroup;
use crate::tsm::reader::TsmReader;
use crate::tsm::ColumnGroupID;
use crate::TskvResult;

#[derive(Clone)]
pub(crate) struct CompactingBlockMeta {
    reader_idx: usize,
    compacting_file_index: usize,
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
        compacting_file_index: usize,
        tsm_reader: Arc<TsmReader>,
        chunk: Arc<Chunk>,
        column_group_id: ColumnGroupID,
    ) -> Self {
        Self {
            reader_idx: tsm_reader_idx,
            compacting_file_index,
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

    pub fn included_in_time_range(&self, time_range: &TimeRange) -> TskvResult<bool> {
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
        Ok(column_group.time_range().min_ts >= time_range.min_ts
            && column_group.time_range().max_ts <= time_range.max_ts)
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

    pub fn meta(&self) -> Arc<Chunk> {
        self.meta.clone()
    }

    pub fn column_group_id(&self) -> ColumnGroupID {
        self.column_group_id
    }

    pub fn reader_idx(&self) -> usize {
        self.reader_idx
    }

    pub fn compacting_file_index(&self) -> usize {
        self.compacting_file_index
    }
}
