use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use models::predicate::domain::TimeRange;
use models::{SeriesId, SeriesKey};
use serde::{Deserialize, Serialize};

use crate::tsm::column_group::ColumnGroup;
use crate::tsm::ColumnGroupID;
use crate::TskvError;

/// A chunk of data for a series at least two columns
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Chunk {
    time_range: TimeRange,

    table_name: String,
    series_id: SeriesId,
    series_key: SeriesKey,

    next_column_group_id: ColumnGroupID,
    column_groups: BTreeMap<ColumnGroupID, Arc<ColumnGroup>>,
}

impl Chunk {
    pub fn new(table_name: String, series_id: SeriesId, series_key: SeriesKey) -> Self {
        Self {
            time_range: TimeRange::none(),
            table_name,
            series_id,
            series_key,
            next_column_group_id: 0,
            column_groups: Default::default(),
        }
    }

    pub fn min_ts(&self) -> i64 {
        self.time_range.min_ts
    }

    pub fn max_ts(&self) -> i64 {
        self.time_range.max_ts
    }

    pub fn len(&self) -> usize {
        self.column_groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.column_groups.is_empty()
    }

    pub fn column_group(&self) -> &BTreeMap<ColumnGroupID, Arc<ColumnGroup>> {
        &self.column_groups
    }

    pub fn next_column_group_id(&mut self) -> ColumnGroupID {
        let id = self.next_column_group_id;
        self.next_column_group_id += 1;
        id
    }

    pub fn current_next_column_group_id(&self) -> ColumnGroupID {
        self.next_column_group_id
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }

    pub fn series_key(&self) -> &SeriesKey {
        &self.series_key
    }

    pub fn serialize(&self) -> crate::TskvResult<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| TskvError::Serialize { source: e.into() })
    }

    pub fn deserialize(bytes: &[u8]) -> crate::TskvResult<Self> {
        bincode::deserialize(bytes).map_err(|e| TskvError::Deserialize { source: e.into() })
    }

    pub fn push(&mut self, column_group: Arc<ColumnGroup>) -> crate::TskvResult<()> {
        if self.time_range.max_ts > column_group.time_range().min_ts {
            return Err(TskvError::TsmColumnGroupError {
                reason: format!(
                    "invalid column group time range, current max_ts: {}, new min_ts: {}",
                    self.time_range.max_ts,
                    column_group.time_range().min_ts
                ),
            });
        }
        self.time_range.merge(column_group.time_range());
        if self
            .column_groups
            .get(&column_group.column_group_id())
            .is_some()
        {
            return Err(TskvError::TsmColumnGroupError {
                reason: format!(
                    "duplicate column group id: {}, failed push pages meta to tsm_meta",
                    column_group.column_group_id()
                ),
            });
        }
        self.column_groups
            .insert(column_group.column_group_id(), column_group);
        Ok(())
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    /// TODO high performance cost
    pub fn schema(&self) -> SchemaRef {
        if let Some((_, cg)) = self.column_group().first_key_value() {
            let fields = cg
                .pages()
                .iter()
                .map(|p| (&p.meta().column).into())
                .collect::<Vec<Field>>();
            return Arc::new(Schema::new(fields));
        }

        Arc::new(Schema::empty())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkWriteSpec {
    pub(crate) series_id: SeriesId,
    pub(crate) chunk_offset: u64,
    pub(crate) chunk_size: usize,
    pub(crate) statics: ChunkStatics,
}

impl ChunkWriteSpec {
    pub fn new(
        series_id: SeriesId,
        chunk_offset: u64,
        chunk_size: usize,
        statics: ChunkStatics,
    ) -> Self {
        Self {
            series_id,
            chunk_offset,
            chunk_size,
            statics,
        }
    }

    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }

    pub fn chunk_offset(&self) -> u64 {
        self.chunk_offset
    }

    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub fn statics(&self) -> &ChunkStatics {
        &self.statics
    }
}

/// ChunkStatics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkStatics {
    pub(crate) time_range: TimeRange,
}
