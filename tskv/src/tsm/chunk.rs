use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use models::predicate::domain::TimeRange;
use models::{SeriesId, SeriesKey};
use serde::{Deserialize, Serialize};
use snafu::IntoError;

use crate::error::{DecodeSnafu, EncodeSnafu, TsmColumnGroupSnafu};
use crate::tsm::column_group::ColumnGroup;
use crate::tsm::ColumnGroupID;

/// A chunk of data for a series at least two columns
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Chunk {
    time_range: TimeRange,

    table_name: Arc<str>,
    series_id: SeriesId,
    series_key: SeriesKey,

    next_column_group_id: ColumnGroupID,
    column_groups: BTreeMap<ColumnGroupID, Arc<ColumnGroup>>,
}

impl Chunk {
    pub fn new(
        table_name: impl Into<Arc<str>>,
        series_id: SeriesId,
        series_key: SeriesKey,
    ) -> Self {
        Self {
            time_range: TimeRange::none(),
            table_name: table_name.into(),
            series_id,
            series_key,
            next_column_group_id: 0,
            column_groups: Default::default(),
        }
    }

    pub fn update_series(&self, series_key: SeriesKey) -> Self {
        Self {
            time_range: self.time_range,
            table_name: self.table_name.clone(),
            series_id: self.series_id,
            series_key,
            next_column_group_id: self.next_column_group_id,
            column_groups: self.column_groups.clone(),
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

    pub fn table_name_owned(&self) -> Arc<str> {
        self.table_name.clone()
    }

    pub fn series_id(&self) -> SeriesId {
        self.series_id
    }

    pub fn series_key(&self) -> &SeriesKey {
        &self.series_key
    }

    pub fn serialize(&self) -> crate::TskvResult<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| EncodeSnafu.into_error(e))
    }

    pub fn deserialize(bytes: &[u8]) -> crate::TskvResult<Self> {
        bincode::deserialize(bytes).map_err(|e| DecodeSnafu.into_error(e))
    }

    pub fn push(&mut self, column_group: Arc<ColumnGroup>) -> crate::TskvResult<()> {
        if self.time_range.max_ts > column_group.time_range().min_ts {
            return Err(TsmColumnGroupSnafu {
                reason: format!(
                    "invalid column group time range, current max_ts: {}, new min_ts: {}",
                    self.time_range.max_ts,
                    column_group.time_range().min_ts
                ),
            }
            .build());
        }
        self.time_range.merge(column_group.time_range());
        if self
            .column_groups
            .contains_key(&column_group.column_group_id())
        {
            return Err(TsmColumnGroupSnafu {
                reason: format!(
                    "duplicate column group id: {}, failed push pages meta to tsm_meta",
                    column_group.column_group_id()
                ),
            }
            .build());
        }
        self.column_groups
            .insert(column_group.column_group_id(), column_group);
        Ok(())
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    /// TODO high performance cost
    pub fn schema_with_metadata(&self, meta: HashMap<String, String>) -> SchemaRef {
        if let Some((_, cg)) = self.column_group().first_key_value() {
            let fields = cg
                .pages()
                .iter()
                .map(|p| (&p.meta().column).into())
                .collect::<Vec<Field>>();
            return Arc::new(Schema::new_with_metadata(fields, meta));
        }

        Arc::new(Schema::empty())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkWriteSpec {
    series_id: SeriesId,
    chunk_offset: u64,
    chunk_size: u64,
    statics: ChunkStatics,
}

impl ChunkWriteSpec {
    pub fn new(
        series_id: SeriesId,
        chunk_offset: u64,
        chunk_size: u64,
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

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn statics(&self) -> &ChunkStatics {
        &self.statics
    }
}

/// ChunkStatics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkStatics {
    time_range: TimeRange,
}

impl ChunkStatics {
    pub fn new(time_range: TimeRange) -> Self {
        Self { time_range }
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }
}
