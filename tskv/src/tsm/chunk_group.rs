use std::collections::BTreeMap;
use std::sync::Arc;

use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::{TskvTableSchema, TskvTableSchemaRef};
use serde::{Deserialize, Serialize};
use snafu::IntoError;

use crate::error::{DecodeSnafu, EncodeSnafu};
use crate::tsm::chunk::ChunkWriteSpec;
use crate::TskvResult;

/// A group of chunks for a table
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct ChunkGroup {
    chunks: Vec<ChunkWriteSpec>,
}

impl ChunkGroup {
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }
    pub fn serialize(&self) -> TskvResult<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| EncodeSnafu.into_error(e))
    }

    pub fn deserialize(bytes: &[u8]) -> TskvResult<Self> {
        bincode::deserialize(bytes).map_err(|e| DecodeSnafu.into_error(e))
    }

    pub fn push(&mut self, chunk: ChunkWriteSpec) {
        self.chunks.push(chunk);
    }

    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    pub fn time_range(&self) -> TimeRange {
        let mut time_range = TimeRange::none();
        for chunk in self.chunks.iter() {
            time_range.merge(chunk.statics().time_range());
        }
        time_range
    }

    pub fn chunks(&self) -> &[ChunkWriteSpec] {
        &self.chunks
    }
}

pub type TableId = u64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkGroupWriteSpec {
    pub(crate) table_schema: Arc<TskvTableSchema>,
    pub(crate) chunk_group_offset: u64,
    pub(crate) chunk_group_size: u64,
    pub(crate) time_range: TimeRange,
    pub(crate) count: usize,
}

impl ChunkGroupWriteSpec {
    pub fn new(
        table_schema: TskvTableSchemaRef,
        chunk_group_offset: u64,
        chunk_group_size: u64,
        time_range: TimeRange,
        count: usize,
    ) -> Self {
        Self {
            table_schema,
            chunk_group_offset,
            chunk_group_size,
            time_range,
            count,
        }
    }

    pub fn name(&self) -> &str {
        &self.table_schema.name
    }

    pub fn chunk_group_offset(&self) -> u64 {
        self.chunk_group_offset
    }

    pub fn chunk_group_size(&self) -> u64 {
        self.chunk_group_size
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    pub fn count(&self) -> usize {
        self.count
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChunkGroupMeta {
    // table name -> chunk group meta
    tables: BTreeMap<Arc<str>, ChunkGroupWriteSpec>,
}

impl Default for ChunkGroupMeta {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkGroupMeta {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    pub fn serialize(&self) -> TskvResult<Vec<u8>> {
        bincode::serialize(&self).map_err(|e| EncodeSnafu.into_error(e))
    }

    pub fn deserialize(bytes: &[u8]) -> TskvResult<Self> {
        bincode::deserialize(bytes).map_err(|e| DecodeSnafu.into_error(e))
    }

    pub fn push(&mut self, table: ChunkGroupWriteSpec) {
        self.tables.insert(table.table_schema.name.clone(), table);
    }

    pub fn len(&self) -> usize {
        self.tables.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    pub fn time_range(&self) -> TimeRange {
        let mut time_range = TimeRange::none();
        for (_, table) in self.tables.iter() {
            time_range.merge(&table.time_range);
        }
        time_range
    }

    pub fn tables(&self) -> &BTreeMap<Arc<str>, ChunkGroupWriteSpec> {
        &self.tables
    }

    pub fn table_schema(&self, table_name: &str) -> Option<Arc<TskvTableSchema>> {
        self.tables.get(table_name).map(|t| t.table_schema.clone())
    }
}
