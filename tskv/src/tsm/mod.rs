pub mod chunk;
pub mod chunk_group;
pub mod codec;
pub mod column_group;
pub mod footer;
pub mod mutable_column;
pub(crate) mod page;
pub mod reader;
pub mod statistics;
mod tombstone;
mod types;
pub mod writer;

// MAX_BLOCK_VALUES is the maximum number of values a Tsm block can store.
use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use models::{SeriesId, SeriesKey};
pub use tombstone::{Tombstone, TsmTombstone, TOMBSTONE_FILE_SUFFIX};

const BLOOM_FILTER_BITS: u64 = 1024 * 1024; // 1MB
const FOOTER_SIZE: usize = 131140;

pub type TsmWriteData = BTreeMap<TskvTableSchemaRef, BTreeMap<SeriesId, (SeriesKey, RecordBatch)>>; // (table, (series_id, pages))

pub type ColumnGroupID = u64;
