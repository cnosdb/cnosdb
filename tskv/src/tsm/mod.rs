pub mod chunk;
pub mod chunk_group;
pub mod codec;
pub mod column_group;
pub mod data_block;
pub mod footer;
pub(crate) mod page;
pub mod reader;
pub mod statistics;
mod tombstone;
mod types;
pub mod writer;

// MAX_BLOCK_VALUES is the maximum number of values a Tsm block can store.
use std::collections::BTreeMap;

use models::{SeriesId, SeriesKey};
pub use tombstone::{Tombstone, TsmTombstone, TOMBSTONE_FILE_SUFFIX};

use crate::tsm::data_block::DataBlock;

const BLOOM_FILTER_BITS: u64 = 512; // 64 * 8
const FOOTER_SIZE: usize = 129;

pub type TsmWriteData = BTreeMap<String, BTreeMap<SeriesId, (SeriesKey, DataBlock)>>; // (table, (series_id, pages))

pub type ColumnGroupID = u64;
