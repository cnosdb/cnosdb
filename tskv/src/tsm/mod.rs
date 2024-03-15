pub mod codec;
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

use crate::tsm::writer::DataBlock;

pub(crate) const MAX_BLOCK_VALUES: u32 = 1000;

const HEADER_SIZE: usize = 5;
const INDEX_META_SIZE: usize = 11;
const BLOCK_META_SIZE: usize = 44;
const BLOOM_FILTER_SIZE: usize = 64;
const BLOOM_FILTER_BITS: u64 = 512; // 64 * 8
const FOOTER_SIZE: usize = 129;

pub type TsmWriteData = BTreeMap<String, BTreeMap<SeriesId, (SeriesKey, DataBlock)>>; // (table, (series_id, pages))

pub type ColumnGroupID = usize;
