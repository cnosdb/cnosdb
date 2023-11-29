use std::collections::BTreeMap;

use models::SeriesId;

use crate::tsm2::writer::DataBlock2;

pub(crate) mod page;
pub mod reader;
mod scan_config;
pub mod statistics;
mod types;
pub mod writer;

pub(crate) const MAX_BLOCK_VALUES: u32 = 1000;

const HEADER_SIZE: usize = 5;
const INDEX_META_SIZE: usize = 11;
const BLOCK_META_SIZE: usize = 44;
const BLOOM_FILTER_SIZE: usize = 64;
const BLOOM_FILTER_BITS: u64 = 512;
// 64 * 8
const FOOTER_SIZE: usize = 129;

pub type TsmWriteData = BTreeMap<String, BTreeMap<SeriesId, DataBlock2>>; // (table, (series_id, pages))

pub type ColumnGroupID = usize;
