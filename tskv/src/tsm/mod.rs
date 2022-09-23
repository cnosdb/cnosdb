mod block;
pub mod codec;
mod index;
mod reader;
mod tombstone;
mod writer;

pub use block::*;
pub use index::*;
pub use reader::*;
pub use tombstone::{Tombstone, TsmTombstone};
pub use writer::*;

// MAX_BLOCK_VALUES is the maximum number of values a TSM block can store.
pub(crate) const MAX_BLOCK_VALUES: u32 = 1000;

const HEADER_SIZE: usize = 5;
const INDEX_META_SIZE: usize = 11;
const BLOCK_META_SIZE: usize = 52;
const BLOOM_FILTER_SIZE: usize = 64;
const BLOOM_FILTER_BITS: u64 = 512; // 64 * 8
const FOOTER_SIZE: usize = BLOOM_FILTER_SIZE + 8; // 72

pub trait BlockReader {
    fn decode(&mut self, block: &BlockMeta) -> crate::error::Result<DataBlock>;
}
