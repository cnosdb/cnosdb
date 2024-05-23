mod block;
pub mod codec;
mod index;
mod reader;
mod tombstone;
mod writer;

pub use block::{DataBlock, DataBlockReader, EncodedDataBlock};
pub use index::{
    get_data_block_meta_unchecked, get_index_meta_unchecked, BlockEntry, BlockMeta, Index,
    IndexEntry, IndexMeta,
};
pub use reader::{
    decode_data_block, print_tsm_statistics, BlockMetaIterator, IndexFile, IndexIterator,
    ReadTsmError, ReadTsmResult, TsmReader,
};
pub use tombstone::{
    tombstone_compact_tmp_path, Tombstone, TsmTombstone, TsmTombstoneCache, TOMBSTONE_FILE_SUFFIX,
};
pub use writer::{new_tsm_writer, TsmVersion, TsmWriter, WriteTsmError, WriteTsmResult};

#[cfg(test)]
pub mod test {
    pub use super::reader::test::read_and_check;
    pub use super::tombstone::test::write_to_tsm_tombstone_v2;
    pub use super::writer::test::write_to_tsm;
}

// MAX_BLOCK_VALUES is the maximum number of values a TSM block can store.
pub(crate) const MAX_BLOCK_VALUES: u32 = 1000;

const HEADER_SIZE: usize = 5;
const INDEX_META_SIZE: usize = 11;
const BLOCK_META_SIZE: usize = 44;
const BLOOM_FILTER_SIZE: usize = 1024 * 1024 / 8; // 128KB
const BLOOM_FILTER_BITS: u64 = 1024 * 1024; // 1MB
const FOOTER_SIZE: usize = BLOOM_FILTER_SIZE + 8 + 4; // 72

const OLD_BLOOM_FILTER_SIZE: usize = 64;
const OLD_BLOOM_FILTER_BITS: usize = 512;
const OLD_FOOTER_SIZE: usize = OLD_BLOOM_FILTER_SIZE + 8;
const FOOTER_MAGIC_V1: &str = "TSM1";
const FOOTER_MAGIC_LEN: u64 = 4;

pub trait BlockReader {
    fn decode(&mut self, block: &BlockMeta) -> crate::error::Result<DataBlock>;
}
