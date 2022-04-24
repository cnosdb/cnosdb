mod errors;
mod file;
mod reader;
mod writer;

use crate::direct_io;
use crate::file_manager;

pub use errors::*;
pub use file::*;
pub use reader::*;
pub use writer::*;

// record
// |      4       |     2     |       1      |     1     | len  |      4       |
// +--------------+-----------+--------------+-----------+------+--------------+
// | magic_number | data_size | data_version | data_type | data | crc32_number |
// +--------------+-----------+--------------+-----------+------+--------------+
//
// the crc32_number is hash(data_size + data_version + data_type + data)

// file
// | block_size | block_size | block_size | ...
// +--------+--------+--------+--------+-------
// | record | record | record | record | ...
// +--------+--------+--------+--------+-------
const MAGIC_NUMBER: u32 = u32::from_le_bytes(['F' as u8, 'l' as u8, 'O' as u8, 'g' as u8]);
const RECORD_MAGIC_NUMBER_LEN: usize = 4;
const RECORD_DATA_SIZE_LEN: usize = 2;
const RECORD_DATA_VERSION_LEN: usize = 1;
const RECORD_DATA_TYPE_LEN: usize = 1;
const RECORD_CRC32_NUMBER_LEN: usize = 4;
const BLOCK_SIZE: usize = 4096;
const READER_BUF_SIZE: usize = 1024 * 1024 * 64; //64MB
