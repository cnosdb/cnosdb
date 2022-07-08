use std::{
    collections::HashMap,
    io::{Seek, SeekFrom, Write},
};

use models::FieldId;
use snafu::ResultExt;
use utils::{BkdrHasher, BloomFilter};

use super::{block, IndexEntry, MAX_BLOCK_VALUES};
use crate::{
    direct_io::{FileCursor, FileSync},
    error::{self, Error, Result},
    new_bloom_filter,
    tsm::{DataBlock, FileBlock},
};

// A TSM file is composed for four sections: header, blocks, index and the footer.
//
// ┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
// │ Header │               Blocks               │    Index    │    Footer    │
// │5 bytes │              N bytes               │   N bytes   │   8 bytes    │
// └────────┴────────────────────────────────────┴─────────────┴──────────────┘
//
// ┌───────────────────┐
// │      Header       │
// ├─────────┬─────────┤
// │  Magic  │ Version │
// │ 4 bytes │ 1 byte  │
// └─────────┴─────────┘
//
// ┌───────────────────────────────────────┐
// │               Blocks                  │
// ├───────────────────┬───────────────────┤
// │                Block                  │
// ├─────────┬─────────┼─────────┬─────────┼
// │  CRC    │ ts      │  CRC    │  value  │
// │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
// └─────────┴─────────┴─────────┴─────────┴
//
// ┌──────────────────────────────────────────────────────────────────────┐
// │                               Index                                  │
// ├─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───────┤
// │ fieldId │ Type │ Count │Min Time │Max Time │ Offset │  Size  │Valoff │
// │ 8 bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │8 bytes │8 bytes│
// └─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───────┘
//
// ┌─────────────────────────┐
// │ Footer                  │
// ├───────────────┬─────────┤
// │ Bloom Filter  │Index Ofs│
// │ 8 bytes       │ 8 bytes │
// └───────────────┴─────────┘

const HEADER_LEN: u64 = 5;
const TSM_MAGIC: u32 = 0x1346613;
const VERSION: u8 = 1;

pub trait TsmWriter {
    fn write_header(&mut self) -> Result<usize>;
    fn write_blocks(&mut self) -> Result<usize>;
    fn write_index(&mut self) -> Result<usize>;
    fn write_footer(&mut self) -> Result<usize>;
    fn commit(&mut self) -> Result<()>;

    fn write(&mut self) -> Result<usize> {
        let mut size = 0_usize;
        self.write_header()
            .and_then(|i| {
                size += i;
                self.write_blocks()
            })
            .and_then(|i| {
                size += i;
                self.write_index()
            })
            .and_then(|i| {
                size += i;
                self.write_footer()
            })
            .and_then(|size| self.commit())
            .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;

        Ok(size)
    }
}

pub struct TsmHeaderWriter {}

impl TsmHeaderWriter {
    pub fn write_to(writer: &mut FileCursor) -> Result<()> {
        writer.write(TSM_MAGIC.to_be_bytes().as_ref())
              .and_then(|_| writer.write(&VERSION.to_be_bytes()[..]))
              .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;

        Ok(())
    }
}

pub struct TsmFooterWriter {}

impl TsmFooterWriter {
    pub fn write_to(writer: &mut FileCursor,
                    bloom_filter: &BloomFilter,
                    index_offset: u64)
                    -> Result<()> {
        writer.write(bloom_filter.bytes())
              .and_then(|_| writer.write(&index_offset.to_be_bytes()[..]))
              .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;

        Ok(())
    }
}
pub struct TsmIndexWriter {}

impl TsmIndexWriter {
    pub fn write_to(writer: &mut FileCursor,
                    indexs: HashMap<FieldId, Vec<FileBlock>>)
                    -> Result<BloomFilter> {
        let mut bloom_filter = new_bloom_filter();
        for (fid, blks) in indexs {
            let mut buf = Vec::new();
            let block = blks.first().unwrap();
            let typ: u8 = block.field_type.into();
            let cnt: u16 = blks.len() as u16;
            buf.extend_from_slice(&fid.to_be_bytes()[..]);
            buf.extend_from_slice(&typ.to_be_bytes()[..]);
            buf.extend_from_slice(&cnt.to_be_bytes()[..]);
            // build index block
            for blk in blks {
                buf.extend_from_slice(&blk.min_ts.to_be_bytes()[..]);
                buf.extend_from_slice(&blk.max_ts.to_be_bytes()[..]);
                buf.extend_from_slice(&blk.offset.to_be_bytes()[..]);
                buf.extend_from_slice(&blk.size.to_be_bytes()[..]);
                buf.extend_from_slice(&blk.val_off.to_be_bytes()[..]);
                bloom_filter.insert(&fid.to_be_bytes()[..]);
            }
            writer.write(&buf[..buf.len()])
                  .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
        }
        Ok(bloom_filter)
    }
}

pub struct TsmBlockWriter {}

impl TsmBlockWriter {
    pub(crate) fn write_to(writer: &mut FileCursor,
                           mut block_set: HashMap<FieldId, DataBlock>)
                           -> Result<HashMap<FieldId, Vec<FileBlock>>> {
        let mut res = HashMap::new();
        for (fid, block) in block_set.iter_mut() {
            let index = Self::write_one_to(writer, block)?;
            res.insert(*fid, index);
        }
        Ok(res)
    }

    fn write_one_to(writer: &mut FileCursor, block: &DataBlock) -> Result<Vec<FileBlock>> {
        let field_type = block.field_type();
        let len = block.len();
        let n = (len - 1) / MAX_BLOCK_VALUES + 1;
        let mut res = Vec::with_capacity(n);
        let mut i = 0;
        let mut last_index = 0;
        while i < n {
            let start = last_index;
            let end = len % MAX_BLOCK_VALUES + i * MAX_BLOCK_VALUES;
            last_index = end;
            let (min_ts, max_ts) = block.time_range(start, end);
            let (ts_buf, data_buf) = block.encode(start, end)?;
            // fill data if err occur reset the pos
            let offset = writer.pos();
            writer.write(&crc32fast::hash(&ts_buf).to_be_bytes()[..])
                  .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            writer.write(&ts_buf).map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;

            let val_off = writer.pos();
            writer.write(&crc32fast::hash(&data_buf).to_be_bytes()[..])
                  .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            writer.write(&data_buf).map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            let size = ts_buf.len() + data_buf.len() + 8;
            res.push(FileBlock { min_ts,
                                 max_ts,
                                 offset,
                                 size: size as u64,
                                 val_off,
                                 field_type,
                                 reader_idx: 0 });
            i += 1;
        }
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use models::FieldId;

    use crate::{
        direct_io::FileSync,
        file_manager::{self, get_file_manager, FileManager},
        memcache::StrCell,
        tsm::{
            coders, DataBlock, FileBlock, TsmBlockWriter, TsmFooterWriter, TsmHeaderWriter,
            TsmIndexWriter,
        },
    };

    #[test]
    fn test_str_encode() {
        // let block = DataBlock::new(10, crate::DataType::Str(StrCell{ts:1, val: vec![]}));
        // block.insert(crate::DataType::Str(StrCell{ts:1, val: vec![1]}));
        // block.insert(crate::DataType::Str(StrCell{ts:2, val: vec![2]}));
        // block.insert(crate::DataType::Str(StrCell{ts:3, val: vec![3]}));
        let mut data = vec![];
        let str = vec![vec![1_u8]];
        let tmp: Vec<&[u8]> = str.iter().map(|x| &x[..]).collect();
        let _ = coders::string::encode(&tmp, &mut data);
    }
    #[test]
    fn test_tsm_write() {
        let fs = get_file_manager();
        let file = fs.create_file("./writer_test.tsm").unwrap();
        let mut fs_cursor = file.into_cursor();

        TsmHeaderWriter::write_to(&mut fs_cursor).unwrap();

        let data = vec![DataBlock::U64 { index: 0, ts: vec![2, 3, 4], val: vec![12, 13, 15] },
                        DataBlock::U64 { index: 0, ts: vec![2, 3, 4], val: vec![101, 102, 103] }];

        let mut file_block_map: HashMap<FieldId, Vec<FileBlock>> = HashMap::new();
        for (k, v) in data.iter().enumerate() {
            let file_blocks = TsmBlockWriter::write_one_to(&mut fs_cursor, v).unwrap();
            file_block_map.insert(k as FieldId, file_blocks);
        }

        let index_pos = fs_cursor.pos();
        let bloom_filter = TsmIndexWriter::write_to(&mut fs_cursor, file_block_map).unwrap();
        let _ = TsmFooterWriter::write_to(&mut fs_cursor, &bloom_filter, index_pos);
        let _ = fs_cursor.sync_all(FileSync::Hard);
        println!("column write finsh");
    }
}
