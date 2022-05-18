use std::{
    collections::HashMap,
    io::{Seek, SeekFrom, Write},
};

use super::{block, MAX_BLOCK_VALUES};
use crate::{
    direct_io::FileCursor,
    error::{Error, Result},
    tsm::{DataBlock, FileBlock},
};

// A TSM file is composed for four sections: header, blocks, index and the footer.
//
// ┌────────┬────────────────────────────────────┬─────────────┬──────────────┐
// │ Header │               Blocks               │    Index    │    Footer    │
// │5 bytes │              N bytes               │   N bytes   │   4 bytes    │
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
//  ──────────────────────────────────────────────────────────────────────┐
// │                               Index                                  │
// ┬─────────┬──────┬───────┬─────────┬─────────┬────────┬────────┬───────┤
// │ filedId │ Type │ Count │Min Time │Max Time │ Offset │  Size  │Valoff │
// │ 8 bytes │1 byte│2 bytes│ 8 bytes │ 8 bytes │8 bytes │8 bytes │8 bytes│
// ┴─────────┴──────┴───────┴─────────┴─────────┴────────┴────────┴───────┘
//
// ┌─────────┐
// │ Footer  │
// ├─────────┤
// │Index Ofs│
// │ 8 bytes │
// └─────────┘

const HEADER_LEN: u64 = 5;
const TSM_MAGIC: u32 = 0x1346613;
const VERSION: u8 = 1;

pub struct FooterBuilder<'a> {
    writer: &'a mut FileCursor,
}
impl<'a> FooterBuilder<'a> {
    pub fn new(writer: &'a mut FileCursor) -> Self {
        Self { writer }
    }
    pub fn build(&mut self, offset: u64) -> Result<()> {
        self.writer
            .write(&mut offset.to_be_bytes().to_vec())
            .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
        Ok(())
    }
}
pub struct TsmIndexBuilder<'a> {
    writer: &'a mut FileCursor,
}

impl<'a> TsmIndexBuilder<'a> {
    pub fn new(writer: &'a mut FileCursor) -> Self {
        Self { writer }
    }

    pub fn build(&mut self, indexs: HashMap<u64, Vec<FileBlock>>) -> Result<u64> {
        let res = self.writer.pos();
        for (fid, blks) in indexs {
            let mut buf = Vec::new();
            let block = blks.first().unwrap();
            let typ: u8 = block.filed_type.into();
            let cnt: u16 = blks.len() as u16;
            buf.append(&mut fid.to_be_bytes().to_vec());
            buf.append(&mut typ.to_be_bytes().to_vec());
            buf.append(&mut cnt.to_be_bytes().to_vec());
            // build index block
            for blk in blks {
                buf.append(&mut blk.min_ts.to_be_bytes().to_vec());
                buf.append(&mut blk.max_ts.to_be_bytes().to_vec());
                buf.append(&mut blk.offset.to_be_bytes().to_vec());
                buf.append(&mut blk.size.to_be_bytes().to_vec());
                buf.append(&mut blk.val_off.to_be_bytes().to_vec());
            }
            self.writer.write(&buf).map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
        }
        Ok(res)
    }
}

pub struct TsmBlockWriter<'a> {
    writer: &'a mut FileCursor,
}

impl<'a> TsmBlockWriter<'a> {
    pub fn new(writer: &'a mut FileCursor) -> Self {
        Self { writer }
    }
    fn build(&mut self, mut block: DataBlock) -> Result<Vec<FileBlock>> {
        let filed_type = block.filed_type();
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
            if self.writer.pos() <= HEADER_LEN {
                let mut buf = Vec::with_capacity(HEADER_LEN as usize);
                buf.append(&mut TSM_MAGIC.to_be_bytes().to_vec());
                buf.append(&mut VERSION.to_be_bytes().to_vec());
                self.writer
                    .seek(SeekFrom::Start(0))
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
                self.writer
                    .write(&buf)
                    .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            }
            // fill data if err occur reset the pos
            let offset = self.writer.pos();
            self.writer
                .write(&mut crc32fast::hash(&ts_buf).to_be_bytes().to_vec())
                .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            self.writer
                .write(&ts_buf)
                .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;

            let val_off = self.writer.pos();
            self.writer
                .write(&mut crc32fast::hash(&data_buf).to_be_bytes().to_vec())
                .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            self.writer
                .write(&data_buf)
                .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
            let size = ts_buf.len() + data_buf.len() + 8;
            res.push(FileBlock { min_ts,
                                 max_ts,
                                 offset,
                                 size: size as u64,
                                 val_off,
                                 filed_type,
                                 reader_idx: 0 });
            i += 1;
        }
        Ok(res)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        direct_io::FileSync,
        file_manager::{self, FileManager},
        memcache::StrCell,
        tsm::{coders, DataBlock, FooterBuilder, TsmBlockWriter, TsmIndexBuilder},
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
        let fs = FileManager::new();
        let file = fs.create_file("./writer_test.tsm").unwrap();
        let mut fs_cursor = file.into_cursor();
        let mut rd = TsmBlockWriter::new(&mut fs_cursor);
        let data = DataBlock::U64Block { index: 0, ts: vec![2, 3, 4], val: vec![12, 13, 15] };
        let res = rd.build(data);
        let mut id = TsmIndexBuilder::new(&mut fs_cursor);
        let mut p = HashMap::new();
        p.insert(1, res.unwrap());
        let pos = id.build(p);
        let mut foot = FooterBuilder::new(&mut fs_cursor);
        let _ = foot.build(pos.unwrap());
        let _ = fs_cursor.sync_all(FileSync::Hard);
        println!("column write finsh");
    }
}
