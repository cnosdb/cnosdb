use std::io::{Read, Seek, SeekFrom};

use integer_encoding::VarInt;
use models::ValueType;

use super::{coders, MAX_BLOCK_VALUES};
use crate::{
    error::{Error, Result},
    BlockReader, DataBlock, FileCursor, IndexEntry,
};

#[derive(Debug, Clone)]
pub struct FileBlock {
    pub min_ts: i64,
    pub max_ts: i64,
    pub offset: u64,
    pub size: u64,
    pub filed_type: ValueType,
    // pub filed_id: u64,
    pub reader_idx: usize,
}

impl FileBlock {
    // unuse maybe remove soon
    pub fn new() -> Self {
        Self { min_ts: 0,
               max_ts: 0,
               offset: 0,
               size: 0,
               filed_type: ValueType::Unknown,
               //    filed_id: 0,
               reader_idx: 0 }
    }
}

// #[derive(Debug)]
pub struct TsmBlockReader {
    reader: FileCursor,
}

impl TsmBlockReader {
    pub fn new(r: FileCursor) -> Self {
        Self { reader: r }
    }
}

impl BlockReader for TsmBlockReader {
    fn decode(&mut self, block: &FileBlock) -> Result<DataBlock> {
        self.reader
            .seek(SeekFrom::Start(block.offset))
            .map_err(|e| Error::ReadTsmErr { reason: ("seek tsmblock err".to_string()) })?;

        let mut data: Vec<u8> = vec![0; block.size as usize];
        self.reader
            .read(&mut data)
            .map_err(|e| Error::ReadTsmErr { reason: ("read tsmblock err".to_string()) })?;

        // TODO: skip 32-bit CRC checksum at beginning of block for now
        let mut idx = 4;

        // first decode the timestamp block.
        let mut ts = Vec::with_capacity(MAX_BLOCK_VALUES); // 1000 is the max block size
        // size of timestamp block 需要确认一下
        let (len, n) = u64::decode_var(&data[idx..]).ok_or_else(|| {
                                                        Error::ReadTsmErr {
                    reason: "unable to decode timestamp".to_string(),
                }
                                                    })?;

        idx += n;
        coders::timestamp::decode(&data[idx..idx + (len as usize)], &mut ts)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        idx += len as usize;
        //// TODO: skip 32-bit data CRC checksum at beginning of block for now
        idx += 4;
        match block.filed_type {
            ValueType::Float => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::float::decode_influxdb(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

                Ok(DataBlock::F64Block { index: 0, ts, val })
            },
            ValueType::Integer => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::integer::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

                Ok(DataBlock::I64Block { index: 0, ts, val })
            },
            ValueType::Boolean => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::boolean::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

                Ok(DataBlock::BoolBlock { index: 0, ts, val })
            },
            ValueType::String => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::string::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
                Ok(DataBlock::StrBlock { index: 0, ts, val })
            },
            ValueType::Unsigned => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::unsigned::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
                Ok(DataBlock::U64Block { index: 0, ts, val })
            },
            _ => Err(Error::ReadTsmErr {
                reason: format!(
                    "cannot decode block {:?} with no unknown value type",
                    block.filed_type
                )
                .to_string(),
            }),
        }
    }
}

pub struct TsmIndexReader {
    r: FileCursor,

    curr_offset: u64,
    end_offset: u64,

    curr: Option<IndexEntry>,
    next: Option<IndexEntry>,
}

impl TsmIndexReader {
    pub fn try_new(mut r: FileCursor, len: usize) -> Result<Self> {
        r.seek(SeekFrom::End(-8)).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        let mut buf = [0u8; 8];
        r.read(&mut buf).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

        let index_offset = u64::from_be_bytes(buf);
        r.seek(SeekFrom::Start(index_offset))
         .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

        Ok(Self { r,
                  curr_offset: index_offset,
                  end_offset: len as u64 - 8,
                  curr: None,
                  next: None })
    }

    fn next_index_entry(&mut self) -> Result<IndexEntry> {

        let mut buf = [0u8; 2];
        let filed_len = 8;
        let mut filed_id = vec![0; 8];
        self.r
            .read(filed_id.as_mut_slice())
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += filed_len as u64;

        // read the block type
        self.r.read(&mut buf[..1]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 1;
        let b_type = buf[0];

        // read how many blocks there are for this entry.
        self.r.read(&mut buf).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 2;
        let count = u16::from_be_bytes(buf);

        let typ = ValueType::try_from(b_type).unwrap();
        Ok(IndexEntry { key: filed_id,
                        block_type: typ,
                        count,
                        curr_block: 1,
                        block: self.next_block_entry(typ)? })
    }

    fn next_block_entry(&mut self, filed_type: ValueType) -> Result<FileBlock> {
        // read min time on block entry
        let mut buf = [0u8; 8];
        self.r.read(&mut buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let min_ts = i64::from_be_bytes(buf);

        // read max time on block entry
        self.r.read(&mut buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let max_ts = i64::from_be_bytes(buf);

        // read block data offset
        self.r.read(&mut buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let offset = u64::from_be_bytes(buf);

        // read block size
        self.r.read(&mut buf[..4]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 4;
        let size = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);

        Ok(FileBlock { min_ts, max_ts, offset, filed_type, size: size as u64, reader_idx: 0 })
    }
}

impl Iterator for TsmIndexReader {
    type Item = Result<IndexEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr_offset == self.end_offset {
            // end of entries
            return None;
        }

        match &self.curr {
            Some(curr) => {
                if curr.curr_block < curr.count {
                    let mut next = curr.clone();
                    match self.next_block_entry(next.block_type) {
                        Ok(block) => next.block = block,
                        Err(e) => return Some(Err(e)),
                    }
                    next.curr_block += 1;
                    self.next = Some(next);
                } else {
                    // no more block entries. Move onto the next entry.
                    match self.next_index_entry() {
                        Ok(entry) => self.next = Some(entry),
                        Err(e) => return Some(Err(e)),
                    }
                }
            },
            None => match self.next_index_entry() {
                Ok(entry) => self.next = Some(entry),
                Err(e) => return Some(Err(e)),
            },
        }

        self.curr = self.next.clone();
        Some(Ok(self.curr.clone().unwrap()))
    }
}

#[cfg(test)]
mod test {
    use crate::{file_manager, FileManager, TsmBlockReader};

    #[test]
    fn tsm_reader_test() {
        let fs = FileManager::new();
        let fs = fs.create_file("./reader_test.log").unwrap();
        let fs_cursor = fs.into_cursor();
        let rd = TsmBlockReader::new(fs_cursor);
    }
}
