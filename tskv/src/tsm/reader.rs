use std::{
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

use integer_encoding::VarInt;
use logger::info;
use models::{FieldId, ValueType};

use super::{coders, BLOOM_FILTER_SIZE, FOOTER_SIZE, MAX_BLOCK_VALUES};
use crate::{
    byte_utils::decode_be_u16,
    direct_io::{File, FileCursor},
    error::{Error, Result},
    tseries_family::TimeRange,
    tsm::{BlockReader, DataBlock, IndexEntry},
};

#[derive(Debug, Clone)]
pub struct FileBlock {
    pub min_ts: i64,
    pub max_ts: i64,
    pub offset: u64,
    pub size: u64,
    pub val_off: u64,
    pub field_type: ValueType,
    // pub field_id: u64,
    pub reader_idx: usize,
}

impl FileBlock {
    // unuse maybe remove soon
    pub fn new() -> Self {
        Self { min_ts: 0,
               max_ts: 0,
               offset: 0,
               size: 0,
               field_type: ValueType::Unknown,
               val_off: 0,
               reader_idx: 0 }
    }
}

// #[derive(Debug)]
pub struct TsmBlockReader<'a> {
    reader: &'a mut FileCursor,
}

impl<'a> TsmBlockReader<'a> {
    pub fn new(reader: &'a mut FileCursor) -> Self {
        Self { reader }
    }

    pub fn read_blocks(&mut self, blocks: &Vec<FileBlock>, time_range: &TimeRange) {
        for block in blocks {
            let mut data = self.decode(block).expect("error decoding block data");
            let mut loopp = true;
            while loopp {
                let datum = data.next();
                match datum {
                    Some(datum) => {
                        if datum.timestamp() > time_range.min_ts
                           && datum.timestamp() < time_range.max_ts
                        {
                            info!("{:?}", datum.clone());
                        }
                    },
                    None => loopp = false,
                }
            }
        }
    }
}

impl<'a> BlockReader for TsmBlockReader<'a> {
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

        let len = block.val_off - block.offset - 4; // 32-bit crc
        // first decode the timestamp block.
        let mut ts = Vec::with_capacity(MAX_BLOCK_VALUES); // 1000 is the max block size
        coders::timestamp::decode(&data[idx..idx+(len as usize)], &mut ts)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        idx += len as usize;
        //// TODO: skip 32-bit data CRC checksum at beginning of block for now
        idx += 4;
        match block.field_type {
            ValueType::Float => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::float::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

                Ok(DataBlock::F64 { index: 0, ts, val })
            },
            ValueType::Integer => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::integer::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

                Ok(DataBlock::I64 { index: 0, ts, val })
            },
            ValueType::Boolean => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::boolean::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

                Ok(DataBlock::Bool { index: 0, ts, val })
            },
            ValueType::String => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::string::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
                Ok(DataBlock::Str { index: 0, ts, val })
            },
            ValueType::Unsigned => {
                // values will be same length as time-stamps.
                let mut val = Vec::with_capacity(ts.len());
                coders::unsigned::decode(&data[idx..], &mut val)
                    .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
                Ok(DataBlock::U64 { index: 0, ts, val })
            },
            _ => {
                Err(Error::ReadTsmErr { reason: format!("cannot decode block {:?} with no unknown value type",
                                                        block.field_type) })
            },
        }
    }
}

pub struct TsmIndexReader<'a> {
    r: &'a mut FileCursor,
    buf: [u8; 8],

    curr_offset: u64,
    end_offset: u64,

    curr: Option<IndexEntry>,
    next: Option<IndexEntry>,
}

impl<'a> TsmIndexReader<'a> {
    pub fn try_new(r: &'a mut FileCursor, len: usize) -> Result<Self> {
        r.seek(SeekFrom::End(-8)).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        let mut buf = [0u8; 8];
        r.read(&mut buf).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        let index_offset = u64::from_be_bytes(buf);
        r.seek(SeekFrom::Start(index_offset))
         .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;

        Ok(Self { r,
                  buf,
                  curr_offset: index_offset,
                  end_offset: (len - FOOTER_SIZE) as u64,
                  curr: None,
                  next: None })
    }

    fn next_index_entry(&mut self) -> Result<IndexEntry> {
        // read 8-bytes field id
        self.r.read(&mut self.buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let field_id = self.buf[0..8].to_vec();

        // read 1-byte block type and 2-bytes block_count
        self.r
            .read(&mut self.buf[..3])
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 3;
        let block_type = ValueType::from(self.buf[0]);
        let count = decode_be_u16(&self.buf[1..3]);

        Ok(IndexEntry { key: field_id,
                        block_type,
                        count,
                        curr_block: 1,
                        block: self.next_block_entry(block_type)? })
    }

    fn next_block_entry(&mut self, field_type: ValueType) -> Result<FileBlock> {
        // read min time on block entry
        self.r.read(&mut self.buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let min_ts = i64::from_be_bytes(self.buf);

        // read max time on block entry
        self.r.read(&mut self.buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let max_ts = i64::from_be_bytes(self.buf);

        // read block data offset
        self.r.read(&mut self.buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let offset = u64::from_be_bytes(self.buf);

        // read block size
        self.r.read(&mut self.buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let size = u64::from_be_bytes(self.buf);

        // read value offset
        self.r.read(&mut self.buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let val_off = u64::from_be_bytes(self.buf);

        Ok(FileBlock { min_ts,
                       max_ts,
                       offset,
                       field_type,
                       size: size as u64,
                       val_off,
                       reader_idx: 0 })
    }
}

impl<'a> Iterator for TsmIndexReader<'a> {
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
