use std::io::{Read, Seek, SeekFrom};

use integer_encoding::VarInt;
use models::ValueType;

use super::{coders, MAX_BLOCK_VALUES};
use crate::{
    direct_io::FileCursor,
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
                            println!("{:?}", datum.clone());
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
                  curr_offset: index_offset,
                  end_offset: len as u64 - 8,
                  curr: None,
                  next: None })
    }

    fn next_index_entry(&mut self) -> Result<IndexEntry> {
        let mut buf = [0u8; 2];
        let field_len = 8;
        let mut field_id = vec![0; 8];
        self.r
            .read(field_id.as_mut_slice())
            .map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += field_len as u64;

        // read the block type
        self.r.read(&mut buf[..1]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 1;
        let b_type = buf[0];

        // read how many blocks there are for this entry.
        self.r.read(&mut buf).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 2;
        let count = u16::from_be_bytes(buf);

        let typ = ValueType::try_from(b_type).unwrap();
        Ok(IndexEntry { key: field_id,
                        block_type: typ,
                        count,
                        curr_block: 1,
                        block: self.next_block_entry(typ)? })
    }

    fn next_block_entry(&mut self, field_type: ValueType) -> Result<FileBlock> {
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
        self.r.read(&mut buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let size = u64::from_be_bytes(buf);

        // read value offset
        self.r.read(&mut buf[..]).map_err(|e| Error::ReadTsmErr { reason: e.to_string() })?;
        self.curr_offset += 8;
        let val_off = u64::from_be_bytes(buf);

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

#[cfg(test)]
mod test {
    use crate::{
        file_manager::FileManager,
        tsm::{BlockReader, DataBlock, TsmBlockReader, TsmIndexReader},
    };

    #[test]
    fn tsm_reader_test() {
        let fs = FileManager::new();
        let fs = fs.open_file("./writer_test.tsm").unwrap();
        let len = fs.len();
        let mut fs_cursor = fs.into_cursor();
        let index = TsmIndexReader::try_new(&mut fs_cursor, len as usize);
        let mut blocks = Vec::new();
        for res in &mut index.unwrap() {
            let entry = res.unwrap();
            let key = entry.field_id();

            blocks.push(entry.block);
        }

        let mut block_reader = TsmBlockReader::new(&mut fs_cursor);
        let ori = DataBlock::U64 { index: 0, ts: vec![2, 3, 4], val: vec![12, 13, 15] };
        for block in blocks {
            let data = block_reader.decode(&block).expect("error decoding block data");
            assert_eq!(ori, data);
        }
    }
}
