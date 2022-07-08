
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