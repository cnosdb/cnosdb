use std::path::Path;

use openraft::EntryPayload;
use trace::error;

use super::{wal_store, WalType, WAL_HEADER_LEN};
use crate::byte_utils::decode_be_u64;
use crate::error::{CommonSnafu, WalTruncatedSnafu};
use crate::{record_file, TskvError, TskvResult};

pub struct WalReader {
    compress: String,
    inner: record_file::Reader,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>, compress: String) -> TskvResult<Self> {
        let reader = record_file::Reader::open(&path).await?;

        Ok(Self {
            compress,
            inner: reader,
        })
    }

    pub(super) fn new(record_reader: record_file::Reader, compress: String) -> Self {
        Self {
            compress,
            inner: record_reader,
        }
    }

    pub async fn next_wal_entry(&mut self) -> TskvResult<Option<WalRecordData>> {
        loop {
            match self.inner.read_record().await {
                Ok(r) => {
                    let record = WalRecordData::new(r.data, r.pos, &self.compress)?;
                    return Ok(Some(record));
                }
                Err(TskvError::Eof) => return Ok(None),
                Err(TskvError::RecordFileHashCheckFailed { .. }) => {
                    error!("next wal entry Record file hash check failed.");
                    continue;
                }
                Err(e) => {
                    error!("Error reading truncated wal: {:?}", e);
                    return Err(WalTruncatedSnafu.build());
                }
            }
        }
    }

    pub async fn read_wal_record_data(&mut self, pos: u64) -> TskvResult<Option<WalRecordData>> {
        self.inner.reload_metadata().await?;
        match self.inner.read_record_at(pos).await {
            Ok(r) => {
                let data = WalRecordData::new(r.data, pos, &self.compress)?;
                Ok(Some(data))
            }
            Err(TskvError::Eof) => Ok(None),
            Err(e @ TskvError::RecordFileHashCheckFailed { .. }) => Err(e),
            Err(e) => {
                error!("Error reading truncated wal: {:?}", e);
                Err(WalTruncatedSnafu.build())
            }
        }
    }

    /// If this record file has some records in it.
    pub fn is_empty(&self) -> bool {
        match self
            .inner
            .len()
            .checked_sub((record_file::FILE_MAGIC_NUMBER_LEN + record_file::FILE_FOOTER_LEN) as u64)
        {
            Some(d) => d == 0,
            None => true,
        }
    }

    pub fn take_record_reader(self) -> record_file::Reader {
        self.inner
    }

    pub fn pos(&self) -> u64 {
        self.inner.pos() as u64
    }
}

#[derive(Debug)]
pub struct WalRecordData {
    pub pos: u64,
    pub typ: WalType,
    pub seq: u64,
    pub block: wal_store::RaftEntry,
}

impl WalRecordData {
    pub fn new(buf: Vec<u8>, pos: u64, encode: &str) -> TskvResult<WalRecordData> {
        if buf.len() < WAL_HEADER_LEN {
            return Err(CommonSnafu {
                reason: format!("Decode wal record data to short: {}", buf.len()),
            }
            .build());
        }

        let seq = decode_be_u64(&buf[1..9]);
        let typ: WalType = buf[0].into();
        let block = super::decode_wal_raft_entry(&buf[WAL_HEADER_LEN..], encode)?;
        Ok(Self {
            pos,
            seq,
            typ,
            block,
        })
    }
}

pub async fn print_wal_statistics(path: impl AsRef<Path>, compress: String) {
    let mut reader = WalReader::open(path, compress).await.unwrap();
    loop {
        let pos = reader.pos();
        match reader.read_wal_record_data(pos).await {
            Ok(Some(entry_block)) => {
                println!("============================================================");
                println!("Seq: {}, Typ: {}", entry_block.seq, entry_block.typ);
                match entry_block.block.payload {
                    EntryPayload::Blank => {
                        println!("Raft log: empty");
                    }
                    EntryPayload::Normal(_data) => {
                        println!("Raft log: normal");
                    }
                    EntryPayload::Membership(_data) => {
                        println!("Raft log: membership");
                    }
                };
            }
            Ok(None) => {
                println!("============================================================");
                break;
            }
            Err(TskvError::WalTruncated {
                location,
                backtrace,
            }) => {
                println!("============================================================");
                println!("WAL file truncated");
                println!("location: {:?}", location);
                println!("backtrace: {:?}", backtrace);
                break;
            }
            Err(e) => {
                panic!("Failed to read wal file: {:?}", e);
            }
        }
    }
}
