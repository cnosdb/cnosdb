use std::path::{Path, PathBuf};

use openraft::EntryPayload;

use super::{wal_store, WalType, WAL_FOOTER_MAGIC_NUMBER, WAL_HEADER_LEN};
use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::{record_file, Error, Result};

/// Reads a wal file and parse footer, returns sequence range
pub async fn read_footer(path: impl AsRef<Path>) -> Result<Option<(u64, u64)>> {
    if LocalFileSystem::try_exists(&path) {
        let reader = WalReader::open(path).await?;
        Ok(Some((reader.min_sequence, reader.max_sequence)))
    } else {
        Ok(None)
    }
}

/// Parses wal footer, returns sequence range.
pub fn parse_footer(footer: [u8; record_file::FILE_FOOTER_LEN]) -> Option<(u64, u64)> {
    let magic_number = decode_be_u32(&footer[0..4]);
    if magic_number != WAL_FOOTER_MAGIC_NUMBER {
        // There is no footer in wal file.
        return None;
    }
    let min_sequence = decode_be_u64(&footer[16..24]);
    let max_sequence = decode_be_u64(&footer[24..32]);
    Some((min_sequence, max_sequence))
}

pub struct WalReader {
    inner: record_file::Reader,
    /// Min write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    min_sequence: u64,
    /// Max write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    max_sequence: u64,
    has_footer: bool,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let reader = record_file::Reader::open(&path).await?;

        let mut has_footer = true;
        let (min_sequence, max_sequence) = match reader.footer() {
            Some(footer) => parse_footer(footer).unwrap_or((0_u64, 0_u64)),
            None => {
                has_footer = false;
                (0_u64, 0_u64)
            }
        };

        Ok(Self {
            inner: reader,
            min_sequence,
            max_sequence,
            has_footer,
        })
    }

    pub(super) fn new(
        record_reader: record_file::Reader,
        min_sequence: u64,
        max_sequence: u64,
        has_footer: bool,
    ) -> Self {
        Self {
            inner: record_reader,
            min_sequence,
            max_sequence,
            has_footer,
        }
    }

    pub fn has_footer(&self) -> bool {
        self.has_footer
    }

    pub async fn next_wal_entry(&mut self) -> Result<Option<WalRecordData>> {
        loop {
            let data = match self.inner.read_record().await {
                Ok(r) => r.data,
                Err(Error::Eof) => {
                    return Ok(None);
                }
                Err(Error::RecordFileHashCheckFailed { .. }) => continue,
                Err(e) => {
                    trace::error!("Error reading wal: {:?}", e);
                    return Err(Error::WalTruncated);
                }
            };
            return Ok(Some(WalRecordData::new(data)));
        }
    }

    pub async fn read_wal_record_data(&mut self, pos: u64) -> Result<Option<WalRecordData>> {
        self.inner.reload_metadata().await?;
        match self.inner.read_record_at(pos as usize).await {
            Ok(r) => Ok(Some(WalRecordData::new(r.data))),
            Err(Error::Eof) => Ok(None),
            Err(e @ Error::RecordFileHashCheckFailed { .. }) => Err(e),
            Err(e) => {
                trace::error!("Error reading wal: {:?}", e);
                Err(Error::WalTruncated)
            }
        }
    }

    pub fn min_sequence(&self) -> u64 {
        self.min_sequence
    }

    pub fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    pub fn path(&self) -> PathBuf {
        self.inner.path()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// If this record file has some records in it.
    pub fn is_empty(&self) -> bool {
        match self
            .len()
            .checked_sub(record_file::FILE_MAGIC_NUMBER_LEN + record_file::FILE_FOOTER_LEN)
        {
            Some(d) => d == 0,
            None => true,
        }
    }

    pub fn take_record_reader(self) -> record_file::Reader {
        self.inner
    }
}

pub struct WalRecordData {
    pub typ: WalType,
    pub seq: u64,
    pub block: Block,
}

impl WalRecordData {
    pub fn new(buf: Vec<u8>) -> WalRecordData {
        if buf.len() < WAL_HEADER_LEN {
            return Self {
                typ: WalType::Unknown,
                seq: 0,
                block: Block::Unknown,
            };
        }
        let seq = decode_be_u64(&buf[1..9]);
        let entry_type: WalType = buf[0].into();
        let block = match entry_type {
            WalType::RaftBlankLog | WalType::RaftNormalLog | WalType::RaftMembershipLog => {
                match super::decode_wal_raft_entry(&buf[WAL_HEADER_LEN..]) {
                    Ok(e) => Block::RaftLog(e),
                    Err(e) => {
                        trace::error!("Failed to decode raft entry from wal: {e}");
                        return Self {
                            typ: WalType::Unknown,
                            seq: 0,
                            block: Block::Unknown,
                        };
                    }
                }
            }

            WalType::Unknown => Block::Unknown,
        };
        Self {
            typ: entry_type,
            seq,
            block,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Block {
    RaftLog(wal_store::RaftEntry),
    Unknown,
}

pub async fn print_wal_statistics(path: impl AsRef<Path>) {
    let mut reader = WalReader::open(path).await.unwrap();
    loop {
        match reader.next_wal_entry().await {
            Ok(Some(entry_block)) => {
                println!("============================================================");
                println!("Seq: {}, Typ: {}", entry_block.seq, entry_block.typ);
                match entry_block.block {
                    Block::RaftLog(entry) => match entry.payload {
                        EntryPayload::Blank => {
                            println!("Raft log: empty");
                        }
                        EntryPayload::Normal(_data) => {
                            println!("Raft log: normal");
                        }
                        EntryPayload::Membership(_data) => {
                            println!("Raft log: membership");
                        }
                    },

                    Block::Unknown => {
                        println!("Unknown WAL entry type.");
                    }
                }
            }
            Ok(None) => {
                println!("============================================================");
                break;
            }
            Err(Error::WalTruncated) => {
                println!("============================================================");
                println!("WAL file truncated");
                break;
            }
            Err(e) => {
                panic!("Failed to read wal file: {:?}", e);
            }
        }
    }
}
