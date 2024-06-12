use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::EntryPayload;
use trace::error;

use super::{wal_store, WalType, WAL_FOOTER_MAGIC_NUMBER, WAL_HEADER_LEN};
use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::error::{CommonSnafu, WalTruncatedSnafu};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::file_utils::get_wal_file_id;
use crate::kv_option::WalOptions;
use crate::record_file::Record;
use crate::wal::writer::WalWriter;
use crate::{record_file, TskvError, TskvResult};

/// Reads a wal file and parse footer, returns sequence range
pub async fn read_footer(
    path: impl AsRef<Path>,
    config: Arc<WalOptions>,
) -> TskvResult<Option<(u64, u64)>> {
    if LocalFileSystem::try_exists(&path) {
        let reader = WalReader::open(path, config).await?;
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

    config: Arc<WalOptions>,
    read_sequence: u64,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>, config: Arc<WalOptions>) -> TskvResult<Self> {
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
            config,
            read_sequence: 0,
        })
    }

    pub(super) fn new(
        record_reader: record_file::Reader,
        min_sequence: u64,
        max_sequence: u64,
        has_footer: bool,
        config: Arc<WalOptions>,
    ) -> Self {
        Self {
            inner: record_reader,
            min_sequence,
            max_sequence,
            has_footer,
            config,
            read_sequence: 0,
        }
    }

    pub fn has_footer(&self) -> bool {
        self.has_footer
    }

    pub async fn next_wal_entry(&mut self) -> TskvResult<Option<Record>> {
        loop {
            match self.inner.read_record().await {
                Ok(r) => return Ok(Some(r)),
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
                let data = WalRecordData::new(r.data);
                self.read_sequence = data.seq;
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

    pub async fn handle_wal_truncated(&mut self, location: u64) -> TskvResult<()> {
        {
            let id = get_wal_file_id(
                self.inner
                    .path()
                    .file_name()
                    .ok_or(
                        CommonSnafu {
                            reason: format!(
                                "Failed to get file name from path: {:?}",
                                self.inner.path()
                            ),
                        }
                        .build(),
                    )?
                    .to_string_lossy()
                    .as_ref(),
            )?;
            let mut writer = WalWriter::open(
                self.config.clone(),
                id,
                self.inner.path(),
                self.min_sequence,
            )
            .await?;
            writer.truncate(location, self.read_sequence + 1).await;
            writer.sync().await?;
        }
        self.inner.reload_metadata().await?;
        self.has_footer = false;
        self.max_sequence = self.read_sequence;
        Ok(())
    }

    pub fn min_sequence(&self) -> u64 {
        self.min_sequence
    }

    pub fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    pub fn path(&self) -> &PathBuf {
        self.inner.path()
    }

    pub fn len(&self) -> u64 {
        self.inner.len()
    }

    /// If this record file has some records in it.
    pub fn is_empty(&self) -> bool {
        match self
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

    pub fn config(&self) -> Arc<WalOptions> {
        self.config.clone()
    }

    pub fn pos(&self) -> u64 {
        self.inner.pos() as u64
    }
}

#[derive(Debug)]
pub struct WalRecordData {
    pub typ: WalType,
    pub seq: u64,
    pub block: Option<wal_store::RaftEntry>,
}

impl WalRecordData {
    pub fn new(buf: Vec<u8>) -> WalRecordData {
        if buf.len() < WAL_HEADER_LEN {
            return Self {
                typ: WalType::Unknown,
                seq: 0,
                block: None,
            };
        }
        let seq = decode_be_u64(&buf[1..9]);
        let entry_type: WalType = buf[0].into();
        let block = match super::decode_wal_raft_entry(&buf[WAL_HEADER_LEN..]) {
            Ok(e) => e,
            Err(e) => {
                trace::error!("Failed to decode raft entry from wal: {e}");
                return Self {
                    typ: WalType::Unknown,
                    seq: 0,
                    block: None,
                };
            }
        };
        Self {
            typ: entry_type,
            seq,
            block: Some(block),
        }
    }
}

pub async fn print_wal_statistics(path: impl AsRef<Path>) {
    let mock_option = WalOptions {
        enabled: true,
        path: path.as_ref().to_path_buf(),
        wal_req_channel_cap: 0,
        max_file_size: 0,
        flush_trigger_total_file_size: 0,
        sync: false,
        sync_interval: Default::default(),
    };
    let mut reader = WalReader::open(path, mock_option.into()).await.unwrap();
    loop {
        let pos = reader.pos();
        match reader.read_wal_record_data(pos).await {
            Ok(Some(entry_block)) => {
                println!("============================================================");
                println!("Seq: {}, Typ: {}", entry_block.seq, entry_block.typ);
                match entry_block.block {
                    Some(entry) => match entry.payload {
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

                    None => {
                        println!("Unknown WAL entry type.");
                    }
                }
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
