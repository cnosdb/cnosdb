use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::kv_option::WalOptions;
use crate::record_file::{RecordDataType, RecordDataVersion};
use crate::wal::reader::WalReader;
use crate::wal::{reader, wal_store, WalType, WAL_FOOTER_MAGIC_NUMBER};
use crate::{record_file, Result};
fn build_footer(min_sequence: u64, max_sequence: u64) -> [u8; record_file::FILE_FOOTER_LEN] {
    let mut footer = [0_u8; record_file::FILE_FOOTER_LEN];
    footer[0..4].copy_from_slice(&WAL_FOOTER_MAGIC_NUMBER.to_be_bytes());
    footer[16..24].copy_from_slice(&min_sequence.to_be_bytes());
    footer[24..32].copy_from_slice(&max_sequence.to_be_bytes());
    footer
}

pub struct WalWriter {
    id: u64,
    inner: record_file::Writer,
    size: u64,
    path: PathBuf,
    config: Arc<WalOptions>,

    min_sequence: u64,
    max_sequence: u64,
    has_footer: bool,
}

impl WalWriter {
    /// Opens a wal file at path, returns a WalWriter with id and config.
    /// If wal file doesn't exist, create new wal file and set it's min_log_sequence(default 0).
    pub async fn open(
        config: Arc<WalOptions>,
        id: u64,
        path: impl AsRef<Path>,
        min_seq: u64,
    ) -> Result<Self> {
        let path = path.as_ref();

        // Use min_sequence existing in file, otherwise in parameter
        let (writer, min_sequence, max_sequence) = if LocalFileSystem::try_exists(path) {
            let writer = record_file::Writer::open(path, RecordDataType::Wal).await?;
            let (min_sequence, max_sequence) = match writer.footer() {
                Some(footer) => reader::parse_footer(footer).unwrap_or((min_seq, min_seq)),
                None => (min_seq, min_seq),
            };
            (writer, min_sequence, max_sequence)
        } else {
            (
                record_file::Writer::open(path, RecordDataType::Wal).await?,
                min_seq,
                min_seq,
            )
        };

        let size = writer.file_size();

        Ok(Self {
            id,
            inner: writer,
            size,
            path: PathBuf::from(path),
            config,
            min_sequence,
            max_sequence,
            has_footer: false,
        })
    }

    pub async fn append_raft_entry(&mut self, raft_entry: &wal_store::RaftEntry) -> Result<usize> {
        let wal_type = match raft_entry.payload {
            openraft::EntryPayload::Blank => WalType::RaftNormalLog,
            openraft::EntryPayload::Normal(_) => WalType::RaftNormalLog,
            openraft::EntryPayload::Membership(_) => WalType::RaftMembershipLog,
        };

        let seq = raft_entry.log_id.index;
        let data = super::encode_wal_raft_entry(raft_entry)?;
        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [&[wal_type as u8][..], &seq.to_be_bytes(), &data].as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }

        self.max_sequence = seq + 1;
        self.size += written_size as u64;
        Ok(written_size)
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.inner.sync().await
    }

    pub async fn close(&mut self) -> Result<usize> {
        trace::info!(
            "Wal '{}' closing with sequence: [{}, {})",
            self.path.display(),
            self.min_sequence,
            self.max_sequence,
        );
        let mut footer = build_footer(self.min_sequence, self.max_sequence);
        let size = self.inner.write_footer(&mut footer).await?;
        self.has_footer = true;
        self.inner.close().await?;
        Ok(size)
    }

    pub async fn new_reader(&self) -> WalReader {
        let record_reader = self.inner.new_reader().await;
        WalReader::new(
            record_reader,
            self.min_sequence,
            self.max_sequence,
            self.has_footer,
        )
    }

    pub async fn truncate(&mut self, size: u64, seq_no: u64) {
        if self.size <= size {
            return;
        }

        let _ = self.inner.truncate(size).await;

        self.size = size;
        let mut new_max_sequence = 0;
        if seq_no > 0 {
            new_max_sequence = seq_no - 1;
        }
        self.set_max_sequence(new_max_sequence);
        if self.min_sequence() > new_max_sequence {
            self.set_min_sequence(new_max_sequence);
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn min_sequence(&self) -> u64 {
        self.min_sequence
    }

    pub fn max_sequence(&self) -> u64 {
        self.max_sequence
    }

    pub fn set_max_sequence(&mut self, new_max_sequence: u64) {
        self.max_sequence = new_max_sequence
    }

    pub fn set_min_sequence(&mut self, new_min_sequence: u64) {
        self.min_sequence = new_min_sequence
    }
}
