use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::kv_option::WalOptions;
use crate::record_file::{RecordDataType, RecordDataVersion, Writer};
use crate::wal::reader::WalReader;
use crate::wal::{wal_store, WalType, WAL_BUFFER_SIZE};
use crate::TskvResult;

// include min_sequence, exclude max_sequence
pub struct WalWriter {
    id: u64,
    inner: Writer,
    size: u64,
    path: PathBuf,
    config: Arc<WalOptions>,
}

impl WalWriter {
    /// Opens a wal file at path, returns a WalWriter with id and config.
    /// If wal file doesn't exist, create new wal file and set it's min_log_sequence(default 0).
    pub async fn open(
        config: Arc<WalOptions>,
        id: u64,
        path: impl AsRef<Path>,
    ) -> TskvResult<Self> {
        let path = path.as_ref();
        let writer = Writer::open(path, RecordDataType::Wal, WAL_BUFFER_SIZE).await?;
        let size = writer.file_size();

        Ok(Self {
            id,
            inner: writer,
            size,
            path: PathBuf::from(path),
            config,
        })
    }

    pub async fn append_raft_entry(
        &mut self,
        raft_entry: &wal_store::RaftEntry,
    ) -> TskvResult<usize> {
        let wal_type = match raft_entry.payload {
            openraft::EntryPayload::Blank => WalType::RaftNormalLog,
            openraft::EntryPayload::Normal(_) => WalType::RaftNormalLog,
            openraft::EntryPayload::Membership(_) => WalType::RaftMembershipLog,
        };

        let seq = raft_entry.log_id.index;
        let data = super::encode_wal_raft_entry(raft_entry, &self.config.compress)?;
        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [&[wal_type as u8][..], &seq.to_be_bytes(), &data].as_slice(),
            )
            .await?;

        if self.config.wal_sync {
            self.inner.sync().await?;
        }

        self.size += written_size as u64;
        Ok(written_size)
    }

    pub async fn sync(&mut self) -> TskvResult<()> {
        self.inner.sync().await
    }

    pub async fn close(&mut self) -> TskvResult<()> {
        trace::info!("Wal '{}' closing", self.path.display(),);
        self.inner.close().await?;
        Ok(())
    }

    pub async fn new_reader(&mut self) -> TskvResult<WalReader> {
        let record_reader = self.inner.new_reader().await?;
        Ok(WalReader::new(record_reader, self.config.compress.clone()))
    }

    pub async fn truncate(&mut self, size: u64) {
        if self.size <= size {
            return;
        }

        let _ = self.inner.truncate(size).await;

        self.size = size;
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}
