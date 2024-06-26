//! # WAL file
//!
//! A WAL file is a [`record_file`].
//!
//! ## Record Data
//! ```text
//! # type = Write
//! +------------+------------+------------+------------+--------------+-----------------+-----------+
//! | 0: 1 byte  | 1: 8 bytes | 9: 4 bytes | 13: 1 byte | 14: 8 bytes  | 22: tenant_size |  n bytes  |
//! +------------+------------+------------+------------+--------------+-----------------+-----------+
//! |    type    |  sequence  |  vnode_id  |  precision | tenant_size  |  tenant         |   data    |
//! +------------+------------+------------+------------+--------------+-----------------+-----------+
//!
//! # type = DeleteVnode
//! +------------+------------+------------+-------------+-------------+----------+
//! | 0: 1 byte  | 1: 8 bytes | 9: 4 bytes | 13: 8 bytes | 21: n bytes | n bytes  |
//! +------------+------------+------------+-------------+-------------+----------+
//! |    type    |  sequence  |  vnode_id  | tenant_size |  tenant     | database |
//! +------------+------------+------------+-------------+-------------+----------+
//!
//! # type = DeleteTable
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! | 0: 1 byte  | 1: 8 bytes | 9: 8 bytes  | 17: 4 bytes   | 21: tenant_size | database_size | n bytes |
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! |    type    |  sequence  | tenant_size | database_size |  tenant         |  database     | table   |
//! +------------+------------+-------------+---------------+-----------------+---------------+---------+
//! ```
//!
//! ## Footer
//! ```text
//! +------------+---------------+--------------+--------------+
//! | 0: 4 bytes | 4: 12 bytes   | 16: 8 bytes  | 24: 8 bytes  |
//! +------------+---------------+--------------+--------------+
//! | "walo"     | padding_zeros | min_sequence | max_sequence |
//! +------------+---------------+--------------+--------------+
//! ```

mod reader;
pub mod wal_store;
pub mod writer;

use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use minivec::MiniVec;
use models::codec::Encoding;
use models::meta_data::VnodeId;
use models::schema::database_schema::DatabaseConfig;
use snafu::{IntoError, OptionExt, ResultExt};

use self::reader::WalReader;
use self::writer::WalWriter;
use crate::error::{CommonSnafu, DecodeSnafu, EncodeSnafu};
use crate::kv_option::WalOptions;
use crate::tsm::codec::{get_str_codec, StringCodec};
pub use crate::wal::reader::print_wal_statistics;
use crate::{error, file_utils, TskvResult};

/// 9 = type(1) + sequence(8)
const WAL_HEADER_LEN: usize = 9;

/// raft log has no cache
const WAL_BUFFER_SIZE: usize = 0;

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum WalType {
    RaftBlankLog = 101,
    RaftNormalLog = 102,
    RaftMembershipLog = 103,
    Unknown = 127,
}

impl From<u8> for WalType {
    fn from(typ: u8) -> Self {
        match typ {
            101 => WalType::RaftBlankLog,
            102 => WalType::RaftNormalLog,
            103 => WalType::RaftMembershipLog,
            _ => WalType::Unknown,
        }
    }
}

impl Display for WalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalType::RaftBlankLog => write!(f, "raft_log_blank"),
            WalType::RaftNormalLog => write!(f, "raft_log_normal"),
            WalType::RaftMembershipLog => write!(f, "raft_log_membership"),
            WalType::Unknown => write!(f, "unknown"),
        }
    }
}

pub struct VnodeWal {
    config: Arc<WalOptions>,
    db_config: Arc<DatabaseConfig>,
    wal_dir: PathBuf,
    tenant_database: Arc<String>,
    vnode_id: VnodeId,
    current_wal: WalWriter,
}

impl VnodeWal {
    pub async fn new(
        config: Arc<WalOptions>,
        db_config: Arc<DatabaseConfig>,
        tenant_database: Arc<String>,
        vnode_id: VnodeId,
    ) -> TskvResult<Self> {
        let wal_dir = config.wal_dir(&tenant_database, vnode_id);
        let writer_file = Self::open_writer(db_config.clone(), &wal_dir).await?;
        Ok(Self {
            config,
            db_config,
            wal_dir,
            tenant_database,
            vnode_id,
            current_wal: writer_file,
        })
    }

    pub async fn open_writer(
        db_config: Arc<DatabaseConfig>,
        wal_dir: &Path,
    ) -> TskvResult<WalWriter> {
        let next_file_id =
            match file_utils::get_max_sequence_file_name(wal_dir, file_utils::get_wal_file_id) {
                Some((_, id)) => id,
                None => 1_u64,
            };

        let new_wal = file_utils::make_wal_file(wal_dir, next_file_id);
        let writer_file = WalWriter::open(db_config, next_file_id, new_wal).await?;
        trace::info!("WAL '{:?}' starts write", writer_file.path());

        Ok(writer_file)
    }

    async fn roll_wal_file(&mut self, max_file_size: u64) -> TskvResult<()> {
        if self.current_wal.size() > max_file_size {
            trace::info!("WAL '{:?}' is full", self.current_wal.path());

            let new_file_id = self.current_wal.id() + 1;
            let new_file_name = file_utils::make_wal_file(&self.wal_dir, new_file_id);

            let new_file =
                WalWriter::open(self.db_config.clone(), new_file_id, new_file_name).await?;

            let mut old_file = std::mem::replace(&mut self.current_wal, new_file);
            old_file.close().await?;
        }
        Ok(())
    }

    pub async fn truncate_wal_file(&mut self, file_id: u64, pos: u64) -> TskvResult<()> {
        if self.current_wal_id() == file_id {
            self.current_wal.truncate(pos).await;
            self.current_wal.sync().await?;
            return Ok(());
        }

        let file_name = file_utils::make_wal_file(&self.wal_dir, file_id);
        let mut new_file = WalWriter::open(self.db_config.clone(), file_id, file_name).await?;
        new_file.truncate(pos).await;
        new_file.sync().await?;

        Ok(())
    }

    pub async fn rollback_wal_writer(&mut self, del_ids: &[u64]) -> TskvResult<()> {
        for wal_id in del_ids {
            let file_path = file_utils::make_wal_file(self.wal_dir(), *wal_id);
            trace::info!("Removing wal file '{}'", file_path.display());
            if let Err(e) = tokio::fs::remove_file(&file_path).await {
                trace::error!("Failed to remove file '{}': {:?}", file_path.display(), e);
            }
        }

        let new_writer = VnodeWal::open_writer(self.db_config.clone(), self.wal_dir()).await?;
        let _ = std::mem::replace(&mut self.current_wal, new_writer);

        Ok(())
    }

    async fn write_raft_entry(
        &mut self,
        raft_entry: &wal_store::RaftEntry,
    ) -> TskvResult<(u64, u64)> {
        if let Err(err) = self.roll_wal_file(self.db_config.wal_max_file_size()).await {
            trace::warn!("roll wal file failed: {}", err);
        }

        let wal_id = self.current_wal_id();
        let pos = self.current_wal_size();
        self.current_wal.append_raft_entry(raft_entry).await?;

        Ok((wal_id, pos))
    }

    pub async fn wal_reader(&mut self, wal_id: u64) -> TskvResult<WalReader> {
        if wal_id == self.current_wal_id() {
            // Use the same wal as the writer.
            let reader = self.current_wal.new_reader().await?;
            Ok(reader)
        } else {
            let wal_dir = self.config.wal_dir(&self.tenant_database, self.vnode_id);
            let wal_path = file_utils::make_wal_file(wal_dir, wal_id);
            let reader = WalReader::open(wal_path).await?;
            Ok(reader)
        }
    }

    pub async fn sync(&mut self) -> TskvResult<()> {
        self.current_wal.sync().await
    }

    /// Close current record file, return count of bytes appended as footer.
    pub async fn close(&mut self) -> TskvResult<()> {
        self.current_wal.close().await
    }

    pub fn config(&self) -> Arc<WalOptions> {
        self.config.clone()
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    pub fn current_wal_id(&self) -> u64 {
        self.current_wal.id()
    }

    pub fn current_wal_size(&self) -> u64 {
        self.current_wal.size()
    }
}

pub struct WalEntryCodec {
    buffer: Vec<MiniVec<u8>>,
    codec: Box<dyn StringCodec + Send + Sync>,
}

impl Default for WalEntryCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl WalEntryCodec {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            codec: get_str_codec(Encoding::Zstd),
        }
    }

    pub fn decode(&mut self, data: &[u8]) -> TskvResult<Option<MiniVec<u8>>> {
        self.buffer.truncate(0);
        self.codec
            .decode(data, &mut self.buffer)
            .context(error::DecodeSnafu)?;
        Ok(self.buffer.drain(..).next())
    }

    pub fn encode(&self, data: &[u8]) -> TskvResult<Vec<u8>> {
        let mut enc_data = Vec::with_capacity(data.len() / 2);
        self.codec
            .encode(&[data], &mut enc_data)
            .with_context(|_| crate::error::EncodeSnafu)?;

        Ok(enc_data)
    }
}

fn decode_wal_raft_entry(buf: &[u8]) -> TskvResult<wal_store::RaftEntry> {
    let mut decoder = WalEntryCodec::new();
    let dec_data = decoder.decode(buf)?.context(CommonSnafu {
        reason: format!("raft entry decode is none, len: {}", buf.len()),
    })?;

    bincode::deserialize(&dec_data).map_err(|e| DecodeSnafu.into_error(e))
}

fn encode_wal_raft_entry(entry: &wal_store::RaftEntry) -> TskvResult<Vec<u8>> {
    let bytes = bincode::serialize(entry).map_err(|e| EncodeSnafu.into_error(e))?;

    let encoder = WalEntryCodec::new();
    encoder.encode(&bytes)
}

#[cfg(test)]
mod test {
    #[test]
    fn test_get_test_config() {
        let _ = config::tskv::get_config_for_test();
    }
}
