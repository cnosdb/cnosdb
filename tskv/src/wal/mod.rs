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
use snafu::ResultExt;
use tokio::sync::oneshot;

use self::reader::WalReader;
use self::writer::WalWriter;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::kv_option::WalOptions;
use crate::tsm::codec::{get_str_codec, StringCodec};
pub use crate::wal::reader::{print_wal_statistics, Block};
use crate::{error, file_utils, Result};

const WAL_TYPE_LEN: usize = 1;
const WAL_SEQUENCE_LEN: usize = 8;
/// 9 = type(1) + sequence(8)
const WAL_HEADER_LEN: usize = 9;

const SERIES_KEYS_LEN: usize = 4;

const WAL_VNODE_ID_LEN: usize = 4;
const WAL_PRECISION_LEN: usize = 1;
const WAL_TENANT_SIZE_LEN: usize = 8;
const WAL_DATABASE_SIZE_LEN: usize = 4;
const WAL_TABLE_SIZE_LEN: usize = 4;

const WAL_FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'w', b'a', b'l', b'o']);
const WAL_FOOTER_MAGIC_NUMBER_LEN: usize = 4;

const SEGMENT_MAGIC: [u8; 4] = [0x57, 0x47, 0x4c, 0x00];

/// A channel sender that send write WAL result: `(seq_no: u64, written_size: usize)`
type WriteResultSender = oneshot::Sender<crate::Result<(u64, usize)>>;
type WriteResultReceiver = oneshot::Receiver<crate::Result<(u64, usize)>>;

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
    wal_dir: PathBuf,
    tenant_database: Arc<String>,
    vnode_id: VnodeId,
    current_wal: WalWriter,
}

impl VnodeWal {
    pub async fn new(
        config: Arc<WalOptions>,
        tenant_database: Arc<String>,
        vnode_id: VnodeId,
    ) -> Result<Self> {
        let wal_dir = config.wal_dir(&tenant_database, vnode_id);
        let writer_file = Self::open_writer(config.clone(), &wal_dir).await?;
        Ok(Self {
            config,
            wal_dir,
            tenant_database,
            vnode_id,
            current_wal: writer_file,
        })
    }

    pub async fn open_writer(config: Arc<WalOptions>, wal_dir: &Path) -> Result<WalWriter> {
        // Create a new wal file every time it starts.
        let (pre_max_seq, next_file_id) =
            match file_utils::get_max_sequence_file_name(wal_dir, file_utils::get_wal_file_id) {
                Some((_, id)) => {
                    let path = file_utils::make_wal_file(wal_dir, id);
                    let (_, max_seq) = reader::read_footer(&path).await?.unwrap_or((1_u64, 1_u64));
                    (max_seq + 1, id)
                }
                None => (1_u64, 1_u64),
            };

        let new_wal = file_utils::make_wal_file(wal_dir, next_file_id);
        let writer_file =
            writer::WalWriter::open(config, next_file_id, new_wal, pre_max_seq).await?;
        trace::info!("WAL '{}' starts write", writer_file.id());

        Ok(writer_file)
    }

    async fn roll_wal_file(&mut self, max_file_size: u64) -> Result<()> {
        if self.current_wal.size() > max_file_size {
            trace::info!(
                "WAL '{}' is full at seq '{}', begin rolling.",
                self.current_wal.id(),
                self.current_wal.max_sequence()
            );

            let new_file_id = self.current_wal.id() + 1;
            let new_file_name = file_utils::make_wal_file(&self.wal_dir, new_file_id);

            let new_file = WalWriter::open(
                self.config.clone(),
                new_file_id,
                new_file_name,
                self.current_wal.max_sequence(),
            )
            .await?;

            let mut old_file = std::mem::replace(&mut self.current_wal, new_file);
            if old_file.max_sequence() <= old_file.min_sequence() {
                old_file.set_max_sequence(old_file.min_sequence());
            } else {
                old_file.set_max_sequence(old_file.max_sequence() - 1);
            }
            old_file.close().await?;

            trace::info!(
                "WAL '{}' starts write at seq {}",
                self.current_wal.id(),
                self.current_wal.max_sequence()
            );
        }
        Ok(())
    }

    pub async fn truncate_wal_file(&mut self, file_id: u64, pos: u64, seq_no: u64) -> Result<()> {
        if self.current_wal_id() == file_id {
            self.current_wal.truncate(pos, seq_no).await;
            self.current_wal.sync().await?;
            return Ok(());
        }

        let file_name = file_utils::make_wal_file(&self.wal_dir, file_id);
        let mut new_file = WalWriter::open(self.config.clone(), file_id, file_name, seq_no).await?;
        new_file.truncate(pos, seq_no).await;
        new_file.sync().await?;

        Ok(())
    }

    pub async fn rollback_wal_writer(&mut self, del_ids: &[u64]) -> Result<()> {
        for wal_id in del_ids {
            let file_path = file_utils::make_wal_file(self.wal_dir(), *wal_id);
            trace::info!("Removing wal file '{}'", file_path.display());
            if let Err(e) = tokio::fs::remove_file(&file_path).await {
                trace::error!("Failed to remove file '{}': {:?}", file_path.display(), e);
            }
        }

        let new_writer = VnodeWal::open_writer(self.config(), self.wal_dir()).await?;
        let _ = std::mem::replace(&mut self.current_wal, new_writer);

        Ok(())
    }

    pub async fn delete_wal_files(&mut self, wal_ids: &[u64]) {
        for wal_id in wal_ids {
            if *wal_id == self.current_wal.id() {
                continue;
            }

            let file_path = file_utils::make_wal_file(self.wal_dir(), *wal_id);
            trace::info!("Removing wal file '{}'", file_path.display());
            if let Err(e) = tokio::fs::remove_file(&file_path).await {
                trace::error!("Failed to remove file '{}': {:?}", file_path.display(), e);
            }
        }
    }

    // delete wal files < seq
    async fn delete_wal_before_seq(&mut self, seq: u64) -> Result<()> {
        let mut delete_ids = vec![];
        let wal_files = LocalFileSystem::list_file_names(self.wal_dir());
        for file_name in wal_files {
            // If file name cannot be parsed to wal id, skip that file.
            let wal_id = match file_utils::get_wal_file_id(&file_name) {
                Ok(id) => id,
                Err(_) => continue,
            };

            if self.current_wal_id() == wal_id {
                continue;
            }

            let path = self.wal_dir().join(&file_name);
            if let Ok(reader) = WalReader::open(&path).await {
                if reader.max_sequence() < seq {
                    delete_ids.push(wal_id);
                }
            }
        }

        self.delete_wal_files(&delete_ids).await;

        Ok(())
    }

    async fn write_raft_entry(&mut self, raft_entry: &wal_store::RaftEntry) -> Result<(u64, u64)> {
        if let Err(err) = self.roll_wal_file(self.config.max_file_size).await {
            trace::warn!("roll wal file failed: {}", err);
        }

        let wal_id = self.current_wal_id();
        let pos = self.current_wal_size();
        self.current_wal.append_raft_entry(raft_entry).await?;

        Ok((wal_id, pos))
    }

    pub async fn wal_reader(&mut self, wal_id: u64) -> Result<WalReader> {
        if wal_id == self.current_wal_id() {
            // Use the same wal as the writer.
            let reader = self.current_wal.new_reader().await;
            Ok(reader)
        } else {
            let wal_dir = self.config.wal_dir(&self.tenant_database, self.vnode_id);
            let wal_path = file_utils::make_wal_file(wal_dir, wal_id);
            let reader = WalReader::open(wal_path).await?;
            Ok(reader)
        }
    }

    pub async fn sync(&mut self) -> Result<()> {
        self.current_wal.sync().await
    }

    /// Close current record file, return count of bytes appended as footer.
    pub async fn close(&mut self) -> Result<usize> {
        self.current_wal.close().await
    }

    pub fn config(&self) -> Arc<WalOptions> {
        self.config.clone()
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    pub fn current_seq_no(&self) -> u64 {
        self.current_wal.max_sequence()
    }

    pub fn current_wal_id(&self) -> u64 {
        self.current_wal.id()
    }

    pub fn current_wal_size(&self) -> u64 {
        self.current_wal.size()
    }

    pub fn sync_interval(&self) -> std::time::Duration {
        self.config.sync_interval
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

    pub fn decode(&mut self, data: &[u8]) -> Result<Option<MiniVec<u8>>> {
        self.buffer.truncate(0);
        self.codec
            .decode(data, &mut self.buffer)
            .context(error::DecodeSnafu)?;
        Ok(self.buffer.drain(..).next())
    }

    pub fn encode(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut enc_data = Vec::with_capacity(data.len() / 2);
        self.codec
            .encode(&[data], &mut enc_data)
            .with_context(|_| crate::error::EncodeSnafu)?;

        Ok(enc_data)
    }
}

fn decode_wal_raft_entry(buf: &[u8]) -> Result<wal_store::RaftEntry> {
    let mut decoder = WalEntryCodec::new();
    let dec_data = decoder.decode(buf)?.ok_or(crate::Error::CommonError {
        reason: format!("raft entry decode is none, len: {}", buf.len()),
    })?;

    bincode::deserialize(&dec_data).map_err(|e| crate::Error::Decode { source: e })
}

fn encode_wal_raft_entry(entry: &wal_store::RaftEntry) -> Result<Vec<u8>> {
    let bytes = bincode::serialize(entry).map_err(|e| crate::Error::Encode { source: e })?;

    let encoder = WalEntryCodec::new();
    encoder.encode(&bytes)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use minivec::MiniVec;
    use models::field_value::FieldVal;
    use models::Timestamp;
    use protos::models::FieldType;
    use protos::models_helper;

    fn random_write_data() -> Vec<u8> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ptr = models_helper::create_random_points_with_delta(&mut fbb, 5);
        fbb.finish(ptr, None);
        fbb.finished_data().to_vec()
    }

    /// Generate flatbuffers data and memcache data
    #[allow(clippy::type_complexity)]
    pub fn const_write_data(
        start_timestamp: i64,
        num: usize,
    ) -> (Vec<u8>, HashMap<String, Vec<(Timestamp, FieldVal)>>) {
        let mut fa_data: Vec<(Timestamp, FieldVal)> = Vec::with_capacity(num);
        let mut fb_data: Vec<(Timestamp, FieldVal)> = Vec::with_capacity(num);
        for i in start_timestamp..start_timestamp + num as i64 {
            fa_data.push((i, FieldVal::Integer(100)));
            fb_data.push((i, FieldVal::Bytes(MiniVec::from("b"))));
        }
        let map = HashMap::from([("fa".to_string(), fa_data), ("fb".to_string(), fb_data)]);

        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ptr = models_helper::create_const_points(
            &mut fbb,
            "dba",
            "tba",
            vec![("ta", "a"), ("tb", "b")],
            vec![("fa", &100_u64.to_be_bytes()), ("fb", b"b")],
            HashMap::from([("fa", FieldType::Unsigned), ("fb", FieldType::String)]),
            start_timestamp,
            num,
        );
        fbb.finish(ptr, None);
        (fbb.finished_data().to_vec(), map)
    }

    #[test]
    fn test_get_test_config() {
        let _ = config::get_config_for_test();
    }
}
