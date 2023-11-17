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

use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use minivec::MiniVec;
use models::codec::Encoding;
use models::meta_data::VnodeId;
use models::schema::{make_owner, Precision};
use snafu::ResultExt;
use tokio::sync::{oneshot, RwLock};
use tokio::time::timeout;

use self::reader::WalReader;
use crate::context::GlobalSequenceContext;
use crate::file_system::file_manager;
use crate::kv_option::WalOptions;
use crate::tsm::codec::{get_str_codec, StringCodec};
use crate::version_set::VersionSet;
pub use crate::wal::reader::{
    print_wal_statistics, DeleteTableBlock, DeleteVnodeBlock, WalEntry, WriteBlock,
};
use crate::{error, file_utils, Result, TseriesFamilyId};

mod reader;
mod writer;

const ENTRY_TYPE_LEN: usize = 1;
const ENTRY_SEQUENCE_LEN: usize = 8;
/// 9 = type(1) + sequence(8)
const ENTRY_HEADER_LEN: usize = 9;

const ENTRY_VNODE_ID_LEN: usize = 4;
const ENTRY_PRECISION_LEN: usize = 1;
const ENTRY_TENANT_SIZE_LEN: usize = 8;
const ENTRY_DATABASE_SIZE_LEN: usize = 4;
const ENTRY_TABLE_SIZE_LEN: usize = 4;

const FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'w', b'a', b'l', b'o']);
const FOOTER_MAGIC_NUMBER_LEN: usize = 4;

const SEGMENT_MAGIC: [u8; 4] = [0x57, 0x47, 0x4c, 0x00];

/// A channel sender that send write WAL result: `(seq_no: u64, written_size: usize)`
type WriteResultSender = oneshot::Sender<crate::Result<(u64, usize)>>;
type WriteResultReceiver = oneshot::Receiver<crate::Result<(u64, usize)>>;

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum WalEntryType {
    Write = 1,
    DeleteVnode = 11,
    DeleteTable = 21,
    Unknown = 127,
}

impl From<u8> for WalEntryType {
    fn from(typ: u8) -> Self {
        match typ {
            1 => WalEntryType::Write,
            11 => WalEntryType::DeleteVnode,
            21 => WalEntryType::DeleteTable,
            _ => WalEntryType::Unknown,
        }
    }
}

impl Display for WalEntryType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalEntryType::Write => write!(f, "write"),
            WalEntryType::DeleteVnode => write!(f, "delete_vnode"),
            WalEntryType::DeleteTable => write!(f, "delete_table"),
            WalEntryType::Unknown => write!(f, "unknown"),
        }
    }
}

pub enum WalTask {
    Write {
        tenant: String,
        database: String,
        vnode_id: VnodeId,
        precision: Precision,
        points: Vec<u8>,
        cb: WriteResultSender,
    },
    DeleteVnode {
        tenant: String,
        database: String,
        vnode_id: VnodeId,
        cb: WriteResultSender,
    },
    DeleteTable {
        tenant: String,
        database: String,
        table: String,
        cb: WriteResultSender,
    },
}

impl WalTask {
    pub fn new_write(
        tenant: String,
        database: String,
        vnode_id: VnodeId,
        precision: Precision,
        points: Vec<u8>,
    ) -> (WalTask, WriteResultReceiver) {
        let (cb, rx) = oneshot::channel();
        (
            WalTask::Write {
                tenant,
                database,
                vnode_id,
                precision,
                points,
                cb,
            },
            rx,
        )
    }

    pub fn new_delete_vnode(
        tenant: String,
        database: String,
        vnode_id: VnodeId,
    ) -> (WalTask, WriteResultReceiver) {
        let (cb, rx) = oneshot::channel();
        (
            WalTask::DeleteVnode {
                tenant,
                database,
                vnode_id,
                cb,
            },
            rx,
        )
    }

    pub fn new_delete_table(
        tenant: String,
        database: String,
        table: String,
    ) -> (WalTask, WriteResultReceiver) {
        let (cb, rx) = oneshot::channel();
        (
            WalTask::DeleteTable {
                tenant,
                database,
                table,
                cb,
            },
            rx,
        )
    }

    pub fn new_from(wal_task: &WalTask, cb: WriteResultSender) -> WalTask {
        match wal_task {
            WalTask::Write {
                tenant,
                database,
                vnode_id,
                precision,
                points,
                ..
            } => WalTask::Write {
                tenant: tenant.clone(),
                database: database.clone(),
                vnode_id: *vnode_id,
                precision: *precision,
                points: points.clone(),
                cb,
            },
            WalTask::DeleteVnode {
                tenant,
                database,
                vnode_id,
                ..
            } => WalTask::DeleteVnode {
                tenant: tenant.clone(),
                database: database.clone(),
                vnode_id: *vnode_id,
                cb,
            },
            WalTask::DeleteTable {
                tenant,
                database,
                table,
                ..
            } => WalTask::DeleteTable {
                tenant: tenant.clone(),
                database: database.clone(),
                table: table.clone(),
                cb,
            },
        }
    }

    pub fn wal_entry_type(&self) -> WalEntryType {
        match self {
            WalTask::Write { .. } => WalEntryType::Write,
            WalTask::DeleteVnode { .. } => WalEntryType::DeleteVnode,
            WalTask::DeleteTable { .. } => WalEntryType::DeleteTable,
        }
    }

    fn write_wal_result_sender(self) -> WriteResultSender {
        match self {
            WalTask::Write { cb, .. } => cb,
            WalTask::DeleteVnode { cb, .. } => cb,
            WalTask::DeleteTable { cb, .. } => cb,
        }
    }

    pub fn fail(self, e: crate::Error) -> crate::Result<()> {
        self.write_wal_result_sender()
            .send(Err(e))
            .map_err(|_| crate::Error::ChannelSend {
                source: crate::error::ChannelSendError::WalTask,
            })
    }
    pub fn owner(&self) -> String {
        match self {
            WalTask::Write {
                tenant, database, ..
            } => make_owner(tenant, database),
            WalTask::DeleteVnode {
                tenant, database, ..
            } => make_owner(tenant, database),
            WalTask::DeleteTable {
                tenant, database, ..
            } => make_owner(tenant, database),
        }
    }
    pub fn vnode_id(&self) -> Option<TseriesFamilyId> {
        match self {
            WalTask::Write { vnode_id, .. } => Some(*vnode_id),
            WalTask::DeleteVnode { vnode_id, .. } => Some(*vnode_id),
            //todo: change delete table to delete time series;
            WalTask::DeleteTable { .. } => None,
        }
    }
}

pub struct VnodeWal {
    vnode_id: TseriesFamilyId,
    wal_dir: PathBuf,
    current_wal: writer::WalWriter,
    total_file_size: u64,
    old_file_max_sequence: HashMap<u64, u64>,
    config: Arc<WalOptions>,
}

impl VnodeWal {
    pub async fn init(
        vnode_id: TseriesFamilyId,
        owner: String,
        config: Arc<WalOptions>,
    ) -> Result<Self> {
        let wal_dir = config.wal_dir(&owner, &vnode_id.to_string());
        let current_file =
            writer::WalWriter::open(config.clone(), 0, file_utils::make_wal_file(&wal_dir, 0), 1)
                .await
                .map_err(|e| error::Error::CommonError {
                    reason: format!("Failed to open wal file: {}", e),
                })?;
        Ok(Self {
            vnode_id,
            wal_dir,
            current_wal: current_file,
            total_file_size: 0,
            old_file_max_sequence: HashMap::new(),
            config,
        })
    }
    async fn roll_wal_file(&mut self, max_file_size: u64, seq: u64) -> Result<()> {
        if self.current_wal.size() > max_file_size {
            trace::info!(
                "WAL '{}' is full at seq '{}', begin rolling.",
                self.current_wal.id(),
                self.current_wal.max_sequence()
            );

            let new_file_id = self.current_wal.id() + 1;
            let new_file_name = file_utils::make_wal_file(&self.wal_dir, new_file_id);

            let new_file = writer::WalWriter::open(
                self.config.clone(),
                new_file_id,
                new_file_name,
                self.current_wal.max_sequence(),
            )
            .await?;
            // Total WALs size add WAL header size.
            self.total_file_size += new_file.size();

            let mut old_file = std::mem::replace(&mut self.current_wal, new_file);
            if old_file.max_sequence() <= old_file.min_sequence() {
                old_file.set_max_sequence(old_file.min_sequence());
            } else {
                old_file.set_max_sequence(old_file.max_sequence() - 1);
            }
            self.old_file_max_sequence
                .insert(old_file.id(), old_file.max_sequence());
            // Total WALs size add WAL footer size.
            self.total_file_size += old_file.close().await? as u64;

            trace::info!(
                "WAL '{}' starts write at seq {}",
                self.current_wal.id(),
                self.current_wal.max_sequence()
            );
            self.check_to_delete(seq).await;
        }
        Ok(())
    }

    pub async fn check_to_delete(&mut self, min_seq: u64) {
        let mut old_files_to_delete: Vec<u64> = Vec::new();
        for (old_file_id, old_file_max_seq) in self.old_file_max_sequence.iter() {
            if *old_file_max_seq < min_seq {
                old_files_to_delete.push(*old_file_id);
            }
        }

        if !old_files_to_delete.is_empty() {
            for file_id in old_files_to_delete {
                let file_path = file_utils::make_wal_file(&self.config.path, file_id);
                trace::debug!("Removing wal file '{}'", file_path.display());
                let file_size = match tokio::fs::metadata(&file_path).await {
                    Ok(m) => m.len(),
                    Err(e) => {
                        trace::error!(
                            "Failed to get WAL file metadata for '{}': {:?}",
                            file_path.display(),
                            e
                        );
                        0
                    }
                };
                if let Err(e) = tokio::fs::remove_file(&file_path).await {
                    trace::error!("Failed to remove file '{}': {:?}", file_path.display(), e);
                }
                // Remove max_sequence record for deleted file.
                self.old_file_max_sequence.remove(&file_id);
                // Subtract deleted file size.
                self.total_file_size -= file_size;
            }
        }
    }

    /// Checks if wal file is full then writes data. Return data sequence and data size.
    pub async fn write(&mut self, wal_task: WalTask, seq: u64) {
        if let Err(e) = self.roll_wal_file(self.config.max_file_size, seq).await {
            trace::error!("Failed to roll WAL file: {}", e);
            if wal_task.fail(e).is_err() {
                trace::error!("Failed to send roll WAL error to tskv");
            }
            return;
        }
        let (write_ret, cb) = match wal_task {
            WalTask::Write {
                tenant,
                database: _,
                vnode_id,
                precision,
                points,
                cb,
            } => (
                self.current_wal
                    .write(tenant, vnode_id, precision, points)
                    .await,
                cb,
            ),
            WalTask::DeleteVnode {
                tenant,
                database,
                vnode_id,
                cb,
            } => (
                self.current_wal
                    .delete_vnode(tenant, database, vnode_id)
                    .await,
                cb,
            ),
            WalTask::DeleteTable {
                tenant,
                database,
                table,
                cb,
            } => (
                self.current_wal.delete_table(tenant, database, table).await,
                cb,
            ),
        };
        let send_ret = match write_ret {
            Ok((seq, size)) => {
                self.total_file_size += size as u64;
                cb.send(Ok((seq, size)))
            }
            Err(e) => cb.send(Err(e)),
        };
        if let Err(e) = send_ret {
            // WAL job closed, leaving this write request.
            trace::warn!("send WAL write result failed: {:?}", e);
        }
    }

    pub async fn recover(&self, min_log_seq: u64) -> Result<Vec<WalReader>> {
        trace::warn!("Recover: reading wal from seq '{}'", min_log_seq);

        let wal_files = file_manager::list_file_names(&self.wal_dir);
        let mut wal_readers = vec![];
        for file_name in wal_files {
            let path = self.wal_dir.join(&file_name);
            if !file_manager::try_exists(&path) {
                continue;
            }
            let reader = reader::WalReader::open(&path).await?;

            // If this file has no footer, try to read all it's records.
            // If max_sequence of this file is greater than min_log_seq, read all it's records.
            if reader.max_sequence() == 0 || reader.max_sequence() >= min_log_seq {
                wal_readers.push(reader);
            }
        }

        Ok(wal_readers)
    }

    pub async fn sync(&self) -> Result<()> {
        self.current_wal.sync().await
    }

    /// Close current record file, return count of bytes appended as footer.
    pub async fn close(&mut self) -> Result<usize> {
        self.current_wal.close().await
    }

    pub fn current_seq_no(&self) -> u64 {
        self.current_wal.max_sequence()
    }

    pub fn sync_interval(&self) -> std::time::Duration {
        self.config.sync_interval
    }

    pub fn is_total_file_size_exceed(&self) -> bool {
        self.total_file_size >= self.config.flush_trigger_total_file_size
    }

    pub fn total_file_size(&self) -> u64 {
        self.total_file_size
    }
}

pub struct WalManager {
    config: Arc<WalOptions>,
    global_seq_ctx: Arc<GlobalSequenceContext>,
    wal_set: HashMap<TseriesFamilyId, VnodeWal>,
}

unsafe impl Send for WalManager {}

unsafe impl Sync for WalManager {}

impl WalManager {
    pub async fn open(
        config: Arc<WalOptions>,
        global_seq_ctx: Arc<GlobalSequenceContext>,
        version_set: Arc<RwLock<VersionSet>>,
    ) -> Result<Self> {
        let mut wal_set = HashMap::new();
        for (db_name, database) in version_set.read().await.get_all_db() {
            for vnode_id in database.read().await.ts_families().keys() {
                let wal_dir = config.wal_dir(db_name, &vnode_id.to_string());
                let mut total_file_size = 0_u64;
                let mut old_file_max_sequence: HashMap<u64, u64> = HashMap::new();
                let file_names = file_manager::list_file_names(wal_dir.clone());
                for f in file_names {
                    let file_path = wal_dir.join(&f);
                    match tokio::fs::metadata(&file_path).await {
                        Ok(m) => {
                            total_file_size += m.len();
                        }
                        Err(e) => {
                            trace::error!("Failed to get WAL file metadata for '{}': {:?}", &f, e)
                        }
                    }
                    match reader::read_footer(file_path).await {
                        Ok(Some((_, max_seq))) => match file_utils::get_wal_file_id(&f) {
                            Ok(file_id) => {
                                old_file_max_sequence.insert(file_id, max_seq);
                            }
                            Err(e) => {
                                trace::error!("Failed to parse WAL file name for '{}': {:?}", &f, e)
                            }
                        },
                        Ok(None) => trace::warn!("Failed to parse WAL file footer for '{}'", &f),
                        Err(e) => {
                            trace::warn!("Failed to parse WAL file footer for '{}': {:?}", &f, e)
                        }
                    }
                }

                // Create a new wal file every time it starts.
                let (pre_max_seq, next_file_id) = match file_utils::get_max_sequence_file_name(
                    &wal_dir,
                    file_utils::get_wal_file_id,
                ) {
                    Some((_, id)) => {
                        let path = file_utils::make_wal_file(&wal_dir, id);
                        let (_, max_seq) =
                            reader::read_footer(&path).await?.unwrap_or((1_u64, 1_u64));
                        (max_seq + 1, id + 1)
                    }
                    None => (1_u64, 1_u64),
                };

                let new_wal = file_utils::make_wal_file(&wal_dir, next_file_id);
                let current_file =
                    writer::WalWriter::open(config.clone(), next_file_id, new_wal, pre_max_seq)
                        .await?;
                total_file_size += current_file.size();
                trace::info!("WAL '{}' starts write", current_file.id());
                let vnode_wal = VnodeWal {
                    vnode_id: *vnode_id,
                    wal_dir: wal_dir.clone(),
                    current_wal: current_file,
                    total_file_size,
                    old_file_max_sequence,
                    config: config.clone(),
                };
                wal_set.insert(*vnode_id, vnode_wal);
            }
        }

        Ok(Self {
            config,
            global_seq_ctx,
            wal_set,
        })
    }

    pub async fn write(&mut self, wal_task: WalTask) -> Result<()> {
        let vnode_id = wal_task.vnode_id();
        match vnode_id {
            None => {
                let mut rxs = Vec::with_capacity(self.wal_set.len());
                for (_, vnode_wal) in self.wal_set.iter_mut() {
                    let (tx, rx) = oneshot::channel();
                    let new_task = WalTask::new_from(&wal_task, tx);
                    rxs.push((rx, vnode_wal.vnode_id));
                    vnode_wal
                        .write(new_task, self.global_seq_ctx.min_seq())
                        .await;
                }

                let mut ok = true;
                for (rx, vnode_id) in rxs {
                    if let Ok(Ok(Ok((_, _)))) = timeout(Duration::from_secs(5), rx).await {
                        continue;
                    }
                    ok = false;
                    trace::error!("Failed to write wal delete table in vnode {}", vnode_id);
                }
                if ok {
                    if let Err(e) = wal_task.write_wal_result_sender().send(Ok((0, 0))) {
                        trace::error!("Failed to send wal delete table result: {:?}", e);
                    }
                } else if let Err(e) =
                    wal_task
                        .write_wal_result_sender()
                        .send(Err(error::Error::CommonError {
                        reason:
                            "Failed to write wal delete table in some vnodes, please check tskv log"
                                .to_string(),
                    }))
                {
                    trace::error!("Failed to send wal delete table result: {:?}", e);
                }
            }
            Some(vnode_id) => {
                if let Some(vnode_wal) = self.wal_set.get_mut(&vnode_id) {
                    vnode_wal
                        .write(wal_task, self.global_seq_ctx.min_seq())
                        .await;
                } else {
                    let mut vnode_wal =
                        VnodeWal::init(vnode_id, wal_task.owner(), self.config.clone()).await?;
                    vnode_wal
                        .write(wal_task, self.global_seq_ctx.min_seq())
                        .await;
                    self.wal_set.insert(vnode_id, vnode_wal);
                };
            }
        }
        Ok(())
    }
    pub async fn recover(&self) -> BTreeMap<VnodeId, Vec<WalReader>> {
        let mut recover_task = BTreeMap::new();
        for (vnode_id, vnode_wal) in self.wal_set.iter() {
            let min_seq = self.global_seq_ctx.vnode_min_seq(*vnode_id);
            if let Ok(readers) = vnode_wal.recover(min_seq).await {
                recover_task.insert(*vnode_id, readers);
            } else {
                panic!("Failed to recover wal for vnode '{}'", vnode_id)
            }
        }
        recover_task
    }

    pub async fn sync(&self) -> Result<()> {
        let mut sync_task = vec![];
        for (_, vnode_wal) in self.wal_set.iter() {
            sync_task.push(vnode_wal.sync());
        }
        for res in futures::future::join_all(sync_task).await {
            if let Err(e) = &res {
                trace::error!("Failed to sync wal: {:?}", e);
            }
            res?
        }
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        let mut close_task = vec![];
        for (_, vnode_wal) in self.wal_set.iter_mut() {
            close_task.push(vnode_wal.close());
        }
        for res in futures::future::join_all(close_task).await {
            if let Err(e) = res {
                trace::error!("Failed to close wal: {:?}", e);
                return Err(e);
            }
        }
        Ok(())
    }
    pub fn sync_interval(&self) -> std::time::Duration {
        self.config.sync_interval
    }

    pub fn wal_set(&mut self) -> &mut HashMap<TseriesFamilyId, VnodeWal> {
        &mut self.wal_set
    }

    pub fn global_seq_ctx(&self) -> Arc<GlobalSequenceContext> {
        self.global_seq_ctx.clone()
    }
}

pub struct WalDecoder {
    buffer: Vec<MiniVec<u8>>,
    decoder: Box<dyn StringCodec + Send + Sync>,
}

impl WalDecoder {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            decoder: get_str_codec(Encoding::Zstd),
        }
    }
    pub fn decode(&mut self, data: &[u8]) -> Result<Option<MiniVec<u8>>> {
        self.buffer.truncate(0);
        self.decoder
            .decode(data, &mut self.buffer)
            .context(error::DecodeSnafu)?;
        Ok(self.buffer.drain(..).next())
    }
}

#[cfg(test)]
mod test {
    use core::panic;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use minivec::MiniVec;
    use models::codec::Encoding;
    use models::field_value::FieldVal;
    use models::schema::{make_owner, Precision};
    use models::Timestamp;
    use protos::models::FieldType;
    use protos::{models as fb_models, models_helper};
    use serial_test::serial;
    use tokio::sync::RwLock;
    use trace::init_default_global_tracing;

    use crate::context::GlobalSequenceContext;
    use crate::file_system::file_manager::list_file_names;
    use crate::kv_option::WalOptions;
    use crate::tsm::codec::{get_str_codec, StringCodec};
    use crate::version_set::VersionSet;
    use crate::wal::reader::{WalEntry, WalReader};
    use crate::wal::{WalManager, WalTask};
    use crate::{error, Error, Result};

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

    async fn check_wal_files(
        wal_dir: impl AsRef<Path>,
        data: Vec<Vec<u8>>,
        is_flatbuffers: bool,
    ) -> Result<()> {
        let wal_dir = wal_dir.as_ref();
        let wal_files = list_file_names(wal_dir);
        let mut data_iter = data.iter();
        for wal_file in wal_files {
            let path = wal_dir.join(wal_file);

            let mut reader = WalReader::open(&path).await.unwrap();
            let decoder = get_str_codec(Encoding::Zstd);
            println!("Reading data from wal file '{}'", path.display());
            loop {
                match reader.next_wal_entry().await {
                    Ok(Some(entry_block)) => {
                        println!("Reading entry from wal file '{}'", path.display());
                        match entry_block.entry {
                            WalEntry::Write(entry) => {
                                let ety_data = entry.points();
                                let ori_data = match data_iter.next() {
                                    Some(d) => d,
                                    None => {
                                        panic!("unexpected data to compare that is less than file count.")
                                    }
                                };
                                if is_flatbuffers {
                                    let mut data_buf = Vec::new();
                                    decoder.decode(ety_data, &mut data_buf).unwrap();
                                    assert_eq!(data_buf[0].as_slice(), ori_data.as_slice());
                                    if let Err(e) =
                                        flatbuffers::root::<fb_models::Points>(&data_buf[0])
                                    {
                                        panic!(
                                            "unexpected data in wal file, ignored file '{}' because '{}'",
                                            wal_dir.display(),
                                            e
                                        );
                                    }
                                } else {
                                    assert_eq!(ety_data, ori_data.as_slice());
                                }
                            }
                            WalEntry::DeleteVnode(_) => todo!(),
                            WalEntry::DeleteTable(_) => todo!(),
                            WalEntry::Unknown => todo!(),
                        }
                    }
                    Ok(None) => {
                        println!("Reae none from wal file '{}'", path.display());
                        break;
                    }
                    Err(Error::WalTruncated) => {
                        println!("WAL file truncated: {}", path.display());
                        return Err(Error::WalTruncated);
                    }
                    Err(e) => {
                        panic!("Failed to recover from {}: {:?}", path.display(), e);
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn test_read_and_write() {
        let dir = "/tmp/test/wal/".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir;
        let wal_config = WalOptions::from(&global_config);
        let tenant = "cnosdb";
        let database = "test";
        let vnode_id = 0;
        let owner = make_owner(tenant, database);
        let dest_dir = wal_config.wal_dir(&owner, &vnode_id.to_string());
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let vs = Arc::new(RwLock::new(VersionSet::build_empty_test(runtime.clone())));
        let feature = async {
            let mut mgr = WalManager::open(
                Arc::new(wal_config),
                GlobalSequenceContext::empty(),
                vs.clone(),
            )
            .await
            .unwrap();
            let mut data_vec = Vec::new();
            for i in 1..=10_u64 {
                let data = b"hello".to_vec();
                data_vec.push(data.clone());
                let (wal_task, rx) = WalTask::new_write(
                    tenant.to_string(),
                    database.to_string(),
                    vnode_id,
                    Precision::NS,
                    data,
                );
                mgr.write(wal_task).await.unwrap();
                let (seq, _) = rx.await.unwrap().unwrap();
                assert_eq!(i, seq)
            }
            mgr.close().await.unwrap();

            check_wal_files(&dest_dir, data_vec, false).await.unwrap();
        };
        runtime.block_on(feature);
    }

    #[serial]
    #[test]
    fn test_roll_wal_file() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let dir = "/tmp/test/wal/2".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir;
        // Argument max_file_size is so small that there must a new wal file created.
        global_config.wal.max_file_size = 1;
        global_config.wal.sync = false;
        global_config.wal.flush_trigger_total_file_size = 100;
        let wal_config = WalOptions::from(&global_config);

        let tenant = "cnosdb";
        let database = "test";
        let vnode_id = 0;
        let owner = make_owner(tenant, database);
        let dest_dir = wal_config.wal_dir(&owner, &vnode_id.to_string());
        let min_seq_no = 6;

        let gcs = GlobalSequenceContext::empty();
        gcs.set_min_seq(min_seq_no);
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let feature = async {
            let mut mgr = WalManager::open(
                Arc::new(wal_config),
                gcs,
                Arc::new(RwLock::new(VersionSet::build_empty_test(runtime.clone()))),
            )
            .await
            .unwrap();
            let mut data_vec: Vec<Vec<u8>> = Vec::new();
            for seq in 1..=10 {
                let data = format!("{}", seq).into_bytes();
                // if seq >= min_seq_no {
                //     // Data in file_id that less than version_set_min_seq_no will be deleted.
                //
                // }
                data_vec.push(data.clone());
                let (wal_task, rx) = WalTask::new_write(
                    tenant.to_string(),
                    database.to_string(),
                    vnode_id,
                    Precision::NS,
                    data,
                );
                mgr.write(wal_task).await.unwrap();
                let (write_seq, _) = rx.await.unwrap().unwrap();
                assert_eq!(seq, write_seq)
            }
            // assert_eq!(mgr.total_file_size(), 364);
            // assert!(mgr.is_total_file_size_exceed());
            mgr.close().await.unwrap();

            check_wal_files(dest_dir, data_vec, false).await.unwrap();
        };
        runtime.block_on(feature);
    }

    #[test]
    fn test_read_truncated() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/wal/3".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = config::get_config_for_test();
        global_config.wal.path = dir.clone();
        let wal_config = WalOptions::from(&global_config);

        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());
        let tenant = "cnosdb".to_string();

        let feature = async {
            let mut mgr = WalManager::open(
                Arc::new(wal_config),
                GlobalSequenceContext::empty(),
                Arc::new(RwLock::new(VersionSet::build_empty_test(runtime.clone()))),
            )
            .await
            .unwrap();
            let coder = get_str_codec(Encoding::Zstd);
            let mut data_vec: Vec<Vec<u8>> = Vec::new();

            for _i in 0..10 {
                let data = random_write_data();
                data_vec.push(data.clone());

                let mut enc_points = Vec::new();
                coder
                    .encode(&[&data], &mut enc_points)
                    .map_err(|_| Error::ChannelSend {
                        source: crate::error::ChannelSendError::WalTask,
                    })
                    .unwrap();
                let (wal_task, rx) = WalTask::new_write(
                    tenant.clone(),
                    "test".to_string(),
                    0,
                    Precision::NS,
                    enc_points,
                );
                mgr.write(wal_task).await.unwrap();
                rx.await.unwrap().unwrap();
            }
            // Do not close wal manager, so footer won't write.

            check_wal_files(dir, data_vec, true).await.unwrap();
        };
        runtime.block_on(feature);
    }

    async fn write_points_to_wal(
        max_ts: i64,
        tenant: String,
        wal_mgr: &mut WalManager,
        coder: Box<dyn StringCodec + Send + Sync>,
        data_vec: &mut Vec<Vec<u8>>,
        wrote_data: &mut HashMap<String, Vec<(Timestamp, FieldVal)>>,
    ) {
        for i in 1..=max_ts {
            let (data, mem_data) = const_write_data(i, 1);
            data_vec.push(data.clone());

            for (col_name, values) in mem_data {
                wrote_data.entry(col_name).or_default().extend(values);
            }

            let mut enc_points = Vec::new();
            coder
                .encode(&[&data], &mut enc_points)
                .map_err(|_| Error::ChannelSend {
                    source: error::ChannelSendError::WalTask,
                })
                .unwrap();
            let (wal_task, rx) = WalTask::new_write(
                tenant.clone(),
                "dba".to_string(),
                10,
                Precision::NS,
                enc_points,
            );
            wal_mgr.write(wal_task).await.unwrap();
            rx.await.unwrap().unwrap();
        }
    }

    #[test]
    fn test_get_test_config() {
        let _ = config::get_config_for_test();
    }
}
