//! # WAL file
//!
//! A WAL file is a [`record_file`].
//!
//! ## Record Data
//! ```text
//! +------------+------------+------------+
//! | 0: 1 byte  | 1: 8 bytes | 9: n bytes |
//! +------------+------------+------------+
//! |    type    |  sequence  |    data    |
//! +------------+------------+------------+
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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::string::String;
use std::sync::Arc;

use datafusion::parquet::data_type::AsBytes;
use models::auth::user::{ROOT, ROOT_PWD};
use models::codec::Encoding;
use protos::kv_service::{Meta, WritePointsRequest};
use snafu::ResultExt;
use tokio::sync::{oneshot, RwLock};
use trace::{debug, error, info, warn};

use crate::byte_utils::{decode_be_u32, decode_be_u64};
use crate::context::{GlobalContext, GlobalSequenceContext};
use crate::error::{self, Error, Result};
use crate::file_system::file_manager::{self, FileManager};
use crate::kv_option::WalOptions;
use crate::record_file::{self, Record, RecordDataType, RecordDataVersion};
use crate::tsm::codec::get_str_codec;
use crate::tsm::{DecodeSnafu, EncodeSnafu};
use crate::version_set::VersionSet;
use crate::{engine, file_utils, TseriesFamilyId};

const ENTRY_TYPE_LEN: usize = 1;
const ENTRY_SEQUENCE_LEN: usize = 8;
const ENTRY_VNODE_LEN: usize = 4;
const ENTRY_TENANT_LEN: usize = 8;
const ENTRY_HEADER_LEN: usize = 21; // 1 + 8 + 4 + 8
const FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'w', b'a', b'l', b'o']);
const FOOTER_MAGIC_NUMBER_LEN: usize = 4;

const SEGMENT_MAGIC: [u8; 4] = [0x57, 0x47, 0x4c, 0x00];

const BLOCK_HEADER_SIZE: usize = 25;

pub enum WalTask {
    Write {
        id: TseriesFamilyId,
        points: Arc<Vec<u8>>,
        tenant: Arc<Vec<u8>>,
        // (seq_no, written_size)
        cb: oneshot::Sender<Result<(u64, usize)>>,
    },
}

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum WalEntryType {
    Write = 1,
    Delete = 2,
    DeleteRange = 3,
    Unknown = 127,
}

impl From<u8> for WalEntryType {
    fn from(typ: u8) -> Self {
        match typ {
            1 => WalEntryType::Write,
            2 => WalEntryType::Delete,
            3 => WalEntryType::DeleteRange,
            _ => WalEntryType::Unknown,
        }
    }
}

pub struct WalEntryBlock {
    pub typ: WalEntryType,
    buf: Vec<u8>,
}

impl WalEntryBlock {
    pub fn new(typ: WalEntryType, buf: Vec<u8>) -> Self {
        Self { typ, buf }
    }

    pub fn seq(&self) -> u64 {
        decode_be_u64(&self.buf[1..9])
    }

    pub fn vnode_id(&self) -> TseriesFamilyId {
        decode_be_u32(&self.buf[9..13])
    }

    pub fn tenant(&self) -> &[u8] {
        let tenant_len = decode_be_u64(&self.buf[13..21]) as usize;
        &self.buf[ENTRY_HEADER_LEN..(ENTRY_HEADER_LEN + tenant_len)]
    }

    pub fn data(&self) -> &[u8] {
        let tenant_len = decode_be_u64(&self.buf[13..21]) as usize;
        &self.buf[(ENTRY_HEADER_LEN + tenant_len)..]
    }
}

fn build_footer(min_sequence: u64, max_sequence: u64) -> [u8; record_file::FILE_FOOTER_LEN] {
    let mut footer = [0_u8; record_file::FILE_FOOTER_LEN];
    footer[0..4].copy_from_slice(&FOOTER_MAGIC_NUMBER.to_be_bytes());
    footer[16..24].copy_from_slice(&min_sequence.to_be_bytes());
    footer[24..32].copy_from_slice(&max_sequence.to_be_bytes());
    footer
}

/// Reads a wal file and parse footer, returns sequence range
async fn read_footer(path: impl AsRef<Path>) -> Result<Option<(u64, u64)>> {
    if file_manager::try_exists(&path) {
        let reader = WalReader::open(path).await?;
        Ok(Some((reader.min_sequence, reader.max_sequence)))
    } else {
        Ok(None)
    }
}

struct WalWriter {
    id: u64,
    inner: record_file::Writer,
    size: u64,
    path: PathBuf,
    config: Arc<WalOptions>,

    buf: Vec<u8>,
    min_sequence: u64,
    max_sequence: u64,
}

impl WalWriter {
    pub async fn open(
        config: Arc<WalOptions>,
        id: u64,
        path: impl AsRef<Path>,
        min_seq: u64,
    ) -> Result<Self> {
        let path = path.as_ref();

        // Use min_sequence existing in file, otherwise in parameter
        let (writer, min_sequence, max_sequence) = if file_manager::try_exists(path) {
            let writer = record_file::Writer::open(path, RecordDataType::Wal).await?;
            let (min_sequence, max_sequence) = match writer.footer() {
                Some(footer) => WalReader::parse_footer(footer).unwrap_or((min_seq, min_seq)),
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
            buf: Vec::new(),
            min_sequence,
            max_sequence,
        })
    }

    /// Writes data, returns data sequence and data size.
    pub async fn write(
        &mut self,
        typ: WalEntryType,
        data: Arc<Vec<u8>>,
        id: TseriesFamilyId,
        tenant: Arc<Vec<u8>>,
    ) -> Result<(u64, usize)> {
        let seq = self.max_sequence;
        let tenant_len = tenant.len() as u64;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [
                    &[typ as u8][..],
                    &seq.to_be_bytes(),
                    &id.to_be_bytes(),
                    &tenant_len.to_be_bytes(),
                    &tenant,
                    &data,
                ]
                .as_slice(),
            )
            .await?;

        if self.config.sync {
            self.inner.sync().await?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn sync(&self) -> Result<()> {
        self.inner.sync().await
    }

    pub async fn close(mut self) -> Result<()> {
        info!(
            "Closing wal with sequence: [{}, {})",
            self.min_sequence, self.max_sequence
        );
        let footer = build_footer(self.min_sequence, self.max_sequence);
        self.inner.write_footer(footer).await?;
        self.inner.close().await
    }
}

pub struct WalManager {
    config: Arc<WalOptions>,
    global_seq_ctx: Arc<GlobalSequenceContext>,
    current_dir: PathBuf,
    current_file: WalWriter,
    old_file_max_sequence: HashMap<u64, u64>,
}

unsafe impl Send for WalManager {}

unsafe impl Sync for WalManager {}

impl WalManager {
    pub async fn open(
        config: Arc<WalOptions>,
        global_seq_ctx: Arc<GlobalSequenceContext>,
    ) -> Result<Self> {
        if !file_manager::try_exists(&config.path) {
            std::fs::create_dir_all(&config.path).unwrap();
        }
        let base_path = config.path.to_path_buf();

        let mut old_file_max_sequence: HashMap<u64, u64> = HashMap::new();
        let file_names = file_manager::list_file_names(&config.path);
        for f in file_names {
            match read_footer(base_path.join(&f)).await {
                Ok(Some((_, max_seq))) => match file_utils::get_wal_file_id(&f) {
                    Ok(file_id) => {
                        old_file_max_sequence.insert(file_id, max_seq);
                    }
                    Err(e) => warn!("Failed to parse WAL file name for '{}': {:?}", &f, e),
                },
                Ok(None) => warn!("Failed to parse WAL file footer for '{}'", &f),
                Err(e) => warn!("Failed to parse WAL file footer for '{}': {:?}", &f, e),
            }
        }

        // Create a new wal file every time it starts.
        let (pre_max_seq, next_file_id) =
            match file_utils::get_max_sequence_file_name(&config.path, file_utils::get_wal_file_id)
            {
                Some((_, id)) => {
                    let path = file_utils::make_wal_file(&config.path, id);
                    let (_, max_seq) = read_footer(&path).await?.unwrap_or((1_u64, 1_u64));
                    (max_seq + 1, id + 1)
                }
                None => (1_u64, 1_u64),
            };

        let new_wal = file_utils::make_wal_file(&config.path, next_file_id);
        let current_file =
            WalWriter::open(config.clone(), next_file_id, new_wal, pre_max_seq).await?;
        info!("WAL '{}' starts write", current_file.id);
        let current_dir = config.path.clone();
        Ok(WalManager {
            config,
            global_seq_ctx,
            current_dir,
            current_file,
            old_file_max_sequence,
        })
    }

    pub fn current_seq_no(&self) -> u64 {
        self.current_file.max_sequence
    }

    async fn roll_wal_file(&mut self, max_file_size: u64) -> Result<()> {
        if self.current_file.size > max_file_size {
            info!(
                "WAL '{}' is full at seq '{}', begin rolling.",
                self.current_file.id, self.current_file.max_sequence
            );

            let new_file_id = self.current_file.id + 1;
            let new_file_name = file_utils::make_wal_file(&self.config.path, new_file_id);

            let new_file = WalWriter::open(
                self.config.clone(),
                new_file_id,
                new_file_name,
                self.current_file.max_sequence,
            )
            .await?;
            info!(
                "WAL '{}' starts write at seq {}",
                self.current_file.id, self.current_file.max_sequence
            );

            let mut old_file = std::mem::replace(&mut self.current_file, new_file);
            if old_file.max_sequence <= old_file.min_sequence {
                old_file.max_sequence = old_file.min_sequence;
            } else {
                old_file.max_sequence -= 1;
            }
            self.old_file_max_sequence
                .insert(old_file.id, old_file.max_sequence);
            old_file.close().await?;

            self.check_to_delete().await;
        }
        Ok(())
    }

    async fn check_to_delete(&mut self) {
        let min_seq = self.global_seq_ctx.min_seq();
        let mut old_files_to_delete: Vec<u64> = Vec::new();
        for (old_file_id, old_file_max_seq) in self.old_file_max_sequence.iter() {
            if *old_file_max_seq < min_seq {
                old_files_to_delete.push(*old_file_id);
            }
        }

        if !old_files_to_delete.is_empty() {
            for file_id in old_files_to_delete {
                let file_path = file_utils::make_wal_file(&self.config.path, file_id);
                debug!("Removing wal file '{}'", file_path.display());
                if let Err(e) = std::fs::remove_file(&file_path) {
                    error!("failed to remove file '{}': {:?}", file_path.display(), e);
                }
                self.old_file_max_sequence.remove(&file_id);
            }
        }
    }

    /// Checks if wal file is full then writes data. Return data sequence and data size.
    pub async fn write(
        &mut self,
        typ: WalEntryType,
        data: Arc<Vec<u8>>,
        id: TseriesFamilyId,
        tenant: Arc<Vec<u8>>,
    ) -> Result<(u64, usize)> {
        self.roll_wal_file(self.config.max_file_size).await?;
        self.current_file.write(typ, data, id, tenant).await
    }

    pub async fn recover(
        &self,
        engine: &impl engine::Engine,
        global_context: Arc<GlobalContext>,
    ) -> Result<()> {
        let min_log_seq = global_context.last_seq();
        warn!("recovering version set from seq '{}'", &min_log_seq);

        let wal_files = file_manager::list_file_names(&self.current_dir);
        // TODO: Parallel get min_sequence at first.
        for file_name in wal_files {
            let id = file_utils::get_wal_file_id(&file_name)?;
            let path = self.current_dir.join(file_name);
            if !file_manager::try_exists(&path) {
                continue;
            }
            let mut reader = WalReader::open(&path).await?;
            if reader.is_empty() {
                continue;
            }
            // If this file has no footer, try to read all it's records.
            // If max_sequence of this file is greater than min_log_seq, read all it's records.
            if reader.max_sequence == 0 || reader.max_sequence >= min_log_seq {
                Self::read_wal_to_engine(&mut reader, engine, min_log_seq).await?;
            }
        }
        Ok(())
    }

    async fn read_wal_to_engine(
        reader: &mut WalReader,
        engine: &impl engine::Engine,
        min_log_seq: u64,
    ) -> Result<bool> {
        let mut seq_gt_min_seq = false;
        let decoder = get_str_codec(Encoding::Zstd);
        loop {
            match reader.next_wal_entry().await {
                Ok(Some(e)) => {
                    let seq = e.seq();
                    if seq < min_log_seq {
                        continue;
                    }
                    seq_gt_min_seq = true;
                    match e.typ {
                        WalEntryType::Write => {
                            let mut dst = Vec::new();
                            decoder.decode(e.data(), &mut dst).context(DecodeSnafu)?;
                            debug_assert_eq!(dst.len(), 1);
                            let id = e.vnode_id();
                            let tenant =
                                unsafe { String::from_utf8_unchecked(e.tenant().to_vec()) };
                            let req = WritePointsRequest {
                                version: 1,
                                meta: Some(Meta {
                                    tenant,
                                    user: None,
                                    password: None,
                                }),
                                points: dst[0].to_vec(),
                            };
                            engine.write_from_wal(id, req, seq).await.unwrap();
                        }
                        WalEntryType::Delete => {
                            // TODO delete a memcache entry
                        }
                        WalEntryType::DeleteRange => {
                            // TODO delete range in a memcache
                        }
                        _ => {}
                    };
                }
                Ok(None) | Err(Error::WalTruncated) => {
                    break;
                }
                Err(e) => {
                    panic!(
                        "Failed to recover from {}: {:?}",
                        reader.path().display(),
                        e
                    );
                }
            }
        }
        Ok(seq_gt_min_seq)
    }

    pub async fn sync(&self) -> Result<()> {
        self.current_file.sync().await
    }

    pub async fn close(self) -> Result<()> {
        self.current_file.close().await
    }

    pub fn sync_interval(&self) -> std::time::Duration {
        self.config.sync_interval
    }
}

pub struct WalReader {
    inner: record_file::Reader,
    /// Min write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    min_sequence: u64,
    /// Max write sequence in the wal file, may be 0 if wal file is new or
    /// CnosDB was crushed or force-killed.
    max_sequence: u64,
}

impl WalReader {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let reader = record_file::Reader::open(&path).await?;

        let (min_sequence, max_sequence) = match reader.footer() {
            Some(footer) => Self::parse_footer(footer).unwrap_or((0_u64, 0_u64)),
            None => (0_u64, 0_u64),
        };

        Ok(Self {
            inner: reader,
            min_sequence,
            max_sequence,
        })
    }

    /// Parses wal footer, returns sequence range.
    pub fn parse_footer(footer: [u8; record_file::FILE_FOOTER_LEN]) -> Option<(u64, u64)> {
        let magic_number = decode_be_u32(&footer[0..4]);
        if magic_number != FOOTER_MAGIC_NUMBER {
            // There is no footer in wal file.
            return None;
        }
        let min_sequence = decode_be_u64(&footer[16..24]);
        let max_sequence = decode_be_u64(&footer[24..32]);
        Some((min_sequence, max_sequence))
    }

    pub async fn next_wal_entry(&mut self) -> Result<Option<WalEntryBlock>> {
        let data = match self.inner.read_record().await {
            Ok(r) => r.data,
            Err(Error::Eof) => {
                return Ok(None);
            }
            Err(e) => {
                error!("Error reading wal: {:?}", e);
                return Err(Error::WalTruncated);
            }
        };
        if data.len() < ENTRY_HEADER_LEN {
            error!("Error reading wal: block length too small: {}", data.len());
            return Ok(None);
        }
        Ok(Some(WalEntryBlock::new(data[0].into(), data)))
    }

    pub fn path(&self) -> PathBuf {
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
}

#[cfg(test)]
mod test {
    use core::panic;
    use std::borrow::BorrowMut;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use chrono::Utc;
    use config::get_config;
    use datafusion::parquet::data_type::AsBytes;
    use flatbuffers::{self, Vector, WIPOffset};
    use lazy_static::lazy_static;
    use meta::meta_manager::RemoteMetaManager;
    use meta::MetaRef;
    use minivec::MiniVec;
    use models::codec::Encoding;
    use models::schema::TenantOptions;
    use models::Timestamp;
    use protos::models::FieldType;
    use protos::{models as fb_models, models_helper};
    use serial_test::serial;
    use tokio::runtime;
    use tokio::sync::RwLock;
    use tokio::time::sleep;
    use trace::{info, init_default_global_tracing};

    use crate::context::GlobalSequenceContext;
    use crate::engine::Engine;
    use crate::file_system::file_manager::{self, list_file_names, FileManager};
    use crate::file_system::FileCursor;
    use crate::kv_option::WalOptions;
    use crate::memcache::test::get_one_series_cache_data;
    use crate::memcache::{FieldVal, MemCache};
    use crate::tsm::codec::get_str_codec;
    use crate::version_set::VersionSet;
    use crate::wal::{self, WalEntryBlock, WalEntryType, WalManager, WalReader};
    use crate::{kv_option, Error, Options, Result, TsKv};

    fn random_write_data() -> Vec<u8> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ptr = models_helper::create_random_points_with_delta(&mut fbb, 5);
        fbb.finish(ptr, None);
        fbb.finished_data().to_vec()
    }

    /// Generate flatbuffers data and memcache data
    #[allow(clippy::type_complexity)]
    fn const_write_data(
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
            vec![
                ("fa", FieldType::Integer, &100_u64.to_be_bytes()),
                ("fb", FieldType::String, b"b"),
            ],
            start_timestamp,
            num,
        );
        fbb.finish(ptr, None);
        (fbb.finished_data().to_vec(), map)
    }

    async fn check_wal_files(
        wal_dir: impl AsRef<Path>,
        data: Vec<Arc<Vec<u8>>>,
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
                    Ok(Some(entry)) => {
                        println!("Reading entry from wal file '{}'", path.display());
                        let ety_data = entry.data();
                        let ori_data = match data_iter.next() {
                            Some(d) => d,
                            None => {
                                panic!("unexpected data to compare that is less than file count.")
                            }
                        };
                        if is_flatbuffers {
                            let mut data_buf = Vec::new();
                            decoder.decode(ety_data, &mut data_buf).unwrap();
                            assert_eq!(data_buf[0].as_slice(), ori_data.as_ref().as_slice());
                            if let Err(e) = flatbuffers::root::<fb_models::Points>(&data_buf[0]) {
                                panic!(
                                    "unexpected data in wal file, ignored file '{}' because '{}'",
                                    wal_dir.display(),
                                    e
                                );
                            }
                        } else {
                            assert_eq!(ety_data, ori_data.as_ref().as_slice());
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

    #[tokio::test]
    async fn test_read_and_write() {
        let dir = "/tmp/test/wal/1".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir.clone();
        let options = Options::from(&global_config);
        let wal_config = WalOptions::from(&global_config);

        let mut mgr = WalManager::open(Arc::new(wal_config), GlobalSequenceContext::empty())
            .await
            .unwrap();
        let mut data_vec = Vec::new();
        for i in 1..=10_u64 {
            let data = Arc::new(b"hello".to_vec());
            data_vec.push(data.clone());

            let (seq, _) = mgr
                .write(WalEntryType::Write, data, 0, Arc::new(b"cnosdb".to_vec()))
                .await
                .unwrap();
            assert_eq!(i, seq)
        }
        mgr.close().await.unwrap();

        check_wal_files(&dir, data_vec, false).await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn test_roll_wal_file() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let dir = "/tmp/test/wal/2".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir.clone();
        // Argument max_file_size is so small that there must a new wal file created.
        global_config.wal.max_file_size = 1;
        global_config.wal.sync = false;
        let options = Options::from(&global_config);
        let wal_config = WalOptions::from(&global_config);

        let tenant = Arc::new(b"cnosdb".to_vec());
        let database = "test_db".to_string();
        let table = "test_table".to_string();
        let min_seq_no = 6;

        let gcs = GlobalSequenceContext::empty();
        gcs.set_min_seq(min_seq_no);

        let mut mgr = WalManager::open(Arc::new(wal_config), gcs).await.unwrap();
        let mut data_vec: Vec<Arc<Vec<u8>>> = Vec::new();
        for seq in 1..=10 {
            let data = Arc::new(format!("{}", seq).as_bytes().to_vec());
            if seq >= min_seq_no {
                // Data in file_id taat less than version_set_min_seq_no will be deleted.
                data_vec.push(data.clone());
            }

            let (write_seq, _) = mgr
                .write(WalEntryType::Write, data.clone(), 0, tenant.clone())
                .await
                .unwrap();
            assert_eq!(seq, write_seq)
        }
        mgr.close().await.unwrap();

        check_wal_files(dir, data_vec, false).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_truncated() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/wal/3".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir.clone();
        let options = Options::from(&global_config);
        let wal_config = WalOptions::from(&global_config);

        let mut mgr = WalManager::open(Arc::new(wal_config), GlobalSequenceContext::empty())
            .await
            .unwrap();
        let coder = get_str_codec(Encoding::Zstd);
        let mut data_vec: Vec<Arc<Vec<u8>>> = Vec::new();

        for i in 0..10 {
            let data = Arc::new(random_write_data());
            data_vec.push(data.clone());

            let mut enc_points = Vec::new();
            coder
                .encode(&[&data], &mut enc_points)
                .map_err(|_| Error::Send)
                .unwrap();
            mgr.write(
                WalEntryType::Write,
                Arc::new(enc_points),
                0,
                Arc::new("cnosdb".as_bytes().to_vec()),
            )
            .await
            .unwrap();
        }
        // Do not close wal manager, so footer won't write.

        check_wal_files(dir, data_vec, true).await.unwrap();
    }

    #[test]
    #[serial]
    fn test_recover_from_wal() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        let dir = "/tmp/test/wal/4/wal";
        let _ = std::fs::remove_dir_all(dir);
        let mut global_config = get_config("../config/config_31001.toml");
        global_config.wal.path = dir.to_string();
        global_config.storage.path = "/tmp/test/wal/4".to_string();
        let options = Options::from(&global_config);
        let wal_config = WalOptions::from(&global_config);

        let mut wrote_data: HashMap<String, Vec<(Timestamp, FieldVal)>> = HashMap::new();
        rt.block_on(async {
            let mut mgr = WalManager::open(Arc::new(wal_config), GlobalSequenceContext::empty())
                .await
                .unwrap();
            let coder = get_str_codec(Encoding::Zstd);
            let mut data_vec: Vec<Arc<Vec<u8>>> = Vec::new();

            for i in 1..=10 {
                let (fb_data, mem_data) = const_write_data(i, 1);
                let data = Arc::new(fb_data);
                data_vec.push(data.clone());

                for (col_name, values) in mem_data {
                    wrote_data
                        .entry(col_name)
                        .or_default()
                        .extend(values.into_iter());
                }

                let mut enc_points = Vec::new();
                coder
                    .encode(&[&data], &mut enc_points)
                    .map_err(|_| Error::Send)
                    .unwrap();
                mgr.write(
                    WalEntryType::Write,
                    Arc::new(enc_points),
                    10,
                    Arc::new("cnosdb".as_bytes().to_vec()),
                )
                .await
                .expect("write succeed");
            }
            mgr.close().await.unwrap();
            check_wal_files(dir, data_vec, true).await.unwrap();
        });
        let rt_2 = rt.clone();
        rt.block_on(async {
            let opt = kv_option::Options::from(&global_config);
            let meta_manager: MetaRef = RemoteMetaManager::new(global_config.cluster).await;
            meta_manager.admin_meta().add_data_node().await.unwrap();
            let _ = meta_manager
                .tenant_manager()
                .create_tenant("cnosdb".to_string(), TenantOptions::default())
                .await;
            let tskv = TsKv::open(meta_manager, opt, rt_2).await.unwrap();
            let ver = tskv
                .get_db_version("cnosdb", "dba", 10)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(ver.ts_family_id, 10);

            let cached_data = get_one_series_cache_data(ver.caches.mut_cache.clone());
            // fa, fb
            assert_eq!(cached_data.len(), 2);
            assert_eq!(wrote_data.len(), 2);
            assert_eq!(wrote_data, cached_data);
        });
    }
}
