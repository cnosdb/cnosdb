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

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use models::codec::Encoding;
use protos::kv_service::WritePointsRpcRequest;
use snafu::ResultExt;
use tokio::sync::oneshot;
use trace::{debug, error, info, warn};

use crate::{
    byte_utils,
    context::GlobalContext,
    engine,
    error::{self, Error, Result},
    file_system::file_manager,
    file_utils,
    kv_option::WalOptions,
    record_file::{self, Record, RecordDataType, RecordDataVersion},
    tsm::{codec::get_str_codec, DecodeSnafu, EncodeSnafu},
};

const ENTRY_TYPE_LEN: usize = 1;
const ENTRY_SEQUENCE_LEN: usize = 8;
const ENTRY_HEADER_LEN: usize = 9; // 1 + 8
const FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'w', b'a', b'l', b'o']);
const FOOTER_MAGIC_NUMBER_LEN: usize = 4;

const SEGMENT_MAGIC: [u8; 4] = [0x57, 0x47, 0x4c, 0x00];
const SEGMENT_SIZE: u64 = 1073741824; // 1 GiB

const BLOCK_HEADER_SIZE: usize = 17;

pub enum WalTask {
    Write {
        points: Arc<Vec<u8>>,
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
        byte_utils::decode_be_u64(&self.buf[1..9])
    }

    pub fn data(&self) -> &[u8] {
        &self.buf[ENTRY_HEADER_LEN..]
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
    pub async fn write(&mut self, typ: WalEntryType, data: Arc<Vec<u8>>) -> Result<(u64, usize)> {
        let mut seq = self.max_sequence;

        let written_size = self
            .inner
            .write_record(
                RecordDataVersion::V1 as u8,
                RecordDataType::Wal as u8,
                [&[typ as u8][..], &seq.to_be_bytes(), &data].as_slice(),
            )
            .await?;
        seq += 1;

        if self.config.sync {
            self.inner.sync().await?;
        }
        // write & fsync succeed
        self.max_sequence += 1;
        self.size += written_size as u64;
        Ok((seq, written_size))
    }

    pub async fn close(mut self) -> Result<()> {
        let footer = build_footer(self.min_sequence, self.max_sequence);
        self.inner.write_footer(footer).await?;
        self.inner.close().await
    }
}

pub struct WalManager {
    config: Arc<WalOptions>,

    current_dir: PathBuf,
    current_file: WalWriter,
}

unsafe impl Send for WalManager {}

unsafe impl Sync for WalManager {}

impl WalManager {
    pub async fn open(config: Arc<WalOptions>) -> Result<Self> {
        if !file_manager::try_exists(&config.path) {
            std::fs::create_dir_all(&config.path).unwrap();
        }

        // Create a new wal file every time it starts.
        let (pre_max_seq, next_file_id) = match file_utils::get_max_sequence_file_name(
            config.path.clone(),
            file_utils::get_wal_file_id,
        ) {
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
        let current_dir = config.path.clone();
        Ok(WalManager {
            config,
            current_dir,
            current_file,
        })
    }

    pub fn current_seq_no(&self) -> u64 {
        self.current_file.max_sequence
    }

    async fn roll_wal_file(&mut self) -> Result<()> {
        if self.current_file.size > SEGMENT_SIZE {
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
            let old_file = std::mem::replace(&mut self.current_file, new_file);
            old_file.close().await?;

            info!("WAL '{}' starts write", self.current_file.id);
        }
        Ok(())
    }

    /// Checks if wal file is full then writes data. Return data sequence and data size.
    pub async fn write(&mut self, typ: WalEntryType, data: Arc<Vec<u8>>) -> Result<(u64, usize)> {
        self.roll_wal_file().await?;
        self.current_file.write(typ, data).await
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
            if reader.len() == 0 {
                continue;
            }

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
                            let decoder = get_str_codec(Encoding::Snappy);
                            let mut dst = Vec::new();
                            decoder.decode(&e.data(), &mut dst).context(DecodeSnafu)?;
                            debug_assert_eq!(dst.len(), 1);
                            let req = WritePointsRpcRequest {
                                version: 1,
                                points: dst[0].to_vec(),
                            };
                            engine.write_from_wal(req, seq).await.unwrap();
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

    pub async fn close(self) -> Result<()> {
        self.current_file.close().await
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
        let magic_number = byte_utils::decode_be_u32(&footer[0..4]);
        if magic_number != FOOTER_MAGIC_NUMBER {
            // There is no footer in wal file.
            return None;
        }
        let min_sequence = byte_utils::decode_be_u64(&footer[16..24]);
        let max_sequence = byte_utils::decode_be_u64(&footer[24..32]);
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
    use std::path::Path;
    use std::{borrow::BorrowMut, path::PathBuf, sync::Arc};

    use chrono::Utc;
    use flatbuffers::{self, Vector, WIPOffset};
    use lazy_static::lazy_static;
    use serial_test::serial;
    use tokio::runtime;

    use config::get_config;
    use models::codec::Encoding;
    use protos::{models as fb_models, models_helper};
    use trace::init_default_global_tracing;

    use crate::engine::Engine;
    use crate::file_system::file_manager::{self, list_file_names, FileManager};
    use crate::tsm::codec::get_str_codec;
    use crate::{
        file_system::FileCursor,
        kv_option::WalOptions,
        wal::{self, WalEntryBlock, WalEntryType, WalManager, WalReader},
    };
    use crate::{kv_option, Error, Result, TsKv};

    fn random_write_data() -> Vec<u8> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ptr = models_helper::create_random_points_with_delta(&mut fbb, 5);
        fbb.finish(ptr, None);
        fbb.finished_data().to_vec()
    }

    fn const_write_data() -> Vec<u8> {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ptr = models_helper::create_const_points(&mut fbb, 5);
        fbb.finish(ptr, None);
        fbb.finished_data().to_vec()
    }

    async fn check_wal_files(
        wal_dir: impl AsRef<Path>,
        data: Vec<Arc<Vec<u8>>>,
        is_flatbuffers: bool,
    ) -> Result<()> {
        let wal_dir = wal_dir.as_ref();
        let wal_files = list_file_names(&wal_dir);
        for wal_file in wal_files {
            let path = wal_dir.join(wal_file);

            let mut data_iter = data.iter();
            let mut reader = WalReader::open(&path).await.unwrap();
            let decoder = get_str_codec(Encoding::Snappy);
            loop {
                match reader.next_wal_entry().await {
                    Ok(Some(entry)) => {
                        let mut data_buf = Vec::new();
                        decoder.decode(&entry.data(), &mut data_buf).unwrap();
                        let ori_data = data_iter.next().unwrap();
                        assert_eq!(data_buf[0].as_slice(), ori_data.as_ref().as_slice());
                        if is_flatbuffers {
                            if let Err(e) = flatbuffers::root::<fb_models::Points>(&data_buf[0]) {
                                panic!(
                                    "unexpected data in wal file, ignored file '{}' because '{}'",
                                    wal_dir.display(),
                                    e
                                );
                            }
                        }
                    }
                    Ok(None) => break,
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
        return Ok(());
    }

    #[tokio::test]
    async fn test_read_and_write() {
        let dir = "/tmp/test/wal/1".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir.clone();
        let wal_config = WalOptions::from(&global_config);

        let mut mgr = WalManager::open(Arc::new(wal_config)).await.unwrap();
        let coder = get_str_codec(Encoding::Snappy);
        let data_vec = vec![Arc::new(b"hello".to_vec()); 10];
        for d in data_vec.iter() {
            let mut enc_points = Vec::new();
            coder
                .encode(&[&d], &mut enc_points)
                .map_err(|_| Error::Send)
                .unwrap();

            let mut dec_points = Vec::new();
            coder.decode(&enc_points, &mut dec_points).unwrap();

            mgr.write(WalEntryType::Write, Arc::new(enc_points))
                .await
                .unwrap();
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
        global_config.wal.sync = false;
        let wal_config = WalOptions::from(&global_config);

        let database = "test_db".to_string();
        let table = "test_table".to_string();
        let mut mgr = WalManager::open(Arc::new(wal_config)).await.unwrap();
        let coder = get_str_codec(Encoding::Snappy);
        let mut data_vec: Vec<Arc<Vec<u8>>> = Vec::new();
        for _i in 0..1 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points = models_helper::create_dev_ops_points(&mut fbb, 10, &database, &table);
            fbb.finish(points, None);

            let data = Arc::new(fbb.finished_data().to_vec());
            data_vec.push(data.clone());

            let mut enc_points = Vec::new();
            coder
                .encode(&[&data], &mut enc_points)
                .map_err(|_| Error::Send)
                .unwrap();
            mgr.write(WalEntryType::Write, Arc::new(enc_points))
                .await
                .unwrap();
        }
        mgr.close().await.unwrap();

        check_wal_files(dir, data_vec, true).await.unwrap();
    }

    #[tokio::test]
    async fn test_read_truncated() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let dir = "/tmp/test/wal/3".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir.clone();
        let wal_config = WalOptions::from(&global_config);

        let mut mgr = WalManager::open(Arc::new(wal_config)).await.unwrap();
        let coder = get_str_codec(Encoding::Snappy);
        let mut data_vec: Vec<Arc<Vec<u8>>> = Vec::new();

        for i in 0..10 {
            let data = Arc::new(random_write_data());
            data_vec.push(data.clone());

            let mut enc_points = Vec::new();
            coder
                .encode(&[&data], &mut enc_points)
                .map_err(|_| Error::Send)
                .unwrap();
            mgr.write(WalEntryType::Write, Arc::new(enc_points))
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
        let dir = "/tmp/test/wal/4";
        let dir_summary = "/tmp/test/wal/4/summary";
        let _ = std::fs::remove_dir_all(dir.clone());
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir.to_string();
        global_config.storage.path = dir_summary.to_string();
        let wal_config = WalOptions::from(&global_config);

        let mut mgr = rt.block_on(WalManager::open(Arc::new(wal_config))).unwrap();
        let coder = get_str_codec(Encoding::Snappy);
        let mut data_vec: Vec<Arc<Vec<u8>>> = Vec::new();

        for _i in 0..10 {
            let data = Arc::new(const_write_data());
            data_vec.push(data.clone());

            let mut enc_points = Vec::new();
            coder
                .encode(&[&data], &mut enc_points)
                .map_err(|_| Error::Send)
                .unwrap();
            rt.block_on(mgr.write(WalEntryType::Write, Arc::new(enc_points)))
                .expect("write succeed");
        }
        rt.block_on(mgr.close()).unwrap();
        rt.block_on(check_wal_files(dir, data_vec, true)).unwrap();

        let opt = kv_option::Options::from(&global_config);
        let tskv = rt.block_on(TsKv::open(opt, rt.clone())).unwrap();
        let ver = tskv.get_db_version("db0").unwrap().unwrap();
        let expect = r#"range: TimeRange { min_ts: 1, max_ts: 1 }, rows: [RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }], size: 736 }] } })]"#;
        let ans = format!("{:?}", ver.caches.mut_cache.read().read_series_data());
        assert_eq!(&ans[573..], expect);
    }
}
