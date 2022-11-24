use datafusion::parquet::data_type::AsBytes;
use std::{
    io::SeekFrom,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use lazy_static::lazy_static;
use parking_lot::{Mutex, RwLock};
use regex::Regex;
use snafu::prelude::*;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use walkdir::IntoIter;

use crate::tsm::{DecodeSnafu, EncodeSnafu};
use engine::EngineRef;
use models::codec::Encoding;
use protos::kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest};
use protos::models as fb_models;
use trace::{debug, error, info, warn};

use crate::file_system::file_manager::{self, FileManager};
use crate::tsm::codec::get_str_codec;
use crate::{
    byte_utils,
    compaction::FlushReq,
    context::GlobalContext,
    error::{self, Error, Result},
    file_system::{DmaFile, FileCursor, FileSync},
    file_utils,
    kv_option::WalOptions,
    memcache::MemCache,
    version_set::VersionSet,
};
use crate::{engine, TseriesFamilyId};

const SEGMENT_HEADER_SIZE: usize = 32;
const SEGMENT_MAGIC: [u8; 4] = [0x57, 0x47, 0x4c, 0x00];
const SEGMENT_SIZE: u64 = 1073741824; // 1 GiB

const BLOCK_HEADER_SIZE: usize = 21;

pub enum WalTask {
    Write {
        id: TseriesFamilyId,
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
    pub id: TseriesFamilyId,
    pub seq: u64,
    pub crc: u32,
    pub len: u32,
    pub buf: Vec<u8>,
}

impl WalEntryBlock {
    pub fn new(typ: WalEntryType, buf: &[u8]) -> Self {
        Self {
            typ,
            seq: 0,
            id: 0,
            crc: crc32fast::hash(buf),
            len: buf.len() as u32,
            buf: buf.into(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let typ = WalEntryType::from(bytes[0]);
        let id = byte_utils::decode_be_u32(&bytes[1..5]);
        let seq = byte_utils::decode_be_u64(&bytes[5..13]);
        let crc = byte_utils::decode_be_u32(&bytes[13..17]);
        let len = byte_utils::decode_be_u32(&bytes[17..21]);
        let buf = bytes[17..].to_vec();
        Self {
            typ,
            id,
            seq,
            crc,
            len,
            buf,
        }
    }

    pub fn size(&self) -> u32 {
        self.len + SEGMENT_HEADER_SIZE as u32
    }
}

struct WalWriter {
    id: u64,
    file: DmaFile,
    size: u64,
    path: PathBuf,
    config: Arc<WalOptions>,

    header_buf: [u8; SEGMENT_HEADER_SIZE],
    min_sequence: u64,
    max_sequence: u64,
}

impl WalWriter {
    fn reade_header(cursor: &mut FileCursor) -> Result<[u8; SEGMENT_HEADER_SIZE]> {
        let mut header_buf = [0_u8; SEGMENT_HEADER_SIZE];

        let min_sequence: u64;
        let max_sequence: u64;
        cursor.seek(SeekFrom::Start(0)).context(error::IOSnafu)?;
        let read = cursor.read(&mut header_buf[..]).context(error::IOSnafu)?;

        Ok(header_buf)
    }

    pub fn open(id: u64, path: impl AsRef<Path>, config: Arc<WalOptions>) -> Result<Self> {
        // TODO: Check path
        let path = path.as_ref();

        // Get file and check if new file
        let mut new_file = false;
        let file = if file_manager::try_exists(path) {
            let f = file_manager::get_file_manager().open_file(path)?;
            if f.is_empty() {
                new_file = true;
            }
            f
        } else {
            new_file = true;
            file_manager::get_file_manager().create_file(path)?
        };

        // Get metadata; if new file then write header
        let min_sequence: u64;
        let max_sequence: u64;
        let mut header_buf = [0_u8; SEGMENT_HEADER_SIZE];
        if new_file {
            min_sequence = 0;
            max_sequence = 0;
            header_buf[..4].copy_from_slice(SEGMENT_MAGIC.as_slice());
            file.write_at(0, &header_buf)
                .and_then(|_| file.sync_all(FileSync::Hard))
                .context(error::IOSnafu)?;
        } else {
            file.read_at(0, &mut header_buf[..])
                .context(error::IOSnafu)?;
            min_sequence = byte_utils::decode_be_u64(&header_buf[4..12]);
            max_sequence = byte_utils::decode_be_u64(&header_buf[12..20]);
        }
        let size = file.len();

        Ok(Self {
            id,
            file,
            size,
            path: PathBuf::from(path),
            config,
            header_buf,
            min_sequence,
            max_sequence,
        })
    }

    pub async fn write(
        &mut self,
        typ: WalEntryType,
        data: &[u8],
        id: TseriesFamilyId,
    ) -> Result<(u64, usize)> {
        let typ = typ as u8;
        let mut pos = self.size;
        let mut seq = self.max_sequence;

        self.file
            // write type
            .write_at(pos, &[typ])
            .and_then(|size| {
                pos += size as u64;
                self.file.write_at(pos, &id.to_be_bytes())
            })
            .and_then(|size| {
                // write seq
                pos += size as u64;
                self.file.write_at(pos, &seq.to_be_bytes())
            })
            .and_then(|size| {
                // write crc
                pos += size as u64;
                let crc = crc32fast::hash(data);
                self.file.write_at(pos, &crc.to_be_bytes())
            })
            .and_then(|size| {
                // write len
                pos += size as u64;
                let len = data.len() as u32;
                self.file.write_at(pos, &len.to_be_bytes())
            })
            .and_then(|size| {
                // write data
                pos += size as u64;
                self.file.write_at(pos, data)
            })
            .and_then(|size| {
                // sync
                pos += size as u64;
                if self.config.sync {
                    self.file.sync_all(FileSync::Soft)
                } else {
                    Ok(())
                }
            })
            .context(error::IOSnafu)?;

        seq += 1;

        // write & fsync succeed
        let written_size = (pos - self.size) as usize;
        self.size = pos;
        self.max_sequence = seq;

        Ok((seq, written_size))
    }

    pub async fn flush(&mut self) -> Result<()> {
        // Write header
        self.header_buf[4..12].copy_from_slice(&self.min_sequence.to_be_bytes());
        self.header_buf[12..20].copy_from_slice(&self.max_sequence.to_be_bytes());

        self.file
            .write_at(0, &self.header_buf)
            .context(error::IOSnafu)?;

        // Do fsync
        self.file.sync_all(FileSync::Hard).context(error::IOSnafu)?;

        Ok(())
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
    pub fn new(config: Arc<WalOptions>) -> Self {
        if !file_manager::try_exists(&config.path) {
            std::fs::create_dir_all(&config.path).unwrap();
        }

        // Create a new wal file every time it starts.
        let new_seq = match file_utils::get_max_sequence_file_name(
            config.path.clone(),
            file_utils::get_wal_file_id,
        ) {
            Some((_, seq)) => seq + 1,
            None => 1,
        };

        let new_wal = file_utils::make_wal_file(config.path.clone(), new_seq);
        let current_file = WalWriter::open(new_seq, new_wal, config.clone()).unwrap();
        let current_dir = config.path.clone();
        WalManager {
            config,
            current_dir,
            current_file,
        }
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

            let new_file = WalWriter::open(new_file_id, new_file_name, self.config.clone())?;
            let mut old_file = std::mem::replace(&mut self.current_file, new_file);
            old_file.flush().await?;

            info!("WAL '{}' starts write", self.current_file.id);
        }
        Ok(())
    }

    pub async fn write(
        &mut self,
        typ: WalEntryType,
        data: &[u8],
        id: TseriesFamilyId,
    ) -> Result<(u64, usize)> {
        self.roll_wal_file().await?;
        self.current_file.write(typ, data, id).await
    }

    pub async fn recover(
        &self,
        engine: &impl engine::Engine,
        global_context: Arc<GlobalContext>,
    ) -> Result<()> {
        let min_log_seq = global_context.last_seq();
        warn!("recovering version set from seq '{}'", &min_log_seq);

        let wal_files = file_manager::list_file_names(&self.current_dir);
        for file_name in wal_files {
            let id = file_utils::get_wal_file_id(&file_name)?;
            let path = self.current_dir.join(file_name);
            if !file_manager::try_exists(&path) {
                continue;
            }
            let file = file_manager::get_file_manager().open_file(&path)?;
            if file.is_empty() {
                continue;
            }
            let mut reader = WalReader::new(file.into())?;
            if reader.max_sequence < min_log_seq {
                // If this file is a new file, or empty file, continue.
                continue;
            }

            loop {
                match reader.next_wal_entry() {
                    Ok(Some(e)) => {
                        if e.seq < min_log_seq {
                            continue;
                        }
                        match e.typ {
                            WalEntryType::Write => {
                                let decoder = get_str_codec(Encoding::Zstd);
                                let mut dst = Vec::new();
                                decoder.decode(&e.buf, &mut dst).context(DecodeSnafu)?;
                                debug_assert_eq!(dst.len(), 1);
                                let req = WritePointsRpcRequest {
                                    version: 1,
                                    points: dst[0].to_vec(),
                                };
                                engine.write_from_wal(e.id, req, e.seq).await.unwrap();
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
                        panic!("Failed to recover from {}: {:?}", path.display(), e);
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.current_file.flush().await
    }
}

pub fn reader(f: DmaFile) -> Result<WalReader> {
    WalReader::new(f.into_cursor())
}

pub struct WalReader {
    cursor: FileCursor,
    header_buf: [u8; SEGMENT_HEADER_SIZE],
    block_header_buf: [u8; BLOCK_HEADER_SIZE],
    max_sequence: u64,
    body_buf: Vec<u8>,
}

impl WalReader {
    pub fn new(mut cursor: FileCursor) -> Result<Self> {
        let header_buf = WalWriter::reade_header(&mut cursor)?;
        let max_sequence = byte_utils::decode_be_u64(&header_buf[12..20]);

        Ok(Self {
            cursor,
            header_buf,
            max_sequence,
            block_header_buf: [0_u8; BLOCK_HEADER_SIZE],
            body_buf: vec![],
        })
    }

    pub fn next_wal_entry(&mut self) -> Result<Option<WalEntryBlock>> {
        if self.cursor.len() - self.cursor.pos() < BLOCK_HEADER_SIZE as u64 {
            return Err(Error::WalTruncated);
        }
        let read_bytes = match self.cursor.read(&mut self.block_header_buf[..]) {
            Ok(v) => v,
            Err(e) => {
                error!("failed read block header buf : {:?}", e);
                return Ok(None);
            }
        };
        if read_bytes < 8 {
            return Ok(None);
        }
        let typ = self.block_header_buf[0];
        let id = byte_utils::decode_be_u32(self.block_header_buf[1..5].into());
        let seq = byte_utils::decode_be_u64(self.block_header_buf[5..13].into());
        let crc = byte_utils::decode_be_u32(self.block_header_buf[13..17].into());
        let key = match self.block_header_buf[17..21].try_into() {
            Ok(v) => v,
            Err(e) => {
                error!("failed try into block header buf : {:?}", e);
                return Ok(None);
            }
        };
        let data_len = byte_utils::decode_be_u32(key);
        if data_len == 0 {
            return Ok(None);
        }
        if self.cursor.len() - self.cursor.pos() < data_len as u64 {
            return Err(Error::WalTruncated);
        }

        if data_len as usize > self.body_buf.len() {
            self.body_buf.resize(data_len as usize, 0);
        }
        let buf = &mut self.body_buf.as_mut_slice()[0..data_len as usize];
        let read_bytes = match self.cursor.read(buf) {
            Ok(v) => v,
            Err(e) => {
                error!("failed read body buf : {:?}", e);
                return Ok(None);
            }
        };

        Ok(Some(WalEntryBlock {
            typ: typ.into(),
            id,
            seq,
            crc,
            len: read_bytes as u32,
            buf: buf.to_vec(),
        }))
    }
}

#[cfg(test)]
mod test {
    use core::panic;
    use std::time::Duration;
    use std::{borrow::BorrowMut, path::PathBuf, sync::Arc};

    use chrono::Utc;
    use flatbuffers::{self, Vector, WIPOffset};
    use lazy_static::lazy_static;
    use tokio::runtime;
    use tokio::time::sleep;

    use config::get_config;
    use models::codec::Encoding;
    use protos::{models as fb_models, models_helper};
    use trace::{info, init_default_global_tracing};

    use crate::engine::Engine;
    use crate::file_system::file_manager::{self, list_file_names, FileManager};
    use crate::tsm::codec::get_str_codec;
    use crate::{
        file_system::{DmaFile, FileCursor, FileSync},
        kv_option::WalOptions,
        wal::{self, WalEntryBlock, WalEntryType, WalManager, WalReader},
    };
    use crate::{kv_option, Error, TsKv};

    impl From<&fb_models::Points<'_>> for WalEntryBlock {
        fn from(entry: &fb_models::Points) -> Self {
            Self::new(WalEntryType::Write, entry._tab.buf)
        }
    }

    impl<'a> From<&'a WalEntryBlock> for fb_models::Points<'a> {
        fn from(block: &'a WalEntryBlock) -> Self {
            flatbuffers::root::<fb_models::Points<'a>>(&block.buf[0..block.len as usize]).unwrap()
        }
    }

    fn random_series_id() -> u64 {
        rand::random::<u64>()
    }

    fn random_field_id() -> u64 {
        rand::random::<u64>()
    }

    fn random_wal_entry_type() -> WalEntryType {
        let rand = rand::random::<u8>() % 3;
        match rand {
            0 => WalEntryType::Write,
            1 => WalEntryType::Delete,
            _ => WalEntryType::DeleteRange,
        }
    }

    fn random_write_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<fb_models::Points<'a>> {
        let fbb = _fbb.borrow_mut();
        models_helper::create_random_points_with_delta(fbb, 5)
    }

    fn write_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<fb_models::Points<'a>> {
        let fbb = _fbb.borrow_mut();
        models_helper::create_const_points(fbb, 5)
    }

    fn random_wal_entry_block(_fbb: &mut flatbuffers::FlatBufferBuilder) -> WalEntryBlock {
        let fbb = _fbb.borrow_mut();
        let ptr = random_write_wal_entry(fbb);
        fbb.finish(ptr, None);
        WalEntryBlock::new(WalEntryType::Write, fbb.finished_data())
    }

    fn wal_entry_block(_fbb: &mut flatbuffers::FlatBufferBuilder) -> WalEntryBlock {
        let fbb = _fbb.borrow_mut();
        let ptr = write_wal_entry(fbb);
        fbb.finish(ptr, None);
        WalEntryBlock::new(WalEntryType::Write, fbb.finished_data())
    }

    fn check_wal_files(wal_dir: PathBuf) {
        let wal_files = list_file_names(&wal_dir);
        for wal_file in wal_files {
            let path = wal_dir.join(wal_file);
            let file = file_manager::get_file_manager().open_file(&path).unwrap();
            let cursor: FileCursor = file.into();

            let mut reader = WalReader::new(cursor).unwrap();
            let mut wrote_crcs = Vec::<u32>::new();
            let mut read_crcs = Vec::<u32>::new();
            loop {
                match reader.next_wal_entry() {
                    Ok(Some(entry)) => {
                        match entry.typ {
                            WalEntryType::Write => {
                                let decoder = get_str_codec(Encoding::Zstd);
                                let mut buf = Vec::new();
                                decoder.decode(&entry.buf, &mut buf).unwrap();
                                let de_block = match flatbuffers::root::<fb_models::Points>(&buf[0])
                                {
                                    Ok(blk) => blk,
                                    Err(e) => {
                                        panic!(
                                            "unexpected data in wal file, ignored file {}: {}",
                                            wal_dir.display(),
                                            e
                                        );
                                    }
                                };
                                wrote_crcs.push(entry.crc);
                                read_crcs.push(crc32fast::hash(&entry.buf[..entry.len as usize]));
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
                    Ok(None) => break,
                    Err(Error::WalTruncated) => {
                        println!("WAL file truncated: {}", path.display());
                        break;
                    }
                    Err(e) => {
                        panic!("Failed to recover from {}: {:?}", path.display(), e);
                    }
                }
            }
            assert_eq!(wrote_crcs, read_crcs);
        }
    }

    #[tokio::test]
    async fn test_read_and_write() {
        let dir = "/tmp/test/wal/1".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir.clone();
        let wal_config = WalOptions::from(&global_config);

        let mut mgr = WalManager::new(Arc::new(wal_config));

        for _i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let entry = random_wal_entry_block(&mut fbb);

            if entry.typ == WalEntryType::Write {
                let mut enc_points = Vec::new();
                let coder = get_str_codec(Encoding::Zstd);
                coder
                    .encode(&[&entry.buf], &mut enc_points)
                    .map_err(|_| Error::Send)
                    .unwrap();
                mgr.write(WalEntryType::Write, &enc_points, 0)
                    .await
                    .unwrap();
            };
        }

        check_wal_files(mgr.current_dir);
    }

    #[tokio::test]
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
        let mut mgr = WalManager::new(Arc::new(wal_config));
        for _i in 0..100 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let points = models_helper::create_dev_ops_points(&mut fbb, 10, &database, &table);
            fbb.finish(points, None);
            let blk = WalEntryBlock::new(WalEntryType::Write, fbb.finished_data());
            let mut enc_points = Vec::new();
            let coder = get_str_codec(Encoding::Zstd);
            coder
                .encode(&[&blk.buf], &mut enc_points)
                .map_err(|_| Error::Send)
                .unwrap();
            mgr.write(WalEntryType::Write, &enc_points, 0)
                .await
                .unwrap();
        }
        mgr.close().await.unwrap();

        check_wal_files(mgr.current_dir);
    }

    #[tokio::test]
    #[should_panic]
    async fn test_read_truncated() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");

        let dir = "/tmp/test/wal/3".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir.clone();
        let wal_config = WalOptions::from(&global_config);

        let mut mgr = WalManager::new(Arc::new(wal_config));

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let mut entry = random_wal_entry_block(&mut fbb);

            if i == 9 {
                entry.buf = entry.buf[10..].to_vec();
            }

            if entry.typ == WalEntryType::Write {
                let mut enc_points = Vec::new();
                let coder = get_str_codec(Encoding::Zstd);
                coder
                    .encode(&[&entry.buf], &mut enc_points)
                    .map_err(|_| Error::Send)
                    .unwrap();
                mgr.write(WalEntryType::Write, &enc_points, 0)
                    .await
                    .unwrap();
            };
        }

        check_wal_files(mgr.current_dir);
    }

    #[test]
    fn test_recover_from_wal() {
        init_default_global_tracing("tskv_log", "tskv.log", "debug");
        let rt = Arc::new(runtime::Runtime::new().unwrap());
        let dir = "/tmp/test/wal/4".to_string();
        let dir_summary = "/tmp/test/wal/summary4".to_string();
        let _ = std::fs::remove_dir_all(dir.clone()); // Ignore errors
        let mut global_config = get_config("../config/config.toml");
        global_config.wal.path = dir;
        global_config.storage.path = dir_summary;
        let wal_config = WalOptions::from(&global_config);
        let mut mgr = WalManager::new(Arc::new(wal_config));
        for _i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();
            let entry = wal_entry_block(&mut fbb);

            if entry.typ == WalEntryType::Write {
                let mut enc_points = Vec::new();
                let coder = get_str_codec(Encoding::Zstd);
                coder
                    .encode(&[&entry.buf], &mut enc_points)
                    .map_err(|_| Error::Send)
                    .unwrap();
                rt.block_on(mgr.write(WalEntryType::Write, &enc_points, 10))
                    .unwrap();
            };
        }

        check_wal_files(mgr.current_dir);
        let opt = kv_option::Options::from(&global_config);
        let tskv = rt.block_on(TsKv::open(opt, rt.clone())).unwrap();
        let ver = tskv.get_db_version("db0", 10).unwrap().unwrap();
        assert_eq!(ver.ts_family_id, 10);
        let expected = r#"[RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }, RowData { ts: 1, fields: [Some(Integer(100)), Some(Float(4.94e-321))] }]"#;
        let ans = format!(
            "{:?}",
            ver.caches.mut_cache.read().read_series_data()[0]
                .1
                .read()
                .groups[0]
                .rows
        );
        assert_eq!(&ans, expected);
    }
}
