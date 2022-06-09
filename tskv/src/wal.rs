use std::{
    io::SeekFrom,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use crc32fast;
use lazy_static::lazy_static;
use models::TagFromParts;
use parking_lot::Mutex;
use protos::models as fb_models;
use regex::Regex;
use snafu::prelude::*;
use tokio::sync::{oneshot, RwLock};
use utils::bkdr_hash::{self, HashWith};
use walkdir::IntoIter;

use crate::{
    compute,
    context::GlobalContext,
    direct_io::{File, FileCursor, FileSync},
    error::{self, Error, Result},
    file_manager::{self, FileManager},
    file_utils, kv_option,
    memcache::MemCache,
    version_set::VersionSet,
};

const SEGMENT_HEADER_SIZE: usize = 32;
const SEGMENT_MAGIC: [u8; 4] = [0x57, 0x47, 0x4c, 0x00];
const SEGMENT_SIZE: u64 = 1073741824; // 1 GiB

const BLOCK_HEADER_SIZE: usize = 17;

pub enum WalTask {
    Write {
        points: Arc<Vec<u8>>,
        // (seq_no, writen_size)
        cb: oneshot::Sender<Result<(u64, usize)>>,
    },
}

#[repr(u8)]
#[derive(Eq, PartialEq, Clone, Copy, Debug)]
pub enum WalEntryType {
    Write   = 1,
    Delete  = 2,
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
    pub seq: u64,
    pub crc: u32,
    pub len: u32,
    pub buf: Vec<u8>,
}

impl WalEntryBlock {
    pub fn new(typ: WalEntryType, buf: &[u8]) -> Self {
        Self { typ: typ.into(),
               seq: 0,
               crc: crc32fast::hash(buf),
               len: buf.len() as u32,
               buf: buf.into() }
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let typ = WalEntryType::from(bytes[0]);
        let seq = compute::decode_be_u64(&bytes[1..9]);
        let crc = compute::decode_be_u32(&bytes[9..13]);
        let len = compute::decode_be_u32(&bytes[13..17]);
        let buf = bytes[17..].to_vec();
        Self { typ, seq, crc, len, buf }
    }

    pub fn size(&self) -> u32 {
        self.len + SEGMENT_HEADER_SIZE as u32
    }
}

struct WalWriter {
    id: u64,
    file: File,
    size: u64,
    path: PathBuf,
    config: Arc<kv_option::WalConfig>,

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
        let readed = cursor.read(&mut header_buf[..]).context(error::IOSnafu)?;

        Ok(header_buf)
    }

    pub fn open(id: u64,
                path: impl AsRef<Path>,
                config: Arc<kv_option::WalConfig>)
                -> Result<Self> {
        // TODO: Check path
        let path = path.as_ref();

        // Get file and check if new file
        let mut new_file = false;
        let file = if file_manager::try_exists(path) {
            let f = file_manager::get_file_manager().open_file(path)?;
            if f.len() == 0 {
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
            file.read_at(0, &mut header_buf[..]).context(error::IOSnafu)?;
            min_sequence = compute::decode_be_u64(&header_buf[4..12]);
            max_sequence = compute::decode_be_u64(&header_buf[12..20]);
        }
        let size = file.len();

        Ok(Self { id,
                  file,
                  size,
                  path: PathBuf::from(path),
                  config,
                  header_buf,
                  min_sequence,
                  max_sequence })
    }

    pub async fn write(&mut self, typ: WalEntryType, data: &[u8]) -> Result<(u64, usize)> {
        let typ = typ as u8;
        let mut pos = self.size;
        let mut seq = self.max_sequence;

        self.file
            // write type
            .write_at(pos, &[typ])
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
                if self.config.sync { self.file.sync_all(FileSync::Soft) } else { Ok(()) }
            })
            .context(error::IOSnafu)?;

        seq += 1;

        // write & fsync succeed
        let writen_size = (pos - self.size) as usize;
        self.size = pos;
        self.max_sequence = seq;

        Ok((seq, writen_size))
    }

    pub async fn flush(&mut self) -> Result<()> {
        // Write header
        self.header_buf[4..12].copy_from_slice(&self.min_sequence.to_be_bytes());
        self.header_buf[12..20].copy_from_slice(&self.max_sequence.to_be_bytes());

        self.file.write_at(0, &self.header_buf).context(error::IOSnafu)?;

        // Do fsync
        self.file.sync_all(FileSync::Hard).context(error::IOSnafu)?;

        Ok(())
    }
}

pub struct WalManager {
    config: Arc<kv_option::WalConfig>,

    current_dir: PathBuf,
    current_file: WalWriter,
}

unsafe impl Send for WalManager {}
unsafe impl Sync for WalManager {}

impl WalManager {
    pub fn new(config: kv_option::WalConfig) -> Self {
        let config = Arc::new(config);
        let current_dir_path = PathBuf::from(config.dir.clone());

        let (last, seq) =
            match file_utils::get_max_sequence_file_name(PathBuf::from(config.dir.clone()),
                                                         file_utils::get_wal_file_id)
            {
                Some((file, seq)) => (current_dir_path.join(file), seq),
                None => {
                    let seq = 1;
                    (file_utils::make_wal_file(&config.dir.clone(), seq), seq)
                },
            };

        if !file_manager::try_exists(&current_dir_path) {
            std::fs::create_dir_all(&current_dir_path).unwrap();
        }
        let file = file_manager::get_file_manager().open_create_file(last.clone()).unwrap();
        let size = file.len();

        let current_file = WalWriter::open(seq, last.clone(), config.clone()).unwrap();

        WalManager { config, current_dir: current_dir_path, current_file }
    }

    pub fn current_seq_no(&self) -> u64 {
        self.current_file.max_sequence
    }

    async fn roll_wal_file(&mut self) -> Result<()> {
        if self.current_file.size > SEGMENT_SIZE {
            let id = self.current_file.id;
            let max_sequence = self.current_file.max_sequence;

            self.current_file.flush().await?;

            let new_file_name = file_utils::make_wal_file(self.config.dir.as_str(), id);
            let new_file = WalWriter::open(id, new_file_name, self.config.clone())?;
            self.current_file = new_file;
        }
        Ok(())
    }

    pub async fn write(&mut self, typ: WalEntryType, data: &[u8]) -> Result<(u64, usize)> {
        self.roll_wal_file().await?;
        self.current_file.write(typ, data).await
    }

    pub async fn recover(&self,
                         version_set: Arc<RwLock<VersionSet>>,
                         global_context: Arc<GlobalContext>)
                         -> Result<()> {
        let min_log_seq = global_context.log_seq();
        println!("[WARN] [wal] recovering version set from seq '{}'", &min_log_seq);
        let mut max_log_seq = min_log_seq;

        let wal_files = file_manager::list_file_names(&self.current_dir);
        for file_name in wal_files {
            let id = file_utils::get_wal_file_id(&file_name)?;
            if id < min_log_seq {
                continue;
            }
            max_log_seq = id;
            let tmp_walfile = WalWriter::open(id,
                                              self.current_dir.join(file_name),
                                              Arc::new(kv_option::WalConfig::default()))?;
            let mut reader = WalReader::new(tmp_walfile.file.into())?;
            let version_set = version_set.read().await;
            while let Some(e) = reader.next_wal_entry() {
                match e.typ {
                    WalEntryType::Write => {
                        let entry = flatbuffers::root::<fb_models::Points>(&e.buf)
                        .context(error::InvalidFlatbufferSnafu)?;
                        if let Some(points) = entry.points() {
                            for p in points.iter() {
                                let mut point_tags: Vec<models::Tag> = vec![];
                                let sid = if let Some(tags) = p.tags() {
                                    for t in tags.iter() {
                                        let tag_key = if let Some(tag_key) = t.key() {
                                            tag_key.to_vec()
                                        } else {
                                            continue;
                                        };
                                        let tag_value = if let Some(tag_value) = t.value() {
                                            tag_value.to_vec()
                                        } else {
                                            vec![]
                                        };
                                        point_tags.push(models::Tag::from_parts(tag_key,
                                                                                tag_value));
                                    }
                                    models::SeriesInfo::cal_sid(&mut point_tags)
                                } else {
                                    // TODO error: no tags
                                    0
                                };
                                if let Some(tsf) = version_set.get_tsfamily(sid) {
                                    if let Some(fields) = p.fields() {
                                        for f in fields.iter() {
                                            let fid = if let Some(field_name) = f.name() {
                                                models::FieldInfo::cal_fid(&field_name.to_vec(),
                                                                           sid)
                                            } else {
                                                // TODO error: no field name
                                                0
                                            };
                                            let val = if let Some(value) = f.value() {
                                                value
                                            } else {
                                                &[0_u8; 0][..]
                                            };
                                            let dtype = match f.type_() {
                                                fb_models::FieldType::Float => {
                                                    models::ValueType::Float
                                                },
                                                fb_models::FieldType::Integer => {
                                                    models::ValueType::Integer
                                                },
                                                fb_models::FieldType::Unsigned => {
                                                    models::ValueType::Unsigned
                                                },
                                                fb_models::FieldType::Boolean => {
                                                    models::ValueType::Boolean
                                                },
                                                fb_models::FieldType::String => {
                                                    models::ValueType::String
                                                },
                                                _ => models::ValueType::Unknown,
                                            };
                                            // todo: change fbs timestamp to i64
                                            tsf.put_mutcache(fid,
                                                             val,
                                                             dtype,
                                                             e.seq,
                                                             p.timestamp() as i64)
                                               .await
                                        }
                                    }
                                } else {
                                    // TODO error: no tseries family
                                }
                            }
                        }
                    },
                    WalEntryType::Delete => {
                        // TODO delete a memcache entry
                    },
                    WalEntryType::DeleteRange => {
                        // TODO delete range in a memcache
                    },
                    _ => {},
                };
            }
        }

        global_context.set_log_seq(max_log_seq);
        println!("[WARN] [wal] version set recovered, log_seq is '{}'", &max_log_seq);

        Ok(())
    }
}

/// Get a WalReader, for loading file to cache.
/// ```
/// use crate::direct_io::{File, FileSystem, Options};
///
/// let file_system: FileSystem = FileSystem::new(&Options::default());
/// let file: File = file_system.open("_00001.wal").unwrap();
///
/// let mut reader: WalReader = reader(file.into_cursor());
/// while let Some(block) = reader.next_wal_entry() {
///     // ...
/// }
/// ```
pub fn reader<'a>(f: File) -> Result<WalReader<'a>> {
    WalReader::new(f.into_cursor())
}

pub struct WalReader<'a> {
    cursor: FileCursor,
    header_buf: [u8; SEGMENT_HEADER_SIZE],
    block_header_buf: [u8; BLOCK_HEADER_SIZE],
    body_buf: Vec<u8>,
    phantom: PhantomData<&'a Self>,
}

impl<'a> WalReader<'_> {
    pub fn new(mut cursor: FileCursor) -> Result<Self> {
        let header_buf = WalWriter::reade_header(&mut cursor)?;

        Ok(Self { cursor,
                  header_buf,
                  block_header_buf: [0_u8; BLOCK_HEADER_SIZE],
                  body_buf: vec![],
                  phantom: PhantomData })
    }

    pub fn next_wal_entry(&mut self) -> Option<WalEntryBlock> {
        println!("[DEBUG] [wal] WalReader: cursor.pos={}", self.cursor.pos());
        let read_bytes = self.cursor.read(&mut self.block_header_buf[..]).unwrap();
        if read_bytes < 8 {
            return None;
        }
        let typ = self.block_header_buf[0];
        let seq = compute::decode_be_u64(self.block_header_buf[1..9].into());
        let crc = compute::decode_be_u32(self.block_header_buf[9..13].into());
        let data_len = compute::decode_be_u32(self.block_header_buf[13..17].try_into().unwrap());
        if data_len <= 0 {
            return None;
        }
        println!("[DEBUG] [wal] WalReader: data_len={}", data_len);

        if data_len as usize > self.body_buf.len() {
            self.body_buf.resize(data_len as usize, 0);
        }
        let buf = &mut self.body_buf.as_mut_slice()[0..data_len as usize];
        let read_bytes = self.cursor.read(buf).unwrap();

        Some(WalEntryBlock { typ: typ.into(), seq, crc, len: read_bytes as u32, buf: buf.to_vec() })
    }
}

#[cfg(test)]
mod test {
    use core::panic;
    use std::{borrow::BorrowMut, sync::Arc};

    use chrono::Utc;
    use flatbuffers::{self, Vector, WIPOffset};
    use lazy_static::lazy_static;
    use protos::{models as fb_models, models_helper};
    use rand;

    use crate::{
        direct_io::{File, FileCursor, FileSync},
        file_manager::{self, list_file_names, FileManager},
        kv_option,
        wal::{self, WalEntryBlock, WalEntryType, WalManager, WalReader},
    };

    const DIR: &'static str = "/tmp/test/";

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

    impl From<&fb_models::ColumnKeys<'_>> for WalEntryBlock {
        fn from(cols: &fb_models::ColumnKeys<'_>) -> Self {
            Self::new(WalEntryType::Delete, cols._tab.buf)
        }
    }

    impl<'a> From<&'a WalEntryBlock> for fb_models::ColumnKeys<'a> {
        fn from(block: &'a WalEntryBlock) -> Self {
            flatbuffers::root::<fb_models::ColumnKeys<'a>>(&block.buf[0..block.len as usize])
                .unwrap()
        }
    }

    impl From<&fb_models::ColumnKeysWithRange<'_>> for WalEntryBlock {
        fn from(cols: &fb_models::ColumnKeysWithRange<'_>) -> Self {
            Self::new(WalEntryType::DeleteRange, cols._tab.buf)
        }
    }

    impl<'a> From<&'a WalEntryBlock> for fb_models::ColumnKeysWithRange<'a> {
        fn from(block: &'a WalEntryBlock) -> Self {
            flatbuffers::root::<fb_models::ColumnKeysWithRange<'a>>(
                &block.buf[0..block.len as usize],
            )
            .unwrap()
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

    fn random_write_wal_entry<'a>(_fbb: &mut flatbuffers::FlatBufferBuilder<'a>)
                                  -> WIPOffset<fb_models::Points<'a>> {
        let fbb = _fbb.borrow_mut();
        models_helper::create_random_points(fbb, 5)
    }

    fn random_delete_wal_entry_item() -> fb_models::ColumnKey {
        fb_models::ColumnKey::new(random_series_id(), random_field_id())
    }

    fn random_delete_wal_entry<'a>(_fbb: &mut flatbuffers::FlatBufferBuilder<'a>)
                                   -> WIPOffset<fb_models::ColumnKeys<'a>> {
        let fbb = _fbb.borrow_mut();

        let mut items: Vec<fb_models::ColumnKey> = vec![];
        for _ in 0..10 {
            items.push(random_delete_wal_entry_item());
        }

        let vec = fbb.create_vector(&items);

        fb_models::ColumnKeys::create(fbb, &fb_models::ColumnKeysArgs { column_keys: Some(vec) })
    }

    fn random_delete_range_wal_entry<'a>(_fbb: &mut flatbuffers::FlatBufferBuilder<'a>)
                                         -> WIPOffset<fb_models::ColumnKeysWithRange<'a>> {
        let fbb = _fbb.borrow_mut();
        let mut items: Vec<fb_models::ColumnKey> = vec![];
        for _ in 0..10 {
            items.push(random_delete_wal_entry_item());
        }

        let vec = fbb.create_vector(&items);

        fb_models::ColumnKeysWithRange::create(
            fbb,
            &fb_models::ColumnKeysWithRangeArgs { column_keys: Some(vec), min: 1, max: 100 },
        )
    }

    fn random_wal_entry_block<'a>(_fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> WalEntryBlock {
        let fbb = _fbb.borrow_mut();

        let entry_type = random_wal_entry_type();
        match entry_type {
            WalEntryType::Write => {
                let ptr = random_write_wal_entry(fbb);
                fbb.finish(ptr, None);
                WalEntryBlock::new(WalEntryType::Write, fbb.finished_data())
            },
            WalEntryType::Delete => {
                let ptr = random_delete_wal_entry(fbb);
                fbb.finish(ptr, None);
                WalEntryBlock::new(WalEntryType::Delete, fbb.finished_data())
            },
            WalEntryType::DeleteRange => {
                let ptr = random_delete_range_wal_entry(fbb);
                fbb.finish(ptr, None);
                WalEntryBlock::new(WalEntryType::DeleteRange, fbb.finished_data())
            },
            _ => panic!("Invalid entry type"),
        }
    }

    #[tokio::test]
    async fn test_write_entry() {
        let wal_config = kv_option::WalConfig { dir: String::from(DIR), ..Default::default() };

        let mut mgr = WalManager::new(wal_config);

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry_block(&mut fbb);

            let bytes = fbb.finished_data();
            dbg!(bytes.len());

            match entry.typ {
                WalEntryType::Write => {
                    let de_block = flatbuffers::root::<fb_models::Points>(&entry.buf).unwrap();
                    mgr.write(WalEntryType::Write, &entry.buf).await.unwrap();
                },
                WalEntryType::Delete => {
                    let de_block = flatbuffers::root::<fb_models::ColumnKeys>(&entry.buf).unwrap();
                    mgr.write(WalEntryType::Delete, &entry.buf).await.unwrap();
                },
                WalEntryType::DeleteRange => {
                    let de_block =
                        flatbuffers::root::<fb_models::ColumnKeysWithRange>(&entry.buf).unwrap();
                    mgr.write(WalEntryType::DeleteRange, &entry.buf).await.unwrap();
                },
                _ => {},
            };
        }
    }

    #[test]
    fn test_read_entry() {
        let wal_config =
            crate::kv_option::WalConfig { dir: String::from("/tmp/test/"), ..Default::default() };

        let mgr = WalManager::new(wal_config);

        let wal_files = list_file_names("/tmp/test/");
        for wal_file in wal_files {
            let file =
                file_manager::get_file_manager().open_file(mgr.current_dir.join(wal_file)).unwrap();
            let cursor: FileCursor = file.into();

            let mut reader = WalReader::new(cursor).unwrap();

            while let Some(entry) = reader.next_wal_entry() {
                dbg!(entry.typ, entry.seq, entry.crc, entry.len);
                match entry.typ {
                    WalEntryType::Write => {
                        let de_block = flatbuffers::root::<fb_models::Points>(&entry.buf).unwrap();
                    },
                    WalEntryType::Delete => {
                        let de_block =
                            flatbuffers::root::<fb_models::ColumnKeys>(&entry.buf).unwrap();
                    },
                    WalEntryType::DeleteRange => {
                        let de_block =
                            flatbuffers::root::<fb_models::ColumnKeysWithRange>(&entry.buf)
                                .unwrap();
                    },
                    _ => panic!("Invalid WalEntry"),
                };
            }
        }
    }

    #[tokio::test]
    async fn test_read_and_write() {
        let wal_config =
            crate::kv_option::WalConfig { dir: String::from(DIR), ..Default::default() };

        let mut mgr = WalManager::new(wal_config);

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry_block(&mut fbb);

            let bytes = fbb.finished_data();
            dbg!(bytes.len());

            match entry.typ {
                WalEntryType::Write => {
                    let de_block = flatbuffers::root::<fb_models::Points>(&entry.buf).unwrap();
                    mgr.write(WalEntryType::Write, &entry.buf).await.unwrap();
                },
                WalEntryType::Delete => {
                    let de_block = flatbuffers::root::<fb_models::ColumnKeys>(&entry.buf).unwrap();
                    mgr.write(WalEntryType::Delete, &entry.buf).await.unwrap();
                },
                WalEntryType::DeleteRange => {
                    let de_block =
                        flatbuffers::root::<fb_models::ColumnKeysWithRange>(&entry.buf).unwrap();
                    mgr.write(WalEntryType::DeleteRange, &entry.buf).await.unwrap();
                },
                _ => {},
            };
        }

        let wal_files = list_file_names(DIR);
        for wal_file in wal_files {
            let file =
                file_manager::get_file_manager().open_file(mgr.current_dir.join(wal_file)).unwrap();
            let cursor: FileCursor = file.into();

            let mut reader = WalReader::new(cursor).unwrap();
            let mut writed_crcs = Vec::<u32>::new();
            let mut readed_crcs = Vec::<u32>::new();
            while let Some(entry) = reader.next_wal_entry() {
                match entry.typ {
                    WalEntryType::Write => {
                        let de_block = flatbuffers::root::<fb_models::Points>(&entry.buf).unwrap();
                        writed_crcs.push(entry.crc);
                        readed_crcs.push(crc32fast::hash(&entry.buf[..entry.len as usize]));
                    },
                    WalEntryType::Delete => {
                        let de_block =
                            flatbuffers::root::<fb_models::ColumnKeys>(&entry.buf).unwrap();
                        writed_crcs.push(entry.crc);
                        readed_crcs.push(crc32fast::hash(&entry.buf[..entry.len as usize]));
                    },
                    WalEntryType::DeleteRange => {
                        let de_block =
                            flatbuffers::root::<fb_models::ColumnKeysWithRange>(&entry.buf)
                                .unwrap();
                        writed_crcs.push(entry.crc);
                        readed_crcs.push(crc32fast::hash(&entry.buf[..entry.len as usize]));
                    },
                    _ => {},
                };
            }
            assert_eq!(writed_crcs, readed_crcs);
        }
    }
}
