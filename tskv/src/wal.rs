use std::{
    any::Any,
    marker::PhantomData,
    path::{Path, PathBuf},
};

use crc32fast;
use lazy_static::lazy_static;
use protos::models;
use regex::Regex;
use snafu::prelude::*;
use tokio::sync::oneshot;
use walkdir::IntoIter;

use crate::{
    compute,
    direct_io::{make_io_task, File, FileCursor, FileSync, FileSystem, Options, TaskType},
    file_manager, kv_option, FileManager,
};

lazy_static! {
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new("_.*\\.wal").unwrap();
}

const SEGMENT_SIZE: u64 = 1073741824; // 1 GiB

pub enum WalTask {
    Write {
        points: Vec<u8>,
        cb: oneshot::Sender<WalResult<()>>,
    },
}

#[derive(Snafu, Debug)]
pub enum WalError {
    #[snafu(display("Unable to walk dir : {}", source))]
    UnableToWalkDir { source: walkdir::Error },

    #[snafu(display("File {} has wrong name format to have an id", file_name))]
    InvalidFileName { file_name: String },

    #[snafu(display("Error with file : {}", source))]
    FailedWithFileManager {
        source: super::file_manager::FileError,
    },

    #[snafu(display("Error with std::io : {}", source))]
    FailedWithStdIO { source: std::io::Error },

    #[snafu(display("Error with receiving from channel"))]
    FailedWithChannelReceive,

    #[snafu(display("Error with sending from channel"))]
    FailedWithChannelSend,

    #[snafu(display("Error with IO task"))]
    FailedWithIoTask,
}

pub type WalResult<T> = std::result::Result<T, WalError>;

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

pub enum WalEntryBlock {
    Write(WalEntryBlockInner),
    Delete(WalEntryBlockInner),
    DeleteRange(WalEntryBlockInner),
}

pub struct WalEntryBlockInner {
    pub typ: WalEntryType,
    pub crc: u32,
    pub len: u32,
    pub buf: Vec<u8>,
}

#[derive(Clone)]
struct WalFile {
    id: u64,
    file: File,
    size: u64,
}

#[derive(Clone)]
pub struct WalFileManager {
    config: kv_option::WalConfig,

    current_dir_path: PathBuf,
    current_file: WalFile,
}

unsafe impl Send for WalFileManager {}
unsafe impl Sync for WalFileManager {}

pub fn get_max_sequence_file_name(dir: impl AsRef<Path>) -> Option<(PathBuf, u64)> {
    let segments = file_manager::list_file_names(dir);
    if segments.is_empty() {
        return None;
    }
    let mut max_id = 1;
    let mut max_index = 0;
    for (i, file_name) in segments.iter().enumerate() {
        match get_id_by_file_name(&file_name) {
            Ok(id) => {
                if max_id < id {
                    max_id = id;
                    max_index = i;
                }
            }
            Err(_) => continue,
        }
    }
    let max_file_name = segments.get(max_index).unwrap();
    Some((PathBuf::from(max_file_name), max_id))
}

fn get_id_by_file_name(file_name: &String) -> WalResult<u64> {
    if !WAL_FILE_NAME_PATTERN.is_match(file_name) {
        return Err(WalError::InvalidFileName {
            file_name: file_name.clone(),
        });
    }
    let parts: Vec<&str> = file_name.split(".").collect();
    if parts.len() != 2 {
        Err(WalError::InvalidFileName {
            file_name: file_name.clone(),
        })
    } else {
        parts
            .first()
            .unwrap()
            .split_at(1)
            .1
            .parse::<u64>()
            .map_err(|err| WalError::InvalidFileName {
                file_name: file_name.clone(),
            })
    }
}

impl WalFileManager {
    pub fn new(config: kv_option::WalConfig) -> Self {
        let dir = config.dir.clone();

        let (last, seq) = match get_max_sequence_file_name(PathBuf::from(dir.clone())) {
            Some((file, seq)) => (file, seq),
            None => {
                let seq = 1;
                (file_manager::make_wal_file_name(&dir, seq), seq)
            }
        };

        let current_dir_path = PathBuf::from(dir);

        let file = file_manager::get_file_manager()
            .open_create_file(current_dir_path.join(last))
            .context(FailedWithFileManagerSnafu)
            .unwrap();
        let size = file.len();

        WalFileManager {
            config,
            current_dir_path,
            current_file: WalFile {
                id: seq,
                file,
                size,
            },
        }
    }

    fn roll_wal_file(&mut self) -> WalResult<()> {
        let current_file = &mut self.current_file;
        if current_file.size > SEGMENT_SIZE {
            current_file.id += 1;
            let file_name =
                file_manager::make_wal_file_name(self.config.dir.as_str(), current_file.id);
            let file = file_manager::get_file_manager()
                .create_file(file_name)
                .context(FailedWithFileManagerSnafu)?;
            current_file.file = file;
        }
        Ok(())
    }

    pub async fn write(&mut self, typ: WalEntryType, data: &[u8]) -> WalResult<()> {
        self.roll_wal_file()?;

        let writer = &mut self.current_file;
        {
            let typ = typ as u8;
            let mut pos = writer.size;
            writer
                .file
                .write_at(pos, &[typ])
                .and_then(|size| {
                    pos += size as u64;
                    let crc = crc32fast::hash(data);
                    writer.file.write_at(pos, &crc.to_be_bytes())
                })
                .and_then(|size| {
                    pos += size as u64;
                    let len = data.len() as u32;
                    writer.file.write_at(pos, &len.to_be_bytes())
                })
                .and_then(|size| {
                    pos += size as u64;
                    writer.file.write_at(pos, data)
                })
                .and_then(|size| {
                    pos += size as u64;
                    if self.config.sync {
                        writer.file.sync_all(FileSync::Soft)
                    } else {
                        Ok(())
                    }
                })
                .map_err(|err| WalError::FailedWithStdIO { source: err })?;

            // TODO codes below may produce "future cannot be sent between threads safely" in `kvcore.rs`."
            // self.file_manager
            //     .write_at(Arc::clone(&writer.file), writer.size, buf.as_mut_slice())
            //     .await;

            // write & fsync succeed
            writer.size = pos;
        }

        Ok(())
    }
}

/// Get a WriteAheadLogReader. Used for loading file to cache.
/// ```
/// use util::direct_fio::{File, FileSystem, Options};
///
/// let file_system: FileSystem = FileSystem::new(&Options::default());
/// let file: File = file_system.open("_00001.wal").unwrap();
///
/// let mut reader: WriteAheadLogReader = reader(file);
/// while let Some(block) = reader.next_wal_entry() {
///     // ...
/// }
/// ```
pub fn reader<'a>(f: File) -> WalReader<'a> {
    WalReader {
        cursor: f.into_cursor(),
        phantom: PhantomData,
    }
}

// struct WalWriter {
//     cursor: FileCursor,
//     size: u64,
// }

// impl WalWriter {
//     pub fn new(cursor: FileCursor) -> Self {
//         Self { cursor, size: 0 }
//     }

//     pub fn write(&mut self, wal_entry: &WalEntryBlock) -> WalResult<()> {
//         if let Some(WalEntryBlockInner { typ, crc, len, buf }) = wal_entry.inner() {
//             let ret = self.append(typ.code(), *crc, *len, &buf);
//             match ret {
//                 Ok(u64) => Ok(()),
//                 Err(e) => Err(e),
//             }
//         } else {
//             // do not need write anything.
//             Ok(())
//         }
//     }

//     fn append(&mut self, typ: u32, crc: u32, data_len: u32, data: &[u8]) -> WalResult<()> {
//         let ret = self
//             .cursor
//             .write(typ.to_be_bytes().as_slice())
//             //.await
//             .and_then(|()| self.cursor.write(crc.to_be_bytes().as_slice()))
//             .and_then(|()| self.cursor.write(data_len.to_be_bytes().as_slice()))
//             .and_then(|()| self.cursor.write(data))
//             // TODO: run sync in a Future
//             .and_then(|()| self.cursor.sync_all(FileSync::Soft))
//             .context(FailedWithStdIOSnafu);

//         self.size += 8 + data_len as u64;

//         ret
//     }

//     pub fn sync(&self) -> WalResult<()> {
//         self.cursor
//             .sync_all(FileSync::Soft)
//             .context(FailedWithStdIOSnafu)
//     }

//     pub fn flush(&self) -> WalResult<()> {
//         self.cursor
//             .sync_all(FileSync::Hard)
//             .context(FailedWithStdIOSnafu)
//     }
// }

pub struct WalReader<'a> {
    cursor: FileCursor,
    phantom: PhantomData<&'a Self>,
}

impl<'a> WalReader<'_> {
    pub fn new(cursor: FileCursor) -> Self {
        Self {
            cursor,
            phantom: PhantomData,
        }
    }

    pub fn next_wal_entry(&mut self) -> Option<WalEntryBlock> {
        let mut header_buf = [0_u8; 9];

        dbg!(self.cursor.pos());
        let read_bytes = self.cursor.read(&mut header_buf[..]).unwrap();
        if read_bytes < 8 {
            return None;
        }
        let typ = header_buf[0];
        let crc = compute::decode_be_u32(header_buf[1..5].into());
        let data_len = compute::decode_be_u32(header_buf[5..9].try_into().unwrap());
        if data_len <= 0 {
            return None;
        }
        dbg!(data_len);

        // TODO use a synchronized pool to get buffer
        let mut buf = vec![0_u8; 1024];
        dbg!(self.cursor.pos());
        let buf = &mut buf.as_mut_slice()[0..data_len as usize];
        let read_bytes = self.cursor.read(buf).unwrap();

        let inner_block = WalEntryBlockInner {
            typ: typ.into(),
            crc,
            len: read_bytes as u32,
            buf: buf.to_vec(),
        };
        match inner_block.typ {
            WalEntryType::Write => Some(WalEntryBlock::Write(inner_block)),
            WalEntryType::Delete => Some(WalEntryBlock::Delete(inner_block)),
            WalEntryType::DeleteRange => Some(WalEntryBlock::DeleteRange(inner_block)),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use core::panic;
    use std::borrow::BorrowMut;

    use chrono::Utc;
    use flatbuffers::{self, Vector, WIPOffset};
    use lazy_static::lazy_static;
    use rand;

    use crate::{
        direct_io::{File, FileCursor, FileSync},
        file_manager::list_file_names,
    };
    use protos::{models, models_helper};

    use crate::{
        file_manager, kv_option,
        wal::{WalEntryBlock, WalEntryType, WalReader},
        FileManager,
    };

    use super::{WalEntryBlockInner, WalFileManager};

    const DIR: &'static str = "/tmp/test/";

    impl crate::wal::WalEntryBlock {
        pub fn new_write(bytes: &[u8]) -> Self {
            Self::Write(WalEntryBlockInner::from_bytes(WalEntryType::Write, bytes))
        }

        pub fn new_delete(bytes: &[u8]) -> Self {
            Self::Delete(WalEntryBlockInner::from_bytes(WalEntryType::Delete, bytes))
        }

        pub fn new_delete_range(bytes: &[u8]) -> Self {
            Self::DeleteRange(WalEntryBlockInner::from_bytes(
                WalEntryType::DeleteRange,
                bytes,
            ))
        }
    }
    impl WalEntryBlockInner {
        pub fn from_bytes(typ: WalEntryType, bytes: &[u8]) -> Self {
            Self {
                typ,
                crc: crc32fast::hash(bytes),
                len: bytes.len() as u32,
                buf: bytes.into(),
            }
        }

        pub fn size(&self) -> u32 {
            self.len + 10
        }
    }

    impl From<&models::Points<'_>> for WalEntryBlockInner {
        fn from(entry: &models::Points) -> Self {
            Self::from_bytes(WalEntryType::Write, entry._tab.buf)
        }
    }

    impl<'a> From<&'a WalEntryBlockInner> for models::Points<'a> {
        fn from(block: &'a WalEntryBlockInner) -> Self {
            flatbuffers::root::<models::Points<'a>>(&block.buf[0..block.len as usize]).unwrap()
        }
    }

    impl From<&models::ColumnKeys<'_>> for WalEntryBlockInner {
        fn from(cols: &models::ColumnKeys<'_>) -> Self {
            Self::from_bytes(WalEntryType::Delete, cols._tab.buf)
        }
    }

    impl<'a> From<&'a WalEntryBlockInner> for models::ColumnKeys<'a> {
        fn from(block: &'a WalEntryBlockInner) -> Self {
            flatbuffers::root::<models::ColumnKeys<'a>>(&block.buf[0..block.len as usize]).unwrap()
        }
    }

    impl From<&models::ColumnKeysWithRange<'_>> for WalEntryBlockInner {
        fn from(cols: &models::ColumnKeysWithRange<'_>) -> Self {
            Self::from_bytes(WalEntryType::DeleteRange, cols._tab.buf)
        }
    }

    impl<'a> From<&'a WalEntryBlockInner> for models::ColumnKeysWithRange<'a> {
        fn from(block: &'a WalEntryBlockInner) -> Self {
            flatbuffers::root::<models::ColumnKeysWithRange<'a>>(&block.buf[0..block.len as usize])
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

    fn random_write_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<models::Points<'a>> {
        let fbb = _fbb.borrow_mut();
        models_helper::create_random_points(fbb, 5)
    }

    fn random_delete_wal_entry_item() -> models::ColumnKey {
        models::ColumnKey::new(random_series_id(), random_field_id())
    }

    fn random_delete_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<models::ColumnKeys<'a>> {
        let fbb = _fbb.borrow_mut();

        let mut items: Vec<models::ColumnKey> = vec![];
        for _ in 0..10 {
            items.push(random_delete_wal_entry_item());
        }

        let vec = fbb.create_vector(&items);

        models::ColumnKeys::create(
            fbb,
            &models::ColumnKeysArgs {
                column_keys: Some(vec),
            },
        )
    }

    fn random_delete_range_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<models::ColumnKeysWithRange<'a>> {
        let fbb = _fbb.borrow_mut();
        let mut items: Vec<models::ColumnKey> = vec![];
        for _ in 0..10 {
            items.push(random_delete_wal_entry_item());
        }

        let vec = fbb.create_vector(&items);

        models::ColumnKeysWithRange::create(
            fbb,
            &models::ColumnKeysWithRangeArgs {
                column_keys: Some(vec),
                min: 1,
                max: 100,
            },
        )
    }

    fn random_wal_entry_block<'a>(_fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> WalEntryBlock {
        let fbb = _fbb.borrow_mut();

        let entry_type = random_wal_entry_type();
        match entry_type {
            WalEntryType::Write => {
                let ptr = random_write_wal_entry(fbb);
                fbb.finish(ptr, None);
                WalEntryBlock::new_write(fbb.finished_data())
            }
            WalEntryType::Delete => {
                let ptr = random_delete_wal_entry(fbb);
                fbb.finish(ptr, None);
                WalEntryBlock::new_delete(fbb.finished_data())
            }
            WalEntryType::DeleteRange => {
                let ptr = random_delete_range_wal_entry(fbb);
                fbb.finish(ptr, None);
                WalEntryBlock::new_delete_range(fbb.finished_data())
            }
            _ => panic!("Invalid entry type"),
        }
    }

    #[tokio::test]
    async fn test_write_entry() {
        let wal_config = kv_option::WalConfig {
            dir: String::from(DIR),
            ..Default::default()
        };

        let mut mgr = WalFileManager::new(wal_config);

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry_block(&mut fbb);

            let bytes = fbb.finished_data();
            dbg!(bytes.len());

            match entry {
                WalEntryBlock::Write(block) => {
                    let de_block = flatbuffers::root::<models::Points>(&block.buf).unwrap();
                    mgr.write(WalEntryType::Write, &block.buf).await.unwrap();
                }
                WalEntryBlock::Delete(block) => {
                    let de_block = flatbuffers::root::<models::ColumnKeys>(&block.buf).unwrap();
                    mgr.write(WalEntryType::Delete, &block.buf).await.unwrap();
                }
                WalEntryBlock::DeleteRange(block) => {
                    let de_block =
                        flatbuffers::root::<models::ColumnKeysWithRange>(&block.buf).unwrap();
                    mgr.write(WalEntryType::DeleteRange, &block.buf)
                        .await
                        .unwrap();
                }
                _ => {}
            };
        }
    }

    #[test]
    fn test_read_entry() {
        let wal_config = crate::kv_option::WalConfig {
            dir: String::from("/tmp/test/"),
            ..Default::default()
        };

        let mgr = WalFileManager::new(wal_config);

        let wal_files = list_file_names("/tmp/test/");
        for wal_file in wal_files {
            let file = file_manager::get_file_manager()
                .open_file(mgr.current_dir_path.join(wal_file))
                .unwrap();
            let cursor: FileCursor = file.into();

            let mut reader = WalReader::new(cursor);

            while let Some(entry) = reader.next_wal_entry() {
                match entry {
                    WalEntryBlock::Write(block) => {
                        let de_block = flatbuffers::root::<models::Points>(&block.buf).unwrap();
                        dbg!(de_block);
                    }
                    WalEntryBlock::Delete(block) => {
                        let de_block = flatbuffers::root::<models::ColumnKeys>(&block.buf).unwrap();
                        dbg!(de_block);
                    }
                    WalEntryBlock::DeleteRange(block) => {
                        let de_block =
                            flatbuffers::root::<models::ColumnKeysWithRange>(&block.buf).unwrap();
                        dbg!(de_block);
                    }
                    _ => panic!("Invalid WalEntry"),
                };
            }
        }
    }

    #[tokio::test]
    async fn test_read_and_write() {
        let wal_config = crate::kv_option::WalConfig {
            dir: String::from(DIR),
            ..Default::default()
        };

        let mut mgr = WalFileManager::new(wal_config);

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry_block(&mut fbb);

            let bytes = fbb.finished_data();
            dbg!(bytes.len());

            match entry {
                WalEntryBlock::Write(block) => {
                    let de_block = flatbuffers::root::<models::Points>(&block.buf).unwrap();
                    mgr.write(WalEntryType::Write, &block.buf).await.unwrap();
                }
                WalEntryBlock::Delete(block) => {
                    let de_block = flatbuffers::root::<models::ColumnKeys>(&block.buf).unwrap();
                    mgr.write(WalEntryType::Delete, &block.buf).await.unwrap();
                }
                WalEntryBlock::DeleteRange(block) => {
                    let de_block =
                        flatbuffers::root::<models::ColumnKeysWithRange>(&block.buf).unwrap();
                    mgr.write(WalEntryType::DeleteRange, &block.buf)
                        .await
                        .unwrap();
                }
                _ => {}
            };
        }

        let wal_files = list_file_names(DIR);
        for wal_file in wal_files {
            let file = file_manager::get_file_manager()
                .open_file(mgr.current_dir_path.join(wal_file))
                .unwrap();
            let cursor: FileCursor = file.into();

            let mut reader = WalReader::new(cursor);
            let mut writed_crcs = Vec::<u32>::new();
            let mut readed_crcs = Vec::<u32>::new();
            while let Some(entry) = reader.next_wal_entry() {
                match entry {
                    WalEntryBlock::Write(block) => {
                        let de_block = flatbuffers::root::<models::Points>(&block.buf).unwrap();
                        writed_crcs.push(block.crc);
                        readed_crcs.push(crc32fast::hash(&block.buf[..block.len as usize]));
                    }
                    WalEntryBlock::Delete(block) => {
                        let de_block = flatbuffers::root::<models::ColumnKeys>(&block.buf).unwrap();
                        writed_crcs.push(block.crc);
                        readed_crcs.push(crc32fast::hash(&block.buf[..block.len as usize]));
                    }
                    WalEntryBlock::DeleteRange(block) => {
                        let de_block =
                            flatbuffers::root::<models::ColumnKeysWithRange>(&block.buf).unwrap();
                        writed_crcs.push(block.crc);
                        readed_crcs.push(crc32fast::hash(&block.buf[..block.len as usize]));
                    }
                    _ => {}
                };
            }
            assert_eq!(writed_crcs, readed_crcs);
        }
    }
}
