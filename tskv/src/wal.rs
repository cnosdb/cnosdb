use std::{
    any::Any,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crc32fast;
use futures::channel::oneshot;
use lazy_static::lazy_static;
use regex::Regex;
use snafu::prelude::*;
use tokio::sync::Mutex as AsyncMutex;

use crate::{
    direct_io::{make_io_task, File, FileCursor, FileSync, FileSystem, Options, TaskType},
    file_manager,
};
use protos::models;
use walkdir::IntoIter;

use crate::{option, FileManager};

lazy_static! {
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new("_.*\\.wal").unwrap();
}

const SEGMENT_SIZE: u64 = 1073741824; // 1 GiB

pub enum WalTask {
    Write {
        rows: Vec<u8>,
        cb: oneshot::Sender<WalResult<()>>,
    },
}

// pub struct WalScheduler {
//     sender: Sender<WalTask>,
// }

// impl WalScheduler {
//     pub async fn write(&mut self, rows: Vec<u8>) -> WalResult<()> {
//         let (cb, rx) = oneshot::channel::<WalResult<()>>();
//         let task = WalTask::Write { rows, cb };
//         self.sender.send(task).await;

//         rx.await.map_err(|_| WalError::FailedWithChannelReceive)?
//     }
// }

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

pub enum WalEntryType {
    Write = 1,
    Delete = 2,
    DeleteRange = 3,
    Unknown = 0,
}

impl From<u32> for WalEntryType {
    fn from(typ: u32) -> Self {
        match typ {
            1 => WalEntryType::Write,
            2 => WalEntryType::Delete,
            3 => WalEntryType::DeleteRange,
            _ => WalEntryType::Unknown,
        }
    }
}

impl From<WalEntryType> for u32 {
    fn from(typ: WalEntryType) -> Self {
        match typ {
            WalEntryType::Write => 1,
            WalEntryType::Delete => 2,
            WalEntryType::DeleteRange => 3,
            WalEntryType::Unknown => 0,
        }
    }
}

impl From<models::Rows<'_>> for WalEntryType {
    fn from(_: models::Rows<'_>) -> Self {
        Self::Write
    }
}

impl From<models::ColumnKeys<'_>> for WalEntryType {
    fn from(_: models::ColumnKeys<'_>) -> Self {
        Self::Delete
    }
}

impl From<models::ColumnKeysWithRange<'_>> for WalEntryType {
    fn from(_: models::ColumnKeysWithRange<'_>) -> Self {
        Self::DeleteRange
    }
}

impl WalEntryType {
    pub fn code(&self) -> u32 {
        match *self {
            WalEntryType::Write => 1,
            WalEntryType::Delete => 2,
            WalEntryType::DeleteRange => 3,
            WalEntryType::Unknown => 0,
        }
    }
}

pub enum WalEntryBlock {
    Write(WalEntryBlockInner),
    Delete(WalEntryBlockInner),
    DeleteRange(WalEntryBlockInner),
    Unknown,
}

impl From<WalEntryBlockInner> for WalEntryBlock {
    fn from(block: WalEntryBlockInner) -> Self {
        match block.typ {
            WalEntryType::Write => Self::Write(block),
            WalEntryType::Delete => Self::Delete(block),
            WalEntryType::DeleteRange => Self::DeleteRange(block),
            WalEntryType::Unknown => Self::Unknown,
        }
    }
}

impl From<models::Rows<'_>> for WalEntryBlock {
    fn from(rows: models::Rows<'_>) -> Self {
        Self::Write((&rows).into())
    }
}

impl From<models::ColumnKeys<'_>> for WalEntryBlock {
    fn from(cols: models::ColumnKeys<'_>) -> Self {
        Self::Delete((&cols).into())
    }
}

impl From<models::ColumnKeysWithRange<'_>> for WalEntryBlock {
    fn from(cols: models::ColumnKeysWithRange<'_>) -> Self {
        Self::DeleteRange((&cols).into())
    }
}

impl WalEntryBlock {
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

    pub fn wal_entry_type(&self) -> WalEntryType {
        match self {
            WalEntryBlock::Write(_) => WalEntryType::Write,
            WalEntryBlock::Delete(_) => WalEntryType::Delete,
            WalEntryBlock::DeleteRange(_) => WalEntryType::DeleteRange,
            _ => WalEntryType::Unknown,
        }
    }

    pub fn inner(&self) -> Option<&WalEntryBlockInner> {
        match self {
            WalEntryBlock::Write(inner) => Some(inner),
            WalEntryBlock::Delete(inner) => Some(inner),
            WalEntryBlock::DeleteRange(inner) => Some(inner),
            _ => None,
        }
    }
}

pub struct WalEntryBlockInner {
    pub typ: WalEntryType,
    pub crc: u32,
    pub len: u32,
    pub buf: Vec<u8>,
}

impl WalEntryBlockInner {
    pub fn from_bytes(typ: WalEntryType, bytes: &[u8]) -> Self {
        // TODO: check
        Self {
            typ,
            crc: crc32fast::hash(bytes),
            len: bytes.len() as u32,
            buf: bytes.into(),
        }
    }

    pub fn size(&self) -> u32 {
        self.len + 12
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0_u8; self.buf.len() + 12];
        buf[..4].copy_from_slice(self.typ.code().to_be_bytes().as_slice());
        buf[4..8].copy_from_slice(self.crc.to_be_bytes().as_slice());
        buf[8..12].copy_from_slice(self.len.to_be_bytes().as_slice());
        buf[12..].copy_from_slice(self.buf.as_slice());

        buf
    }
}

impl From<&models::Rows<'_>> for WalEntryBlockInner {
    fn from(entry: &models::Rows) -> Self {
        Self::from_bytes(WalEntryType::Write, entry._tab.buf)
    }
}

impl<'a> From<&'a WalEntryBlockInner> for models::Rows<'a> {
    fn from(block: &'a WalEntryBlockInner) -> Self {
        flatbuffers::root::<models::Rows<'a>>(&block.buf[0..block.len as usize]).unwrap()
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

struct WalConfig {}

#[derive(Clone)]
struct WalFile {
    id: u64,
    file: Arc<File>,
    size: u64,
}

#[derive(Clone)]
pub struct WalFileManager {
    config: option::WalConfig,

    file_manager: Arc<FileManager>,

    current_dir_path: PathBuf,
    current_file: WalFile,
}

unsafe impl Send for WalFileManager {}
unsafe impl Sync for WalFileManager {}

impl WalFileManager {
    pub fn new(file_manager: Arc<FileManager>, config: option::WalConfig) -> Self {
        let mut fs_options = Options::default();
        let fs_options = fs_options
            .max_resident(1)
            .max_non_resident(0)
            .page_len_scale(1);

        let dir = config.dir.clone();

        let segments = list_filenames(dir.clone());
        let (last, id) = if segments.len() > 0 {
            let last = segments.last().unwrap();
            (
                Box::from(last.clone()),
                Self::get_id_by_file_name(last).unwrap(),
            )
        } else {
            let id = 1;
            let last = format!("_{:05}.wal", id);
            (Box::new(last), 1)
        };

        let current_dir_path = PathBuf::from(dir);
        let dir = current_dir_path.join(*last);

        let current_file = Self::get_or_create_wal_file(id, file_manager.as_ref(), dir).unwrap();
        // let current_file = Arc::new(Mutex::new(current_file));

        WalFileManager {
            config,

            file_manager,

            current_dir_path,
            current_file,
        }
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

    fn get_or_create_wal_file<P: AsRef<Path>>(
        id: u64,
        file_manager: &FileManager,
        path: P,
    ) -> WalResult<WalFile> {
        let file = if file_manager::try_exists(path.as_ref()) {
            file_manager.open_file(path)
        } else {
            file_manager.create_file(path)
        };
        let file = file
            .and_then(|f| Ok(f))
            .context(FailedWithFileManagerSnafu)?;

        let size = file.len();

        Ok(WalFile {
            id,
            file: Arc::new(file),
            size,
        })
    }

    fn get_file<P: AsRef<Path>>(&self, path: P) -> WalResult<File> {
        self.file_manager
            .open_file(path)
            .context(FailedWithFileManagerSnafu)
    }

    fn roll_wal_file(&mut self) -> WalResult<()> {
        // let mut current_file = self.current_file.lock().unwrap();
        let current_file = &mut self.current_file;
        if current_file.size > SEGMENT_SIZE {
            current_file.id += 1;
            let file_name = self
                .current_dir_path
                .join(format!("_{:05}.wal", current_file.id));
            let file = self
                .file_manager
                .create_file(file_name)
                .context(FailedWithFileManagerSnafu)?;
            current_file.file = Arc::new(file);
        }
        Ok(())
    }

    pub async fn write(&mut self, rows: &[u8]) -> WalResult<()> {
        self.roll_wal_file()?;

        // let mut writer = self.current_file.lock().unwrap();
        // writer.writer.write(&WalEntryBlock::new_write(rows))

        let wal_block = WalEntryBlockInner::from_bytes(WalEntryType::Write, rows);
        let buf = wal_block.to_bytes();

        let writer = &mut self.current_file;
        {
            writer
                .file
                .write_at(writer.size, buf.as_slice())
                .map_err(|err| WalError::FailedWithStdIO { source: err })?;

            // TODO codes below may produce "future cannot be sent between threads safely" in `kvcore.rs`."
            // self.file_manager
            //     .write_at(Arc::clone(&writer.file), writer.size, buf.as_mut_slice())
            //     .await;
        }
        writer.size += buf.len() as u64;
        writer
            .file
            .sync_all(FileSync::Soft)
            .context(FailedWithStdIOSnafu)?;

        Ok(())
    }

    pub async fn delete(&mut self, columns: &[u8]) -> WalResult<()> {
        self.roll_wal_file()?;

        // let mut writer = self.current_file.lock().unwrap();
        // writer.writer.write(&WalEntryBlock::new_delete(columns))

        let wal_block = WalEntryBlockInner::from_bytes(WalEntryType::Delete, columns);
        let mut buf = wal_block.to_bytes();

        let writer = &mut self.current_file;
        self.file_manager
            .write_at(Arc::clone(&writer.file), writer.size, buf.as_mut_slice())
            .await;
        writer.size += buf.len() as u64;
        writer
            .file
            .sync_all(FileSync::Soft)
            .context(FailedWithStdIOSnafu)?;

        Ok(())
    }

    pub async fn delete_range(&mut self, columns_with_range: &[u8]) -> WalResult<()> {
        self.roll_wal_file()?;

        // let mut writer = self.current_file.lock().unwrap();
        // writer.writer.write(&WalEntryBlock::new_delete_range(columns_with_range))

        let wal_block =
            WalEntryBlockInner::from_bytes(WalEntryType::DeleteRange, columns_with_range);
        let mut buf = wal_block.to_bytes();

        let writer = &mut self.current_file;
        self.file_manager
            .write_at(Arc::clone(&writer.file), writer.size, buf.as_mut_slice())
            .await;
        writer.size += buf.len() as u64;
        writer
            .file
            .sync_all(FileSync::Soft)
            .context(FailedWithStdIOSnafu)?;

        Ok(())
    }
}

pub fn list_filenames<P: AsRef<Path>>(dir: P) -> Vec<String> {
    let mut list = Vec::new();

    for file_name in walkdir::WalkDir::new(dir)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| {
            let dir_entry = match e {
                Ok(dir_entry) if dir_entry.file_type().is_file() => dir_entry,
                _ | Err(_) => {
                    return None;
                }
            };
            dir_entry
                .file_name()
                .to_str()
                .map(|file_name| file_name.to_string())
        })
    {
        list.push(file_name);
    }

    list
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

// pub(crate) fn pread_exact_or_eof(
//     file: &File,
//     mut buf: &mut [u8],
//     offset: u64,
// ) -> Result<usize> {
//     let mut total = 0_usize;
//     while !buf.is_empty() {
//         match file.read_at(buf, offset + u64::try_from(total).unwrap()) {
//             Ok(0) => break,
//             Ok(n) => {
//                 total += n;
//                 let tmp = buf;
//                 buf = &mut tmp[n..];
//             }
//             Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
//             Err(e) => return Err(e.into()),
//         }
//     }
//     Ok(total)
// }

impl<'a> WalReader<'_> {
    pub fn new(cursor: FileCursor) -> Self {
        Self {
            cursor,
            phantom: PhantomData,
        }
    }

    pub fn next_wal_entry(&mut self) -> Option<WalEntryBlock> {
        let mut header_buf = [0_u8; 12];

        dbg!(self.cursor.pos());
        let read_bytes = self.cursor.read(&mut header_buf[..]).unwrap();
        if read_bytes < 8 {
            return None;
        }
        let typ = u32::from_be_bytes(header_buf[0..4].try_into().unwrap());
        let crc = u32::from_be_bytes(header_buf[4..8].try_into().unwrap());
        let data_len = u32::from_be_bytes(header_buf[8..12].try_into().unwrap());
        if data_len <= 0 {
            return None;
        }
        dbg!(data_len);

        // TODO use a synchronized pool to get buffer
        let mut buf = vec![0_u8; 1024];
        dbg!(self.cursor.pos());
        let buf = &mut buf.as_mut_slice()[0..data_len as usize];
        let read_bytes = self.cursor.read(buf).unwrap();

        Some(
            WalEntryBlockInner {
                typ: typ.into(),
                crc,
                len: read_bytes as u32,
                buf: buf.to_vec(),
            }
            .into(),
        )
    }
}

#[cfg(test)]
mod test {
    use std::{borrow::BorrowMut, sync::Arc, time};

    use chrono::Utc;
    use flatbuffers::{self, Vector, WIPOffset};
    use lazy_static::lazy_static;
    use rand;

    use crate::{
        direct_io::{FileCursor, FileSync},
        wal::list_filenames,
        File,
    };
    use protos::models;

    use crate::{
        file_manager, option,
        wal::{WalEntryBlock, WalEntryType, WalReader},
        FileManager,
    };

    use super::{WalEntryBlockInner, WalFileManager};

    const DIR: &'static str = "/tmp/test/";

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

    fn random_field<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        field_id: u64,
        type_: models::FieldType,
        value: WIPOffset<Vector<u8>>,
    ) -> WIPOffset<models::RowField<'a>> {
        let fbb = _fbb.borrow_mut();
        models::RowField::create(
            fbb,
            &models::RowFieldArgs {
                field_id,
                type_,
                value: Some(value),
            },
        )
    }

    fn random_row<'a>(_fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> WIPOffset<models::Row<'a>> {
        let fbb = _fbb.borrow_mut();

        let series_id = random_series_id();
        let timestamp = Utc::now().timestamp() as u64;
        let float_v = fbb.create_vector(rand::random::<f64>().to_be_bytes().as_slice());
        let string_v = fbb.create_vector("Hello world.".as_bytes());

        let mut fields: Vec<WIPOffset<models::RowField>> = vec![];
        fields.push(random_field(
            fbb,
            random_field_id(),
            models::FieldType::Float,
            float_v,
        ));
        fields.push(random_field(
            fbb,
            random_field_id(),
            models::FieldType::Float,
            string_v,
        ));
        let vec = fbb.create_vector(&fields);

        let mut row_builder = models::RowBuilder::new(fbb);
        row_builder.add_key(&models::RowKey::new(series_id, timestamp));
        row_builder.add_fields(vec);

        row_builder.finish()
    }

    fn random_write_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<models::Rows<'a>> {
        let fbb = _fbb.borrow_mut();

        let mut rows: Vec<WIPOffset<models::Row>> = vec![];
        rows.push(random_row(fbb));
        rows.push(random_row(fbb));
        let vec = fbb.create_vector(&rows);

        models::Rows::create(fbb, &models::RowsArgs { rows: Some(vec) })
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
        let file_manager = Arc::new(file_manager::FileManager::new());

        let wal_config = option::WalConfig {
            dir: String::from(DIR),
            ..Default::default()
        };

        let mut mgr = WalFileManager::new(file_manager.clone(), wal_config);

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry_block(&mut fbb);

            let bytes = fbb.finished_data();
            dbg!(bytes.len());

            match entry {
                WalEntryBlock::Write(block) => {
                    let de_block = flatbuffers::root::<models::Rows>(&block.buf).unwrap();
                    mgr.write(&block.buf).await.unwrap();
                }
                WalEntryBlock::Delete(block) => {
                    let de_block = flatbuffers::root::<models::ColumnKeys>(&block.buf).unwrap();
                    mgr.delete(&block.buf).await.unwrap();
                }
                WalEntryBlock::DeleteRange(block) => {
                    let de_block =
                        flatbuffers::root::<models::ColumnKeysWithRange>(&block.buf).unwrap();
                    mgr.delete_range(&block.buf).await.unwrap();
                }
                _ => {}
            };
        }
    }

    #[test]
    fn test_read_entry() {
        let file_manager = Arc::new(file_manager::FileManager::new());

        let wal_config = crate::option::WalConfig {
            dir: String::from("/tmp/test/"),
            ..Default::default()
        };

        let mgr = WalFileManager::new(file_manager.clone(), wal_config);

        let wal_files = list_filenames("/tmp/test/");
        for wal_file in wal_files {
            let file = mgr
                .file_manager
                .open_file(mgr.current_dir_path.join(wal_file))
                .unwrap();
            let cursor: FileCursor = file.into();

            let mut reader = WalReader::new(cursor);

            while let Some(entry) = reader.next_wal_entry() {
                match entry {
                    WalEntryBlock::Write(block) => {
                        let de_block = flatbuffers::root::<models::Rows>(&block.buf).unwrap();
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
                    _ => {}
                };
            }
        }
    }

    #[tokio::test]
    async fn test_read_and_write() {
        let file_manager = Arc::new(file_manager::FileManager::new());

        let wal_config = crate::option::WalConfig {
            dir: String::from(DIR),
            ..Default::default()
        };

        let mut mgr = WalFileManager::new(file_manager.clone(), wal_config);

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry_block(&mut fbb);

            let bytes = fbb.finished_data();
            dbg!(bytes.len());

            match entry {
                WalEntryBlock::Write(block) => {
                    let de_block = flatbuffers::root::<models::Rows>(&block.buf).unwrap();
                    mgr.write(&block.buf).await.unwrap();
                }
                WalEntryBlock::Delete(block) => {
                    let de_block = flatbuffers::root::<models::ColumnKeys>(&block.buf).unwrap();
                    mgr.delete(&block.buf).await.unwrap();
                }
                WalEntryBlock::DeleteRange(block) => {
                    let de_block =
                        flatbuffers::root::<models::ColumnKeysWithRange>(&block.buf).unwrap();
                    mgr.delete_range(&block.buf).await.unwrap();
                }
                _ => {}
            };
        }

        let wal_files = list_filenames(DIR);
        for wal_file in wal_files {
            let file = mgr
                .file_manager
                .open_file(mgr.current_dir_path.join(wal_file))
                .unwrap();
            let cursor: FileCursor = file.into();

            let mut reader = WalReader::new(cursor);
            let mut writed_crcs = Vec::<u32>::new();
            let mut readed_crcs = Vec::<u32>::new();
            while let Some(entry) = reader.next_wal_entry() {
                match entry {
                    WalEntryBlock::Write(block) => {
                        let de_block = flatbuffers::root::<models::Rows>(&block.buf).unwrap();
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
