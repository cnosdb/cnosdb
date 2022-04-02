use std::{
    path::{Path, PathBuf},
};

use lazy_static::lazy_static;
use regex::Regex;
use snafu::prelude::*;

use crate::direct_io::{File, FileCursor, FileSync, FileSystem, Options};
use protos::models::*;
use walkdir::IntoIter;

use crate::FileManager;

lazy_static! {
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new("_.*\\.wal").unwrap();
}

const SEGMENT_SIZE: u64 = 1073741824; // 1 GiB

const WAL_CRC_ALGORITHM: &crc::Algorithm<u32> = &crc::Algorithm {
    poly: 0x8005,
    init: 0xffff,
    refin: false,
    refout: false,
    xorout: 0x0000,
    check: 0xaee7,
    residue: 0x0000,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Unable to walk dir : {}", source))]
    UnableToWalkDir { source: walkdir::Error },

    #[snafu(display("File {} has wrong name format to have an id", file_name))]
    InvalidFileName { file_name: String },

    #[snafu(display("Error with file : {}", source))]
    FailedWithFileManager { source: super::file_manager::Error },

    #[snafu(display("Error with std::io : {}", source))]
    FailedWithStdIO { source: std::io::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

struct WalConfig {}

struct WriteAheadLogManager {
    dir: String,
    file_manager: &'static FileManager,
    crc_algorithm: &'static crc::Algorithm<u32>,

    current_dir_path: PathBuf,
    current_file_id: u64,
    current_file_writer: Option<WriteAheadLogDiskFileWriter>,
}

impl WriteAheadLogManager {
    pub fn new(file_manager: &'static FileManager, dir: String) -> Self {
        let mut fs_options = Options::default();
        let fs_options = fs_options
            .max_resident(1)
            .max_non_resident(0)
            .page_len_scale(1);

        WriteAheadLogManager {
            dir: dir.clone(),
            file_manager,
            crc_algorithm: &WAL_CRC_ALGORITHM,

            current_dir_path: PathBuf::from(dir),
            current_file_id: 0,
            current_file_writer: None,
        }
    }

    fn list_wal_filenames(dir: &String) -> Vec<String> {
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

    fn get_id_by_file_name(file_name: &String) -> Result<u64> {
        if !WAL_FILE_NAME_PATTERN.is_match(file_name) {
            return Err(Error::InvalidFileName {
                file_name: file_name.clone(),
            });
        }
        let parts: Vec<&str> = file_name.split(".").collect();
        if parts.len() != 2 {
            Err(Error::InvalidFileName {
                file_name: file_name.clone(),
            })
        } else {
            parts
                .first()
                .unwrap()
                .split_at(1)
                .1
                .parse::<u64>()
                .map_err(|err| Error::InvalidFileName {
                    file_name: file_name.clone(),
                })
        }
    }

    fn get_wal_file<P: AsRef<Path>>(&self, path: P) -> Result<File> {
        self.file_manager
            .open_file(path)
            .context(FailedWithFileManagerSnafu)
    }

    fn new_wal_file(&mut self) -> Result<()> {
        self.current_file_id = self.current_file_id + 1;
        if self.current_file_writer.is_none() {
            let file_name = self
                .current_dir_path
                .join(format!("_{:05}.wal", self.current_file_id));
            self.current_file_writer = Some(
                self.file_manager
                    .create_file(file_name)
                    .context(FailedWithFileManagerSnafu)
                    .map(|file| WriteAheadLogDiskFileWriter::new(file.into()))?,
            );
        }
        Ok(())
    }

    fn roll_wal_file(&mut self) -> Result<()> {
        let writer = &self.current_file_writer;
        match writer {
            Some(w) if w.size > SEGMENT_SIZE => self.new_wal_file(),
            None => self.new_wal_file(),
            _ => Ok(()),
        }
    }

    pub fn open(&mut self) -> Result<()> {
        let segments = Self::list_wal_filenames(&self.dir);
        if segments.len() > 0 {
            let last = segments.last().unwrap();
            let id = Self::get_id_by_file_name(last)?;
            self.current_file_id = id;
            let file = self.get_wal_file(self.current_dir_path.join(last))?;
            self.current_file_writer = Some(WriteAheadLogDiskFileWriter::new(file.into()));
        }

        Ok(())
    }

    pub fn write(&mut self, entry: &WALEntry) -> Result<()> {
        self.roll_wal_file()?;

        let writer = &mut self.current_file_writer;
        match writer {
            Some(w) => w.write(entry),
            None => Err(Error::FailedWithStdIO {
                source: std::io::Error::new(std::io::ErrorKind::Other, "No writer initialized."),
            }),
        }
    }
}

fn get_checksum(data: &[u8]) -> u32 {
    // TODO: can crc::Crc be global, or a singleton instance
    let crc_builder = crc::Crc::<u32>::new(&WAL_CRC_ALGORITHM);
    let mut crc_digest = crc_builder.digest();
    crc_digest.update(data);
    crc_digest.finalize()
}

pub struct WriteAheadLogBlock {
    crc: u32,
    len: u32,
    buf: Vec<u8>,
}

impl WriteAheadLogBlock {
    pub fn size(&self) -> u32 {
        self.len + 8
    }
}

impl From<&WALEntry<'_>> for WriteAheadLogBlock {
    fn from(entry: &WALEntry) -> Self {
        Self {
            crc: get_checksum(entry._tab.buf),
            len: entry._tab.buf.len() as u32,
            buf: entry._tab.buf.into(),
        }
    }
}

impl<'a> From<&'a WriteAheadLogBlock> for WALEntry<'a> {
    fn from(block: &'a WriteAheadLogBlock) -> Self {
        flatbuffers::root::<WALEntry<'a>>(&block.buf[0..block.len as usize]).unwrap()
    }
}

pub struct WriteAheadLogDiskFileWriter {
    cursor: FileCursor,
    size: u64,
}

impl WriteAheadLogDiskFileWriter {
    pub fn new(cursor: FileCursor) -> Self {
        Self { cursor, size: 0 }
    }

    pub fn write(&mut self, wal_entry: &WALEntry) -> Result<()> {
        let crc = get_checksum(wal_entry._tab.buf);

        let ret = self.append(crc, wal_entry._tab.buf.len() as u32, wal_entry._tab.buf);
        match ret {
            Ok(u64) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn append(&mut self, crc: u32, data_len: u32, data: &[u8]) -> Result<()> {
        let ret = self
            .cursor
            .write(crc.to_be_bytes().as_slice())
            .and_then(|()| self.cursor.write(data_len.to_be_bytes().as_slice()))
            .and_then(|()| self.cursor.write(data))
            .and_then(|()| self.cursor.sync_all(FileSync::Soft))
            .context(FailedWithStdIOSnafu);

        self.size += 8 + data_len as u64;

        ret
    }

    pub fn sync(&self) -> Result<()> {
        self.cursor
            .sync_all(FileSync::Soft)
            .context(FailedWithStdIOSnafu)
    }

    pub fn flush(&self) -> Result<()> {
        self.cursor
            .sync_all(FileSync::Hard)
            .context(FailedWithStdIOSnafu)
    }
}

struct WriteAheadLogReader<'a> {
    cursor: FileCursor,

    data_buf: Option<Vec<u8>>,
    wal_entry: Option<WALEntry<'a>>,
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

impl WriteAheadLogReader<'_> {
    pub fn new(cursor: FileCursor) -> Self {
        Self {
            cursor,
            data_buf: None,
            wal_entry: None,
        }
    }

    pub fn next_wal_entry<'a>(&mut self) -> Option<WriteAheadLogBlock> {
        let mut header_buf = [0_u8; 8];

        dbg!(self.cursor.pos());
        let read_bytes = self.cursor.read(&mut header_buf[..]).unwrap();
        dbg!(self.cursor.pos());
        if read_bytes < 8 {
            return None;
        }
        let crc = u32::from_be_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let data_len =
            u32::from_be_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]]);
        if data_len <= 0 {
            return None;
        }
        dbg!(data_len);

        // TODO use a sync pool
        let mut buf = vec![0_u8; 1024];
        dbg!(self.cursor.pos());
        let read_bytes = self
            .cursor
            .read(&mut buf.as_mut_slice()[0..data_len as usize])
            .unwrap();
        dbg!(self.cursor.pos());

        Some(WriteAheadLogBlock {
            crc,
            len: read_bytes as u32,
            buf,
        })
    }
}

#[cfg(test)]
mod test {
    use std::{borrow::BorrowMut, time};

    use chrono::Utc;
    use flatbuffers::{self, Vector, WIPOffset};
    use lazy_static::lazy_static;
    use rand;

    use protos::models::*;
    use crate::direct_io::{FileCursor, FileSync};

    use crate::{
        file_manager,
        wal::{get_checksum, WriteAheadLogReader},
        FileManager,
    };

    use super::{WriteAheadLogBlock, WriteAheadLogManager};

    fn random_series_id() -> u64 {
        rand::random::<u64>()
    }
    fn random_field_id() -> u64 {
        rand::random::<u64>()
    }

    fn random_wal_entry_type() -> WALEntryType {
        let rand = rand::random::<u8>() % 3;
        match rand {
            0 => WALEntryType::Write,
            1 => WALEntryType::Delete,
            _ => WALEntryType::DeleteRange,
        }
    }

    fn random_field<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
        field_id: u64,
        type_: FieldType,
        value: WIPOffset<Vector<u8>>,
    ) -> WIPOffset<RowField<'a>> {
        let fbb = _fbb.borrow_mut();
        RowField::create(
            fbb,
            &RowFieldArgs {
                field_id,
                type_,
                value: Some(value),
            },
        )
    }

    fn random_row<'a>(_fbb: &mut flatbuffers::FlatBufferBuilder<'a>) -> WIPOffset<Row<'a>> {
        let fbb = _fbb.borrow_mut();

        let series_id = random_series_id();
        let timestamp = Utc::now().timestamp() as u64;
        let float_v = fbb.create_vector(rand::random::<f64>().to_be_bytes().as_slice());
        let string_v = fbb.create_vector("Hello world.".as_bytes());

        let mut fields: Vec<WIPOffset<RowField>> = vec![];
        fields.push(random_field(
            fbb,
            random_field_id(),
            FieldType::Float,
            float_v,
        ));
        fields.push(random_field(
            fbb,
            random_field_id(),
            FieldType::Float,
            string_v,
        ));
        let vec = fbb.create_vector(&fields);

        let mut row_builder = RowBuilder::new(fbb);
        row_builder.add_key(&RowKey::new(series_id, timestamp));
        row_builder.add_fields(vec);

        row_builder.finish()
    }

    fn random_write_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<WriteWALEntry<'a>> {
        let fbb = _fbb.borrow_mut();

        let mut rows: Vec<WIPOffset<Row>> = vec![];
        rows.push(random_row(fbb));
        rows.push(random_row(fbb));
        let vec = fbb.create_vector(&rows);

        WriteWALEntry::create(fbb, &WriteWALEntryArgs { rows: Some(vec) })
    }

    fn random_delete_wal_entry_item() -> DeleteWALEntryItem {
        DeleteWALEntryItem::new(random_series_id(), random_field_id())
    }

    fn random_delete_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<DeleteWALEntry<'a>> {
        let fbb = _fbb.borrow_mut();

        let mut items: Vec<DeleteWALEntryItem> = vec![];
        for _ in 0..10 {
            items.push(random_delete_wal_entry_item());
        }

        let vec = fbb.create_vector(&items);

        DeleteWALEntry::create(fbb, &DeleteWALEntryArgs { items: Some(vec) })
    }

    fn random_delete_range_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<DeleteRangeWALEntry<'a>> {
        let fbb = _fbb.borrow_mut();
        let mut items: Vec<DeleteWALEntryItem> = vec![];
        for _ in 0..10 {
            items.push(random_delete_wal_entry_item());
        }

        let vec = fbb.create_vector(&items);

        DeleteRangeWALEntry::create(
            fbb,
            &DeleteRangeWALEntryArgs {
                items: Some(vec),
                min: 1,
                max: 100,
            },
        )
    }

    fn random_wal_entry<'a>(
        _fbb: &mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> WIPOffset<WALEntry<'a>> {
        let fbb = _fbb.borrow_mut();

        let entry_type = random_wal_entry_type();
        let entry_union = match entry_type {
            WALEntryType::Write => (
                WALEntryUnion::Write,
                random_write_wal_entry(fbb).as_union_value(),
            ),
            WALEntryType::Delete => (
                WALEntryUnion::Delete,
                random_delete_wal_entry(fbb).as_union_value(),
            ),
            WALEntryType::DeleteRange => (
                WALEntryUnion::DeleteRange,
                random_delete_range_wal_entry(fbb).as_union_value(),
            ),
            _ => panic!("Invalid entry type"),
        };

        WALEntry::create(
            fbb,
            &WALEntryArgs {
                seq: 1,
                type_: entry_type,
                series_id: random_series_id(),
                value_type: entry_union.0,
                value: Some(entry_union.1),
            },
        )
    }

    #[test]
    fn test_write_entry() {
        let file_manager = file_manager::FileManager::get_instance();

        let mut mgr = WriteAheadLogManager::new(&file_manager, String::from("/tmp/test/"));
        mgr.open().unwrap();

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry(&mut fbb);
            fbb.finish(entry, None);

            let bytes = fbb.finished_data();
            dbg!(bytes.len());

            let de_entry = flatbuffers::root::<WALEntry>(bytes).unwrap();
            mgr.write(&de_entry).unwrap();
        }
    }

    #[test]
    fn test_read_entry() {
        let file_manager = file_manager::FileManager::get_instance();

        let mut mgr = WriteAheadLogManager::new(&file_manager, String::from("/tmp/test"));
        mgr.open().unwrap();

        let cursor = mgr.current_file_writer.unwrap().cursor;
        let mut reader = WriteAheadLogReader::new(cursor);

        let mut i = 0;
        while let Some(block) = reader.next_wal_entry() {
            i += 1;
            dbg!(i);
            let wal_entry =
                flatbuffers::root::<WALEntry>(&block.buf[..block.len as usize]).unwrap();
            dbg!(wal_entry);
        }
    }

    #[test]
    fn test_read_and_write() {
        let file_manager = file_manager::FileManager::get_instance();

        let mut mgr = WriteAheadLogManager::new(&file_manager, String::from("/tmp/test/"));
        mgr.open().unwrap();

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry(&mut fbb);
            fbb.finish(entry, None);

            let bytes = fbb.finished_data();
            dbg!(bytes.len());

            let de_entry = flatbuffers::root::<WALEntry>(bytes).unwrap();
            mgr.write(&de_entry).unwrap();
        }

        let mut cursor = mgr.current_file_writer.unwrap().cursor;
        cursor.seek(std::io::SeekFrom::Start(0)).unwrap();
        let mut reader = WriteAheadLogReader::new(cursor);
        let mut i = 0;
        let mut writed_crcs = Vec::<u32>::new();
        let mut readed_crcs = Vec::<u32>::new();
        while let Some(block) = reader.next_wal_entry() {
            i += 1;
            dbg!(i);
            let wal_entry =
                flatbuffers::root::<WALEntry>(&block.buf[..block.len as usize]).unwrap();
            dbg!(wal_entry);

            writed_crcs.push(block.crc);
            readed_crcs.push(get_checksum(&block.buf[..block.len as usize]));
        }

        assert_eq!(writed_crcs, readed_crcs);
    }
}
