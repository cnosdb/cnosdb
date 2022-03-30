use std::path::{Path, PathBuf};

use lazy_static::lazy_static;
use regex::Regex;
use snafu::Snafu;

use protos::models::*;
use util::direct_fio::{File, FileSync, FileSystem, Options};

use crate::FileManager;

lazy_static! {
    static ref WAL_FILE_NAME_PATTERN: Regex = Regex::new("_.*\\.wal").unwrap();
}

const SEGMENT_SIZE: u64 = 1073741824; // 1 GiB

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Unable to walk dir: {}", source))]
    UnableToWalkDir { source: walkdir::Error },

    #[snafu(display("File {} has wrong name format to have an id", file_name))]
    InvalidFileName { file_name: String },

    #[snafu(display("Error with file: {}", source))]
    FileManagerError { source: super::file_manager::Error },

    #[snafu(display("{}", source))]
    FileIOError { source: std::io::Error },
}

type Result<T> = std::result::Result<T, Error>;

struct WalConfig {}

struct WriteAheadLogManager {
    dir: String,
    file_manager: &'static FileManager,

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
            .map_err(|err| Error::FileManagerError { source: err })
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
                    .map_err(|err| Error::FileManagerError { source: err })
                    .map(|file| WriteAheadLogDiskFileWriter::new(file))?,
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
            self.current_file_writer = Some(WriteAheadLogDiskFileWriter::new(file));
        }

        Ok(())
    }

    pub fn write(&mut self, entry: &WALEntry) -> Result<()> {
        self.roll_wal_file()?;

        let writer = &mut self.current_file_writer;
        match writer {
            Some(w) => w.write(entry),
            None => Err(Error::FileIOError {
                source: std::io::Error::new(std::io::ErrorKind::Other, "No writer initialized."),
            }),
        }
    }
}

pub struct WriteAheadLogDiskFileWriter {
    writer: File,
    size: u64,
}

impl WriteAheadLogDiskFileWriter {
    pub fn new(file: File) -> Self {
        Self {
            writer: file,
            size: 0,
        }
    }

    pub fn write(&mut self, wal_entry: &WALEntry) -> Result<()> {
        let ret = self.append(wal_entry._tab.buf);
        match ret {
            Ok(u64) => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn append(&mut self, buf: &[u8]) -> Result<u64> {
        // todo: 1. wrap buf (option: compress)
        // 2. write entry
        // 3. return bytes writed
        let ret = match self.writer.write_at(self.size, buf) {
            Ok(_) => Ok(buf.len() as u64),
            Err(err) => Err(Error::FileIOError { source: err }),
        };

        self.writer.sync_all(FileSync::Soft);

        self.size += buf.len() as u64;

        ret
    }

    pub fn sync(&self) -> Result<()> {
        self.writer
            .sync_all(FileSync::Soft)
            .map_err(|err| Error::FileIOError { source: err })
    }

    pub fn flush(&self) -> Result<()> {
        self.writer
            .sync_all(FileSync::Hard)
            .map_err(|err| Error::FileIOError { source: err })
    }
}

struct WriteAheadLogReader {
    reader: File,
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

#[cfg(test)]
mod test {
    use std::{borrow::BorrowMut, time};

    use chrono::Utc;
    use flatbuffers::{self, Vector, WIPOffset};
    use lazy_static::lazy_static;
    use rand;

    use protos::models::*;

    use crate::{file_manager, FileManager};

    use super::WriteAheadLogManager;

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
    fn write_entry() {
        let file_manager = file_manager::FileManager::get_instance();

        let mut writer = WriteAheadLogManager::new(&file_manager, String::from("/tmp/test/"));
        writer.open().unwrap();

        for i in 0..10 {
            let mut fbb = flatbuffers::FlatBufferBuilder::new();

            let entry = random_wal_entry(&mut fbb);
            fbb.finish(entry, None);

            let bytes = fbb.finished_data();
            println!("{:?}", bytes);

            let de_entry = flatbuffers::root::<WALEntry>(bytes).unwrap();
            writer.write(&de_entry).unwrap();
        }
    }
}
