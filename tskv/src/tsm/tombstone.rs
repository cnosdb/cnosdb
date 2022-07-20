use std::{
    fmt::write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::buf;
use models::{FieldId, SeriesId, Timestamp, ValueType};
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;

use crate::{
    byte_utils,
    direct_io::{File, FileCursor, FileSync},
    error, file_manager, file_utils,
    tseries_family::{ColumnFile, TimeRange},
    Error, Result,
};

const TOMBSTONE_FILE_SUFFI: &str = ".tombstone";
const TOMBSTONE_MAGIC: u32 = 0x544F4D42;

#[derive(Debug, Clone, Copy)]
pub struct Tombstone {
    pub field_id: FieldId,
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
}

/// Tombstones for a tsm file
///
/// - file_name: _%06d.tombstone
/// - header: b"TOMB" 4 bytes
/// - loop begin
/// - - field_id: u64 8 bytes
/// - - min: i64 8 bytes
/// - - max: i64 8 bytes
/// - loop end
pub struct TsmTombstone {
    tombstones: Vec<Tombstone>,
    tomb_accessor: File,
    tomb_size: u64,
    mutex: Mutex<()>,
}

impl TsmTombstone {
    pub fn tombstones(&self) -> &[Tombstone] {
        self.tombstones.as_slice()
    }

    #[cfg(test)]
    pub fn with_path(path: impl AsRef<Path>) -> Result<Self> {
        let file = file_manager::get_file_manager().create_file(&path)?;
        Self::write_header_to(&file)?;
        file.sync_data(FileSync::Hard).context(error::IOSnafu)?;
        let tomb_size = file.len();

        Ok(Self { tombstones: vec![], tomb_accessor: file, tomb_size, mutex: Mutex::new(()) })
    }

    pub fn with_tsm_file_id(path: impl AsRef<Path>, file_id: u64) -> Result<Self> {
        let path = file_utils::make_tsm_tombstone_file_name(path, file_id);
        let mut is_new = false;
        if !file_manager::try_exists(&path) {
            is_new = true;
        }
        let file = file_manager::get_file_manager().open_create_file(&path)?;
        if is_new {
            Self::write_header_to(&file)?;
            file.sync_data(FileSync::Hard).context(error::IOSnafu)?;
        }
        let tomb_size = file.len();

        Ok(Self { tombstones: vec![], tomb_accessor: file, tomb_size, mutex: Mutex::new(()) })
    }

    pub fn load(&mut self) -> Result<()> {
        let tombstones = Self::load_all(&self.tomb_accessor)?;
        self.tombstones = tombstones;

        Ok(())
    }

    fn load_all(reader: &File) -> Result<Vec<Tombstone>> {
        const HEADER_SIZE: usize = 4;
        let file_len = reader.len() as usize;
        let mut header = vec![0_u8; HEADER_SIZE];
        // TODO: unable to read tombstone file
        reader.read_at(0, &mut header).context(error::ReadFileSnafu)?;

        const BUF_SIZE: usize = 1024 * 64;
        let (mut buf, buf_len) = if file_len < BUF_SIZE {
            let buf_len = (file_len - 4) as usize;
            (vec![0_u8; buf_len], buf_len)
        } else {
            (vec![0_u8; BUF_SIZE], BUF_SIZE)
        };
        let mut pos = HEADER_SIZE;
        let mut tombstones = Vec::new();
        while pos < file_len {
            reader.read_at(pos as u64, &mut buf).context(error::ReadFileSnafu)?;
            pos += buf_len;
            let mut buf_pos = 0;
            while buf_pos <= buf_len - 24 {
                let field_id = byte_utils::decode_be_u64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let min = byte_utils::decode_be_i64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let max = byte_utils::decode_be_i64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let tombstone = Tombstone { field_id, min_ts: min, max_ts: max };
                tombstones.push(tombstone);
            }
        }
        Ok(tombstones)
    }

    fn write_header_to(writer: &File) -> Result<usize> {
        writer.write_at(0, &TOMBSTONE_MAGIC.to_be_bytes()[..]).context(error::IOSnafu)
    }

    fn write_to(writer: &File, pos: u64, tombstone: &Tombstone) -> Result<usize> {
        let mut size = 0_usize;
        let ret = writer.write_at(pos, &tombstone.field_id.to_be_bytes()[..])
                        .and_then(|s| {
                            size += s;
                            writer.write_at(pos + size as u64, &tombstone.min_ts.to_be_bytes()[..])
                        })
                        .and_then(|s| {
                            size += s;
                            writer.write_at(pos + size as u64, &tombstone.max_ts.to_be_bytes()[..])
                        })
                        .map(|s| {
                            size += s;
                            size
                        })
                        .map_err(|e| {
                            // Write fail, recover writer offset
                            writer.set_len(pos);
                            Error::IO { source: e }
                        });

        ret
    }

    pub fn add_range(&mut self,
                     field_ids: &[FieldId],
                     min: Timestamp,
                     max: Timestamp)
                     -> Result<()> {
        for field_id in field_ids.iter() {
            let tomb = Tombstone { field_id: *field_id, min_ts: min, max_ts: max };
            Self::write_to(&self.tomb_accessor, self.tomb_size, &tomb).map(|s| {
                                                                          self.tomb_size +=
                                                                              s as u64;
                                                                          self.tombstones
                                                                              .push(tomb);
                                                                      })?;
        }
        Ok(())
    }

    pub fn overlaps(&self, field_id: FieldId, timerange: &TimeRange) -> bool {
        for t in self.tombstones.iter() {
            if t.field_id == field_id
               && t.max_ts >= timerange.min_ts
               && t.min_ts <= timerange.max_ts
            {
                return true;
            }
        }

        false
    }

    pub fn flush(&self) -> Result<()> {
        self.tomb_accessor.set_len(self.tomb_size);
        self.tomb_accessor.sync_all(FileSync::Hard).context(error::IOSnafu)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{
        path::{Path, PathBuf},
        str::FromStr,
        sync::Arc,
    };

    use super::TsmTombstone;
    use crate::{byte_utils, file_manager, file_utils, tseries_family::TimeRange};

    #[test]
    fn test_write_read() {
        let dir = PathBuf::from("/tmp/test/tombstone".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let path = file_utils::make_tsm_tombstone_file_name(&dir, 1);

        let mut tombstone = TsmTombstone::with_path(&path).unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone.add_range(&[1, 2, 3], 1, 100).unwrap();
        tombstone.flush().unwrap();

        tombstone.load().unwrap();
        assert_eq!(true, tombstone.overlaps(1, &TimeRange { max_ts: 2, min_ts: 99 }));
        assert_eq!(true, tombstone.overlaps(2, &TimeRange { max_ts: 2, min_ts: 99 }));
        assert_eq!(false, tombstone.overlaps(3, &TimeRange { max_ts: 101, min_ts: 103 }));
    }

    #[test]
    fn test_write_read_tiny() {
        let dir = PathBuf::from("/tmp/test/tombstone/1".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let path = file_utils::make_tsm_tombstone_file_name(&dir, 1);

        let mut tombstone = TsmTombstone::with_path(&path).unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone.add_range(&[0], 0, 0).unwrap();
        tombstone.flush().unwrap();

        tombstone.load().unwrap();

        assert_eq!(true, tombstone.overlaps(0, &TimeRange { max_ts: 0, min_ts: 0 }));
    }

    #[test]
    fn test_write_read_slow() {
        let dir = PathBuf::from("/tmp/test/tombstone/slow".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let path = file_utils::make_tsm_tombstone_file_name(&dir, 1);

        let mut tombstone = TsmTombstone::with_path(&path).unwrap();
        // tsm_tombstone.load().unwrap();
        for i in 0..10000 {
            tombstone.add_range(&[3 * i as u64 + 1, 3 * i as u64 + 2, 3 * i as u64 + 3],
                                i as i64 * 2,
                                i as i64 * 2 + 100)
                     .unwrap();
        }
        tombstone.flush().unwrap();

        tombstone.load().unwrap();
        assert_eq!(true, tombstone.overlaps(1, &TimeRange { max_ts: 2, min_ts: 99 }));
        assert_eq!(true, tombstone.overlaps(2, &TimeRange { max_ts: 3, min_ts: 100 }));
        assert_eq!(false, tombstone.overlaps(3, &TimeRange { max_ts: 4, min_ts: 101 }));
    }
}
