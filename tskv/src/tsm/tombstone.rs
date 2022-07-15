use std::{fmt::write, path::Path, sync::Arc};

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
    path: String,
    tombstones: RwLock<Vec<Tombstone>>,
    tomb_accessor: Mutex<File>,
    tomb_size: u64,
}

impl TsmTombstone {
    #[cfg(test)]
    pub fn with_path(path: &str) -> Result<Self> {
        let file = file_manager::get_file_manager().create_file(&path)?;
        Self::write_header_to(&file)?;
        file.sync_data(FileSync::Hard).context(error::IOSnafu)?;
        let tomb_size = file.len();

        Ok(Self { path: path.to_string(),
                  tombstones: RwLock::new(vec![]),
                  tomb_accessor: Mutex::new(file),
                  tomb_size })
    }

    pub fn with_tsm_file_id(path: &str, file_id: u64) -> Result<Self> {
        let path = file_utils::make_tsm_tombstone_file_name(path, file_id);
        let path = path.to_str().expect("UTF-8 tombstone path").to_string();
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

        Ok(Self { path,
                  tombstones: RwLock::new(vec![]),
                  tomb_accessor: Mutex::new(file),
                  tomb_size })
    }

    pub fn load(&self) -> Result<()> {
        let file = self.tomb_accessor.lock();
        let mut tombstones = self.tombstones.write();
        tombstones.truncate(0);
        let file_len = file.len();

        let tombstones = Self::load_all(&file);

        Ok(())
    }

    pub fn load_all(reader: &File) -> Result<Vec<Tombstone>> {
        const HEADER_SIZE: usize = 4;
        let file_len = reader.len();
        let mut header = vec![0_u8; HEADER_SIZE];
        // TODO: unable to read tombstone file
        reader.read_at(0, &mut header).context(error::ReadFileSnafu)?;

        const BUF_SIZE: usize = 1024 * 64;
        let (mut buf, buf_len) = if file_len < BUF_SIZE as u64 {
            let buf_len = (file_len - 4) as usize;
            (vec![0_u8; buf_len], buf_len)
        } else {
            (vec![0_u8; BUF_SIZE], BUF_SIZE)
        };
        let mut pos = HEADER_SIZE;
        let mut tombstones = Vec::new();
        while pos < buf_len {
            reader.read_at(pos as u64, &mut buf).context(error::ReadFileSnafu)?;
            let mut buf_pos = 0;
            while buf_pos < buf_len {
                let field_id = byte_utils::decode_be_u64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let min = byte_utils::decode_be_i64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let max = byte_utils::decode_be_i64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let tombstone = Tombstone { field_id, min_ts: min, max_ts: max };
                tombstones.push(tombstone);
            }
            pos += buf_len
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
        let file = self.tomb_accessor.lock();
        for field_id in field_ids.iter() {
            Self::write_to(&file, self.tomb_size, &Tombstone { field_id: *field_id, min_ts: min, max_ts: max })
                .map(|s| self.tomb_size = s as u64)?;
        }
        Ok(())
    }

    pub fn overlaps(&self, timerange: &TimeRange) -> bool {
        let tombstones = self.tombstones.read();
        for t in tombstones.iter() {
            if t.max_ts >= timerange.max_ts && t.min_ts <= timerange.min_ts {
                return true;
            }
        }

        false
    }

    pub fn flush(&self) -> Result<()> {
        let file_cursor = self.tomb_accessor.lock();
        file_cursor.sync_all(FileSync::Hard).context(error::IOSnafu)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::TsmTombstone;
    use crate::{byte_utils, file_manager, tseries_family::TimeRange};

    #[test]
    fn test_write_read() {
        let mut tombstone = TsmTombstone::with_tsm_file_id("/tmp/", 1).unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone.add_range(&[1, 2, 3], 1, 100).unwrap();
        tombstone.flush().unwrap();

        tombstone.load().unwrap();
        let b = tombstone.overlaps(&TimeRange { max_ts: 2, min_ts: 99 });
    }
}
