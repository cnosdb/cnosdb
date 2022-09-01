use std::{
    collections::HashMap,
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

use super::DataBlock;
use crate::{
    byte_utils,
    direct_io::{File, FileCursor, FileSync},
    error, file_manager, file_utils,
    tseries_family::{ColumnFile, TimeRange},
    Error, Result,
};

const TOMBSTONE_FILE_SUFFIX: &str = ".tombstone";
const TOMBSTONE_MAGIC: u32 = 0x544F4D42;

#[derive(Debug, Clone, Copy)]
pub struct Tombstone {
    pub field_id: FieldId,
    pub time_range: TimeRange,
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
    tombstones: HashMap<FieldId, Vec<TimeRange>>,
    tomb_accessor: File,
    tomb_size: u64,
    mutex: Mutex<()>,
}

impl TsmTombstone {
    #[cfg(test)]
    pub fn with_path(path: impl AsRef<Path>) -> Result<Self> {
        let file = file_manager::get_file_manager().create_file(&path)?;
        Self::write_header_to(&file)?;
        file.sync_data(FileSync::Hard).context(error::IOSnafu)?;
        let tomb_size = file.len();

        Ok(Self {
            tombstones: HashMap::new(),
            tomb_accessor: file,
            tomb_size,
            mutex: Mutex::new(()),
        })
    }

    pub fn open_for_read(path: impl AsRef<Path>, file_id: u64) -> Result<Option<Self>> {
        let path = file_utils::make_tsm_tombstone_file_name(path, file_id);
        if !file_manager::try_exists(&path) {
            return Ok(None);
        }
        let file = file_manager::get_file_manager().open_file(&path)?;
        let tomb_size = file.len();

        Ok(Some(Self {
            tombstones: HashMap::new(),
            tomb_accessor: file,
            tomb_size,
            mutex: Mutex::new(()),
        }))
    }

    pub fn open_for_write(path: impl AsRef<Path>, file_id: u64) -> Result<Self> {
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

        Ok(Self {
            tombstones: HashMap::new(),
            tomb_accessor: file,
            tomb_size,
            mutex: Mutex::new(()),
        })
    }

    pub fn load(&mut self) -> Result<()> {
        let mut tombstones = HashMap::new();
        Self::load_all(&self.tomb_accessor, &mut tombstones)?;
        self.tombstones = tombstones;

        Ok(())
    }

    fn load_all(reader: &File, tombstones: &mut HashMap<FieldId, Vec<TimeRange>>) -> Result<()> {
        const HEADER_SIZE: usize = 4;
        let file_len = reader.len() as usize;
        let mut header = vec![0_u8; HEADER_SIZE];
        // TODO: unable to read tombstone file
        reader
            .read_at(0, &mut header)
            .context(error::ReadFileSnafu)?;

        const BUF_SIZE: usize = 1024 * 64;
        let (mut buf, buf_len) = if file_len < BUF_SIZE {
            let buf_len = (file_len - 4) as usize;
            (vec![0_u8; buf_len], buf_len)
        } else {
            (vec![0_u8; BUF_SIZE], BUF_SIZE)
        };
        let mut pos = HEADER_SIZE;
        while pos < file_len {
            reader
                .read_at(pos as u64, &mut buf)
                .context(error::ReadFileSnafu)?;
            pos += buf_len;
            let mut buf_pos = 0;
            while buf_pos <= buf_len - 24 {
                let field_id = byte_utils::decode_be_u64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let min = byte_utils::decode_be_i64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let max = byte_utils::decode_be_i64(&buf[buf_pos..buf_pos + 8]);
                buf_pos += 8;
                let bucket = tombstones.entry(field_id).or_insert(Vec::new());
                bucket.push(TimeRange {
                    min_ts: min,
                    max_ts: max,
                });
            }
        }
        Ok(())
    }

    pub fn add_range(&mut self, field_ids: &[FieldId], time_range: &TimeRange) -> Result<()> {
        for field_id in field_ids.iter() {
            let tomb = Tombstone {
                field_id: *field_id,
                time_range: *time_range,
            };
            Self::write_to(&self.tomb_accessor, self.tomb_size, &tomb).map(|s| {
                self.tomb_size += s as u64;
                self.tombstones
                    .entry(*field_id)
                    .or_insert(Vec::new())
                    .push(*time_range);
            })?;
        }
        Ok(())
    }

    fn write_header_to(writer: &File) -> Result<usize> {
        writer
            .write_at(0, &TOMBSTONE_MAGIC.to_be_bytes()[..])
            .context(error::IOSnafu)
    }

    fn write_to(writer: &File, pos: u64, tombstone: &Tombstone) -> Result<usize> {
        let mut size = 0_usize;

        writer
            .write_at(pos, &tombstone.field_id.to_be_bytes()[..])
            .and_then(|s| {
                size += s;
                writer.write_at(
                    pos + size as u64,
                    &tombstone.time_range.min_ts.to_be_bytes()[..],
                )
            })
            .and_then(|s| {
                size += s;
                writer.write_at(
                    pos + size as u64,
                    &tombstone.time_range.max_ts.to_be_bytes()[..],
                )
            })
            .map(|s| {
                size += s;
                size
            })
            .map_err(|e| {
                // Write fail, recover writer offset
                writer.set_len(pos);
                Error::IO { source: e }
            })
    }

    pub fn flush(&self) -> Result<()> {
        self.tomb_accessor.set_len(self.tomb_size);
        self.tomb_accessor
            .sync_all(FileSync::Hard)
            .context(error::IOSnafu)?;
        Ok(())
    }

    /// Returns all TimeRanges for a FieldId cloned from TsmTombstone.
    pub(crate) fn get_cloned_time_ranges(&self, field_id: FieldId) -> Option<Vec<TimeRange>> {
        self.tombstones.get(&field_id).cloned()
    }

    pub fn overlaps(&self, field_id: FieldId, time_range: &TimeRange) -> bool {
        if let Some(time_ranges) = self.tombstones.get(&field_id) {
            for t in time_ranges.iter() {
                if t.overlaps(time_range) {
                    return true;
                }
            }
        }

        false
    }

    /// Returns all tombstone `TimeRange`s that overlaps the given `TimeRange`.
    /// Returns None if there is nothing to return, or `TimeRange`s is empty.
    pub fn get_overlapped_time_ranges(
        &self,
        field_id: FieldId,
        time_range: &TimeRange,
    ) -> Option<Vec<TimeRange>> {
        if let Some(time_ranges) = self.tombstones.get(&field_id) {
            let mut trs = Vec::new();
            for t in time_ranges.iter() {
                if t.overlaps(time_range) {
                    trs.push(*t);
                }
            }
            if trs.is_empty() {
                return None;
            }
            return Some(trs);
        }

        None
    }

    pub fn data_block_exclude_tombstones(&self, field_id: FieldId, data_block: &mut DataBlock) {
        if let Some(tr_tuple) = data_block.time_range() {
            let time_range: &TimeRange = &tr_tuple.into();
            if let Some(time_ranges) = self.tombstones.get(&field_id) {
                for t in time_ranges.iter() {
                    if t.overlaps(time_range) {
                        data_block.exclude(t);
                    }
                }
            }
        }
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
    fn test_write_read_1() {
        let dir = PathBuf::from("/tmp/test/tombstone/1".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let path = file_utils::make_tsm_tombstone_file_name(&dir, 1);

        let mut tombstone = TsmTombstone::with_path(&path).unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone.add_range(&[0], &TimeRange::new(0, 0)).unwrap();
        tombstone.flush().unwrap();

        tombstone.load().unwrap();
        assert!(tombstone.overlaps(
            0,
            &TimeRange {
                max_ts: 0,
                min_ts: 0
            }
        ));
    }

    #[test]
    fn test_write_read_2() {
        let dir = PathBuf::from("/tmp/test/tombstone/2".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let path = file_utils::make_tsm_tombstone_file_name(&dir, 1);

        let mut tombstone = TsmTombstone::with_path(&path).unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone
            .add_range(&[1, 2, 3], &TimeRange::new(1, 100))
            .unwrap();
        tombstone.flush().unwrap();

        tombstone.load().unwrap();
        assert!(tombstone.overlaps(
            1,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(tombstone.overlaps(
            2,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(!tombstone.overlaps(
            3,
            &TimeRange {
                max_ts: 101,
                min_ts: 103
            }
        ));
    }

    #[test]
    fn test_write_read_3() {
        let dir = PathBuf::from("/tmp/test/tombstone/3".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let path = file_utils::make_tsm_tombstone_file_name(&dir, 1);

        let mut tombstone = TsmTombstone::with_path(&path).unwrap();
        // tsm_tombstone.load().unwrap();
        for i in 0..10000 {
            tombstone
                .add_range(
                    &[3 * i as u64 + 1, 3 * i as u64 + 2, 3 * i as u64 + 3],
                    &TimeRange::new(i as i64 * 2, i as i64 * 2 + 100),
                )
                .unwrap();
        }
        tombstone.flush().unwrap();

        tombstone.load().unwrap();
        assert!(tombstone.overlaps(
            1,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(tombstone.overlaps(
            2,
            &TimeRange {
                max_ts: 3,
                min_ts: 100
            }
        ));
        assert!(!tombstone.overlaps(
            3,
            &TimeRange {
                max_ts: 4,
                min_ts: 101
            }
        ));
    }
}
