//! # Tombstone file
//!
//! A tombstone file is a [`record_file`].
//!
//! ## Record Data
//! ```text
//! +------------+---------------+---------------+
//! | 0: 8 bytes | 8: 8 bytes    | 16: 8 bytes   |
//! +------------+---------------+---------------+
//! |  field_id  | min_timestamp | max_timestamp |
//! +------------+---------------+---------------+
//! ```

use std::{
    collections::HashMap,
    fmt::write,
    io::IoSlice,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::buf;
use datafusion::parquet::record;
use models::{FieldId, SeriesId, Timestamp, ValueType};
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;
use trace::error;

use crate::{
    byte_utils, error,
    file_system::{file_manager, AsyncFile, FileCursor, IFile},
    file_utils,
    record_file::{self, RecordDataType, RecordDataVersion},
    tseries_family::{ColumnFile, TimeRange},
    Error, Result,
};

use super::DataBlock;

const TOMBSTONE_FILE_SUFFIX: &str = ".tombstone";
const FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'r', b'o', b'm', b'b']);
const FOOTER_MAGIC_NUMBER_LEN: usize = 4;
const ENTRY_LEN: usize = 24; // 8 + 8 + 8

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

    path: PathBuf,
    writer: Option<record_file::Writer>,
    write_buf: [u8; ENTRY_LEN],
}

impl TsmTombstone {
    pub async fn open(path: impl AsRef<Path>, file_id: u64) -> Result<Self> {
        let path = file_utils::make_tsm_tombstone_file_name(path, file_id);
        let (mut reader, writer) = if file_manager::try_exists(&path) {
            (
                Some(record_file::Reader::open(&path).await?),
                Some(record_file::Writer::open(&path, RecordDataType::Tombstone).await?),
            )
        } else {
            (None, None)
        };
        let mut tombstones: HashMap<u64, Vec<TimeRange>> = HashMap::new();
        if let Some(r) = reader.as_mut() {
            Self::load_all(r, &mut tombstones).await?;
        }

        Ok(Self {
            tombstones,
            path,
            writer,
            write_buf: [0_u8; ENTRY_LEN],
        })
    }

    #[cfg(test)]
    pub async fn with_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let parent = path.parent().expect("a valid tsm/tombstone file path");
        let tsm_id = file_utils::get_tsm_file_id_by_path(&path)?;
        Self::open(parent, tsm_id).await
    }

    async fn load_all(
        reader: &mut record_file::Reader,
        tombstones: &mut HashMap<FieldId, Vec<TimeRange>>,
    ) -> Result<()> {
        loop {
            let data = match reader.read_record().await {
                Ok(r) => r.data,
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            };
            if data.len() < ENTRY_LEN {
                error!(
                    "Error reading tombstone: block length too small: {}",
                    data.len()
                );
                break;
            }
            let field_id = byte_utils::decode_be_u64(&data[0..8]);
            let min_ts = byte_utils::decode_be_i64(&data[8..16]);
            let max_ts = byte_utils::decode_be_i64(&data[16..24]);
            tombstones
                .entry(field_id)
                .or_insert(Vec::new())
                .push(TimeRange { min_ts, max_ts });
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty()
    }

    pub async fn add_range(&mut self, field_ids: &[FieldId], time_range: &TimeRange) -> Result<()> {
        if self.writer.is_none() {
            self.writer =
                Some(record_file::Writer::open(&self.path, RecordDataType::Tombstone).await?);
        }
        let writer = self.writer.as_mut().expect("initialized file");

        for field_id in field_ids.iter() {
            self.write_buf[0..8].copy_from_slice((*field_id).to_be_bytes().as_slice());
            self.write_buf[8..16].copy_from_slice(time_range.min_ts.to_be_bytes().as_slice());
            self.write_buf[16..24].copy_from_slice(time_range.max_ts.to_be_bytes().as_slice());
            writer
                .write_record(
                    RecordDataVersion::V1 as u8,
                    RecordDataType::Tombstone as u8,
                    &[&self.write_buf],
                )
                .await?;

            self.tombstones
                .entry(*field_id)
                .or_insert(Vec::new())
                .push(*time_range);
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        if let Some(w) = self.writer.as_mut() {
            w.sync().await?;
        }
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
    use crate::file_system::file_manager;
    use crate::{byte_utils, file_utils, tseries_family::TimeRange};

    #[tokio::test]
    async fn test_write_read_1() {
        let dir = PathBuf::from("/tmp/test/tombstone/1".to_string());
        let _ = std::fs::remove_dir(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone
            .add_range(&[0], &TimeRange::new(0, 0))
            .await
            .unwrap();
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps(
            0,
            &TimeRange {
                max_ts: 0,
                min_ts: 0
            }
        ));
    }

    #[tokio::test]
    async fn test_write_read_2() {
        let dir = PathBuf::from("/tmp/test/tombstone/2".to_string());
        let _ = std::fs::remove_dir(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone
            .add_range(&[1, 2, 3], &TimeRange::new(1, 100))
            .await
            .unwrap();
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
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

    #[tokio::test]
    async fn test_write_read_3() {
        let dir = PathBuf::from("/tmp/test/tombstone/3".to_string());
        let _ = std::fs::remove_dir(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        for i in 0..10000 {
            tombstone
                .add_range(
                    &[3 * i as u64 + 1, 3 * i as u64 + 2, 3 * i as u64 + 3],
                    &TimeRange::new(i as i64 * 2, i as i64 * 2 + 100),
                )
                .await
                .unwrap();
        }
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
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
