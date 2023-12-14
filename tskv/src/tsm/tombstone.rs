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

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use models::predicate::domain::TimeRange;
use models::{ColumnId, SeriesId};
use trace::error;

use super::DataBlock;
use crate::file_system::file_manager;
use crate::record_file::{self, RecordDataType, RecordDataVersion};
use crate::{byte_utils, file_utils, Error, Result};

const TOMBSTONE_FILE_SUFFIX: &str = ".tombstone";
const FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'r', b'o', b'm', b'b']);
const FOOTER_MAGIC_NUMBER_LEN: usize = 4;
const ENTRY_LEN: usize = 24; // 4 + 4 + 8 + 8

#[derive(Debug, Clone, Copy)]
pub struct Tombstone {
    pub series_id: SeriesId,
    pub column_id: ColumnId,
    pub time_range: TimeRange,
}

/// Tombstones for a tsm file
///
/// - file_name: _%06d.tombstone
/// - header: b"TOMB" 4 bytes
/// - loop begin
/// - - series_id: u32 4 bytes
/// - - column_id: u64 4 bytes
/// - - min: i64 8 bytes
/// - - max: i64 8 bytes
/// - loop end
pub struct TsmTombstone {
    tombstones: HashMap<(SeriesId, ColumnId), Vec<TimeRange>>,

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
        let mut tombstones = HashMap::new();
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
        let tsm_id = file_utils::get_tsm_file_id_by_path(path)?;
        Self::open(parent, tsm_id).await
    }

    async fn load_all(
        reader: &mut record_file::Reader,
        tombstones: &mut HashMap<(SeriesId, ColumnId), Vec<TimeRange>>,
    ) -> Result<()> {
        loop {
            let data = match reader.read_record().await {
                Ok(r) => r.data,
                Err(Error::Eof) => break,
                Err(Error::RecordFileHashCheckFailed { .. }) => continue,
                Err(e) => return Err(e),
            };
            if data.len() < ENTRY_LEN {
                error!(
                    "Error reading tombstone: block length too small: {}",
                    data.len()
                );
                break;
            }
            let series_id = byte_utils::decode_be_u32(&data[0..4]);
            let column_id = byte_utils::decode_be_u32(&data[4..8]);
            let min_ts = byte_utils::decode_be_i64(&data[8..16]);
            let max_ts = byte_utils::decode_be_i64(&data[16..24]);
            tombstones
                .entry((series_id, column_id))
                .or_default()
                .push(TimeRange { min_ts, max_ts });
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty()
    }

    pub async fn add_range(
        &mut self,
        columns: &[(SeriesId, ColumnId)],
        time_range: &TimeRange,
    ) -> Result<()> {
        if self.writer.is_none() {
            self.writer =
                Some(record_file::Writer::open(&self.path, RecordDataType::Tombstone).await?);
        }
        let writer = self.writer.as_mut().expect("initialized file");

        for (series_id, column_id) in columns {
            self.write_buf[0..4].copy_from_slice((*series_id).to_be_bytes().as_slice());
            self.write_buf[4..8].copy_from_slice((*column_id).to_be_bytes().as_slice());
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
                .entry((*series_id, *column_id))
                .or_default()
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
    pub(crate) fn get_cloned_time_ranges(
        &self,
        _series_id: SeriesId,
        _column_id: ColumnId,
    ) -> Option<Vec<TimeRange>> {
        // self.tombstones.get(&(series_id, column_id)).cloned()?
        None
    }

    pub fn overlaps(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> bool {
        if let Some(time_ranges) = self.tombstones.get(&(series_id, column_id)) {
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
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> Vec<TimeRange> {
        if let Some(time_ranges) = self.tombstones.get(&(series_id, column_id)) {
            let mut trs = Vec::new();
            for t in time_ranges.iter() {
                if t.overlaps(time_range) {
                    trs.push(*t);
                }
            }
            return trs;
        }

        vec![]
    }

    pub fn data_block_exclude_tombstones(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        data_block: &mut DataBlock,
    ) {
        if let Some(tr_tuple) = data_block.time_range() {
            let time_range: &TimeRange = &tr_tuple.into();
            if let Some(time_ranges) = self.tombstones.get(&(series_id, column_id)) {
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
    use std::path::PathBuf;

    use models::predicate::domain::TimeRange;

    use super::TsmTombstone;
    use crate::file_system::file_manager;

    #[tokio::test]
    async fn test_write_read_1() {
        let dir = PathBuf::from("/tmp/test/tombstone/1".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone
            .add_range(&[(0, 0)], &TimeRange::new(0, 0))
            .await
            .unwrap();
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps(
            0,
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
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone
            .add_range(&[(0, 1), (0, 2), (0, 3)], &TimeRange::new(1, 100))
            .await
            .unwrap();
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps(
            0,
            1,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(tombstone.overlaps(
            0,
            2,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(!tombstone.overlaps(
            0,
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
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        for i in 0..10000 {
            tombstone
                .add_range(
                    &[
                        (0, 3 * i as u32 + 1),
                        (0, 3 * i as u32 + 2),
                        (0, 3 * i as u32 + 3),
                    ],
                    &TimeRange::new(i as i64 * 2, i as i64 * 2 + 100),
                )
                .await
                .unwrap();
        }
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps(
            0,
            1,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(tombstone.overlaps(
            0,
            2,
            &TimeRange {
                max_ts: 3,
                min_ts: 100
            }
        ));
        assert!(!tombstone.overlaps(
            0,
            3,
            &TimeRange {
                max_ts: 4,
                min_ts: 101
            }
        ));
    }
}
