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
use std::sync::Arc;

use models::predicate::domain::TimeRange;
use models::FieldId;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use trace::{debug, error};
use utils::BloomFilter;

use super::DataBlock;
use crate::file_system::file_manager;
use crate::record_file::{self, RecordDataType, RecordDataVersion};
use crate::{byte_utils, file_utils, Error, Result};

pub const TOMBSTONE_FILE_SUFFIX: &str = "tombstone";
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
    /// Tombstone caches.
    tombstones: Mutex<HashMap<FieldId, Vec<TimeRange>>>,

    path: PathBuf,
    /// Async record file writer.
    ///
    /// If you want to use self::writer and self::tombstones at the same time,
    /// lock writer first then tombstones.
    writer: Arc<AsyncMutex<Option<record_file::Writer>>>,
}

impl TsmTombstone {
    pub async fn open(path: impl AsRef<Path>, tsm_file_id: u64) -> Result<Self> {
        let path = file_utils::make_tsm_tombstone_file(path, tsm_file_id);
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
            tombstones: Mutex::new(tombstones),
            path,
            writer: Arc::new(AsyncMutex::new(writer)),
        })
    }

    #[cfg(test)]
    pub async fn with_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let parent = path.parent().expect("a valid tsm/tombstone file path");
        let tsm_file_id = file_utils::get_tsm_file_id_by_path(path)?;
        Self::open(parent, tsm_file_id).await
    }

    async fn load_all(
        reader: &mut record_file::Reader,
        tombstones: &mut HashMap<FieldId, Vec<TimeRange>>,
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
            let field_id = byte_utils::decode_be_u64(&data[0..8]);
            let min_ts = byte_utils::decode_be_i64(&data[8..16]);
            let max_ts = byte_utils::decode_be_i64(&data[16..24]);
            tombstones
                .entry(field_id)
                .or_default()
                .push(TimeRange { min_ts, max_ts });
        }

        Ok(())
    }

    async fn save_all(
        writer: &mut record_file::Writer,
        tombstones: &HashMap<FieldId, Vec<TimeRange>>,
    ) -> Result<()> {
        let mut write_buf = [0_u8; ENTRY_LEN];
        for (field_id, time_ranges) in tombstones.iter() {
            for time_range in time_ranges.iter() {
                write_buf[0..8].copy_from_slice(field_id.to_be_bytes().as_slice());
                write_buf[8..16].copy_from_slice(time_range.min_ts.to_be_bytes().as_slice());
                write_buf[16..24].copy_from_slice(time_range.max_ts.to_be_bytes().as_slice());
                writer
                    .write_record(
                        RecordDataVersion::V1 as u8,
                        RecordDataType::Tombstone as u8,
                        &[&write_buf],
                    )
                    .await?;
            }
        }

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.tombstones.lock().is_empty()
    }

    pub async fn add_range(
        &self,
        field_ids: &[FieldId],
        time_range: &TimeRange,
        bloom_filter: Option<Arc<BloomFilter>>,
    ) -> Result<()> {
        let mut writer_lock = self.writer.lock().await;
        if writer_lock.is_none() {
            *writer_lock =
                Some(record_file::Writer::open(&self.path, RecordDataType::Tombstone).await?);
        }
        let writer = writer_lock
            .as_mut()
            .expect("initialized record file writer");

        let mut write_buf = [0_u8; ENTRY_LEN];
        for field_id in field_ids.iter() {
            write_buf[0..8].copy_from_slice((*field_id).to_be_bytes().as_slice());
            if let Some(filter) = bloom_filter.as_ref() {
                if !filter.contains(&write_buf[0..8]) {
                    continue;
                }
            }
            write_buf[8..16].copy_from_slice(time_range.min_ts.to_be_bytes().as_slice());
            write_buf[16..24].copy_from_slice(time_range.max_ts.to_be_bytes().as_slice());
            writer
                .write_record(
                    RecordDataVersion::V1 as u8,
                    RecordDataType::Tombstone as u8,
                    &[&write_buf],
                )
                .await?;

            self.tombstones
                .lock()
                .entry(*field_id)
                .or_default()
                .push(*time_range);
        }
        Ok(())
    }

    pub async fn add_range_and_compact_to_tmp(
        &self,
        field_ids: &[FieldId],
        time_range: &TimeRange,
        bloom_filter: Option<Arc<BloomFilter>>,
    ) -> Result<()> {
        let mut tombstones = self.tombstones.lock().clone();
        for field_id in field_ids.iter() {
            if let Some(filter) = bloom_filter.as_ref() {
                if !filter.contains(&field_id.to_be_bytes()) {
                    continue;
                }
            }
            tombstones.entry(*field_id).or_default().push(*time_range);
        }
        if tombstones.is_empty() {
            debug!("No tombstone to compact and save to compact_tmp file");
            return Ok(());
        }
        for time_ranges in tombstones.values_mut() {
            TimeRange::compact(time_ranges);
        }

        let tmp_path = tombstone_compact_tmp_path(&self.path)?;
        let mut writer = record_file::Writer::open(&tmp_path, RecordDataType::Tombstone).await?;
        trace::info!("Saving compact_tmp tombstone file '{}'", tmp_path.display());
        Self::save_all(&mut writer, &tombstones).await?;
        writer.close().await
    }

    pub async fn replace_with_compact_tmp(&self) -> Result<()> {
        let mut writer_lock = self.writer.lock().await;
        if let Some(w) = writer_lock.as_mut() {
            w.close().await?;
        }
        // Drop the old Writer, reopen in other time.
        *writer_lock = None;

        let tmp_path = tombstone_compact_tmp_path(&self.path)?;
        if file_manager::try_exists(&tmp_path) {
            trace::info!(
                "Converting compact_tmp tombstone file '{}' to real tombstone file '{}'",
                tmp_path.display(),
                self.path.display()
            );
            file_utils::rename(tmp_path, &self.path).await
        } else {
            debug!("No compact_tmp tombstone file to convert to real tombstone file");
            Ok(())
        }
    }

    pub async fn flush(&self) -> Result<()> {
        if let Some(w) = self.writer.lock().await.as_mut() {
            w.sync().await?;
        }
        Ok(())
    }

    /// Returns all TimeRanges for a FieldId cloned from TsmTombstone.
    pub(crate) fn get_cloned_time_ranges(&self, field_id: FieldId) -> Option<Vec<TimeRange>> {
        self.tombstones.lock().get(&field_id).cloned()
    }

    pub fn overlaps(&self, field_id: FieldId, time_range: &TimeRange) -> bool {
        if let Some(time_ranges) = self.tombstones.lock().get(&field_id) {
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
        if let Some(time_ranges) = self.tombstones.lock().get(&field_id) {
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
            if let Some(time_ranges) = self.tombstones.lock().get(&field_id) {
                time_ranges
                    .iter()
                    .filter(|t| t.overlaps(time_range))
                    .for_each(|t| data_block.exclude(t));
            }
        }
    }

    // if no exclude, return None
    pub fn data_block_exclude_tombstones_new(
        &self,
        field_id: FieldId,
        data_block: &DataBlock,
    ) -> Option<DataBlock> {
        let block_tr: TimeRange = data_block.time_range()?.into();
        let tombstone_lock = self.tombstones.lock();
        let tombstone = tombstone_lock
            .get(&field_id)?
            .iter()
            .filter(|tr| tr.overlaps(&block_tr))
            .collect::<Vec<_>>();
        if tombstone.is_empty() {
            return None;
        }
        Some(data_block.exclude_time_ranges(tombstone.as_slice()))
    }
}

/// Generate pathbuf by tombstone path.
/// - For tombstone file: /tmp/test/000001.tombstone, return /tmp/test/000001.compact.tmp
pub fn tombstone_compact_tmp_path(tombstone_path: &Path) -> Result<PathBuf> {
    match tombstone_path.file_name().map(|os_str| {
        let mut s = os_str.to_os_string();
        s.push(".compact.tmp");
        s
    }) {
        Some(name) => {
            let mut p = tombstone_path.to_path_buf();
            p.set_file_name(name);
            Ok(p)
        }
        None => Err(Error::InvalidFileName {
            file_name: tombstone_path.display().to_string(),
            message: "invalid tombstone file name".to_string(),
        }),
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
        let dir = PathBuf::from("/tmp/test/tombstone/io_1".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        tombstone
            .add_range(&[0], &TimeRange::new(0, 0), None)
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
        let dir = PathBuf::from("/tmp/test/tombstone/io_2".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone
            .add_range(&[1, 2, 3], &TimeRange::new(1, 100), None)
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
        let dir = PathBuf::from("/tmp/test/tombstone/io_3".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        for i in 0..10000 {
            tombstone
                .add_range(
                    &[3 * i as u64 + 1, 3 * i as u64 + 2, 3 * i as u64 + 3],
                    &TimeRange::new(i as i64 * 2, i as i64 * 2 + 100),
                    None,
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

    // #[tokio::test]
    // async fn test_compact() {
    //     let dir = PathBuf::from("/tmp/test/tombstone/compact".to_string());
    //     let _ = std::fs::remove_dir_all(&dir);
    //     std::fs::create_dir_all(&dir).unwrap();

    //     let time_ranges = vec![
    //         TimeRange::new(0, 50),
    //         TimeRange::new(49, 60),
    //         TimeRange::new(60, 70),
    //         TimeRange::new(80, 90),
    //         TimeRange::new(75, 100),
    //     ];
    //     let field_ids_vec = vec![vec![1, 2, 3], vec![2, 3, 4], vec![3, 4, 5]];

    //     let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
    //     for field_ids in field_ids_vec.iter() {
    //         for time_range in time_ranges.iter() {
    //             tombstone
    //                 .add_range(field_ids, time_range, None)
    //                 .await
    //                 .unwrap();
    //         }
    //     }
    //     tombstone.flush().await.unwrap();
    //     {
    //         let tombstones = tombstone.tombstones.lock().clone();
    //         assert_eq!(tombstones.len(), 5);
    //         let mut time_ranges_exp_2 = time_ranges.clone();
    //         time_ranges_exp_2.extend_from_slice(&time_ranges);
    //         let mut time_ranges_exp_3 = time_ranges_exp_2.clone();
    //         time_ranges_exp_3.extend_from_slice(&time_ranges);
    //         for field_id in 1..=5 {
    //             assert!(tombstones.contains_key(&field_id));
    //             let time_ranges_tomb = tombstones.get(&field_id).unwrap();
    //             if field_id == 1 || field_id == 5 {
    //                 assert_eq!(time_ranges_tomb, &time_ranges);
    //             } else if field_id == 3 {
    //                 assert_eq!(time_ranges_tomb, &time_ranges_exp_3);
    //             } else {
    //                 assert_eq!(time_ranges_tomb, &time_ranges_exp_2);
    //             }
    //         }
    //     }

    //     tombstone.compact();
    //     {
    //         let tombstones = tombstone.tombstones.lock().clone();
    //         assert_eq!(tombstones.len(), 5);
    //         let time_ranges_exp = vec![TimeRange::new(0, 70), TimeRange::new(75, 100)];
    //         for field_id in 1..=5 {
    //             assert!(tombstones.contains_key(&field_id));
    //             let time_ranges_tomb = tombstones.get(&field_id).unwrap();
    //             assert_eq!(time_ranges_tomb, &time_ranges_exp);
    //         }
    //     }
    // }
}
