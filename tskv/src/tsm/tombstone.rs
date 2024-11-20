//! # Tombstone file
//!
//! A tombstone file is a [`record_file`].
//!
//! ## Record Data (v1)
//! ```text
//! +----------------------+---------------+---------------+
//! | 0: 8 bytes           | 8: 8 bytes    | 16: 8 bytes   |
//! +----------------------+---------------+---------------+
//! |  seriesid columnid   | min_timestamp | max_timestamp |
//! +----------------------+---------------+---------------+
//! ```
//!
//! ## Record Data (v2)
//! ```text
//! # field_typ = FIELD_TYPE_ONE(0x00)
//! +-----------------+--------------------+-----------------+---------------+---------------+----
//! | 0: 1 byte       | 1: 8 bytes         | 9: 4 bytes      | 8: 8 bytes    | 16: 8 bytes   | ...
//! +-----------------+--------------------+-----------------+---------------+---------------+----
//! | field_typ(0x00) | seriesid columnid  | time_ranges_num | min_timestamp | max_timestamp | ...
//! +-----------------+--------------------+-----------------+---------------+---------------+----
//!
//! # field_typ = FIELD_TYPE_ALL(0x01)
//! +-----------------+-----------------+---------------+---------------+----
//! | 0: 1 byte       | 9: 4 bytes      | 8: 8 bytes    | 16: 8 bytes   | ...
//! +-----------------+-----------------+---------------+---------------+----
//! | field_typ(0x01) | time_ranges_num | min_timestamp | max_timestamp | ...
//! +-----------------+-----------------+---------------+---------------+----
//! ```

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::{ColumnId, SeriesId};
use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;
use trace::{debug, error};
use utils::BloomFilter;

use crate::error::{InvalidFileNameSnafu, TombstoneSnafu};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::record_file::{self, RecordDataType, RecordDataVersion};
use crate::{byte_utils, file_utils, ColumnFileId, TskvError, TskvResult};

pub const TOMBSTONE_FILE_SUFFIX: &str = "tombstone";
const MAX_RECORD_LEN: usize = 1024 * 128;
const MAX_RECORD_TIME_RANGES_NUM: usize = 1000;

const TOMBSTONE_HEADER_LEN_ALL: usize = 5; // 1 + 4
const TOMBSTONE_HEADER_LEN_ONE: usize = 13; // 1 + 8 + 4

pub(crate) const TOMBSTONE_BUFFER_SIZE: usize = 1024 * 1024;

const FIELD_TYPE_ONE: u8 = 0x00;
const FIELD_TYPE_ALL: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TombstoneField {
    One(SeriesId, ColumnId),
    All,
}

#[derive(Debug, Clone)]
pub struct Tombstone {
    pub field: TombstoneField,
    pub time_ranges: Vec<TimeRange>,
}

pub struct TsmTombstone {
    /// Tombstone caches.
    cache: RwLock<TsmTombstoneCache>,

    path: PathBuf,
    /// Async record file writer.
    ///
    /// If you want to use self::writer and self::tombstones at the same time,
    /// lock writer first then tombstones.
    writer: Arc<AsyncMutex<Option<record_file::Writer>>>,
}

impl TsmTombstone {
    pub async fn open(path: impl AsRef<Path>, tsm_file_id: ColumnFileId) -> TskvResult<Self> {
        let path = file_utils::make_tsm_tombstone_file(path, tsm_file_id);
        let (mut reader, writer) = if LocalFileSystem::try_exists(&path) {
            (
                Some(record_file::Reader::open(&path).await?),
                Some(record_file::Writer::open(&path, TOMBSTONE_BUFFER_SIZE).await?),
            )
        } else {
            (None, None)
        };
        let cache = if let Some(r) = reader.as_mut() {
            TsmTombstoneCache::load_from(r, false).await?
        } else {
            TsmTombstoneCache::default()
        };

        Ok(Self {
            cache: RwLock::new(cache),
            path,
            writer: Arc::new(AsyncMutex::new(writer)),
        })
    }

    #[cfg(test)]
    pub async fn with_path(path: impl AsRef<Path>) -> TskvResult<Self> {
        let path = path.as_ref();
        let parent = path.parent().expect("a valid tsm/tombstone file path");
        let tsm_file_id = file_utils::get_tsm_file_id_by_path(path)?;
        Self::open(parent, tsm_file_id).await
    }

    pub fn decode_v1(data: &[u8]) -> (SeriesId, ColumnId, TimeRange) {
        let series_id = byte_utils::decode_be_u32(&data[0..4]);
        let column_id = byte_utils::decode_be_u32(&data[4..8]);
        let min_ts = byte_utils::decode_be_i64(&data[8..16]);
        let max_ts = byte_utils::decode_be_i64(&data[16..24]);
        (series_id, column_id, TimeRange::new(min_ts, max_ts))
    }

    pub fn decode_v2(data: &[u8]) -> TskvResult<Vec<Tombstone>> {
        let mut sli = data;
        let mut ans = Vec::new();
        while !sli.is_empty() {
            if sli[0] == FIELD_TYPE_ALL {
                if sli.len() < TOMBSTONE_HEADER_LEN_ALL {
                    return Err(TombstoneSnafu {
                        message: format!(
                            "block length too small for tomb_header_ALL: {}",
                            sli.len()
                        ),
                    }
                    .build());
                }
                let field = TombstoneField::All;
                let time_ranges_num =
                    byte_utils::decode_be_u32(&sli[1..TOMBSTONE_HEADER_LEN_ALL]) as usize;
                let time_ranges =
                    Self::decode_time_ranges(&sli[TOMBSTONE_HEADER_LEN_ALL..], time_ranges_num)?;
                ans.push(Tombstone { field, time_ranges });
                sli = &sli[TOMBSTONE_HEADER_LEN_ALL + (8 + 8) * time_ranges_num..];
            } else {
                if sli.len() < TOMBSTONE_HEADER_LEN_ONE {
                    return Err(TombstoneSnafu {
                        message: format!(
                            "field length too small for tomb_header_FIELD: {}",
                            sli.len()
                        ),
                    }
                    .build());
                }
                let series_id = byte_utils::decode_be_u32(&sli[1..5]);
                let column_id = byte_utils::decode_be_u32(&sli[5..13]);
                let field = TombstoneField::One(series_id, column_id);
                let time_ranges_num =
                    byte_utils::decode_be_u32(&sli[9..TOMBSTONE_HEADER_LEN_ONE]) as usize;
                let time_ranges =
                    Self::decode_time_ranges(&sli[TOMBSTONE_HEADER_LEN_ONE..], time_ranges_num)?;
                ans.push(Tombstone { field, time_ranges });
                sli = &sli[TOMBSTONE_HEADER_LEN_ONE + (8 + 8) * time_ranges_num..];
            }
        }
        Ok(ans)
    }

    fn decode_time_ranges(data: &[u8], time_ranges_num: usize) -> TskvResult<Vec<TimeRange>> {
        if data.len() < (8 + 8) * time_ranges_num {
            return Err(TombstoneSnafu {
                message: format!(
                    "block length too small for tomb_time_ranges: {}",
                    data.len()
                ),
            }
            .build());
        }
        let mut time_ranges = Vec::with_capacity(time_ranges_num);
        let mut pos = 0;
        for _ in 0..time_ranges_num {
            let min_ts = byte_utils::decode_be_i64(&data[pos..pos + 8]);
            let max_ts = byte_utils::decode_be_i64(&data[pos + 8..pos + 16]);
            time_ranges.push((min_ts, max_ts).into());
            pos += 16;
        }

        Ok(time_ranges)
    }

    fn encode_field_time_ranges(
        buffer: &mut Vec<u8>,
        field: &TombstoneField,
        time_ranges: &[TimeRange],
    ) -> usize {
        if let TombstoneField::One(series_id, column_id) = field {
            buffer.push(FIELD_TYPE_ONE);
            buffer.extend_from_slice(series_id.to_be_bytes().as_slice());
            buffer.extend_from_slice(column_id.to_be_bytes().as_slice());
        } else {
            buffer.push(FIELD_TYPE_ALL);
        }
        let time_ranges_len = time_ranges.len().min(MAX_RECORD_TIME_RANGES_NUM);
        buffer.extend_from_slice((time_ranges_len as u32).to_be_bytes().as_slice());
        for time_range in &time_ranges[..time_ranges_len] {
            buffer.extend_from_slice(time_range.min_ts.to_be_bytes().as_slice());
            buffer.extend_from_slice(time_range.max_ts.to_be_bytes().as_slice());
        }
        time_ranges_len
    }

    pub fn is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    pub async fn add_range(
        &mut self,
        columns: &[(SeriesId, ColumnId)],
        time_range: TimeRange,
        bloom_filter: Option<Arc<BloomFilter>>,
    ) -> TskvResult<()> {
        let mut writer_lock = self.writer.lock().await;
        if writer_lock.is_none() {
            *writer_lock =
                Some(record_file::Writer::open(&self.path, TOMBSTONE_BUFFER_SIZE).await?);
        }
        let writer = writer_lock
            .as_mut()
            .expect("initialized record file writer");

        let mut write_buf = Vec::with_capacity(MAX_RECORD_LEN);
        let mut tomb_tmp: HashMap<(SeriesId, ColumnId), Vec<TimeRange>> =
            HashMap::with_capacity(columns.len());
        for (series_id, column_id) in columns {
            if let Some(ref filter) = bloom_filter {
                if !filter.maybe_contains(series_id.to_be_bytes().as_slice()) {
                    continue;
                }
            }
            let tomb = TombstoneField::One(*series_id, *column_id);
            Self::encode_field_time_ranges(&mut write_buf, &tomb, &[time_range]);
            if write_buf.len() >= MAX_RECORD_LEN {
                write_tombstone_record(writer, &write_buf).await?;
                write_buf.clear();
            }

            tomb_tmp
                .entry((*series_id, *column_id))
                .or_default()
                .push(time_range);
        }
        if !write_buf.is_empty() {
            write_tombstone_record(writer, &write_buf).await?;
        }
        self.cache.write().insert_batch(tomb_tmp);
        Ok(())
    }

    #[cfg(test)]
    pub async fn add_range_all(&mut self, time_range: TimeRange) {
        let mut writer_lock = self.writer.lock().await;
        if writer_lock.is_none() {
            *writer_lock = Some(
                record_file::Writer::open(&self.path, TOMBSTONE_BUFFER_SIZE)
                    .await
                    .unwrap(),
            );
        }
        let writer = writer_lock
            .as_mut()
            .expect("initialized record file writer");

        let mut write_buf = Vec::with_capacity(MAX_RECORD_LEN);
        Self::encode_field_time_ranges(&mut write_buf, &TombstoneField::All, &[time_range]);
        write_tombstone_record(writer, &write_buf).await.unwrap();
        self.cache.write().insert(TombstoneField::All, time_range);
    }

    /// Add a time range to `all_excluded` and then compact tombstones,
    /// return compacted `all_excluded`.
    pub async fn add_range_and_compact_to_tmp(
        &self,
        time_range: TimeRange,
    ) -> TskvResult<TimeRanges> {
        let tmp_path = tombstone_compact_tmp_path(&self.path)?;
        let mut writer = record_file::Writer::open(&tmp_path, TOMBSTONE_BUFFER_SIZE).await?;
        let mut cache = self.cache.write().clone();
        cache.insert(TombstoneField::All, time_range);
        cache.compact();
        trace::info!(
            "Tombstone: Saving compact_tmp tombstone file '{}', all_excluded_ranges: {}",
            tmp_path.display(),
            cache.all_excluded()
        );
        cache.save_to(&mut writer).await?;
        writer.close().await?;

        Ok(cache.all_excluded)
    }

    /// Replace current tombstone file with compact_tmp tombstone file.
    pub async fn replace_with_compact_tmp(&self) -> TskvResult<()> {
        let mut writer_lock = self.writer.lock().await;
        if let Some(w) = writer_lock.as_mut() {
            w.close().await?;
        }
        // Drop the old Writer, reopen in other time.
        *writer_lock = None;

        let tmp_path = tombstone_compact_tmp_path(&self.path)?;
        if LocalFileSystem::try_exists(&tmp_path) {
            debug!(
                "Tombstone: Converting compact_tmp tombstone file '{}' to real tombstone file '{}'",
                tmp_path.display(),
                self.path.display()
            );
            file_utils::rename(tmp_path, &self.path).await?;
            let mut reader = record_file::Reader::open(&self.path).await?;
            let cache = TsmTombstoneCache::load_from(&mut reader, false).await?;
            *self.cache.write() = cache;
        } else {
            debug!("No compact_tmp tombstone file to convert to real tombstone file");
        }
        Ok(())
    }

    pub async fn flush(&self) -> TskvResult<()> {
        if let Some(w) = self.writer.lock().await.as_mut() {
            w.sync().await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub fn overlaps_column_time_range(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> bool {
        self.cache
            .read()
            .overlaps_field_time_range(series_id, column_id, time_range)
    }
    #[cfg(test)]
    pub fn check_all_fields_excluded_time_range(&self, time_range: &TimeRange) -> bool {
        self.cache
            .read()
            .check_all_fields_excluded_time_range(time_range)
    }

    /// Returns all tombstone `TimeRange`s that overlaps the given `TimeRange`.
    /// Returns None if there is nothing to return, or `TimeRange`s is empty.
    pub fn get_column_overlapped_time_ranges(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> Vec<TimeRange> {
        self.cache
            .read()
            .get_column_overlapped_time_ranges(series_id, column_id, time_range)
    }

    pub fn get_all_fields_excluded_time_range(&self, time_range: &TimeRange) -> Vec<TimeRange> {
        self.cache
            .read()
            .get_all_fields_excluded_time_range(time_range)
    }
}

async fn write_tombstone_record(
    writer: &mut record_file::Writer,
    data: &[u8],
) -> TskvResult<usize> {
    writer
        .write_record(
            RecordDataVersion::V1 as u8,
            RecordDataType::TombstoneV2 as u8,
            &[data],
        )
        .await
}

/// Generate pathbuf by tombstone path.
/// - For tombstone file: /tmp/test/000001.tombstone, return /tmp/test/000001.compact.tmp
pub fn tombstone_compact_tmp_path(tombstone_path: &Path) -> TskvResult<PathBuf> {
    let name = tombstone_path.file_name().map(|os_str| {
        let mut s = os_str.to_os_string();
        s.push(".compact.tmp");
        s
    });
    match name {
        Some(name) => {
            let mut p = tombstone_path.to_path_buf();
            p.set_file_name(name);
            Ok(p)
        }
        None => Err(InvalidFileNameSnafu {
            file_name: tombstone_path.display().to_string(),
            message: "invalid tombstone file name".to_string(),
        }
        .build()),
    }
}

#[derive(Clone)]
pub struct TsmTombstoneCache {
    /// The excluded time ranges of each field.
    /// Got data by executing DELETE_FROM_TABLE or DROP_TABLE statements.
    column_excluded: HashMap<(SeriesId, ColumnId), TimeRanges>,
    /// Stores the size of all `TimeRange`s.
    column_excluded_size: usize,

    /// The excluded time range of all fields.
    /// Got data by compacting part of current TSM file with another TSM file.
    all_excluded: TimeRanges,
}

impl Default for TsmTombstoneCache {
    fn default() -> Self {
        Self {
            column_excluded: HashMap::new(),
            column_excluded_size: 0,
            all_excluded: TimeRanges::empty(),
        }
    }
}

impl TsmTombstoneCache {
    #[cfg(test)]
    pub fn with_all_excluded(all_excluded: TimeRange) -> Self {
        Self {
            all_excluded: TimeRanges::new(vec![all_excluded]),
            ..Default::default()
        }
    }

    ///  Append time_range, but if time_range is None, does nothing.
    pub fn insert(&mut self, tombstone_field: TombstoneField, time_range: TimeRange) {
        if time_range.is_none() {
            return;
        }
        match tombstone_field {
            TombstoneField::One(series_id, column_id) => {
                // Adding for ONE field: merge with existing tombstones.
                if self.all_excluded.includes(&time_range) {
                    return;
                }
                self.column_excluded
                    .entry((series_id, column_id))
                    .or_insert_with(TimeRanges::empty)
                    .push(time_range);
                self.column_excluded_size += 1;
            }
            TombstoneField::All => {
                // Adding for ALL: just merge with all_excluded.
                self.all_excluded.push(time_range);
            }
        }
    }

    pub fn insert_batch(&mut self, batch: HashMap<(SeriesId, ColumnId), Vec<TimeRange>>) {
        for (field_id, time_ranges) in batch {
            self.column_excluded_size += time_ranges.len();
            let tr_entry = self
                .column_excluded
                .entry(field_id)
                .or_insert_with(TimeRanges::empty);
            tr_entry.extend_from_slice(&time_ranges);
        }
    }

    pub fn compact(&mut self) {
        // If all compacted time ranges equals to `all_excluded`, remove all fields.
        let mut all_excluded = true;
        let mut fields_excluded_size = 0_usize;
        for time_ranges in self.column_excluded.values_mut() {
            // Compact time ranges of field with all_excluded.
            time_ranges.extend_from_slice(&self.all_excluded.time_ranges().collect::<Vec<_>>());
            fields_excluded_size += time_ranges.len();
            // Check if all compacted time ranges equals to `all_excluded`.
            if all_excluded {
                let mut field_excluded_not_need = true;
                for tr in time_ranges.time_ranges() {
                    if !self.all_excluded.includes(&tr) {
                        field_excluded_not_need = false;
                        break;
                    }
                }
                if !field_excluded_not_need {
                    all_excluded = false;
                }
            }
        }
        if all_excluded {
            self.column_excluded.clear();
            self.column_excluded_size = 0;
        } else {
            self.column_excluded_size = fields_excluded_size;
        }
    }

    pub fn overlaps_field_time_range(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> bool {
        if let Some(time_ranges) = self.column_excluded.get(&(series_id, column_id)) {
            for t in time_ranges.time_ranges() {
                if t.overlaps(time_range) {
                    return true;
                }
            }
        }
        false
    }

    pub fn get_column_overlapped_time_ranges(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> Vec<TimeRange> {
        let mut trs = Vec::new();
        if let Some(time_ranges) = self.column_excluded.get(&(series_id, column_id)) {
            for t in time_ranges.time_ranges() {
                if t.overlaps(time_range) {
                    trs.push(t);
                }
            }
        }

        trs
    }

    pub fn check_all_fields_excluded_time_range(&self, time_range: &TimeRange) -> bool {
        self.all_excluded.includes(time_range)
    }

    pub fn get_all_fields_excluded_time_range(&self, time_range: &TimeRange) -> Vec<TimeRange> {
        let mut trs = Vec::new();
        for all in self.all_excluded.time_ranges() {
            if all.overlaps(time_range) {
                trs.push(all);
            }
        }
        trs
    }

    pub async fn load(path: impl AsRef<Path>) -> TskvResult<Option<Self>> {
        let mut reader = if LocalFileSystem::try_exists(&path) {
            record_file::Reader::open(&path).await?
        } else {
            return Ok(None);
        };

        let cache = Self::load_from(&mut reader, true).await?;
        Ok(Some(cache))
    }

    /// Load tombstones from a record file, if `just_stop_if_error` is true,
    /// errors except EOF only stops the read of remaining data and return.
    pub async fn load_from(
        reader: &mut record_file::Reader,
        just_stop_if_error: bool,
    ) -> TskvResult<Self> {
        let mut column_excluded: HashMap<(SeriesId, ColumnId), TimeRanges> = HashMap::new();
        let mut column_excluded_size = 0_usize;
        let mut all_excluded = TimeRanges::empty();
        let mut buffer: Vec<Tombstone> = Vec::new();
        loop {
            let record = match reader.read_record().await {
                Ok(r) => r,
                Err(TskvError::Eof) => break,
                Err(e) => {
                    if just_stop_if_error {
                        error!("Invalid tombstone record: {e}");
                        break;
                    } else {
                        return Err(e);
                    }
                }
            };

            buffer.clear();
            if record.data_type == RecordDataType::TombstoneV1 as u8 {
                // In version v1, each record only contains one tombstone.
                let (series_id, column_id, time_range) = TsmTombstone::decode_v1(&record.data);
                column_excluded
                    .entry((series_id, column_id))
                    .or_insert_with(TimeRanges::empty)
                    .push(time_range);
            } else {
                // In version v2, each record may contain multiple tombstones.
                match TsmTombstone::decode_v2(&record.data) {
                    Ok(buffer) => {
                        for Tombstone { field, time_ranges } in buffer.iter() {
                            match field {
                                TombstoneField::One(series_id, column_id) => {
                                    column_excluded
                                        .entry((*series_id, *column_id))
                                        .or_insert_with(TimeRanges::empty)
                                        .extend_from_slice(time_ranges.as_slice());
                                    column_excluded_size += 1;
                                }
                                TombstoneField::All => {
                                    all_excluded.extend_from_slice(time_ranges.as_slice())
                                }
                            };
                        }
                    }
                    Err(e) => {
                        error!("Invalid tombstone record({:?}): {e}", record);
                    }
                }
            }
        }

        Ok(Self {
            column_excluded,
            column_excluded_size,
            all_excluded,
        })
    }

    pub async fn save_to(&self, writer: &mut record_file::Writer) -> TskvResult<()> {
        let mut write_buf = Vec::with_capacity(MAX_RECORD_LEN);
        // Save field_excluded tombstones.
        for ((series_id, column_id), time_ranges) in self.column_excluded.iter() {
            let time_ranges = time_ranges.time_ranges().collect::<Vec<_>>();
            let mut index = 0_usize;
            while index < time_ranges.len() {
                let encoded_time_ranges_num = TsmTombstone::encode_field_time_ranges(
                    &mut write_buf,
                    &TombstoneField::One(*series_id, *column_id),
                    &time_ranges[index..],
                );
                index += encoded_time_ranges_num;
                if write_buf.len() >= MAX_RECORD_LEN {
                    write_tombstone_record(writer, &write_buf).await?;
                    write_buf.clear();
                }
            }
        }
        // Save all_excluded tombstone.
        let time_ranges = self.all_excluded.time_ranges().collect::<Vec<_>>();
        let mut index = 0_usize;
        while index < time_ranges.len() {
            let encoded_time_ranges_num = TsmTombstone::encode_field_time_ranges(
                &mut write_buf,
                &TombstoneField::All,
                &time_ranges[index..],
            );
            index += encoded_time_ranges_num;
            if write_buf.len() >= MAX_RECORD_LEN {
                write_tombstone_record(writer, &write_buf).await?;
                write_buf.clear();
            }
        }
        if !write_buf.is_empty() {
            write_tombstone_record(writer, &write_buf).await?;
        }

        Ok(())
    }

    /// Check if there is no field-time_range being excluded.
    pub fn is_empty(&self) -> bool {
        self.column_excluded.is_empty() && self.all_excluded.is_empty()
    }

    /// Immutably borrow `fields_excluded`.
    pub fn fields_excluded(&self) -> &HashMap<(SeriesId, ColumnId), TimeRanges> {
        &self.column_excluded
    }

    pub fn fields_excluded_size(&self) -> usize {
        self.column_excluded_size
    }

    /// Immutably borrow `all_excluded`.
    pub fn all_excluded(&self) -> &TimeRanges {
        &self.all_excluded
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use models::predicate::domain::TimeRange;

    use super::TsmTombstone;
    use crate::file_system::async_filesystem::LocalFileSystem;
    use crate::file_system::FileSystem;

    #[tokio::test]
    async fn test_write_read_1() {
        let dir = PathBuf::from("/tmp/test/tombstone/1".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !LocalFileSystem::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        tombstone
            .add_range(&[(0, 0)], TimeRange::new(0, 0), None)
            .await
            .unwrap();
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps_column_time_range(
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
        if !LocalFileSystem::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        // tsm_tombstone.load().unwrap();
        tombstone
            .add_range(&[(0, 1), (0, 2), (0, 3)], TimeRange::new(1, 100), None)
            .await
            .unwrap();
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps_column_time_range(
            0,
            1,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(tombstone.overlaps_column_time_range(
            0,
            2,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(!tombstone.overlaps_column_time_range(
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
        if !LocalFileSystem::try_exists(&dir) {
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
                    TimeRange::new(i as i64 * 2, i as i64 * 2 + 100),
                    None,
                )
                .await
                .unwrap();
        }
        tombstone.flush().await.unwrap();
        drop(tombstone);

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps_column_time_range(
            0,
            1,
            &TimeRange {
                max_ts: 2,
                min_ts: 99
            }
        ));
        assert!(tombstone.overlaps_column_time_range(
            0,
            2,
            &TimeRange {
                max_ts: 3,
                min_ts: 100
            }
        ));
        assert!(!tombstone.overlaps_column_time_range(
            0,
            3,
            &TimeRange {
                max_ts: 4,
                min_ts: 101
            }
        ));
    }
}
