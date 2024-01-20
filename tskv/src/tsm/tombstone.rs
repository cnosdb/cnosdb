//! # Tombstone file
//!
//! A tombstone file is a [`record_file`].
//!
//! ## Record Data (v1)
//! ```text
//! +------------+---------------+---------------+
//! | 0: 8 bytes | 8: 8 bytes    | 16: 8 bytes   |
//! +------------+---------------+---------------+
//! |  field_id  | min_timestamp | max_timestamp |
//! +------------+---------------+---------------+
//! ```
//!
//! ## Record Data (v2)
//! ```text
//! # field_typ = FIELD_TYPE_ONE(0x00)
//! +-----------------+------------+-----------------+---------------+---------------+----
//! | 0: 1 byte       | 1: 8 bytes | 9: 4 bytes      | 8: 8 bytes    | 16: 8 bytes   | ...
//! +-----------------+------------+-----------------+---------------+---------------+----
//! | field_typ(0x00) |  field_id  | time_ranges_num | min_timestamp | max_timestamp | ...
//! +-----------------+------------+-----------------+---------------+---------------+----
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
use models::FieldId;
use parking_lot::Mutex;
use tokio::sync::Mutex as AsyncMutex;
use trace::{debug, error};
use utils::BloomFilter;

use super::DataBlock;
use crate::file_system::file_manager;
use crate::record_file::{self, RecordDataType};
use crate::{byte_utils, file_utils, Error, Result};

pub const TOMBSTONE_FILE_SUFFIX: &str = "tombstone";
const FOOTER_MAGIC_NUMBER: u32 = u32::from_be_bytes([b'r', b'o', b'm', b'b']);
const FOOTER_MAGIC_NUMBER_LEN: usize = 4;
const RECORD_VERSION_V1: u8 = 1;
const RECORD_VERSION_V2: u8 = 2;
const MAX_RECORD_LEN: usize = 1024 * 128;
const MAX_RECORD_TIME_RANGES_NUM: usize = 1000;
const MAX_TOMBSTONE_LEN: usize = 25;
const TOMBSTONE_HEADER_LEN_ONE: usize = 13; // 1 + 8 + 4
const TOMBSTONE_HEADER_LEN_ALL: usize = 5; // 1 + 4
const FIELD_TYPE_ONE: u8 = 0x00;
const FIELD_TYPE_ALL: u8 = 0x01;

#[derive(Debug, Clone)]
pub struct Tombstone {
    pub field: TombstoneField,
    pub time_ranges: Vec<TimeRange>,
}

impl Tombstone {
    pub fn decode(data: &[u8], buffer: &mut Vec<Self>) -> Result<()> {
        let mut sli: &[u8] = data;
        while !sli.is_empty() {
            if sli[0] == FIELD_TYPE_ALL {
                if sli.len() < TOMBSTONE_HEADER_LEN_ALL {
                    return Err(Error::InvalidTombstone {
                        message: format!(
                            "block length too small for tomb_header_ALL: {}",
                            sli.len()
                        ),
                    });
                }
                let field = TombstoneField::All;
                let time_ranges_num =
                    byte_utils::decode_be_u32(&sli[1..TOMBSTONE_HEADER_LEN_ALL]) as usize;
                let time_ranges =
                    Self::decode_time_ranges(&sli[TOMBSTONE_HEADER_LEN_ALL..], time_ranges_num)?;
                buffer.push(Self { field, time_ranges });
                sli = &sli[TOMBSTONE_HEADER_LEN_ALL + (8 + 8) * time_ranges_num..];
            } else {
                if sli.len() < TOMBSTONE_HEADER_LEN_ONE {
                    return Err(Error::InvalidTombstone {
                        message: format!(
                            "field length too small for tomb_header_FIELD: {}",
                            sli.len()
                        ),
                    });
                }
                let field = TombstoneField::One(byte_utils::decode_be_u64(&sli[1..9]));
                let time_ranges_num =
                    byte_utils::decode_be_u32(&sli[9..TOMBSTONE_HEADER_LEN_ONE]) as usize;
                let time_ranges =
                    Self::decode_time_ranges(&sli[TOMBSTONE_HEADER_LEN_ONE..], time_ranges_num)?;
                buffer.push(Self { field, time_ranges });
                sli = &sli[TOMBSTONE_HEADER_LEN_ONE + (8 + 8) * time_ranges_num..];
            }
        }

        Ok(())
    }

    fn decode_time_ranges(data: &[u8], time_ranges_num: usize) -> Result<Vec<TimeRange>> {
        if data.len() < (8 + 8) * time_ranges_num {
            return Err(Error::InvalidTombstone {
                message: format!(
                    "block length too small for tomb_time_ranges: {}",
                    data.len()
                ),
            });
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
        if let TombstoneField::One(field_id) = field {
            buffer.push(FIELD_TYPE_ONE);
            buffer.extend_from_slice(field_id.to_be_bytes().as_slice());
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

    pub fn decode_legacy_v1(data: &[u8]) -> Result<(FieldId, TimeRange)> {
        if data.len() < 24 {
            return Err(Error::InvalidTombstone {
                message: format!("field length too small: {}", data.len()),
            });
        }
        let field_id = byte_utils::decode_be_u64(&data[0..8]);
        let min_ts = byte_utils::decode_be_i64(&data[8..16]);
        let max_ts = byte_utils::decode_be_i64(&data[16..24]);
        Ok((field_id, TimeRange::new(min_ts, max_ts)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TombstoneField {
    One(FieldId),
    All,
}

/// Tombstones for a tsm file
///
/// - file_name: _%06d.tombstone
/// - header: b"TOMB" 4 bytes
/// - loop begin
/// - - field_type: u8 1 byte
/// - - field_id: u64 8 bytes
/// - - time_range_num: u32 4 bytes
/// - - min: i64 8 bytes
/// - - max: i64 8 bytes
/// - loop end
pub struct TsmTombstone {
    cache: Mutex<TsmTombstoneCache>,

    path: PathBuf,
    /// Async record file writer.
    ///
    /// If you want to use self::writer and self::cache at the same time,
    /// lock writer first then cache.
    writer: Arc<AsyncMutex<Option<record_file::Writer>>>,
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

        let cache = if let Some(r) = reader.as_mut() {
            TsmTombstoneCache::load_from(r, false).await?
        } else {
            TsmTombstoneCache::default()
        };

        Ok(Self {
            cache: Mutex::new(cache),
            path,
            writer: Arc::new(AsyncMutex::new(writer)),
        })
    }

    #[cfg(test)]
    pub async fn with_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let parent = path.parent().expect("a valid tsm/tombstone file path");
        let tsm_id = file_utils::get_tsm_file_id_by_path(path)?;
        Self::open(parent, tsm_id).await
    }

    pub fn is_empty(&self) -> bool {
        self.cache.lock().is_empty()
    }

    pub async fn add_range(
        &self,
        field_ids: &[FieldId],
        time_range: TimeRange,
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

        let mut write_buf = Vec::with_capacity(MAX_RECORD_LEN);
        let mut tomb_tmp: HashMap<u64, Vec<TimeRange>> = HashMap::with_capacity(field_ids.len());
        for field_id in field_ids.iter() {
            if let Some(filter) = bloom_filter.as_ref() {
                if !filter.contains(&field_id.to_be_bytes()) {
                    continue;
                }
            }

            let tomb_field = TombstoneField::One(*field_id);
            Tombstone::encode_field_time_ranges(&mut write_buf, &tomb_field, &[time_range]);
            if write_buf.len() >= MAX_RECORD_LEN {
                write_tombstone_record(writer, &write_buf).await?;
                write_buf.clear();
            }

            tomb_tmp.entry(*field_id).or_default().push(time_range);
        }
        if !write_buf.is_empty() {
            write_tombstone_record(writer, &write_buf).await?;
        }
        self.cache.lock().insert_batch(tomb_tmp);
        Ok(())
    }

    #[cfg(test)]
    pub async fn add_range_legacy_v1(
        &self,
        field_ids: &[FieldId],
        time_range: TimeRange,
        bloom_filter: Option<Arc<BloomFilter>>,
    ) -> Result<()> {
        use crate::record_file::RecordDataVersion;

        let mut writer_lock = self.writer.lock().await;
        if writer_lock.is_none() {
            *writer_lock =
                Some(record_file::Writer::open(&self.path, RecordDataType::Tombstone).await?);
        }
        let writer = writer_lock
            .as_mut()
            .expect("initialized record file writer");

        let mut write_buf = [0_u8; 24];
        let mut tomb_tmp: HashMap<u64, Vec<TimeRange>> = HashMap::with_capacity(field_ids.len());
        for field_id in field_ids.iter() {
            if let Some(filter) = bloom_filter.as_ref() {
                if !filter.contains(&field_id.to_be_bytes()) {
                    continue;
                }
            }
            write_buf[0..8].copy_from_slice((*field_id).to_be_bytes().as_slice());
            write_buf[8..16].copy_from_slice(time_range.min_ts.to_be_bytes().as_slice());
            write_buf[16..24].copy_from_slice(time_range.max_ts.to_be_bytes().as_slice());
            writer
                .write_record(
                    RecordDataVersion::V1 as u8,
                    RecordDataType::Tombstone as u8,
                    &[&write_buf],
                )
                .await?;

            tomb_tmp.entry(*field_id).or_default().push(time_range);
        }
        self.cache.lock().insert_batch(tomb_tmp);
        Ok(())
    }

    /// Add a time range to `all_excluded` and then compact tombstones,
    /// return compacted `all_excluded`.
    pub async fn add_range_and_compact_to_tmp(&self, time_range: TimeRange) -> Result<TimeRanges> {
        let tmp_path = tombstone_compact_tmp_path(&self.path)?;
        let mut writer = record_file::Writer::open(&tmp_path, RecordDataType::Tombstone).await?;
        let mut cache = self.cache.lock().clone();
        cache.insert(TombstoneField::All, time_range);
        cache.compact();
        trace::info!("Saving compact_tmp tombstone file '{}'", tmp_path.display());
        cache.save_to(&mut writer).await?;
        writer.close().await?;

        Ok(cache.all_excluded)
    }

    /// Replace current tombstone file with compact_tmp tombstone file.
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
            file_utils::rename(tmp_path, &self.path).await?;
            let mut reader = record_file::Reader::open(&self.path).await?;
            let cache = TsmTombstoneCache::load_from(&mut reader, false).await?;
            *self.cache.lock() = cache;
        } else {
            debug!("No compact_tmp tombstone file to convert to real tombstone file");
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        if let Some(w) = self.writer.lock().await.as_mut() {
            w.sync().await?;
        }
        Ok(())
    }

    pub(crate) fn fields_excluded_cloned(&self) -> HashMap<FieldId, TimeRanges> {
        self.cache.lock().fields_excluded().clone()
    }

    pub(crate) fn all_excluded_cloned(&self) -> TimeRanges {
        self.cache.lock().all_excluded().clone()
    }

    #[cfg(test)]
    pub fn overlaps_field_time_range(&self, field_id: FieldId, time_range: &TimeRange) -> bool {
        self.cache
            .lock()
            .overlaps_field_time_range(field_id, time_range)
    }

    pub fn check_all_fields_excluded_time_range(&self, time_range: &TimeRange) -> bool {
        self.cache
            .lock()
            .check_all_fields_excluded_time_range(time_range)
    }

    pub fn data_block_exclude_tombstones(&self, field_id: FieldId, data_block: &mut DataBlock) {
        if let Some(block_tr_tuple) = data_block.time_range() {
            let block_tr: &TimeRange = &block_tr_tuple.into();
            let cache = self.cache.lock();

            if cache.all_excluded.max_time_range().overlaps(block_tr) {
                for excluded_tr in cache.all_excluded.time_ranges() {
                    if block_tr.overlaps(excluded_tr) {
                        data_block.exclude(excluded_tr);
                    }
                }
            }

            if let Some(time_ranges) = cache.fields_excluded.get(&field_id) {
                for t in time_ranges.time_ranges() {
                    if t.overlaps(block_tr) {
                        data_block.exclude(t);
                    }
                }
            }
        }
    }
}

async fn write_tombstone_record(writer: &mut record_file::Writer, data: &[u8]) -> Result<usize> {
    writer
        .write_record(RECORD_VERSION_V2, RecordDataType::Tombstone as u8, &[data])
        .await
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

#[derive(Clone)]
pub struct TsmTombstoneCache {
    /// The excluded time ranges of each field.
    /// Got data by executing DELETE_FROM_TABLE or DROP_TABLE statements.
    fields_excluded: HashMap<FieldId, TimeRanges>,
    /// Stores the size of all `TimeRange`s.
    fields_excluded_size: usize,

    /// The excluded time range of all fields.
    /// Got data by compacting part of current TSM file with another TSM file.
    all_excluded: TimeRanges,
}

impl Default for TsmTombstoneCache {
    fn default() -> Self {
        Self {
            fields_excluded: HashMap::new(),
            fields_excluded_size: 0,
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

    pub fn insert(&mut self, tombstone_field: TombstoneField, time_range: TimeRange) {
        match tombstone_field {
            TombstoneField::One(field_id) => {
                // Adding for ONE field: merge with existing tombstones.
                if self.all_excluded.includes(&time_range) {
                    return;
                }
                self.fields_excluded
                    .entry(field_id)
                    .or_insert_with(TimeRanges::empty)
                    .push(time_range);
                self.fields_excluded_size += 1;
            }
            TombstoneField::All => {
                // Adding for ALL: just merge with all_excluded.
                self.all_excluded.push(time_range);
            }
        }
    }

    pub fn insert_batch(&mut self, batch: HashMap<FieldId, Vec<TimeRange>>) {
        for (field_id, time_ranges) in batch {
            self.fields_excluded_size += time_ranges.len();
            let tr_entry = self
                .fields_excluded
                .entry(field_id)
                .or_insert_with(TimeRanges::empty);
            tr_entry.extend_from_slice(&time_ranges);
            tr_entry.compact();
        }
    }

    pub fn compact(&mut self) {
        self.all_excluded.compact();
        // If all compacted time ranges equals to `all_excluded`, remove all fields.
        let mut all_excluded = true;
        let mut fields_excluded_size = 0_usize;
        for time_ranges in self.fields_excluded.values_mut() {
            // Compact time ranges of field with all_excluded.
            time_ranges.extend_from_slice(self.all_excluded.time_ranges());
            time_ranges.compact();
            fields_excluded_size += time_ranges.len();
            // Check if all compacted time ranges equals to `all_excluded`.
            if all_excluded {
                let mut field_excluded_not_need = true;
                for tr in time_ranges.time_ranges() {
                    if !self.all_excluded.includes(tr) {
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
            self.fields_excluded.clear();
            self.fields_excluded_size = 0;
        } else {
            self.fields_excluded_size = fields_excluded_size;
        }
    }

    #[cfg(test)]
    pub fn overlaps_field_time_range(&self, field_id: FieldId, time_range: &TimeRange) -> bool {
        if self.all_excluded.includes(time_range) {
            return true;
        }
        if let Some(time_ranges) = self.fields_excluded.get(&field_id) {
            for t in time_ranges.time_ranges() {
                if t.overlaps(time_range) {
                    return true;
                }
            }
        }
        false
    }

    pub fn check_all_fields_excluded_time_range(&self, time_range: &TimeRange) -> bool {
        self.all_excluded.includes(time_range)
    }

    pub async fn load(path: impl AsRef<Path>) -> Result<Option<Self>> {
        let mut reader = if file_manager::try_exists(&path) {
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
    ) -> Result<Self> {
        let mut fields_excluded: HashMap<FieldId, TimeRanges> = HashMap::new();
        let mut fields_excluded_size = 0_usize;
        let mut all_excluded = TimeRanges::empty();
        let mut buffer: Vec<Tombstone> = Vec::new();
        loop {
            let record = match reader.read_record().await {
                Ok(r) => r,
                Err(Error::Eof) => break,
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
            if record.data_version == RECORD_VERSION_V1 {
                // In version v1, each record only contains one tombstone.
                let (field_id, time_range) = Tombstone::decode_legacy_v1(&record.data)?;
                fields_excluded
                    .entry(field_id)
                    .or_insert_with(TimeRanges::empty)
                    .push(time_range);
            } else {
                // In version v2, each record may contain multiple tombstones.
                match Tombstone::decode(&record.data, &mut buffer) {
                    Ok(_) => {
                        for Tombstone { field, time_ranges } in buffer.iter() {
                            match field {
                                TombstoneField::One(field_id) => {
                                    fields_excluded
                                        .entry(*field_id)
                                        .or_insert_with(TimeRanges::empty)
                                        .extend_from_slice(time_ranges.as_slice());
                                    fields_excluded_size += 1;
                                }
                                TombstoneField::All => {
                                    all_excluded.extend_from_slice(time_ranges.as_slice())
                                }
                            };
                        }
                    }
                    Err(e) => {
                        error!("Invalid tombstone record({record}): {e}");
                    }
                }
            }
        }

        Ok(Self {
            fields_excluded,
            fields_excluded_size,
            all_excluded,
        })
    }

    pub async fn save_to(&self, writer: &mut record_file::Writer) -> Result<()> {
        let mut write_buf = Vec::with_capacity(MAX_RECORD_LEN);
        // Save field_excluded tombstones.
        for (field_id, time_ranges) in self.fields_excluded.iter() {
            let mut time_ranges = time_ranges.time_ranges();
            while !time_ranges.is_empty() {
                let encoded_time_ranges_num = Tombstone::encode_field_time_ranges(
                    &mut write_buf,
                    &TombstoneField::One(*field_id),
                    time_ranges,
                );
                time_ranges = &time_ranges[encoded_time_ranges_num..];
                if write_buf.len() >= MAX_RECORD_LEN {
                    write_tombstone_record(writer, &write_buf).await?;
                    write_buf.clear();
                }
            }
        }
        // Save all_excluded tombstone.
        let mut time_ranges = self.all_excluded.time_ranges();
        while !time_ranges.is_empty() {
            let encoded_time_ranges_num = Tombstone::encode_field_time_ranges(
                &mut write_buf,
                &TombstoneField::All,
                time_ranges,
            );
            time_ranges = &time_ranges[encoded_time_ranges_num..];
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
        self.fields_excluded.is_empty() && self.all_excluded.is_empty()
    }

    /// Immutably borrow `fields_excluded`.
    pub fn fields_excluded(&self) -> &HashMap<FieldId, TimeRanges> {
        &self.fields_excluded
    }

    pub fn fields_excluded_size(&self) -> usize {
        self.fields_excluded_size
    }

    /// Immutably borrow `all_excluded`.
    pub fn all_excluded(&self) -> &TimeRanges {
        &self.all_excluded
    }
}

#[cfg(test)]
pub mod test {
    use std::path::{Path, PathBuf};

    use models::predicate::domain::TimeRanges;

    use super::{TombstoneField, TsmTombstone, TsmTombstoneCache};
    use crate::file_system::file_manager;
    use crate::record_file;

    pub async fn write_to_tsm_tombstone(path: impl AsRef<Path>, data: &TsmTombstoneCache) {
        let mut writer = record_file::Writer::open(path, record_file::RecordDataType::Tombstone)
            .await
            .unwrap();
        data.save_to(&mut writer).await.unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_write() {
        let dir = PathBuf::from("/tmp/test/tombstone/read_write".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        {
            let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
            tombstone
                .add_range(&[1, 2, 3], (1, 100).into(), None)
                .await
                .unwrap();
            tombstone.flush().await.unwrap();

            assert_eq!(tombstone.all_excluded_cloned(), TimeRanges::empty());
            let fields_excluded = tombstone.fields_excluded_cloned();
            assert_eq!(
                fields_excluded.get(&1),
                Some(&TimeRanges::new(vec![(1, 100).into()]))
            );
            assert_eq!(
                fields_excluded.get(&2),
                Some(&TimeRanges::new(vec![(1, 100).into()]))
            );
            assert_eq!(
                fields_excluded.get(&2),
                Some(&TimeRanges::new(vec![(1, 100).into()]))
            );
            assert!(tombstone.overlaps_field_time_range(1, &(2, 99).into()));
            assert!(tombstone.overlaps_field_time_range(2, &(1, 100).into()));
            assert!(!tombstone.overlaps_field_time_range(3, &(101, 103).into()));
            assert!(tombstone.overlaps_field_time_range(1, &(0, 101).into()));
            assert!(tombstone.overlaps_field_time_range(2, &(0, 1).into()));
            assert!(tombstone.overlaps_field_time_range(3, &(100, 101).into()));
            assert!(!tombstone.overlaps_field_time_range(1, &(-1, 0).into()));
            assert!(!tombstone.overlaps_field_time_range(2, &(101, 102).into()));
        }

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps_field_time_range(1, &(2, 99).into()));
        assert!(tombstone.overlaps_field_time_range(2, &(1, 100).into()));
        assert!(!tombstone.overlaps_field_time_range(3, &(101, 103).into()));
        assert!(tombstone.overlaps_field_time_range(1, &(0, 101).into()));
        assert!(tombstone.overlaps_field_time_range(2, &(0, 1).into()));
        assert!(tombstone.overlaps_field_time_range(3, &(100, 101).into()));
        assert!(!tombstone.overlaps_field_time_range(1, &(-1, 0).into()));
        assert!(!tombstone.overlaps_field_time_range(2, &(101, 102).into()));
    }

    #[tokio::test]
    async fn test_read_write_legacy_v1() {
        let dir = PathBuf::from("/tmp/test/tombstone/read_write_legacy_v1".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        {
            let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
            tombstone
                .add_range_legacy_v1(&[1, 2, 3], (1, 100).into(), None)
                .await
                .unwrap();
            tombstone.flush().await.unwrap();

            assert_eq!(tombstone.all_excluded_cloned(), TimeRanges::empty());
            let fields_excluded = tombstone.fields_excluded_cloned();
            assert_eq!(
                fields_excluded.get(&1),
                Some(&TimeRanges::new(vec![(1, 100).into()]))
            );
            assert_eq!(
                fields_excluded.get(&2),
                Some(&TimeRanges::new(vec![(1, 100).into()]))
            );
            assert_eq!(
                fields_excluded.get(&2),
                Some(&TimeRanges::new(vec![(1, 100).into()]))
            );
            assert!(tombstone.overlaps_field_time_range(1, &(2, 99).into()));
            assert!(tombstone.overlaps_field_time_range(2, &(1, 100).into()));
            assert!(!tombstone.overlaps_field_time_range(3, &(101, 103).into()));
            assert!(tombstone.overlaps_field_time_range(1, &(0, 101).into()));
            assert!(tombstone.overlaps_field_time_range(2, &(0, 1).into()));
            assert!(tombstone.overlaps_field_time_range(3, &(100, 101).into()));
            assert!(!tombstone.overlaps_field_time_range(1, &(-1, 0).into()));
            assert!(!tombstone.overlaps_field_time_range(2, &(101, 102).into()));
        }

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps_field_time_range(1, &(2, 99).into()));
        assert!(tombstone.overlaps_field_time_range(2, &(1, 100).into()));
        assert!(!tombstone.overlaps_field_time_range(3, &(101, 103).into()));
        assert!(tombstone.overlaps_field_time_range(1, &(0, 101).into()));
        assert!(tombstone.overlaps_field_time_range(2, &(0, 1).into()));
        assert!(tombstone.overlaps_field_time_range(3, &(100, 101).into()));
        assert!(!tombstone.overlaps_field_time_range(1, &(-1, 0).into()));
        assert!(!tombstone.overlaps_field_time_range(2, &(101, 102).into()));
    }

    #[tokio::test]
    async fn test_compact() {
        let dir = PathBuf::from("/tmp/test/tombstone/test_compact".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        {
            let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
            tombstone
                .add_range(&[1, 2, 3], (1, 100).into(), None)
                .await
                .unwrap();
            tombstone.flush().await.unwrap();
            tombstone
                .add_range_and_compact_to_tmp((101, 200).into())
                .await
                .unwrap();
            tombstone.replace_with_compact_tmp().await.unwrap();

            assert!(tombstone.overlaps_field_time_range(1, &(0, 1).into()));
            assert!(tombstone.overlaps_field_time_range(2, &(100, 101).into()));
            assert!(tombstone.overlaps_field_time_range(3, &(102, 103).into()));
            assert!(tombstone.overlaps_field_time_range(1, &(1, 200).into()));
            assert!(tombstone.overlaps_field_time_range(2, &(199, 200).into()));
            assert!(tombstone.overlaps_field_time_range(3, &(200, 201).into()));
            assert!(!tombstone.overlaps_field_time_range(1, &(-1, 0).into()));
            assert!(!tombstone.overlaps_field_time_range(2, &(201, 202).into()));

            assert!(tombstone.check_all_fields_excluded_time_range(&(101, 200).into()));
            assert!(tombstone.check_all_fields_excluded_time_range(&(102, 199).into()));
            assert!(!tombstone.check_all_fields_excluded_time_range(&(99, 102).into()));
            assert!(!tombstone.check_all_fields_excluded_time_range(&(-1, 0).into()));
            assert!(!tombstone.check_all_fields_excluded_time_range(&(201, 202).into()));
        }

        let tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        assert!(tombstone.overlaps_field_time_range(1, &(0, 1).into()));
        assert!(tombstone.overlaps_field_time_range(2, &(100, 101).into()));
        assert!(tombstone.overlaps_field_time_range(3, &(102, 103).into()));
        assert!(tombstone.overlaps_field_time_range(1, &(1, 200).into()));
        assert!(tombstone.overlaps_field_time_range(2, &(199, 200).into()));
        assert!(tombstone.overlaps_field_time_range(3, &(200, 201).into()));
        assert!(!tombstone.overlaps_field_time_range(1, &(-1, 0).into()));
        assert!(!tombstone.overlaps_field_time_range(2, &(201, 202).into()));

        assert!(tombstone.check_all_fields_excluded_time_range(&(101, 200).into()));
        assert!(tombstone.check_all_fields_excluded_time_range(&(102, 199).into()));
        assert!(!tombstone.check_all_fields_excluded_time_range(&(99, 102).into()));
        assert!(!tombstone.check_all_fields_excluded_time_range(&(-1, 0).into()));
        assert!(!tombstone.check_all_fields_excluded_time_range(&(201, 202).into()));
    }

    #[test]
    fn test_tombstone_cache() {
        let mut cache = TsmTombstoneCache::default();
        cache.insert(TombstoneField::All, (3, 4).into());
        for (field, time_range) in [
            (TombstoneField::One(1), (1, 2)),
            (TombstoneField::One(2), (10, 20)),
            (TombstoneField::One(1), (10, 20)),
            (TombstoneField::One(2), (100, 200)),
            (TombstoneField::One(1), (100, 200)),
            (TombstoneField::One(2), (1000, 2000)),
        ] {
            cache.insert(field, time_range.into());
        }
        cache.insert(TombstoneField::All, (3000, 4000).into());

        assert_eq!(cache.fields_excluded_size(), 6);
        assert_eq!(
            cache.fields_excluded.get(&1),
            Some(&TimeRanges::new(vec![
                (1, 2).into(),
                (10, 20).into(),
                (100, 200).into(),
            ]))
        );
        assert_eq!(
            cache.fields_excluded.get(&2),
            Some(&TimeRanges::new(vec![
                (10, 20).into(),
                (100, 200).into(),
                (1000, 2000).into(),
            ]))
        );
        assert_eq!(
            cache.all_excluded(),
            &TimeRanges::new(vec![(3, 4).into(), (3000, 4000).into()])
        );
    }
}
