use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use arrow_array::RecordBatch;
use cache::{AsyncCache, ShardedAsyncCache};
use memory_pool::MemoryPoolRef;
use metrics::gauge::U64Gauge;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeStatus;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::database_schema::{split_owner, DatabaseConfig};
use models::schema::tskv_table_schema::TableColumn;
use models::{ColumnId, FieldId, SeriesId, SeriesKey, Timestamp};
use parking_lot::RwLock;
use snafu::ResultExt;
use tokio::sync::RwLock as TokioRwLock;
use tokio::time::Instant;
use trace::{debug, error, info};
use utils::BloomFilter;

use crate::error::{CommonSnafu, IndexErrSnafu, TskvResult};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::file_utils::{make_delta_file, make_tsm_file};
use crate::index::ts_index::TSIndex;
use crate::kv_option::StorageOptions;
use crate::memcache::{MemCache, MemCacheStatistics, RowGroup};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tsm::page::PageMeta;
use crate::tsm::reader::TsmReader;
use crate::tsm::{ColumnGroupID, TsmTombstone};
use crate::{tsm, ColumnFileId, LevelId, Options, TseriesFamilyId};

#[derive(Debug)]
pub struct ColumnFile {
    file_id: ColumnFileId,
    level: LevelId,
    is_delta: bool,
    time_range: TimeRange,
    size: u64,
    series_id_filter: TokioRwLock<Option<Arc<BloomFilter>>>,
    deleted: AtomicBool,
    compacting: AtomicBool,

    path: PathBuf,
    tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TsmReader>>>,
}

impl ColumnFile {
    pub fn with_compact_data(
        meta: &CompactMeta,
        path: impl AsRef<Path>,
        series_id_filter: TokioRwLock<Option<Arc<BloomFilter>>>,
        tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TsmReader>>>,
    ) -> Self {
        Self {
            file_id: meta.file_id,
            level: meta.level,
            is_delta: meta.is_delta,
            time_range: TimeRange::new(meta.min_ts, meta.max_ts),
            size: meta.file_size,
            series_id_filter,
            deleted: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            path: path.as_ref().into(),
            tsm_reader_cache,
        }
    }

    pub fn file_id(&self) -> ColumnFileId {
        self.file_id
    }

    pub fn level(&self) -> LevelId {
        self.level
    }

    pub fn is_delta(&self) -> bool {
        self.is_delta
    }

    pub fn time_range(&self) -> &TimeRange {
        &self.time_range
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn file_path(&self) -> &PathBuf {
        &self.path
    }

    pub fn tombstone_path(&self) -> PathBuf {
        let mut path = self.path.clone();
        path.set_extension(tsm::TOMBSTONE_FILE_SUFFIX);
        path
    }

    pub fn overlap(&self, time_range: &TimeRange) -> bool {
        self.time_range.overlaps(time_range)
    }

    pub async fn maybe_contains_series_id(&self, series_id: SeriesId) -> TskvResult<bool> {
        let bloom_filter = self.load_bloom_filter().await?;
        let res = bloom_filter.maybe_contains(&series_id.to_be_bytes());
        Ok(res)
    }

    async fn load_bloom_filter(&self) -> TskvResult<Arc<BloomFilter>> {
        {
            if let Some(filter) = self.series_id_filter.read().await.as_ref() {
                return Ok(filter.clone());
            }
        }
        let mut filter_w = self.series_id_filter.write().await;
        if let Some(filter) = filter_w.as_ref() {
            return Ok(filter.clone());
        }
        let bloom_filter = if let Some(tsm_reader_cache) = self.tsm_reader_cache.upgrade() {
            let reader = match tsm_reader_cache
                .get(&format!("{}", self.path.display()))
                .await
            {
                Some(r) => r,
                None => {
                    let reader = TsmReader::open(&self.path).await?;
                    let reader = Arc::new(reader);
                    tsm_reader_cache
                        .insert(self.path.display().to_string(), reader.clone())
                        .await;
                    reader
                }
            };
            reader.footer().series().bloom_filter().clone()
        } else {
            TsmReader::open(&self.path)
                .await?
                .footer()
                .series()
                .bloom_filter()
                .clone()
        };
        let bloom_filter = Arc::new(bloom_filter);
        filter_w.replace(bloom_filter.clone());
        Ok(bloom_filter)
    }

    pub async fn contains_any_series_id(&self, series_ids: &[SeriesId]) -> TskvResult<bool> {
        for series_id in series_ids {
            if self.maybe_contains_series_id(*series_id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn contains_any_field_id(&self, _series_ids: &[FieldId]) -> bool {
        unimplemented!("contains_any_field_id")
    }

    pub async fn add_tombstone(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> TskvResult<()> {
        let dir = self.path.parent().expect("file has parent");
        // TODO flock tombstone file.
        let mut tombstone = TsmTombstone::open(dir, self.file_id).await?;
        tombstone
            .add_range(&[(series_id, column_id)], time_range)
            .await?;
        tombstone.flush().await?;
        Ok(())
    }
}

impl ColumnFile {
    pub fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::Acquire)
    }

    pub fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::Release);
    }

    pub fn is_compacting(&self) -> bool {
        self.compacting.load(Ordering::Acquire)
    }

    pub fn mark_compacting(&self) -> bool {
        self.compacting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn unmark_compacting(&self) {
        self.compacting.store(false, Ordering::Release);
    }
}

impl Drop for ColumnFile {
    fn drop(&mut self) {
        debug!(
            "Removing tsm file {} and it's tombstone if exists.",
            self.file_id
        );
        if self.is_deleted() {
            let path = self.file_path();
            if let Some(cache) = self.tsm_reader_cache.upgrade() {
                let k = format!("{}", path.display());
                tokio::spawn(async move {
                    cache.remove(&k).await;
                });
            }
            if let Err(e) = std::fs::remove_file(path) {
                error!(
                    "Failed to remove tsm file {} at '{}': {e}",
                    self.file_id,
                    path.display()
                );
            } else {
                info!("Removed tsm file {} at '{}", self.file_id, path.display());
            }

            let tombstone_path = self.tombstone_path();
            if LocalFileSystem::try_exists(&tombstone_path) {
                if let Err(e) = std::fs::remove_file(&tombstone_path) {
                    error!(
                        "Failed to remove tsm tombstone '{}': {e}",
                        tombstone_path.display()
                    );
                } else {
                    info!("Removed tsm tombstone '{}", tombstone_path.display());
                }
            }
        }
    }
}

#[cfg(test)]
impl ColumnFile {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_id: ColumnFileId,
        level: LevelId,
        time_range: TimeRange,
        size: u64,
        is_delta: bool,
        path: impl AsRef<Path>,
    ) -> Self {
        Self {
            file_id,
            level,
            is_delta,
            time_range,
            size,
            series_id_filter: TokioRwLock::new(Some(Arc::new(BloomFilter::default()))),
            deleted: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            path: path.as_ref().into(),
            tsm_reader_cache: Weak::new(),
        }
    }

    pub fn set_series_id_filter(
        &mut self,
        series_id_filter: TokioRwLock<Option<Arc<BloomFilter>>>,
    ) {
        self.series_id_filter = series_id_filter;
    }
}

#[derive(Debug, Clone)]
pub struct LevelInfo {
    /// the time_range of column file is overlap in L0,
    /// the time_range of column file is not overlap in L0,
    pub files: Vec<Arc<ColumnFile>>,
    pub database: Arc<String>,
    pub tsf_id: u32,
    pub storage_opt: Arc<StorageOptions>,
    pub level: u32,
    pub cur_size: u64,
    pub max_size: u64,
    pub time_range: TimeRange,
}

impl LevelInfo {
    pub fn init(
        database: Arc<String>,
        level: u32,
        tsf_id: u32,
        storage_opt: Arc<StorageOptions>,
    ) -> Self {
        let max_size = storage_opt.level_max_file_size(level);
        Self {
            files: Vec::new(),
            database,
            tsf_id,
            storage_opt,
            level,
            cur_size: 0,
            max_size,
            time_range: TimeRange {
                min_ts: Timestamp::MAX,
                max_ts: Timestamp::MIN,
            },
        }
    }

    pub fn init_levels(
        database: Arc<String>,
        tsf_id: u32,
        storage_opt: Arc<StorageOptions>,
    ) -> [LevelInfo; 5] {
        [
            Self::init(database.clone(), 0, tsf_id, storage_opt.clone()),
            Self::init(database.clone(), 1, tsf_id, storage_opt.clone()),
            Self::init(database.clone(), 2, tsf_id, storage_opt.clone()),
            Self::init(database.clone(), 3, tsf_id, storage_opt.clone()),
            Self::init(database, 4, tsf_id, storage_opt),
        ]
    }

    pub fn push_compact_meta(
        &mut self,
        compact_meta: &CompactMeta,
        series_filter: TokioRwLock<Option<Arc<BloomFilter>>>,
        tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TsmReader>>>,
    ) {
        let file_path = if compact_meta.is_delta {
            let base_dir = self.storage_opt.delta_dir(&self.database, self.tsf_id);
            make_delta_file(base_dir, compact_meta.file_id)
        } else {
            let base_dir = self.storage_opt.tsm_dir(&self.database, self.tsf_id);
            make_tsm_file(base_dir, compact_meta.file_id)
        };
        self.files.push(Arc::new(ColumnFile::with_compact_data(
            compact_meta,
            file_path,
            series_filter,
            tsm_reader_cache,
        )));
        self.tsf_id = compact_meta.tsf_id;
        self.cur_size += compact_meta.file_size;
        self.time_range.max_ts = self.time_range.max_ts.max(compact_meta.max_ts);
        self.time_range.min_ts = self.time_range.min_ts.min(compact_meta.min_ts);

        self.sort_file_asc();
    }

    pub fn push_column_file(&mut self, file: Arc<ColumnFile>) {
        self.cur_size += file.size;
        self.time_range.max_ts = self.time_range.max_ts.max(file.time_range.max_ts);
        self.time_range.min_ts = self.time_range.min_ts.min(file.time_range.min_ts);
        self.files.push(file);

        self.sort_file_asc();
    }

    /// Update time_range by a scan with files.
    /// If files is empty, time_range will be (i64::MAX, i64::MIN).
    pub(crate) fn update_time_range(&mut self) {
        let mut min_ts = Timestamp::MAX;
        let mut max_ts = Timestamp::MIN;
        for f in self.files.iter() {
            min_ts = min_ts.min(f.time_range.min_ts);
            max_ts = max_ts.max(f.time_range.max_ts);
        }
        self.time_range = TimeRange::new(min_ts, max_ts);
    }

    pub fn sort_file_asc(&mut self) {
        self.files
            .sort_by(|a, b| a.file_id.partial_cmp(&b.file_id).unwrap());
    }

    pub fn disk_storage(&self) -> u64 {
        self.files.iter().map(|f| f.size).sum()
    }

    pub fn level(&self) -> u32 {
        self.level
    }

    pub async fn overlaps_column_files(
        &self,
        time_ranges: &TimeRanges,
        series_id: SeriesId,
    ) -> TskvResult<Vec<Arc<ColumnFile>>> {
        let mut res = Vec::new();
        for file in self.files.iter() {
            if time_ranges.overlaps(file.time_range())
                && file.maybe_contains_series_id(series_id).await?
            {
                res.push(file.clone());
            }
        }
        res.sort_by_key(|f| *f.time_range());
        Ok(res)
    }
}

#[derive(Debug)]
pub struct Version {
    ts_family_id: TseriesFamilyId,
    tenant_database: Arc<String>,
    storage_opt: Arc<StorageOptions>,
    /// The max seq_no of write batch in wal flushed to column file.
    last_seq: u64,
    /// The max timestamp of write batch in wal flushed to column file.
    max_level_ts: i64,
    levels_info: [LevelInfo; 5],
    tsm_reader_cache: Arc<ShardedAsyncCache<String, Arc<TsmReader>>>,
}

impl Version {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ts_family_id: TseriesFamilyId,
        tenant_database: Arc<String>,
        storage_opt: Arc<StorageOptions>,
        last_seq: u64,
        levels_info: [LevelInfo; 5],
        max_level_ts: i64,
        tsm_reader_cache: Arc<ShardedAsyncCache<String, Arc<TsmReader>>>,
    ) -> Self {
        Self {
            ts_family_id,
            tenant_database,
            storage_opt,
            last_seq,
            max_level_ts,
            levels_info,
            tsm_reader_cache,
        }
    }

    /// Creates new Version using current Version and `VersionEdit`s.
    pub fn copy_apply_version_edits(
        &self,
        ve: VersionEdit,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) -> Version {
        let mut added_files: Vec<Vec<CompactMeta>> = vec![vec![]; 5];
        let mut deleted_files: Vec<HashSet<ColumnFileId>> = vec![HashSet::new(); 5];
        if !ve.add_files.is_empty() {
            ve.add_files.into_iter().for_each(|f| {
                added_files[f.level as usize].push(f);
            });
        }
        if !ve.del_files.is_empty() {
            ve.del_files.into_iter().for_each(|f| {
                deleted_files[f.level as usize].insert(f.file_id);
            });
        }

        let mut new_levels = LevelInfo::init_levels(
            self.tenant_database.clone(),
            self.ts_family_id,
            self.storage_opt.clone(),
        );
        let weak_tsm_reader_cache = Arc::downgrade(&self.tsm_reader_cache);
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if deleted_files[file.level as usize].contains(&file.file_id) {
                    file.mark_deleted();
                    continue;
                }
                new_levels[level.level as usize].push_column_file(file.clone());
            }
            for file in added_files[level.level as usize].iter() {
                let series_filter = file_metas.remove(&file.file_id).unwrap_or_else(|| {
                    error!("missing bloom filter for file_id: {}", file.file_id);
                    Arc::new(BloomFilter::default())
                });
                new_levels[level.level as usize].push_compact_meta(
                    file,
                    TokioRwLock::new(Some(series_filter)),
                    weak_tsm_reader_cache.clone(),
                );
            }
            new_levels[level.level as usize].update_time_range();
        }

        let mut new_version = Self {
            last_seq: ve.seq_no,
            ts_family_id: self.ts_family_id,
            tenant_database: self.tenant_database.clone(),
            storage_opt: self.storage_opt.clone(),
            max_level_ts: self.max_level_ts,
            levels_info: new_levels,
            tsm_reader_cache: self.tsm_reader_cache.clone(),
        };
        new_version.update_max_level_ts();
        new_version
    }

    fn update_max_level_ts(&mut self) {
        if self.levels_info.is_empty() {
            return;
        }
        let mut max_ts = Timestamp::MIN;
        for level in self.levels_info.iter() {
            if level.files.is_empty() {
                continue;
            }
            for file in level.files.iter() {
                max_ts = file.time_range.max_ts.max(max_ts);
            }
        }

        self.max_level_ts = max_ts;
    }

    pub fn tf_id(&self) -> TseriesFamilyId {
        self.ts_family_id
    }

    pub fn tenant_database(&self) -> Arc<String> {
        self.tenant_database.clone()
    }

    pub fn levels_info(&self) -> &[LevelInfo; 5] {
        &self.levels_info
    }

    pub fn storage_opt(&self) -> Arc<StorageOptions> {
        self.storage_opt.clone()
    }

    // todo:
    pub fn get_ts_overlap(&self, _level: u32, _ts_min: i64, _ts_max: i64) -> Vec<Arc<ColumnFile>> {
        vec![]
    }

    pub async fn get_tsm_reader(&self, path: impl AsRef<Path>) -> TskvResult<Arc<TsmReader>> {
        let path = path.as_ref().display().to_string();
        let tsm_reader = match self.tsm_reader_cache.get(&path).await {
            Some(val) => val,
            None => match self.tsm_reader_cache.get(&path).await {
                Some(val) => val,
                None => {
                    let tsm_reader = Arc::new(TsmReader::open(&path).await?);
                    self.tsm_reader_cache.insert(path, tsm_reader.clone()).await;
                    tsm_reader
                }
            },
        };
        Ok(tsm_reader)
    }

    pub fn max_level_ts(&self) -> i64 {
        self.max_level_ts
    }

    pub fn tsm_reader_cache(&self) -> &Arc<ShardedAsyncCache<String, Arc<TsmReader>>> {
        &self.tsm_reader_cache
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_predicate: TimeRange,
    ) -> BTreeMap<ColumnFileId, BTreeMap<SeriesId, Vec<(ColumnGroupID, Vec<PageMeta>)>>> {
        let mut result = BTreeMap::new();
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if file.is_deleted() || !file.overlap(&time_predicate) {
                    continue;
                }
                let reader = self.get_tsm_reader(file.file_path()).await.unwrap();
                let fid = reader.file_id();
                let sts = reader.statistics(series_ids, time_predicate).await.unwrap();
                result.insert(fid, sts);
            }
        }
        result
    }

    #[cfg(test)]
    pub fn levels_info_mut(&mut self) -> &mut [LevelInfo; 5] {
        &mut self.levels_info
    }

    #[cfg(test)]
    pub fn inner(&self) -> Self {
        Self {
            ts_family_id: self.ts_family_id,
            tenant_database: self.tenant_database.clone(),
            storage_opt: self.storage_opt.clone(),
            last_seq: self.last_seq,
            max_level_ts: self.max_level_ts,
            levels_info: self.levels_info.clone(),
            tsm_reader_cache: self.tsm_reader_cache.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheGroup {
    pub mut_cache: Arc<RwLock<MemCache>>,
    pub immut_cache: Vec<Arc<RwLock<MemCache>>>,
}

impl CacheGroup {
    pub fn read_series_timestamps(
        &self,
        series_ids: &[SeriesId],
        time_ranges: &TimeRanges,
        mut handle_data: impl FnMut(Timestamp),
    ) {
        self.immut_cache.iter().for_each(|m| {
            m.read()
                .read_series_timestamps(series_ids, time_ranges, &mut handle_data);
        });

        self.mut_cache
            .read()
            .read_series_timestamps(series_ids, time_ranges, &mut handle_data);
    }
    /// todo：原来的实现里面 memcache中的数据被copy了出来，在cache中命中的数据较多且查询的并发量大的时候，会引发oom的问题。
    /// 内存结构变成一种按照时间排序的结构，查询的时候就返回引用，支持 stream 迭代。
    pub fn stream_read(
        _series_ids: &[SeriesId],
        _project: &[usize],
        _time_predicate: impl FnMut(Timestamp) -> bool,
    ) -> Option<RecordBatch> {
        None
    }

    pub fn cache_statistics(
        &self,
        series_ids: &[SeriesId],
        time_predicate: TimeRange,
    ) -> BTreeMap<u64, MemCacheStatistics> {
        let mut result = BTreeMap::new();
        let sts = self.mut_cache.read().statistics(series_ids, time_predicate);
        result.insert(sts.seq_no(), sts);
        self.immut_cache.iter().for_each(|m| {
            let sts = m.read().statistics(series_ids, time_predicate);
            result.insert(sts.seq_no(), sts);
        });
        result
    }
}

#[derive(Debug)]
pub struct SuperVersion {
    pub ts_family_id: u32,
    pub caches: CacheGroup,
    pub version: Arc<Version>,
    pub version_number: u64,
}

impl SuperVersion {
    pub fn new(
        ts_family_id: u32,
        caches: CacheGroup,
        version: Arc<Version>,
        version_number: u64,
    ) -> Self {
        Self {
            ts_family_id,
            caches,
            version,
            version_number,
        }
    }

    pub fn column_files(&self, time_ranges: &TimeRanges) -> Vec<Arc<ColumnFile>> {
        let mut files = Vec::new();

        for lv in self.version.levels_info.iter() {
            if !time_ranges.overlaps(&lv.time_range) {
                continue;
            }
            for cf in lv.files.iter() {
                if time_ranges.overlaps(&cf.time_range) {
                    files.push(cf.clone());
                }
            }
        }
        files
    }

    pub async fn column_files_by_sid_and_time(
        &self,
        sids: &[SeriesId],
        time_ranges: &TimeRanges,
    ) -> TskvResult<Vec<Arc<ColumnFile>>> {
        let mut files = Vec::new();

        for lv in self.version.levels_info.iter() {
            if !time_ranges.overlaps(&lv.time_range) {
                continue;
            }
            for cf in lv.files.iter() {
                if time_ranges.overlaps(&cf.time_range) && cf.contains_any_series_id(sids).await? {
                    files.push(cf.clone());
                }
            }
        }
        Ok(files)
    }

    pub fn cache_group(&self) -> &CacheGroup {
        &self.caches
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_predicate: TimeRange,
    ) -> (
        BTreeMap<u64, MemCacheStatistics>,
        BTreeMap<ColumnFileId, BTreeMap<SeriesId, Vec<(ColumnGroupID, Vec<PageMeta>)>>>,
    ) {
        let cache = self.caches.cache_statistics(series_ids, time_predicate);
        let sts = self.version.statistics(series_ids, time_predicate).await;
        (cache, sts)
    }

    pub async fn add_tombstone(
        &self,
        series_ids: &[SeriesId],
        column_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> TskvResult<()> {
        let column_files = self
            .column_files_by_sid_and_time(series_ids, &TimeRanges::new(vec![*time_range]))
            .await?;
        for sid in series_ids {
            for column_file in column_files.iter() {
                if column_file.maybe_contains_series_id(*sid).await? {
                    for column_id in column_ids {
                        column_file
                            .add_tombstone(*sid, *column_id, time_range)
                            .await?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct TsfMetrics {
    vnode_disk_storage: U64Gauge,
    vnode_cache_size: U64Gauge,
}

impl TsfMetrics {
    pub fn new(register: &MetricsRegister, owner: &str, vnode_id: u64) -> Self {
        let (tenant, db) = split_owner(owner);
        let metric = register.metric::<U64Gauge>("vnode_disk_storage", "disk storage of vnode");
        let disk_storage_gauge = metric.recorder([
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.to_string().as_str()),
        ]);

        let metric = register.metric::<U64Gauge>("vnode_cache_size", "cache size of vnode");
        let cache_gauge = metric.recorder([
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.to_string().as_str()),
        ]);

        Self {
            vnode_disk_storage: disk_storage_gauge,
            vnode_cache_size: cache_gauge,
        }
    }

    pub fn record_disk_storage(&self, size: u64) {
        self.vnode_disk_storage.set(size)
    }

    pub fn record_cache_size(&self, size: u64) {
        self.vnode_cache_size.set(size)
    }

    pub fn drop(register: &MetricsRegister, owner: &str, vnode_id: u64) {
        let (tenant, db) = split_owner(owner);
        let metric = register.metric::<U64Gauge>("vnode_disk_storage", "disk storage of vnode");
        metric.remove([
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.to_string().as_str()),
        ]);

        let metric = register.metric::<U64Gauge>("vnode_cache_size", "cache size of vnode");
        metric.remove([
            ("tenant", tenant),
            ("database", db),
            ("vnode_id", vnode_id.to_string().as_str()),
        ]);
    }
}

#[derive(Debug)]
pub struct TsfFactory {
    // "tenant.db"
    database: Arc<String>,
    options: Arc<Options>,
    db_config: Arc<DatabaseConfig>,
    memory_pool: MemoryPoolRef,
    metrics_register: Arc<MetricsRegister>,
}
impl TsfFactory {
    pub fn new(
        database: Arc<String>,
        options: Arc<Options>,
        db_config: Arc<DatabaseConfig>,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Self {
        Self {
            database,
            options,
            db_config,
            memory_pool,
            metrics_register,
        }
    }

    pub fn create_tsf(&self, tf_id: TseriesFamilyId, version: Arc<Version>) -> TseriesFamily {
        let mut_cache = Arc::new(RwLock::new(MemCache::new(
            tf_id,
            self.db_config.max_memcache_size(),
            self.db_config.memcache_partitions() as usize,
            version.last_seq,
            &self.memory_pool,
        )));
        let tsf_metrics =
            TsfMetrics::new(&self.metrics_register, self.database.as_str(), tf_id as u64);
        let super_version = Arc::new(SuperVersion::new(
            tf_id,
            CacheGroup {
                mut_cache: mut_cache.clone(),
                immut_cache: vec![],
            },
            version.clone(),
            0,
        ));

        TseriesFamily {
            tf_id,
            tenant_database: self.database.clone(),
            mut_cache,
            immut_cache: vec![],
            super_version,
            super_version_id: AtomicU64::new(0),
            db_config: self.db_config.clone(),
            storage_opt: self.options.storage.clone(),
            last_modified: Arc::new(Default::default()),
            memory_pool: self.memory_pool.clone(),
            tsf_metrics,
            status: VnodeStatus::Running,
        }
    }

    pub fn drop_tsf(&self, tf_id: u32) {
        //todo other's thing may need to drop
        TsfMetrics::drop(&self.metrics_register, self.database.as_str(), tf_id as u64);
    }
}

#[derive(Debug)]
pub struct TseriesFamily {
    tf_id: TseriesFamilyId,
    tenant_database: Arc<String>,
    mut_cache: Arc<RwLock<MemCache>>,
    immut_cache: Vec<Arc<RwLock<MemCache>>>,
    super_version: Arc<SuperVersion>,
    super_version_id: AtomicU64,
    db_config: Arc<DatabaseConfig>,
    storage_opt: Arc<StorageOptions>,
    last_modified: Arc<tokio::sync::RwLock<Option<Instant>>>,
    memory_pool: MemoryPoolRef,
    tsf_metrics: TsfMetrics,
    status: VnodeStatus,
}

impl TseriesFamily {
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub fn new(
        tf_id: TseriesFamilyId,
        tenant_database: Arc<String>,
        cache: MemCache,
        version: Arc<Version>,
        db_config: Arc<DatabaseConfig>,
        storage_opt: Arc<StorageOptions>,
        memory_pool: MemoryPoolRef,
        register: &Arc<MetricsRegister>,
    ) -> Self {
        let mm = Arc::new(RwLock::new(cache));

        Self {
            tf_id,
            tenant_database: tenant_database.clone(),
            mut_cache: mm.clone(),
            immut_cache: Default::default(),
            super_version: Arc::new(SuperVersion::new(
                tf_id,
                CacheGroup {
                    mut_cache: mm,
                    immut_cache: Default::default(),
                },
                version.clone(),
                0,
            )),
            super_version_id: AtomicU64::new(0),
            db_config,
            storage_opt,
            last_modified: Arc::new(tokio::sync::RwLock::new(None)),
            memory_pool,
            tsf_metrics: TsfMetrics::new(register, tenant_database.as_str(), tf_id as u64),
            status: VnodeStatus::Running,
        }
    }

    fn new_super_version(&mut self, version: Arc<Version>) {
        self.super_version_id.fetch_add(1, Ordering::SeqCst);
        self.tsf_metrics.record_disk_storage(self.disk_storage());
        self.tsf_metrics.record_cache_size(self.cache_size());
        self.super_version = Arc::new(SuperVersion::new(
            self.tf_id,
            CacheGroup {
                mut_cache: self.mut_cache.clone(),
                immut_cache: self.immut_cache.clone(),
            },
            version,
            self.super_version_id.load(Ordering::SeqCst),
        ))
    }

    /// Set new Version into current TsFamily, drop unused immutable caches,
    /// then create new SuperVersion, update seq_no
    pub fn new_version(
        &mut self,
        version: Arc<Version>,
        flushed_mem_caches: Option<&Vec<Arc<RwLock<MemCache>>>>,
    ) {
        debug!("New version for ts_family ({})", self.tf_id,);
        if let Some(flushed_mem_caches) = flushed_mem_caches {
            let mut new_caches = Vec::with_capacity(self.immut_cache.len());
            for c in self.immut_cache.iter() {
                let mut cache_not_flushed = true;
                for fc in flushed_mem_caches {
                    if c.data_ptr() as usize == fc.data_ptr() as usize {
                        cache_not_flushed = false;
                        break;
                    }
                }
                if cache_not_flushed {
                    new_caches.push(c.clone());
                }
            }
            self.immut_cache = new_caches;
        }
        self.new_super_version(version.clone());
    }

    pub fn switch_to_immutable(&mut self) {
        let seq_no = self.mut_cache.read().seq_no();
        self.immut_cache.push(self.mut_cache.clone());

        self.mut_cache = Arc::from(RwLock::new(MemCache::new(
            self.tf_id,
            self.db_config.max_memcache_size(),
            self.db_config.memcache_partitions() as usize,
            seq_no,
            &self.memory_pool,
        )));

        self.new_super_version(self.version());
    }

    pub fn put_points(
        &self,
        seq: u64,
        points: HashMap<SeriesId, (SeriesKey, RowGroup)>,
    ) -> TskvResult<u64> {
        if self.status == VnodeStatus::Copying {
            return Err(CommonSnafu {
                reason: "vnode is moving please retry later".to_string(),
            }
            .build());
        }
        let mut res = 0;
        for (sid, (series_key, group)) in points {
            let mem = self.mut_cache.read();
            res += group.rows.get_ref_rows().len();
            mem.write_group(sid, series_key, seq, group)?;
        }
        Ok(res as u64)
    }

    pub async fn check_to_flush(&mut self) -> bool {
        if self.mut_cache.read().is_full() {
            info!(
                "mut_cache is full, switch to immutable. current pool_size : {}",
                self.memory_pool.reserved()
            );
            self.switch_to_immutable();

            true
        } else {
            false
        }
    }

    pub async fn update_last_modified(&self) {
        *self.last_modified.write().await = Some(Instant::now());
    }

    pub async fn get_last_modified(&self) -> Option<Instant> {
        *self.last_modified.read().await
    }

    pub fn update_status(&mut self, status: VnodeStatus) {
        self.status = status;
    }

    pub fn drop_columns(&self, series_ids: &[SeriesId], column_ids: &[ColumnId]) {
        self.mut_cache.read().drop_columns(series_ids, column_ids);
        for memcache in self.immut_cache.iter() {
            memcache.read().drop_columns(series_ids, column_ids);
        }
    }

    pub fn change_column(&self, sids: &[SeriesId], column_name: &str, new_column: &TableColumn) {
        self.mut_cache
            .read()
            .change_column(sids, column_name, new_column);
        for memcache in self.immut_cache.iter() {
            memcache.read().change_column(sids, column_name, new_column);
        }
    }

    pub fn add_column(&self, sids: &[SeriesId], new_column: &TableColumn) {
        self.mut_cache.read().add_column(sids, new_column);
        for memcache in self.immut_cache.iter() {
            memcache.read().add_column(sids, new_column);
        }
    }

    pub fn delete_series(&self, sids: &[SeriesId], time_range: &TimeRange) {
        self.mut_cache.read().delete_series(sids, time_range);
        for memcache in self.immut_cache.iter() {
            memcache.read().delete_series(sids, time_range);
        }
    }

    pub fn delete_series_by_time_ranges(&self, sids: &[SeriesId], time_ranges: &TimeRanges) {
        self.mut_cache
            .read()
            .delete_series_by_time_ranges(sids, time_ranges);
        for memcache in self.immut_cache.iter() {
            memcache
                .read()
                .delete_series_by_time_ranges(sids, time_ranges);
        }
    }

    /// Snapshots last version before `last_seq` of this vnode.
    ///
    /// Db-files' index data (field-id filter) will be inserted into `file_metas`.
    pub fn build_version_edit(&self) -> VersionEdit {
        let version = self.version();
        let owner = (*self.tenant_database).clone();
        let seq_no = version.last_seq();
        let max_level_ts = version.max_level_ts;

        let mut version_edit = VersionEdit::new_add_vnode(self.tf_id, owner, seq_no);
        for files in version.levels_info.iter() {
            for file in files.files.iter() {
                let mut meta = CompactMeta::from(file.as_ref());
                meta.tsf_id = files.tsf_id;
                version_edit.add_file(meta, max_level_ts);
            }
        }

        version_edit
    }

    pub async fn column_files_bloom_filter(
        &self,
    ) -> TskvResult<HashMap<ColumnFileId, Arc<BloomFilter>>> {
        let version = self.version();
        let mut file_metas = HashMap::new();
        for files in version.levels_info.iter() {
            for file in files.files.iter() {
                let bloom_filter = file.load_bloom_filter().await?;
                file_metas.insert(file.file_id, bloom_filter);
            }
        }

        Ok(file_metas)
    }

    pub async fn rebuild_index(&self) -> TskvResult<Arc<tokio::sync::RwLock<TSIndex>>> {
        let path = self
            .storage_opt
            .index_dir(self.tenant_database.as_str(), self.tf_id);
        let _ = std::fs::remove_dir_all(path.clone());

        let index = TSIndex::new(path).await.context(IndexErrSnafu)?;
        let index_clone = index.clone();
        let mut index_w = index_clone.write().await;

        // cache index
        let mut series_data = self.mut_cache.read().read_all_series_data();
        for imut_cache in self.immut_cache.iter() {
            series_data.extend(imut_cache.read().read_all_series_data());
        }
        for (sid, data) in series_data {
            let series_key = data.read().series_key.clone();
            index_w
                .add_series_for_rebuild(sid, &series_key)
                .await
                .context(IndexErrSnafu)?;
        }

        // tsm index
        for level in self.version().levels_info.iter() {
            for file in level.files.iter() {
                let reader = self.version().get_tsm_reader(file.file_path()).await?;
                for chunk in reader.chunk().values() {
                    index_w
                        .add_series_for_rebuild(chunk.series_id(), chunk.series_key())
                        .await
                        .context(IndexErrSnafu)?;
                }
            }
        }

        index_w.flush().await.context(IndexErrSnafu)?;

        Ok(index)
    }

    pub fn tf_id(&self) -> TseriesFamilyId {
        self.tf_id
    }

    pub fn tenant_database(&self) -> Arc<String> {
        self.tenant_database.clone()
    }

    pub fn cache(&self) -> &Arc<RwLock<MemCache>> {
        &self.mut_cache
    }

    pub fn im_cache(&self) -> &Vec<Arc<RwLock<MemCache>>> {
        &self.immut_cache
    }

    pub fn last_seq(&self) -> u64 {
        self.mut_cache.read().seq_no()
    }

    pub fn super_version(&self) -> Arc<SuperVersion> {
        self.super_version.clone()
    }

    pub fn version(&self) -> Arc<Version> {
        self.super_version.version.clone()
    }

    pub fn storage_opt(&self) -> Arc<StorageOptions> {
        self.storage_opt.clone()
    }

    pub fn get_delta_dir(&self) -> PathBuf {
        self.storage_opt
            .delta_dir(&self.tenant_database, self.tf_id)
    }

    pub fn get_tsm_dir(&self) -> PathBuf {
        self.storage_opt.tsm_dir(&self.tenant_database, self.tf_id)
    }

    pub fn disk_storage(&self) -> u64 {
        self.version()
            .levels_info
            .iter()
            .map(|l| l.disk_storage())
            .sum()
    }

    pub fn cache_size(&self) -> u64 {
        self.immut_cache
            .iter()
            .map(|c| c.read().cache_size())
            .sum::<u64>()
            + self.mut_cache.read().cache_size()
    }
    pub fn can_compaction(&self) -> bool {
        self.status == VnodeStatus::Running
    }
}

#[cfg(test)]
pub mod test_tseries_family {
    use std::collections::HashMap;
    use std::sync::Arc;

    use cache::ShardedAsyncCache;
    use models::Timestamp;

    use super::{ColumnFile, LevelInfo};
    use crate::file_utils::make_tsm_file;
    use crate::kv_option::Options;
    use crate::summary::{CompactMeta, VersionEdit};
    use crate::tseries_family::{TimeRange, Version};

    #[tokio::test]
    async fn test_version_apply_version_edits_1() {
        //! There is a Version with two levels:
        //! - Lv.0: [ ]
        //! - Lv.1: [ (3, 3001~3000) ]
        //! - Lv.2: [ (1, 1~1000), (2, 1001~2000) ]
        //! - Lv.3: [ ]
        //! - Lv.4: [ ]
        //!
        //! Add (4, 3051~3150) into lv.1, and delete (3, 3001~3000).
        //!
        //! The new Version will like this:
        //! - Lv.0: [ ]
        //! - Lv.1: [ (3, 3051~3150) ]
        //! - Lv.2: [ (1, 1~1000), (2, 1001~2000) ]
        //! - Lv.3: [ ]
        //! - Lv.4: [ ]
        let dir = "/tmp/test/ts_family/1";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::tskv::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let database = Arc::new("test".to_string());
        let ts_family_id = 1;
        let tsm_dir = opt.storage.tsm_dir(&database, ts_family_id);
        #[rustfmt::skip]
            let levels = [
            LevelInfo::init(database.clone(), 0, 0, opt.storage.clone()),
            LevelInfo {
                files: vec![
                    Arc::new(ColumnFile::new(3, 1, TimeRange::new(3001, 3100), 100, false, make_tsm_file(&tsm_dir, 3))),
                ],
                database: database.clone(),
                tsf_id: 1,
                storage_opt: opt.storage.clone(),
                level: 1,
                cur_size: 100,
                max_size: 1000,
                time_range: TimeRange::new(3001, 3100),
            },
            LevelInfo {
                files: vec![
                    Arc::new(ColumnFile::new(1, 2, TimeRange::new(1, 1000), 1000, false, make_tsm_file(&tsm_dir, 1))),
                    Arc::new(ColumnFile::new(2, 2, TimeRange::new(1001, 2000), 1000, false, make_tsm_file(&tsm_dir, 2))),
                ],
                database: database.clone(),
                tsf_id: 1,
                storage_opt: opt.storage.clone(),
                level: 2,
                cur_size: 2000,
                max_size: 10000,
                time_range: TimeRange::new(1, 2000),
            },
            LevelInfo::init(database.clone(), 3, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 4, 0, opt.storage.clone()),
        ];
        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(16));
        let version = Version::new(
            1,
            database.clone(),
            opt.storage.clone(),
            1,
            levels,
            3100,
            tsm_reader_cache,
        );

        let mut ve = VersionEdit::new_update_vnode(1, database.to_string(), 1);
        #[rustfmt::skip]
        ve.add_file(
            CompactMeta {
                file_id: 4,
                file_size: 100,
                tsf_id: 1,
                level: 1,
                min_ts: 3051,
                max_ts: 3150,
                is_delta: false,
            },
            3100,
        );
        let version = version.copy_apply_version_edits(ve, &mut HashMap::new());

        let mut ve = VersionEdit::new_update_vnode(1, database.to_string(), 3);
        ve.del_file(1, 3, false);
        let new_version = version.copy_apply_version_edits(ve, &mut HashMap::new());

        assert_eq!(new_version.last_seq, 3);
        assert_eq!(new_version.max_level_ts, 3150);

        let lvl = new_version.levels_info.get(1).unwrap();
        assert_eq!(lvl.time_range, TimeRange::new(3051, 3150));
        assert_eq!(lvl.files.len(), 1);
        let col_file = lvl.files.first().unwrap();
        assert_eq!(col_file.time_range, TimeRange::new(3051, 3150));
    }

    #[tokio::test]
    async fn test_version_apply_version_edits_2() {
        //! There is a Version with two levels:
        //! - Lv.1: [ (3, 3001~3000), (4, 3051~3150) ]
        //! - Lv.2: [ (1, 1~1000), (2, 1001~2000) ]
        //! - Lv.3: [ ]
        //! - Lv.4: [ ]
        //!
        //! 1. Compact [ (3, 3001~3000), (4, 3051~3150) ] into lv.2, and delete them.
        //! 2. Compact [ (1, 1~1000), (2, 1001~2000) ] into lv.3, and delete them.
        //!
        //! The new Version will like this:
        //! - Lv.0: [ ]
        //! - Lv.1: [  ]
        //! - Lv.2: [ (5, 3001~3150) ]
        //! - Lv.3: [ (6, 1~2000) ]
        //! - Lv.4: [ ]
        let dir = "/tmp/test/ts_family/2";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::tskv::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let database = Arc::new("cnosdb.test".to_string());
        let ts_family_id = 1;
        let tsm_dir = opt.storage.tsm_dir(&database, ts_family_id);
        #[rustfmt::skip]
            let levels = [
            LevelInfo::init(database.clone(), 0, 1, opt.storage.clone()),
            LevelInfo {
                files: vec![
                    Arc::new(ColumnFile::new(3, 1, TimeRange::new(3001, 3100), 100, false, make_tsm_file(&tsm_dir, 3))),
                    Arc::new(ColumnFile::new(4, 1, TimeRange::new(3051, 3150), 100, false, make_tsm_file(&tsm_dir, 4))),
                ],
                database: database.clone(),
                tsf_id: 1,
                storage_opt: opt.storage.clone(),
                level: 1,
                cur_size: 100,
                max_size: 1000,
                time_range: TimeRange::new(3001, 3150),
            },
            LevelInfo {
                files: vec![
                    Arc::new(ColumnFile::new(1, 2, TimeRange::new(1, 1000), 1000, false, make_tsm_file(&tsm_dir, 1))),
                    Arc::new(ColumnFile::new(2, 2, TimeRange::new(1001, 2000), 1000, false, make_tsm_file(&tsm_dir, 2))),
                ],
                database: database.clone(),
                tsf_id: 1,
                storage_opt: opt.storage.clone(),
                level: 2,
                cur_size: 2000,
                max_size: 10000,
                time_range: TimeRange::new(1, 2000),
            },
            LevelInfo::init(database.clone(), 3, 1, opt.storage.clone()),
            LevelInfo::init(database.clone(), 4, 1, opt.storage.clone()),
        ];
        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(16));
        #[rustfmt::skip]
            let version = Version::new(1, database.clone(), opt.storage.clone(), 1, levels, 3150, tsm_reader_cache);

        let mut ve = VersionEdit::new_update_vnode(1, database.to_string(), 1);
        #[rustfmt::skip]
        ve.add_file(
            CompactMeta {
                file_id: 5,
                file_size: 150,
                tsf_id: 1,
                level: 2,
                min_ts: 3001,
                max_ts: 3150,
                is_delta: false,
            },
            3150,
        );
        #[rustfmt::skip]
        ve.add_file(
            CompactMeta {
                file_id: 6,
                file_size: 2000,
                tsf_id: 1,
                level: 3,
                min_ts: 1,
                max_ts: 2000,
                is_delta: false,
            },
            3150,
        );
        let version = version.copy_apply_version_edits(ve, &mut HashMap::new());

        let mut ve = VersionEdit::new_update_vnode(1, database.to_string(), 3);
        ve.del_file(1, 3, false);
        ve.del_file(1, 4, false);
        ve.del_file(2, 1, false);
        ve.del_file(2, 2, false);
        let new_version = version.copy_apply_version_edits(ve, &mut HashMap::new());

        assert_eq!(new_version.last_seq, 3);
        assert_eq!(new_version.max_level_ts, 3150);

        let lvl = new_version.levels_info.get(1).unwrap();
        assert_eq!(
            lvl.time_range,
            TimeRange::new(Timestamp::MAX, Timestamp::MIN)
        );
        assert_eq!(lvl.files.len(), 0);

        let lvl = new_version.levels_info.get(2).unwrap();
        assert_eq!(lvl.time_range, TimeRange::new(3001, 3150));
        let col_file = lvl.files.last().unwrap();
        assert_eq!(col_file.time_range, TimeRange::new(3001, 3150));

        let lvl = new_version.levels_info.get(3).unwrap();
        assert_eq!(lvl.time_range, TimeRange::new(1, 2000));
        assert_eq!(lvl.files.len(), 1);
        let col_file = lvl.files.last().unwrap();
        assert_eq!(col_file.time_range, TimeRange::new(1, 2000));
    }
}
