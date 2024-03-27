use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use arrow_array::RecordBatch;
use cache::{AsyncCache, ShardedAsyncCache};
use memory_pool::MemoryPoolRef;
use metrics::gauge::U64Gauge;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeStatus;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::{split_owner, TableColumn};
use models::{ColumnId, FieldId, SeriesId, SeriesKey, Timestamp};
use parking_lot::RwLock;
use snafu::ResultExt as _;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use trace::{debug, error, info, warn};
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::error::Result;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::file_utils::{self, make_delta_file, make_tsm_file};
use crate::index::ts_index::TSIndex;
use crate::kv_option::{CacheOptions, StorageOptions};
use crate::memcache::{MemCache, MemCacheStatistics, RowGroup};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tsm::TsmTombstone;
use crate::tsm2::page::PageMeta;
use crate::tsm2::reader::TSM2Reader;
use crate::tsm2::ColumnGroupID;
use crate::Error::CommonError;
use crate::{tsm, ColumnFileId, LevelId, Options, TseriesFamilyId};

#[derive(Debug)]
pub struct ColumnFile {
    file_id: ColumnFileId,
    level: LevelId,
    is_delta: bool,
    time_range: TimeRange,
    size: u64,
    series_id_filter: Arc<BloomFilter>,
    deleted: AtomicBool,
    compacting: AtomicBool,

    path: PathBuf,
    tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TSM2Reader>>>,
}

impl ColumnFile {
    pub fn with_compact_data(
        meta: &CompactMeta,
        path: impl AsRef<Path>,
        series_id_filter: Arc<BloomFilter>,
        tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TSM2Reader>>>,
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

    pub fn maybe_contains_series_id(&self, series_id: SeriesId) -> bool {
        self.series_id_filter
            .maybe_contains(&series_id.to_be_bytes())
    }

    pub fn contains_any_series_id(&self, series_ids: &[SeriesId]) -> bool {
        for series_id in series_ids {
            if self
                .series_id_filter
                .maybe_contains(&series_id.to_be_bytes())
            {
                return true;
            }
        }
        false
    }

    pub fn contains_any_field_id(&self, _series_ids: &[FieldId]) -> bool {
        unimplemented!("contains_any_field_id")
    }

    pub async fn add_tombstone(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> Result<()> {
        let dir = self.path.parent().expect("file has parent");
        // TODO flock tombstone file.
        let mut tombstone = TsmTombstone::open(dir, self.file_id).await?;
        tombstone
            .add_range(&[(series_id, column_id)], time_range)
            .await?;
        tombstone.flush().await?;
        Ok(())
    }

    pub fn series_id_filter(&self) -> &BloomFilter {
        &self.series_id_filter
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
            series_id_filter: Arc::new(BloomFilter::default()),
            deleted: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            path: path.as_ref().into(),
            tsm_reader_cache: Weak::new(),
        }
    }

    pub fn set_field_id_filter(&mut self, field_id_filter: Arc<BloomFilter>) {
        self.series_id_filter = field_id_filter;
    }
}

#[derive(Debug)]
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
        series_filter: Arc<BloomFilter>,
        tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TSM2Reader>>>,
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

    pub fn overlaps_column_files(
        &self,
        time_ranges: &TimeRanges,
        field_id: FieldId,
    ) -> Vec<Arc<ColumnFile>> {
        let mut res = self
            .files
            .iter()
            .filter(|f| {
                time_ranges.overlaps(f.time_range())
                    && f.maybe_contains_series_id(field_id as SeriesId)
            })
            .cloned()
            .collect::<Vec<Arc<ColumnFile>>>();
        res.sort_by_key(|f| *f.time_range());
        res
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
    tsm2_reader_cache: Arc<ShardedAsyncCache<String, Arc<TSM2Reader>>>,
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
        tsm2_reader_cache: Arc<ShardedAsyncCache<String, Arc<TSM2Reader>>>,
    ) -> Self {
        Self {
            ts_family_id,
            tenant_database,
            storage_opt,
            last_seq,
            max_level_ts,
            levels_info,
            tsm2_reader_cache,
        }
    }

    /// Creates new Version using current Version and `VersionEdit`s.
    pub fn copy_apply_version_edits(
        &self,
        version_edits: Vec<VersionEdit>,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
        last_seq: Option<u64>,
    ) -> Version {
        let mut added_files: Vec<Vec<CompactMeta>> = vec![vec![]; 5];
        let mut deleted_files: Vec<HashSet<ColumnFileId>> = vec![HashSet::new(); 5];
        for ve in version_edits.into_iter() {
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
        }

        let mut new_levels = LevelInfo::init_levels(
            self.tenant_database.clone(),
            self.ts_family_id,
            self.storage_opt.clone(),
        );
        let weak_tsm_reader_cache = Arc::downgrade(&self.tsm2_reader_cache);
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if deleted_files[file.level as usize].contains(&file.file_id) {
                    file.mark_deleted();
                    continue;
                }
                new_levels[level.level as usize].push_column_file(file.clone());
            }
            for file in added_files[level.level as usize].iter() {
                let series_filter = file_metas.remove(&file.file_id).unwrap_or_default();
                new_levels[level.level as usize].push_compact_meta(
                    file,
                    series_filter,
                    weak_tsm_reader_cache.clone(),
                );
            }
            new_levels[level.level as usize].update_time_range();
        }

        let mut new_version = Self {
            ts_family_id: self.ts_family_id,
            tenant_database: self.tenant_database.clone(),
            storage_opt: self.storage_opt.clone(),
            last_seq: last_seq.unwrap_or(self.last_seq),
            max_level_ts: self.max_level_ts,
            levels_info: new_levels,
            tsm2_reader_cache: self.tsm2_reader_cache.clone(),
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

    pub async fn get_tsm_reader2(&self, path: impl AsRef<Path>) -> Result<Arc<TSM2Reader>> {
        let path = path.as_ref().display().to_string();
        let tsm_reader = match self.tsm2_reader_cache.get(&path).await {
            Some(val) => val,
            None => match self.tsm2_reader_cache.get(&path).await {
                Some(val) => val,
                None => {
                    let tsm_reader = Arc::new(TSM2Reader::open(&path).await?);
                    self.tsm2_reader_cache
                        .insert(path, tsm_reader.clone())
                        .await;
                    tsm_reader
                }
            },
        };
        Ok(tsm_reader)
    }

    // return: l0 , l1-l4 files
    pub fn get_level_files(
        &self,
        time_ranges: &TimeRanges,
        field_id: FieldId,
    ) -> [Option<Vec<Arc<ColumnFile>>>; 5] {
        let mut res: [Option<Vec<Arc<ColumnFile>>>; 5] = [None, None, None, None, None];
        for (res, level_info) in res.iter_mut().zip(self.levels_info.iter()) {
            let files = level_info.overlaps_column_files(time_ranges, field_id);
            if !files.is_empty() {
                *res = Some(files)
            }
        }
        res
    }

    pub fn max_level_ts(&self) -> i64 {
        self.max_level_ts
    }

    pub fn tsm2_reader_cache(&self) -> &Arc<ShardedAsyncCache<String, Arc<TSM2Reader>>> {
        &self.tsm2_reader_cache
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    #[cfg(test)]
    pub fn levels_info_mut(&mut self) -> &mut [LevelInfo; 5] {
        &mut self.levels_info
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
                let reader = self.get_tsm_reader2(file.file_path()).await.unwrap();
                let fid = reader.file_id();
                let sts = reader.statistics(series_ids, time_predicate).await.unwrap();
                result.insert(fid, sts);
            }
        }
        result
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
    pub storage_opt: Arc<StorageOptions>,
    pub caches: CacheGroup,
    pub version: Arc<Version>,
    pub version_number: u64,
}

impl SuperVersion {
    pub fn new(
        ts_family_id: u32,
        storage_opt: Arc<StorageOptions>,
        caches: CacheGroup,
        version: Arc<Version>,
        version_number: u64,
    ) -> Self {
        Self {
            ts_family_id,
            storage_opt,
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

    pub fn column_files_by_sid_and_time(
        &self,
        sids: &[SeriesId],
        time_ranges: &TimeRanges,
    ) -> Vec<Arc<ColumnFile>> {
        let mut files = Vec::new();

        for lv in self.version.levels_info.iter() {
            if !time_ranges.overlaps(&lv.time_range) {
                continue;
            }
            for cf in lv.files.iter() {
                if time_ranges.overlaps(&cf.time_range) && cf.contains_any_series_id(sids) {
                    files.push(cf.clone());
                }
            }
        }
        files
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
    ) -> Result<()> {
        let column_files =
            self.column_files_by_sid_and_time(series_ids, &TimeRanges::new(vec![*time_range]));
        for sid in series_ids {
            for column_file in column_files.iter() {
                if column_file
                    .series_id_filter()
                    .maybe_contains(sid.to_be_bytes().as_slice())
                {
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
}

#[derive(Debug)]
pub struct TsfFactory {
    // "tenant.db"
    database: Arc<String>,
    options: Arc<Options>,
    memory_pool: MemoryPoolRef,
    metrics_register: Arc<MetricsRegister>,
}
impl TsfFactory {
    pub fn new(
        database: Arc<String>,
        options: Arc<Options>,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Self {
        Self {
            database,
            options,
            memory_pool,
            metrics_register,
        }
    }

    pub fn create_tsf(&self, tf_id: TseriesFamilyId, version: Arc<Version>) -> TseriesFamily {
        let mut_cache = Arc::new(RwLock::new(MemCache::new(
            tf_id,
            self.options.cache.max_buffer_size,
            self.options.cache.partition,
            version.last_seq,
            &self.memory_pool,
        )));
        let tsf_metrics =
            TsfMetrics::new(&self.metrics_register, self.database.as_str(), tf_id as u64);
        let super_version = Arc::new(SuperVersion::new(
            tf_id,
            self.options.storage.clone(),
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
            version: version.clone(),
            cache_opt: self.options.cache.clone(),
            storage_opt: self.options.storage.clone(),
            seq_no: version.last_seq,
            last_modified: Arc::new(Default::default()),
            cancellation_token: CancellationToken::new(),
            memory_pool: self.memory_pool.clone(),
            tsf_metrics,
            status: VnodeStatus::Running,
        }
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
    version: Arc<Version>,
    cache_opt: Arc<CacheOptions>,
    storage_opt: Arc<StorageOptions>,
    seq_no: u64,
    last_modified: Arc<tokio::sync::RwLock<Option<Instant>>>,
    cancellation_token: CancellationToken,
    memory_pool: MemoryPoolRef,
    tsf_metrics: TsfMetrics,
    status: VnodeStatus,
}

impl TseriesFamily {
    pub const MAX_DATA_BLOCK_SIZE: u32 = 1000;
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub fn new(
        tf_id: TseriesFamilyId,
        tenant_database: Arc<String>,
        cache: MemCache,
        version: Arc<Version>,
        cache_opt: Arc<CacheOptions>,
        storage_opt: Arc<StorageOptions>,
        memory_pool: MemoryPoolRef,
        register: &Arc<MetricsRegister>,
    ) -> Self {
        let mm = Arc::new(RwLock::new(cache));

        Self {
            tf_id,
            tenant_database: tenant_database.clone(),
            seq_no: version.last_seq,
            mut_cache: mm.clone(),
            immut_cache: Default::default(),
            super_version: Arc::new(SuperVersion::new(
                tf_id,
                storage_opt.clone(),
                CacheGroup {
                    mut_cache: mm,
                    immut_cache: Default::default(),
                },
                version.clone(),
                0,
            )),
            super_version_id: AtomicU64::new(0),
            version,
            cache_opt,
            storage_opt,
            last_modified: Arc::new(tokio::sync::RwLock::new(None)),
            cancellation_token: CancellationToken::new(),
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
            self.storage_opt.clone(),
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
        new_version: Version,
        flushed_mem_caches: Option<&Vec<Arc<RwLock<MemCache>>>>,
    ) {
        let version = Arc::new(new_version);
        debug!(
            "New version(level_info) for ts_family({}): {:?}",
            self.tf_id,
            &version.levels_info()
        );
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
        self.seq_no = version.last_seq;
        self.version = version;
    }

    pub fn switch_to_immutable(&mut self) {
        self.immut_cache.push(self.mut_cache.clone());
        self.mut_cache = Arc::from(RwLock::new(MemCache::new(
            self.tf_id,
            self.cache_opt.max_buffer_size,
            self.cache_opt.partition,
            self.seq_no,
            &self.memory_pool,
        )));
        self.new_super_version(self.version.clone());
    }

    /// Check if there are immutable caches to flush and build a `FlushReq`,
    /// or else return None.
    ///
    /// If argument `force` is false, total count of immutable caches that
    /// are not flushing or flushed should be greater than configuration `max_immutable_number`.
    /// If argument `force` is set to true, then do not check the total count.
    pub(crate) fn build_flush_req(&mut self, force: bool) -> Option<FlushReq> {
        let mut filtered_caches: Vec<Arc<RwLock<MemCache>>> = self
            .immut_cache
            .iter()
            .filter(|c| !c.read().is_flushing())
            .cloned()
            .collect();

        if !force && filtered_caches.is_empty() {
            return None;
        }

        // Mark these caches marked as `flushing` in current thread and collect them.
        filtered_caches.retain(|c| c.read().mark_flushing());
        if filtered_caches.is_empty() {
            return None;
        }
        let (mut high_seq_no, mut low_seq_no) = (0, u64::MAX);
        for mem in filtered_caches.iter() {
            let seq_no = mem.read().seq_no();
            high_seq_no = seq_no.max(high_seq_no);
            low_seq_no = seq_no.min(low_seq_no);
        }

        Some(FlushReq {
            ts_family_id: self.tf_id,
            mems: filtered_caches,
            force_flush: force,
            low_seq_no,
            high_seq_no,
        })
    }

    /// Try to build a `FlushReq` by immutable caches,
    /// if succeed, send it to flush job.
    pub(crate) async fn send_flush_req(&mut self, sender: Sender<FlushReq>, force: bool) {
        if let Some(req) = self.build_flush_req(force) {
            sender
                .send(req)
                .await
                .expect("error send flush req to kvcore");
        }
    }

    pub fn put_points(
        &self,
        seq: u64,
        points: HashMap<SeriesId, (SeriesKey, RowGroup)>,
    ) -> Result<u64> {
        if self.status == VnodeStatus::Copying {
            return Err(CommonError {
                reason: "vnode is moving please retry later".to_string(),
            });
        }
        let mut res = 0;
        for (sid, (series_key, group)) in points {
            let mem = self.mut_cache.read();
            res += group.rows.get_ref_rows().len();
            mem.write_group(sid, series_key, seq, group)?;
        }
        Ok(res as u64)
    }

    pub async fn check_to_flush(&mut self, sender: Sender<FlushReq>) {
        if self.mut_cache.read().is_full() {
            info!(
                "mut_cache is full, switch to immutable. current pool_size : {}",
                self.memory_pool.reserved()
            );
            self.switch_to_immutable();
        }
        if !self.immut_cache.is_empty() {
            self.send_flush_req(sender, false).await;
        }
    }

    pub async fn update_last_modified(&self) {
        *self.last_modified.write().await = Some(Instant::now());
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

    pub fn schedule_compaction(&self, runtime: Arc<Runtime>, sender: Sender<CompactTask>) {
        let tsf_id = self.tf_id;
        let compact_trigger_cold_duration = self.storage_opt.compact_trigger_cold_duration;
        let last_modified = self.last_modified.clone();
        let cancellation_token = self.cancellation_token.clone();
        let _jh = runtime.spawn(async move {
            if compact_trigger_cold_duration == Duration::ZERO {} else {
                let mut cold_check_interval = tokio::time::interval(Duration::from_secs(10));
                cold_check_interval.tick().await;
                loop {
                    tokio::select! {
                        _ = cold_check_interval.tick() => {
                            let last_modified = last_modified.read().await;
                            if let Some(t) = *last_modified {
                                if t.elapsed() >= compact_trigger_cold_duration {
                                    if let Err(e) = sender.send(CompactTask::ColdVnode(tsf_id)).await {
                                        warn!("failed to send compact task({}), {}", tsf_id, e);
                                    }
                                }
                            }
                        }
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }
                }
            }
        });
    }

    pub fn close(&self) {
        self.cancellation_token.cancel();
    }

    /// Snapshots last version before `last_seq` of this vnode.
    ///
    /// Db-files' index data (field-id filter) will be inserted into `file_metas`.
    pub fn build_version_edit(
        &self,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) -> VersionEdit {
        let mut version_edit =
            VersionEdit::new_add_vnode(self.tf_id, (*self.tenant_database).clone(), self.seq_no);
        let version = self.version();
        let max_level_ts = version.max_level_ts;
        for files in version.levels_info.iter() {
            for file in files.files.iter() {
                let mut meta = CompactMeta::from(file.as_ref());
                meta.tsf_id = files.tsf_id;
                meta.high_seq = self.seq_no;
                version_edit.add_file(meta, max_level_ts);
                file_metas.insert(file.file_id, file.series_id_filter.clone());
            }
        }

        version_edit
    }

    pub async fn backup(&self, ve: &VersionEdit, snap_id: &str) -> Result<()> {
        let opt = self.storage_opt();
        let owner = self.tenant_database();
        let tsm_dir = opt.tsm_dir(owner.as_str(), self.tf_id);
        let delta_dir = opt.delta_dir(owner.as_str(), self.tf_id);
        let snap_tsm_dir = opt.snapshot_tsm_dir(owner.as_str(), self.tf_id, snap_id);
        let snap_delta_dir = opt.snapshot_delta_dir(owner.as_str(), self.tf_id, snap_id);

        std::fs::create_dir_all(&snap_tsm_dir)?;
        std::fs::create_dir_all(&snap_delta_dir)?;
        for f in &ve.add_files {
            // Get tsm/delta file path and snapshot file path
            let (file_path, snapshot_path) = if f.is_delta {
                (
                    file_utils::make_delta_file(&delta_dir, f.file_id),
                    file_utils::make_delta_file(&snap_delta_dir, f.file_id),
                )
            } else {
                (
                    file_utils::make_tsm_file(&tsm_dir, f.file_id),
                    file_utils::make_tsm_file(&snap_tsm_dir, f.file_id),
                )
            };

            // Create hard link to tsm/delta file.
            info!(
                "Bakcup: creating hard link {} to {}.",
                file_path.display(),
                snapshot_path.display()
            );
            if let Err(e) =
                std::fs::hard_link(&file_path, &snapshot_path).context(crate::error::IOSnafu)
            {
                error!(
                    "Bakcup: failed to create hard link {} to {}: {e}.",
                    file_path.display(),
                    snapshot_path.display()
                );
                return Err(e);
            }
        }

        Ok(())
    }

    pub async fn rebuild_index(&self) -> Result<Arc<TSIndex>> {
        let path = self
            .storage_opt
            .index_dir(self.tenant_database.as_str(), self.tf_id);
        let _ = std::fs::remove_dir_all(path.clone());

        let index = TSIndex::new(path).await?;

        // cache index
        let mut series_data = self.mut_cache.read().read_series_data();
        for imut_cache in self.immut_cache.iter() {
            series_data.extend(imut_cache.read().read_series_data());
        }
        for (sid, data) in series_data {
            let series_key = data.read().series_key.clone();
            index.add_series_for_rebuild(sid, &series_key).await?;
        }

        // tsm index
        for level in self.version.levels_info.iter() {
            for file in level.files.iter() {
                let reader = self.version.get_tsm_reader2(file.file_path()).await?;
                for chunk in reader.chunk().values() {
                    index
                        .add_series_for_rebuild(chunk.series_id(), chunk.series_key())
                        .await?;
                }
            }
        }

        index.flush().await?;

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

    pub fn super_version(&self) -> Arc<SuperVersion> {
        self.super_version.clone()
    }

    pub fn version(&self) -> Arc<Version> {
        self.version.clone()
    }

    pub fn storage_opt(&self) -> Arc<StorageOptions> {
        self.storage_opt.clone()
    }

    pub fn seq_no(&self) -> u64 {
        self.seq_no
    }

    pub fn get_delta_dir(&self) -> PathBuf {
        self.storage_opt
            .delta_dir(&self.tenant_database, self.tf_id)
    }

    pub fn get_tsm_dir(&self) -> PathBuf {
        self.storage_opt.tsm_dir(&self.tenant_database, self.tf_id)
    }

    pub fn disk_storage(&self) -> u64 {
        self.version
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

impl Drop for TseriesFamily {
    fn drop(&mut self) {
        self.cancellation_token.cancel();
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
    use crate::kv_option::{Options, StorageOptions};
    use crate::summary::{CompactMeta, VersionEdit};
    use crate::tseries_family::{TimeRange, Version};
    use crate::TseriesFamilyId;

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
        let mut global_config = config::get_config_for_test();
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
            database,
            opt.storage.clone(),
            1,
            levels,
            3100,
            tsm_reader_cache,
        );
        let mut version_edits = Vec::new();
        let mut ve = VersionEdit::new(1);
        #[rustfmt::skip]
        ve.add_file(
            CompactMeta {
                file_id: 4,
                file_size: 100,
                tsf_id: 1,
                level: 1,
                min_ts: 3051,
                max_ts: 3150,
                high_seq: 2,
                low_seq: 2,
                is_delta: false,
            },
            3100,
        );
        version_edits.push(ve);
        let mut ve = VersionEdit::new(1);
        ve.del_file(1, 3, false);
        version_edits.push(ve);
        let new_version =
            version.copy_apply_version_edits(version_edits, &mut HashMap::new(), Some(3));

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
        let mut global_config = config::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let database = Arc::new("test".to_string());
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
            let version = Version::new(1, database, opt.storage.clone(), 1, levels, 3150, tsm_reader_cache);

        let mut version_edits = Vec::new();
        let mut ve = VersionEdit::new(1);
        #[rustfmt::skip]
        ve.add_file(
            CompactMeta {
                file_id: 5,
                file_size: 150,
                tsf_id: 1,
                level: 2,
                min_ts: 3001,
                max_ts: 3150,
                high_seq: 2,
                low_seq: 2,
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
                high_seq: 2,
                low_seq: 2,
                is_delta: false,
            },
            3150,
        );
        version_edits.push(ve);
        let mut ve = VersionEdit::new(1);
        ve.del_file(1, 3, false);
        ve.del_file(1, 4, false);
        ve.del_file(2, 1, false);
        ve.del_file(2, 2, false);
        version_edits.push(ve);
        let new_version =
            version.copy_apply_version_edits(version_edits, &mut HashMap::new(), Some(3));

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

    pub(crate) fn build_version_by_column_files(
        storage_opt: Arc<StorageOptions>,
        database: Arc<String>,
        ts_family_id: TseriesFamilyId,
        mut files: Vec<Arc<ColumnFile>>,
    ) -> Version {
        files.sort_by_key(|f| f.file_id);
        let mut levels =
            LevelInfo::init_levels(database.clone(), ts_family_id, storage_opt.clone());
        let max_level_ts = i64::MIN;
        for file in files {
            let lv = &mut levels[file.level as usize];
            lv.cur_size += file.size;
            lv.time_range.merge(file.time_range());
            lv.files.push(file);
        }

        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(16));
        Version::new(
            ts_family_id,
            database,
            storage_opt,
            0,
            levels,
            max_level_ts,
            tsm_reader_cache,
        )
    }
}
