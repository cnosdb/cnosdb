use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use lru_cache::asynchronous::ShardedCache;
use memory_pool::MemoryPoolRef;
use metrics::gauge::U64Gauge;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeStatus;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::{split_owner, TableColumn};
use models::{FieldId, SchemaId, SeriesId, Timestamp};
use parking_lot::RwLock;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock as AsyncRwLock;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use trace::{debug, error, info, warn};
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::error::Result;
use crate::file_utils::{make_delta_file_name, make_tsm_file_name};
use crate::kv_option::{CacheOptions, StorageOptions};
use crate::memcache::{DataType, FieldVal, MemCache, RowGroup};
use crate::summary::{CompactMeta, VersionEdit};
use crate::tsm::{DataBlock, TsmReader, TsmTombstone};
use crate::Error::CommonError;
use crate::{ColumnFileId, LevelId, TseriesFamilyId};

#[derive(Debug)]
pub struct ColumnFile {
    file_id: ColumnFileId,
    level: LevelId,
    is_delta: bool,
    time_range: TimeRange,
    size: u64,
    field_id_filter: Arc<BloomFilter>,
    deleted: AtomicBool,
    compacting: AtomicBool,

    path: PathBuf,
    tsm_reader_cache: Weak<ShardedCache<String, Arc<TsmReader>>>,
}

impl ColumnFile {
    pub fn with_compact_data(
        meta: &CompactMeta,
        path: impl AsRef<Path>,
        field_id_filter: Arc<BloomFilter>,
        tsm_reader_cache: Weak<ShardedCache<String, Arc<TsmReader>>>,
    ) -> Self {
        Self {
            file_id: meta.file_id,
            level: meta.level,
            is_delta: meta.is_delta,
            time_range: TimeRange::new(meta.min_ts, meta.max_ts),
            size: meta.file_size,
            field_id_filter,
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

    pub fn file_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn overlap(&self, time_range: &TimeRange) -> bool {
        self.time_range.overlaps(time_range)
    }

    pub fn contains_field_id(&self, field_id: FieldId) -> bool {
        self.field_id_filter.contains(&field_id.to_be_bytes())
    }

    pub fn contains_any_field_id(&self, field_ids: &[FieldId]) -> bool {
        for field_id in field_ids {
            if self.field_id_filter.contains(&field_id.to_be_bytes()) {
                return true;
            }
        }
        false
    }

    pub async fn add_tombstone(&self, field_ids: &[FieldId], time_range: &TimeRange) -> Result<()> {
        let dir = self.path.parent().expect("file has parent");
        // TODO flock tombstone file.
        let mut tombstone = TsmTombstone::open(dir, self.file_id).await?;
        tombstone.add_range(field_ids, time_range).await?;
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
        debug!("Removing file {}", self.file_id);
        if self.is_deleted() {
            let path = self.file_path();
            if let Some(cache) = self.tsm_reader_cache.upgrade() {
                let k = format!("{}", path.display());
                tokio::spawn(async move {
                    cache.remove(&k).await;
                });
            }

            if let Err(e) = std::fs::remove_file(&path) {
                error!(
                    "Error when removing file {} at '{}': {}",
                    self.file_id,
                    path.display(),
                    e.to_string()
                );
            }
            info!("Removed file {} at '{}", self.file_id, path.display());
        }
    }
}

impl Display for ColumnFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{ level:{}, file_id:{}, compacting:{}, time_range:{}-{}, file size:{} }}",
            self.level,
            self.file_id,
            if self.is_compacting() {
                "True"
            } else {
                "False"
            },
            self.time_range.min_ts,
            self.time_range.max_ts,
            self.size,
        )
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
            field_id_filter: Arc::new(BloomFilter::default()),
            deleted: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            path: path.as_ref().into(),
            tsm_reader_cache: Weak::new(),
        }
    }

    pub fn set_field_id_filter(&mut self, field_id_filter: Arc<BloomFilter>) {
        self.field_id_filter = field_id_filter;
    }
}

#[derive(Debug)]
pub struct LevelInfo {
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
        field_filter: Arc<BloomFilter>,
        tsm_reader_cache: Weak<ShardedCache<String, Arc<TsmReader>>>,
    ) {
        let file_path = if compact_meta.is_delta {
            let base_dir = self.storage_opt.delta_dir(&self.database, self.tsf_id);
            make_delta_file_name(base_dir, compact_meta.file_id)
        } else {
            let base_dir = self.storage_opt.tsm_dir(&self.database, self.tsf_id);
            make_tsm_file_name(base_dir, compact_meta.file_id)
        };
        self.files.push(Arc::new(ColumnFile::with_compact_data(
            compact_meta,
            file_path,
            field_filter,
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

    pub async fn read_column_file(
        &self,
        _tf_id: u32,
        field_id: FieldId,
        time_range: &TimeRange,
    ) -> Vec<DataBlock> {
        let time_ranges = Arc::new(TimeRanges::with_inclusive_bounds(
            time_range.min_ts,
            time_range.max_ts,
        ));
        let mut data = vec![];
        for file in self.files.iter() {
            if file.is_deleted() || !file.overlap(time_range) {
                continue;
            }

            let tsm_reader = match TsmReader::open(file.file_path()).await {
                Ok(tr) => tr,
                Err(e) => {
                    error!("failed to load tsm reader, in case {:?}", e);
                    return vec![];
                }
            };
            for idx in tsm_reader.index_iterator_opt(field_id) {
                for blk in idx.block_iterator_opt(time_ranges.clone()) {
                    if let Ok(blk) = tsm_reader.get_data_block(&blk).await {
                        data.push(blk);
                    }
                }
            }
        }
        data
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
}

impl Display for LevelInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{ L:{}, files: [ ", self.level)?;
        for (i, file) in self.files.iter().enumerate() {
            write!(f, "{}", file.as_ref())?;
            if i < self.files.len() - 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, "] }}")
    }
}

#[derive(Debug)]
pub struct Version {
    pub ts_family_id: TseriesFamilyId,
    pub database: Arc<String>,
    pub storage_opt: Arc<StorageOptions>,
    /// The max seq_no of write batch in wal flushed to column file.
    pub last_seq: u64,
    /// The max timestamp of write batch in wal flushed to column file.
    pub max_level_ts: i64,
    pub levels_info: [LevelInfo; 5],
    pub tsm_reader_cache: Arc<ShardedCache<String, Arc<TsmReader>>>,
}

impl Version {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ts_family_id: TseriesFamilyId,
        database: Arc<String>,
        storage_opt: Arc<StorageOptions>,
        last_seq: u64,
        levels_info: [LevelInfo; 5],
        max_level_ts: i64,
        tsm_reader_cache: Arc<ShardedCache<String, Arc<TsmReader>>>,
    ) -> Self {
        Self {
            ts_family_id,
            database,
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
            self.database.clone(),
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
                let field_filter = file_metas.remove(&file.file_id).unwrap_or_default();
                new_levels[level.level as usize].push_compact_meta(
                    file,
                    field_filter,
                    weak_tsm_reader_cache.clone(),
                );
            }
            new_levels[level.level as usize].update_time_range();
        }

        let mut new_version = Self {
            ts_family_id: self.ts_family_id,
            database: self.database.clone(),
            storage_opt: self.storage_opt.clone(),
            last_seq: last_seq.unwrap_or(self.last_seq),
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

    pub fn database(&self) -> Arc<String> {
        self.database.clone()
    }

    pub fn levels_info(&self) -> &[LevelInfo; 5] {
        &self.levels_info
    }

    pub fn storage_opt(&self) -> Arc<StorageOptions> {
        self.storage_opt.clone()
    }

    pub fn column_files(
        &self,
        field_ids: &[FieldId],
        time_range: &TimeRange,
    ) -> Vec<Arc<ColumnFile>> {
        self.levels_info
            .iter()
            .filter(|level| level.time_range.overlaps(time_range))
            .flat_map(|level| {
                level.files.iter().filter(|f| {
                    f.time_range().overlaps(time_range) && f.contains_any_field_id(field_ids)
                })
            })
            .cloned()
            .collect()
    }

    // todo:
    pub fn get_ts_overlap(&self, _level: u32, _ts_min: i64, _ts_max: i64) -> Vec<Arc<ColumnFile>> {
        vec![]
    }

    pub async fn get_tsm_reader(&self, path: impl AsRef<Path>) -> Result<Arc<TsmReader>> {
        let path = format!("{}", path.as_ref().display());
        let tsm_reader = match self.tsm_reader_cache.get(&path).await {
            Some(val) => val.clone(),
            None => {
                let mut lock = self.tsm_reader_cache.lock_shard(&path).await;
                match lock.get(&path) {
                    Some(val) => val.clone(),
                    None => {
                        let tsm_reader = TsmReader::open(&path).await?;
                        lock.insert(path, Arc::new(tsm_reader)).unwrap().clone()
                    }
                }
            }
        };
        Ok(tsm_reader)
    }
}

#[derive(Debug)]
pub struct CacheGroup {
    pub mut_cache: Arc<RwLock<MemCache>>,
    pub immut_cache: Vec<Arc<RwLock<MemCache>>>,
}

impl CacheGroup {
    pub fn read_field_data(
        &self,
        field_id: FieldId,
        mut time_predicate: impl FnMut(Timestamp) -> bool,
        mut value_predicate: impl FnMut(&FieldVal) -> bool,
        mut handle_data: impl FnMut(DataType),
    ) {
        self.immut_cache.iter().for_each(|m| {
            m.read().read_field_data(
                field_id,
                &mut time_predicate,
                &mut value_predicate,
                &mut handle_data,
            );
        });

        self.mut_cache.read().read_field_data(
            field_id,
            time_predicate,
            value_predicate,
            handle_data,
        );
    }

    pub fn read_series_timestamps(
        &self,
        series_ids: &[SeriesId],
        mut time_predicate: impl FnMut(Timestamp) -> bool,
        mut handle_data: impl FnMut(Timestamp),
    ) {
        self.immut_cache.iter().for_each(|m| {
            m.read()
                .read_series_timestamps(series_ids, &mut time_predicate, &mut handle_data);
        });

        self.mut_cache
            .read()
            .read_series_timestamps(series_ids, time_predicate, &mut handle_data);
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
pub struct TseriesFamily {
    tf_id: TseriesFamilyId,
    database: Arc<String>,
    mut_cache: Arc<RwLock<MemCache>>,
    immut_cache: Vec<Arc<RwLock<MemCache>>>,
    super_version: Arc<SuperVersion>,
    super_version_id: AtomicU64,
    version: Arc<Version>,
    cache_opt: Arc<CacheOptions>,
    storage_opt: Arc<StorageOptions>,
    seq_no: u64,
    last_modified: Arc<tokio::sync::RwLock<Option<Instant>>>,
    flush_task_sender: Sender<FlushReq>,
    compact_task_sender: Sender<CompactTask>,
    cancellation_token: CancellationToken,
    memory_pool: MemoryPoolRef,
    tsf_metrics: TsfMetrics,
    status: VnodeStatus,
}

impl TseriesFamily {
    pub const MAX_DATA_BLOCK_SIZE: u32 = 1000;
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tf_id: TseriesFamilyId,
        database: Arc<String>,
        cache: MemCache,
        version: Arc<Version>,
        cache_opt: Arc<CacheOptions>,
        storage_opt: Arc<StorageOptions>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
        memory_pool: MemoryPoolRef,
        register: &Arc<MetricsRegister>,
    ) -> Self {
        let mm = Arc::new(RwLock::new(cache));

        Self {
            tf_id,
            database: database.clone(),
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
            flush_task_sender,
            compact_task_sender,
            cancellation_token: CancellationToken::new(),
            memory_pool,
            tsf_metrics: TsfMetrics::new(register, database.as_str(), tf_id as u64),
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

        if !force && filtered_caches.len() < self.cache_opt.max_immutable_number as usize {
            return None;
        }

        // Mark these caches marked as `flushing` in current thread and collect them.
        filtered_caches.retain(|c| c.read().mark_flushing());
        if filtered_caches.is_empty() {
            return None;
        }

        Some(FlushReq {
            ts_family_id: self.tf_id,
            mems: filtered_caches,
            force_flush: force,
        })
    }

    /// Try to build a `FlushReq` by immutable caches,
    /// if succeed, send it to flush job.
    pub(crate) async fn send_flush_req(&mut self, force: bool) {
        if let Some(req) = self.build_flush_req(force) {
            self.flush_task_sender
                .send(req)
                .await
                .expect("error send flush req to kvcore");
        }
    }

    pub fn put_points(
        &self,
        seq: u64,
        points: HashMap<(SeriesId, SchemaId), RowGroup>,
    ) -> Result<u64> {
        if self.status == VnodeStatus::Copying {
            return Err(CommonError {
                reason: "vnode is moving please retry later".to_string(),
            });
        }
        let mut res = 0;
        for ((sid, _schema_id), group) in points {
            let mem = self.mut_cache.read();
            res += group.rows.len();
            mem.write_group(sid, seq, group)?;
        }
        Ok(res as u64)
    }

    pub async fn check_to_flush(&mut self) {
        if self.mut_cache.read().is_full() {
            info!(
                "mut_cache is full, switch to immutable. current pool_size : {}",
                self.memory_pool.reserved()
            );
            self.switch_to_immutable();
        }
        if self.immut_cache.len() >= self.cache_opt.max_immutable_number as usize {
            self.send_flush_req(false).await;
        }
    }

    pub async fn update_last_modified(&self) {
        *self.last_modified.write().await = Some(Instant::now());
    }

    pub fn update_status(&mut self, status: VnodeStatus) {
        self.status = status;
    }

    pub fn delete_columns(&self, field_ids: &[FieldId]) {
        self.mut_cache.read().delete_columns(field_ids);
        for memcache in self.immut_cache.iter() {
            memcache.read().delete_columns(field_ids);
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

    pub fn close(&self) {
        self.cancellation_token.cancel();
    }

    /// Snapshots last version before `last_seq` of this vnode.
    ///
    /// Db-files' index data (field-id filter) will be inserted into `file_metas`.
    pub fn snapshot(
        &self,
        owner: Arc<String>,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) -> VersionEdit {
        let mut version_edit =
            VersionEdit::new_add_vnode(self.tf_id, owner.as_ref().clone(), self.seq_no);
        let version = self.version();
        let max_level_ts = version.max_level_ts;
        for files in version.levels_info.iter() {
            for file in files.files.iter() {
                let mut meta = CompactMeta::from(file.as_ref());
                meta.tsf_id = files.tsf_id;
                meta.high_seq = self.seq_no;
                version_edit.add_file(meta, max_level_ts);
                file_metas.insert(file.file_id, file.field_id_filter.clone());
            }
        }

        version_edit
    }

    pub fn tf_id(&self) -> TseriesFamilyId {
        self.tf_id
    }

    pub fn database(&self) -> Arc<String> {
        self.database.clone()
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
        self.storage_opt.delta_dir(&self.database, self.tf_id)
    }

    pub fn get_tsm_dir(&self) -> PathBuf {
        self.storage_opt.tsm_dir(&self.database, self.tf_id)
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
        if !self.cancellation_token.is_cancelled() {
            self.cancellation_token.cancel();
        }
    }
}

pub fn schedule_vnode_compaction(runtime: Arc<Runtime>, vnode: Arc<AsyncRwLock<TseriesFamily>>) {
    let _jh = runtime.spawn(async move {
        let vnode_rlock = vnode.read().await;
        let tsf_id = vnode_rlock.tf_id;
        let compact_trigger_cold_duration = vnode_rlock.storage_opt.compact_trigger_cold_duration;
        let last_modified = vnode_rlock.last_modified.clone();
        let compact_trigger_file_num = vnode_rlock.storage_opt.compact_trigger_file_num as usize;
        let compact_task_sender = vnode_rlock.compact_task_sender.clone();
        let cancellation_token = vnode_rlock.cancellation_token.clone();
        drop(vnode_rlock);

        if compact_trigger_cold_duration == Duration::ZERO {} else {
            let mut check_interval = tokio::time::interval(Duration::from_secs(10));
            check_interval.tick().await;

            loop {
                tokio::select! {
                    _ = check_interval.tick() => {
                        // Check if vnode is cold.
                        let ts_rlock = last_modified.read().await;
                        if let Some(t) = *ts_rlock {
                            if t.elapsed() >= compact_trigger_cold_duration {
                                drop(ts_rlock);
                                let mut ts_wlock = last_modified.write().await;
                                *ts_wlock = Some(Instant::now());
                                if let Err(e) = compact_task_sender.send(CompactTask::Cold(tsf_id)).await {
                                    warn!("failed to send compact task({}), {}", tsf_id, e);
                                }
                            }
                        }

                        // Check if level-0 files is more than DEFAULT_COMPACT_TRIGGER_DETLA_FILE_NUM
                        let version = vnode.read().await.super_version().version.clone();
                        let mut level0_files = 0_usize;
                        for file in version.levels_info()[0].files.iter() {
                            if !file.is_compacting() {
                                level0_files += 1;
                            }
                        }
                        if level0_files >= compact_trigger_file_num {
                            if let Err(e) = compact_task_sender.send(CompactTask::Delta(tsf_id)).await {
                                warn!("failed to send compact task({}), {}", tsf_id, e);
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

#[cfg(test)]
pub mod test_tseries_family {
    use std::collections::HashMap;
    use std::mem::size_of;
    use std::sync::Arc;

    use lru_cache::asynchronous::ShardedCache;
    use memory_pool::{GreedyMemoryPool, MemoryPoolRef};
    use meta::model::meta_admin::AdminMeta;
    use meta::model::MetaRef;
    use metrics::metric_register::MetricsRegister;
    use models::schema::{DatabaseSchema, TenantOptions};
    use models::Timestamp;
    use parking_lot::RwLock;
    use tokio::sync::mpsc::{self, Receiver};
    use tokio::sync::RwLock as AsyncRwLock;

    use super::{ColumnFile, LevelInfo};
    use crate::compaction::flush_tests::default_table_schema;
    use crate::compaction::{run_flush_memtable_job, FlushReq};
    use crate::context::{GlobalContext, GlobalSequenceContext};
    use crate::file_utils::make_tsm_file_name;
    use crate::kv_option::{Options, StorageOptions};
    use crate::kvcore::{COMPACT_REQ_CHANNEL_CAP, SUMMARY_REQ_CHANNEL_CAP};
    use crate::memcache::{FieldVal, MemCache, RowData, RowGroup};
    use crate::summary::{CompactMeta, SummaryTask, VersionEdit};
    use crate::tseries_family::{TimeRange, TseriesFamily, Version};
    use crate::tsm::TsmTombstone;
    use crate::version_set::VersionSet;
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
                    Arc::new(ColumnFile::new(3, 1, TimeRange::new(3001, 3100), 100, false, make_tsm_file_name(&tsm_dir, 3))),
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
                    Arc::new(ColumnFile::new(1, 2, TimeRange::new(1, 1000), 1000, false, make_tsm_file_name(&tsm_dir, 1))),
                    Arc::new(ColumnFile::new(2, 2, TimeRange::new(1001, 2000), 1000, false, make_tsm_file_name(&tsm_dir, 2))),
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
        let tsm_reader_cache = Arc::new(ShardedCache::with_capacity(16));
        #[rustfmt::skip]
            let version = Version::new(1, database, opt.storage.clone(), 1, levels, 3100, tsm_reader_cache);
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
                    Arc::new(ColumnFile::new(3, 1, TimeRange::new(3001, 3100), 100, false, make_tsm_file_name(&tsm_dir, 3))),
                    Arc::new(ColumnFile::new(4, 1, TimeRange::new(3051, 3150), 100, false, make_tsm_file_name(&tsm_dir, 4))),
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
                    Arc::new(ColumnFile::new(1, 2, TimeRange::new(1, 1000), 1000, false, make_tsm_file_name(&tsm_dir, 1))),
                    Arc::new(ColumnFile::new(2, 2, TimeRange::new(1001, 2000), 1000, false, make_tsm_file_name(&tsm_dir, 2))),
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
        let tsm_reader_cache = Arc::new(ShardedCache::with_capacity(16));
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

        let tsm_reader_cache = Arc::new(ShardedCache::with_capacity(16));
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

    #[tokio::test]
    pub async fn test_tsf_delete() {
        let dir = "/tmp/test/ts_family/tsf_delete";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));
        let memory_pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let (flush_task_sender, _) = mpsc::channel(opt.storage.flush_req_channel_cap);
        let (compact_task_sender, _) = mpsc::channel(COMPACT_REQ_CHANNEL_CAP);
        let database = Arc::new("db".to_string());
        let tsf = TseriesFamily::new(
            0,
            database.clone(),
            MemCache::new(0, 500, 2, 0, &memory_pool),
            Arc::new(Version::new(
                0,
                database.clone(),
                opt.storage.clone(),
                0,
                LevelInfo::init_levels(database, 0, opt.storage.clone()),
                0,
                Arc::new(ShardedCache::with_capacity(1)),
            )),
            opt.cache.clone(),
            opt.storage.clone(),
            flush_task_sender,
            compact_task_sender,
            memory_pool,
            &Arc::new(MetricsRegister::default()),
        );

        let row_group = RowGroup {
            schema: default_table_schema(vec![0, 1, 2]).into(),
            range: TimeRange {
                min_ts: 1,
                max_ts: 100,
            },
            rows: vec![RowData {
                ts: 10,
                fields: vec![
                    Some(FieldVal::Integer(11)),
                    Some(FieldVal::Integer(12)),
                    Some(FieldVal::Integer(13)),
                ],
            }],
            size: size_of::<RowGroup>() + 3 * size_of::<u32>() + size_of::<Option<FieldVal>>() + 8,
        };
        let mut points = HashMap::new();
        points.insert((0, 0), row_group);
        let _ = tsf.put_points(0, points);

        let mut cached_data = vec![];
        tsf.mut_cache
            .read()
            .read_field_data(0, |_| true, |_| true, |d| cached_data.push(d));
        assert_eq!(cached_data.len(), 1);
        tsf.delete_series(
            &[0],
            &TimeRange {
                min_ts: 0,
                max_ts: 200,
            },
        );
        cached_data.clear();
        tsf.mut_cache
            .read()
            .read_field_data(0, |_| true, |_| true, |d| cached_data.push(d));
        assert!(cached_data.is_empty());
    }

    // Util function for testing with summary modification.
    async fn update_ts_family_version(
        version_set: Arc<tokio::sync::RwLock<VersionSet>>,
        ts_family_id: TseriesFamilyId,
        mut summary_task_receiver: Receiver<SummaryTask>,
    ) {
        let mut version_edits: Vec<VersionEdit> = Vec::new();
        let mut min_seq: u64 = 0;
        while let Some(summary_task) = summary_task_receiver.recv().await {
            for edit in summary_task.request.version_edits.into_iter() {
                if edit.tsf_id == ts_family_id {
                    version_edits.push(edit.clone());
                    if edit.has_seq_no {
                        min_seq = edit.seq_no;
                    }
                }
            }
        }
        let version_set = version_set.write().await;
        if let Some(ts_family) = version_set.get_tsfamily_by_tf_id(ts_family_id).await {
            let mut ts_family = ts_family.write().await;
            let new_version = ts_family.version().copy_apply_version_edits(
                version_edits,
                &mut HashMap::new(),
                Some(min_seq),
            );
            ts_family.new_version(new_version, None);
        }
    }

    #[test]
    pub fn test_read_with_tomb() {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .build()
                .unwrap(),
        );

        let config = config::get_config_for_test();
        let meta_manager: MetaRef = runtime.block_on(async {
            let meta_manager: MetaRef = AdminMeta::new(config.clone()).await;

            meta_manager.add_data_node().await.unwrap();

            let _ = meta_manager
                .create_tenant("cnosdb".to_string(), TenantOptions::default())
                .await;
            meta_manager
        });
        let memory_pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem = MemCache::new(0, 1000, 2, 0, &memory_pool);
        let row_group = RowGroup {
            schema: default_table_schema(vec![0, 1, 2]).into(),
            range: TimeRange {
                min_ts: 1,
                max_ts: 100,
            },
            rows: vec![RowData {
                ts: 10,
                fields: vec![
                    Some(FieldVal::Integer(11)),
                    Some(FieldVal::Integer(12)),
                    Some(FieldVal::Integer(13)),
                ],
            }],
            size: size_of::<RowGroup>() + 3 * size_of::<u32>() + size_of::<Option<FieldVal>>() + 8,
        };
        mem.write_group(1, 0, row_group).unwrap();

        let mem = Arc::new(RwLock::new(mem));
        let req_mem = vec![mem];
        let flush_seq = FlushReq {
            ts_family_id: 0,
            mems: req_mem,
            force_flush: false,
        };

        let dir = "/tmp/test/ts_family/read_with_tomb";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let tenant = "cnosdb".to_string();
        let database = "test_db".to_string();
        let global_ctx = Arc::new(GlobalContext::new());
        let global_seq_ctx = GlobalSequenceContext::empty();
        let (summary_task_sender, summary_task_receiver) = mpsc::channel(SUMMARY_REQ_CHANNEL_CAP);
        let (compact_task_sender, _compact_task_receiver) = mpsc::channel(COMPACT_REQ_CHANNEL_CAP);
        let (flush_task_sender, _) = mpsc::channel(opt.storage.flush_req_channel_cap);
        let runtime_ref = runtime.clone();
        runtime.block_on(async move {
            let version_set = Arc::new(AsyncRwLock::new(
                VersionSet::new(
                    meta_manager.clone(),
                    opt.clone(),
                    runtime_ref.clone(),
                    memory_pool.clone(),
                    HashMap::new(),
                    flush_task_sender.clone(),
                    compact_task_sender.clone(),
                    Arc::new(MetricsRegister::default()),
                )
                .await
                .unwrap(),
            ));
            version_set
                .write()
                .await
                .create_db(
                    DatabaseSchema::new(&tenant, &database),
                    meta_manager.clone(),
                    memory_pool,
                )
                .await
                .unwrap();
            let db = version_set
                .write()
                .await
                .get_db(&tenant, &database)
                .unwrap();
            let cxt = Arc::new(GlobalContext::new());
            let ts_family_id = db
                .write()
                .await
                .add_tsfamily(
                    0,
                    0,
                    None,
                    summary_task_sender.clone(),
                    flush_task_sender.clone(),
                    compact_task_sender.clone(),
                    cxt.clone(),
                )
                .await
                .unwrap()
                .read()
                .await
                .tf_id();

            run_flush_memtable_job(
                flush_seq,
                global_ctx,
                global_seq_ctx,
                version_set.clone(),
                summary_task_sender,
                Some(compact_task_sender),
            )
            .await
            .unwrap();

            update_ts_family_version(version_set.clone(), ts_family_id, summary_task_receiver)
                .await;

            let version_set = version_set.write().await;
            let tsf = version_set
                .get_database_tsfs(&tenant, &database)
                .await
                .unwrap()
                .first()
                .cloned()
                .unwrap();
            let version = tsf.write().await.version();
            version.levels_info[1]
                .read_column_file(
                    ts_family_id,
                    0,
                    &TimeRange {
                        max_ts: 0,
                        min_ts: 0,
                    },
                )
                .await;

            let file = version.levels_info[1].files[0].clone();

            let dir = opt.storage.tsm_dir(&database, ts_family_id);
            let mut tombstone = TsmTombstone::open(dir, file.file_id).await.unwrap();
            tombstone
                .add_range(&[0], &TimeRange::new(0, 0))
                .await
                .unwrap();
            tombstone.flush().await.unwrap();

            version.levels_info[1]
                .read_column_file(
                    0,
                    0,
                    &TimeRange {
                        max_ts: 0,
                        min_ts: 0,
                    },
                )
                .await;
        });
    }
}
