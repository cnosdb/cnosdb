use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use memory_pool::MemoryPoolRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeStatus;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::database_schema::DatabaseConfig;
use models::schema::tskv_table_schema::TableColumn;
use models::{ColumnId, SeriesId, SeriesKey};
use parking_lot::RwLock;
use snafu::ResultExt;
use tokio::sync::RwLock as TokioRwLock;
use tokio::time::Instant;
use trace::{debug, info};
use utils::BloomFilter;

use super::cache_group::CacheGroup;
use super::super_version::SuperVersion;
use super::tsf_metrics::TsfMetrics;
use super::version::Version;
use crate::context::GlobalContext;
use crate::error::{CommonSnafu, IndexErrSnafu, TskvResult};
use crate::index::ts_index::TSIndex;
use crate::kv_option::StorageOptions;
use crate::mem_cache::memcache::MemCache;
use crate::mem_cache::series_data::RowGroup;
use crate::summary::{CompactMeta, VersionEdit};
use crate::{ColumnFileId, Options, TseriesFamilyId};

#[derive(Debug)]
pub struct TsfFactory {
    // "tenant.db"
    owner: Arc<String>,
    options: Arc<Options>,
    ctx: Arc<GlobalContext>,
    db_config: Arc<DatabaseConfig>,
    memory_pool: MemoryPoolRef,
    metrics_register: Arc<MetricsRegister>,
}
impl TsfFactory {
    pub fn new(
        owner: Arc<String>,
        options: Arc<Options>,
        ctx: Arc<GlobalContext>,
        db_config: Arc<DatabaseConfig>,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Self {
        Self {
            owner,
            options,
            ctx,
            db_config,
            memory_pool,
            metrics_register,
        }
    }

    pub fn create_tsf(
        &self,
        tf_id: TseriesFamilyId,
        version: Arc<Version>,
    ) -> Arc<TokioRwLock<TseriesFamily>> {
        let mut_cache = Arc::new(RwLock::new(MemCache::new(
            tf_id,
            self.ctx.file_id_next(),
            self.ctx.file_id_next(),
            self.db_config.max_memcache_size(),
            self.db_config.memcache_partitions() as usize,
            version.last_seq(),
            &self.memory_pool,
        )));
        let tsf_metrics =
            TsfMetrics::new(&self.metrics_register, self.owner.as_str(), tf_id as u64);
        let super_version = Arc::new(SuperVersion::new(
            tf_id,
            CacheGroup {
                mut_cache: mut_cache.clone(),
                immut_cache: vec![],
            },
            version.clone(),
            0,
        ));

        let tsfamily = Arc::new(TokioRwLock::new(TseriesFamily {
            tf_id,
            ctx: self.ctx.clone(),
            owner: self.owner.clone(),
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
        }));
        let weak_tsfamily = Arc::downgrade(&tsfamily);
        tokio::spawn(TseriesFamily::update_vnode_metrics(weak_tsfamily));

        tsfamily
    }

    pub fn drop_tsf(&self, tf_id: u32) {
        //todo other's thing may need to drop
        TsfMetrics::drop(&self.metrics_register, self.owner.as_str(), tf_id as u64);
    }
}

#[derive(Debug)]
pub struct TseriesFamily {
    tf_id: TseriesFamilyId,
    ctx: Arc<GlobalContext>,
    owner: Arc<String>,
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
        owner: Arc<String>,
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
            ctx: Arc::new(GlobalContext::new()),
            owner: owner.clone(),
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
            tsf_metrics: TsfMetrics::new(register, owner.as_str(), tf_id as u64),
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
            self.ctx.file_id_next(),
            self.ctx.file_id_next(),
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
        let owner = (*self.owner).clone();
        let seq_no = version.last_seq();
        let max_level_ts = version.max_level_ts();

        let mut version_edit = VersionEdit::new_add_vnode(self.tf_id, owner, seq_no);
        for files in version.levels_info().iter() {
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
        for files in version.levels_info().iter() {
            for file in files.files.iter() {
                let bloom_filter = file.load_bloom_filter().await?;
                file_metas.insert(file.file_id(), bloom_filter);
            }
        }

        Ok(file_metas)
    }

    pub async fn rebuild_index(&self) -> TskvResult<Arc<tokio::sync::RwLock<TSIndex>>> {
        let path = self.storage_opt.index_dir(self.owner.as_str(), self.tf_id);
        let _ = std::fs::remove_dir_all(path.clone());

        let capacity = self.storage_opt.index_cache_capacity;
        let index = TSIndex::new(path, capacity).await.context(IndexErrSnafu)?;
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
        for level in self.version().levels_info().iter() {
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

    pub fn owner(&self) -> Arc<String> {
        self.owner.clone()
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
        self.storage_opt.delta_dir(&self.owner, self.tf_id)
    }

    pub fn get_tsm_dir(&self) -> PathBuf {
        self.storage_opt.tsm_dir(&self.owner, self.tf_id)
    }

    pub fn disk_storage(&self) -> u64 {
        self.version()
            .levels_info()
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

    async fn update_vnode_metrics(tsfamily: Weak<TokioRwLock<TseriesFamily>>) {
        let start = tokio::time::Instant::now() + Duration::from_secs(10);
        let interval = Duration::from_secs(10);
        let mut intv = tokio::time::interval_at(start, interval);
        loop {
            intv.tick().await;
            match tsfamily.upgrade() {
                Some(tsf_strong_ref) => {
                    let cache_size = {
                        let tsfamily = tsf_strong_ref.read().await;
                        tsfamily.cache_size()
                    };
                    tsf_strong_ref
                        .write()
                        .await
                        .tsf_metrics
                        .record_cache_size(cache_size);
                }
                None => {
                    break;
                }
            }
        }
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
    use models::predicate::domain::TimeRange;
    use models::Timestamp;

    use crate::file_utils::make_tsm_file;
    use crate::kv_option::Options;
    use crate::summary::{CompactMeta, VersionEdit};
    use crate::tsfamily::column_file::ColumnFile;
    use crate::tsfamily::level_info::LevelInfo;
    use crate::tsfamily::version::Version;

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
                owner: database.clone(),
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
                owner: database.clone(),
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

        assert_eq!(new_version.last_seq(), 3);
        assert_eq!(new_version.max_level_ts(), 3150);

        let lvl = new_version.levels_info().get(1).unwrap();
        assert_eq!(lvl.time_range, TimeRange::new(3051, 3150));
        assert_eq!(lvl.files.len(), 1);
        let col_file = lvl.files.first().unwrap();
        assert_eq!(*col_file.time_range(), TimeRange::new(3051, 3150));
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
                owner: database.clone(),
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
                owner: database.clone(),
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

        assert_eq!(new_version.last_seq(), 3);
        assert_eq!(new_version.max_level_ts(), 3150);

        let lvl = new_version.levels_info().get(1).unwrap();
        assert_eq!(
            lvl.time_range,
            TimeRange::new(Timestamp::MAX, Timestamp::MIN)
        );
        assert_eq!(lvl.files.len(), 0);

        let lvl = new_version.levels_info().get(2).unwrap();
        assert_eq!(lvl.time_range, TimeRange::new(3001, 3150));
        let col_file = lvl.files.last().unwrap();
        assert_eq!(*col_file.time_range(), TimeRange::new(3001, 3150));

        let lvl = new_version.levels_info().get(3).unwrap();
        assert_eq!(lvl.time_range, TimeRange::new(1, 2000));
        assert_eq!(lvl.files.len(), 1);
        let col_file = lvl.files.last().unwrap();
        assert_eq!(*col_file.time_range(), TimeRange::new(1, 2000));
    }
}
