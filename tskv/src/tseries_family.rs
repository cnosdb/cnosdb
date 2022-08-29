use std::{
    borrow::{Borrow, BorrowMut},
    cmp::{max, min},
    collections::{HashMap, HashSet},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    rc::Rc,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc,
    },
};

use config::get_config;
use lazy_static::lazy_static;
use models::{FieldId, InMemPoint, Timestamp, ValueType};
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc::UnboundedSender;
use trace::{debug, error, info, warn};
use utils::BloomFilter;

use crate::tsm::DataBlock;
use crate::{
    compaction::{CompactReq, FlushReq, LevelCompactionPicker, Picker},
    direct_io::{File, FileCursor},
    error::{Error, Result},
    file_manager, file_utils,
    kv_option::{TseriesFamDesc, TseriesFamOpt},
    memcache::{DataType, MemCache, MemRaw},
    summary::{CompactMeta, VersionEdit},
    tsm::{ColumnReader, IndexReader, TsmReader, TsmTombstone},
    ColumnFileId, LevelId, TseriesFamilyId,
};

lazy_static! {
    pub static ref FLUSH_REQ: Arc<Mutex<Vec<FlushReq>>> = Arc::new(Mutex::new(vec![]));
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub min_ts: i64,
    pub max_ts: i64,
}

impl TimeRange {
    pub fn new(min_ts: i64, max_ts: i64) -> Self {
        Self { min_ts, max_ts }
    }

    #[inline(always)]
    pub fn overlaps(&self, range: &TimeRange) -> bool {
        !(self.min_ts > range.max_ts || self.max_ts < range.min_ts)
    }

    #[inline(always)]
    pub fn includes(&self, other: &TimeRange) -> bool {
        self.min_ts <= other.min_ts && self.max_ts >= other.max_ts
    }

    #[inline(always)]
    pub fn merge(&mut self, other: &TimeRange) {
        self.min_ts = self.min_ts.min(other.min_ts);
        self.max_ts = self.max_ts.max(other.max_ts);
    }
}

impl From<(Timestamp, Timestamp)> for TimeRange {
    fn from(time_range: (Timestamp, Timestamp)) -> Self {
        Self {
            min_ts: time_range.0,
            max_ts: time_range.1,
        }
    }
}

impl From<TimeRange> for (Timestamp, Timestamp) {
    fn from(t: TimeRange) -> Self {
        (t.min_ts, t.max_ts)
    }
}

#[derive(Debug)]
pub struct ColumnFile {
    file_id: ColumnFileId,
    level: LevelId,
    is_delta: bool,
    time_range: TimeRange,
    size: u64,
    field_id_bloom_filter: BloomFilter,
    deleted: AtomicBool,
    compacting: AtomicBool,

    ts_family_opt: Arc<TseriesFamOpt>,
}

impl ColumnFile {
    pub fn new(
        file_id: ColumnFileId,
        level: LevelId,
        time_range: TimeRange,
        size: u64,
        is_delta: bool,
        ts_family_opt: Arc<TseriesFamOpt>,
    ) -> Self {
        Self {
            file_id,
            level,
            is_delta,
            time_range,
            size,
            field_id_bloom_filter: BloomFilter::new(512),
            deleted: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            ts_family_opt,
        }
    }

    pub fn with_compact_data(meta: &CompactMeta, ts_family_opt: Arc<TseriesFamOpt>) -> Self {
        Self::new(
            meta.file_id,
            meta.level,
            TimeRange::new(meta.min_ts, meta.max_ts),
            meta.file_size,
            meta.is_delta,
            ts_family_opt,
        )
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

    pub fn file_path(&self, tsf_opt: Arc<TseriesFamOpt>, tf_id: u32) -> PathBuf {
        if self.is_delta {
            file_utils::make_delta_file_name(tsf_opt.delta_dir(tf_id), self.file_id)
        } else {
            file_utils::make_tsm_file_name(tsf_opt.tsm_dir(tf_id), self.file_id)
        }
    }

    pub fn file_reader(&self, tf_id: u32) -> Result<(FileCursor, u64), Error> {
        let fs = if self.is_delta {
            let file_name = format!("_{:06}.delta", self.file_id());
            file_manager::open_file(self.ts_family_opt.delta_dir(tf_id).join(file_name))
        } else {
            let file_name = format!("_{:06}.tsm", self.file_id());
            file_manager::open_file(self.ts_family_opt.tsm_dir(tf_id).join(file_name))
        };
        match fs {
            Ok(v) => {
                let len = v.len();
                Ok((v.into_cursor(), len))
            }
            Err(err) => Err(err),
        }
    }

    pub fn overlap(&self, time_range: &TimeRange) -> bool {
        self.time_range.overlaps(time_range)
    }
}

impl ColumnFile {
    pub fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::Acquire)
    }

    pub fn mark_removed(&self) {
        self.deleted.store(true, Ordering::Release);
    }

    pub fn is_compacting(&self) -> bool {
        self.compacting.load(Ordering::Acquire)
    }

    pub fn mark_compaction(&self) {
        self.compacting.store(true, Ordering::Release);
    }

    pub fn contains_field_id(&self, field_id: FieldId) -> bool {
        self.field_id_bloom_filter.contains(&field_id.to_be_bytes())
    }
}

#[derive(Debug)]
pub struct LevelInfo {
    pub files: Vec<Arc<ColumnFile>>,
    pub tsf_id: u32,
    pub tsf_opt: Arc<TseriesFamOpt>,
    pub level: u32,
    pub cur_size: u64,
    pub max_size: u64,
    pub time_range: TimeRange,
}

impl LevelInfo {
    pub fn init(level: u32, tsf_opt: Arc<TseriesFamOpt>) -> Self {
        let max_size = tsf_opt.level_file_size(level);
        Self {
            files: Vec::new(),
            tsf_id: 0,
            tsf_opt,
            level,
            cur_size: 0,
            max_size,
            time_range: TimeRange {
                min_ts: Timestamp::MAX,
                max_ts: Timestamp::MIN,
            },
        }
    }

    pub fn init_levels(tsf_opt: Arc<TseriesFamOpt>) -> [LevelInfo; 5] {
        [
            Self::init(0, tsf_opt.clone()),
            Self::init(1, tsf_opt.clone()),
            Self::init(2, tsf_opt.clone()),
            Self::init(3, tsf_opt.clone()),
            Self::init(4, tsf_opt),
        ]
    }

    pub fn push_compact_meta(&mut self, compact_meta: &CompactMeta) {
        self.files.push(Arc::new(ColumnFile::with_compact_data(
            compact_meta,
            self.tsf_opt.clone(),
        )));
        self.tsf_id = compact_meta.tsf_id;
        self.cur_size += compact_meta.file_size;
        self.time_range.max_ts = self.time_range.max_ts.max(compact_meta.max_ts);
        self.time_range.min_ts = self.time_range.min_ts.min(compact_meta.min_ts);
    }

    pub fn push_column_file(&mut self, file: Arc<ColumnFile>) {
        self.cur_size += file.size;
        self.time_range.max_ts = self.time_range.max_ts.max(file.time_range.max_ts);
        self.time_range.min_ts = self.time_range.min_ts.min(file.time_range.min_ts);
        self.files.push(file);
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

    pub fn read_column_file(
        &self,
        tf_id: u32,
        field_id: FieldId,
        time_range: &TimeRange,
    ) -> Vec<DataBlock> {
        let mut data = vec![];
        for file in self.files.iter() {
            if file.is_deleted() || !file.overlap(time_range) {
                continue;
            }

            let tsm_reader = match TsmReader::open(file.file_path(self.tsf_opt.clone(), tf_id)) {
                Ok(tr) => tr,
                Err(e) => {
                    error!("failed to load tsm reader, in case {:?}", e);
                    return vec![];
                }
            };
            for idx in tsm_reader.index_iterator_opt(field_id) {
                for blk in idx.block_iterator_opt(time_range) {
                    if let Ok(blk) = tsm_reader.get_data_block(&blk) {
                        data.push(blk);
                    }
                }
            }
        }
        data
    }

    pub fn level(&self) -> u32 {
        self.level
    }
}

#[derive(Debug)]
pub struct Version {
    pub ts_family_id: TseriesFamilyId,
    pub ts_family_name: String,
    pub ts_family_opt: Arc<TseriesFamOpt>,
    /// The max seq_no of write batch in wal flushed to column file.
    pub last_seq: u64,
    /// The max timestamp of write batch in wal flushed to column file.
    pub max_level_ts: i64,
    pub levels_info: [LevelInfo; 5],
}

impl Version {
    pub fn new(
        ts_family_id: TseriesFamilyId,
        ts_family_name: String,
        ts_family_opt: Arc<TseriesFamOpt>,
        last_seq: u64,
        levels_info: [LevelInfo; 5],
        max_level_ts: i64,
    ) -> Self {
        Self {
            ts_family_id,
            ts_family_name,
            ts_family_opt,
            last_seq,
            max_level_ts,
            levels_info,
        }
    }

    /// Creates new Version using current Version and `VersionEdit`s.
    pub fn copy_apply_version_edits(
        &self,
        version_edits: Vec<VersionEdit>,
        last_seq: Option<u64>,
    ) -> Version {
        let mut added_files: HashMap<LevelId, Vec<CompactMeta>> = HashMap::new();
        let mut deleted_files: HashMap<LevelId, HashSet<ColumnFileId>> = HashMap::new();
        for ve in version_edits {
            if !ve.add_files.is_empty() {
                ve.add_files.into_iter().for_each(|f| {
                    added_files.entry(f.level).or_insert(Vec::new()).push(f);
                });
            }
            if !ve.del_files.is_empty() {
                ve.del_files.into_iter().for_each(|f| {
                    deleted_files
                        .entry(f.level)
                        .or_insert(HashSet::new())
                        .insert(f.file_id);
                });
            }
        }

        let mut new_levels = LevelInfo::init_levels(self.ts_family_opt.clone());
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if let Some(true) = deleted_files
                    .get(&file.level)
                    .map(|file_ids| file_ids.contains(&file.file_id))
                {
                    continue;
                }
                new_levels[level.level as usize].push_column_file(file.clone());
            }
            if let Some(files) = added_files.get(&level.level) {
                for file in files.iter() {
                    new_levels[level.level as usize].push_compact_meta(file);
                }
            }
            added_files.remove(&level.level);
            new_levels[level.level as usize].update_time_range();
        }

        let mut new_version = Self {
            ts_family_id: self.ts_family_id,
            ts_family_name: self.ts_family_name.clone(),
            ts_family_opt: self.ts_family_opt.clone(),
            last_seq: last_seq.unwrap_or(self.last_seq),
            max_level_ts: self.max_level_ts,
            levels_info: new_levels,
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

    pub fn tf_name(&self) -> &str {
        &self.ts_family_name
    }

    pub fn levels_info(&self) -> &[LevelInfo; 5] {
        &self.levels_info
    }

    pub fn ts_family_opt(&self) -> Arc<TseriesFamOpt> {
        self.ts_family_opt.clone()
    }

    // todo:
    pub fn get_ts_overlap(&self, level: u32, ts_min: i64, ts_max: i64) -> Vec<Arc<ColumnFile>> {
        vec![]
    }
}

#[derive(Debug)]
pub struct CacheGroup {
    pub delta_mut_cache: Arc<RwLock<MemCache>>,
    pub delta_immut_cache: Vec<Arc<RwLock<MemCache>>>,
    pub mut_cache: Arc<RwLock<MemCache>>,
    pub immut_cache: Vec<Arc<RwLock<MemCache>>>,
}

#[derive(Debug)]
pub struct SuperVersion {
    pub ts_family_id: u32,
    pub ts_family_opt: Arc<TseriesFamOpt>,
    pub caches: CacheGroup,
    pub version: Arc<Version>,
    pub version_number: u64,
}

impl SuperVersion {
    pub fn new(
        ts_family_id: u32,
        ts_family_opt: Arc<TseriesFamOpt>,
        caches: CacheGroup,
        version: Arc<Version>,
        version_number: u64,
    ) -> Self {
        Self {
            ts_family_id,
            ts_family_opt,
            caches,
            version,
            version_number,
        }
    }
}

#[derive(Debug)]
pub struct TseriesFamily {
    tf_id: TseriesFamilyId,
    delta_mut_cache: Arc<RwLock<MemCache>>,
    delta_immut_cache: Vec<Arc<RwLock<MemCache>>>,
    mut_cache: Arc<RwLock<MemCache>>,
    immut_cache: Vec<Arc<RwLock<MemCache>>>,
    // todo: need to del RwLock in memcache
    super_version: Arc<SuperVersion>,
    super_version_id: AtomicU64,
    version: Arc<Version>,
    opts: Arc<TseriesFamOpt>,
    compact_picker: Arc<dyn Picker>,
    // min seq_no keep in the tsfam memcache
    seq_no: u64,
    immut_ts_min: AtomicI64,
    mut_ts_max: AtomicI64,
}

// todo: cal ref count
impl TseriesFamily {
    pub fn new(
        tf_id: TseriesFamilyId,
        name: String,
        cache: MemCache,
        version: Arc<Version>,
        tsf_opt: Arc<TseriesFamOpt>,
    ) -> Self {
        let mm = Arc::new(RwLock::new(cache));
        let seq = version.last_seq;
        let max_level_ts = version.max_level_ts;
        let delta_mm = Arc::new(RwLock::new(MemCache::new(
            tf_id,
            tsf_opt.max_memcache_size,
            seq,
            true,
        )));
        Self {
            tf_id,
            seq_no: seq,
            delta_mut_cache: delta_mm.clone(),
            delta_immut_cache: Default::default(),
            mut_cache: mm.clone(),
            immut_cache: Default::default(),
            super_version: Arc::new(SuperVersion::new(
                tf_id,
                tsf_opt.clone(),
                CacheGroup {
                    delta_mut_cache: delta_mm,
                    delta_immut_cache: Default::default(),
                    mut_cache: mm,
                    immut_cache: Default::default(),
                },
                version.clone(),
                0,
            )),
            super_version_id: AtomicU64::new(0),
            version,
            opts: tsf_opt,
            compact_picker: Arc::new(LevelCompactionPicker::new()),
            immut_ts_min: AtomicI64::new(max_level_ts),
            mut_ts_max: AtomicI64::new(i64::MIN),
        }
    }

    pub async fn switch_memcache(&mut self, cache: Arc<RwLock<MemCache>>) {
        self.super_version
            .caches
            .mut_cache
            .write()
            .switch_to_immutable();
        self.immut_cache.push(self.mut_cache.clone());
        self.new_super_version(self.version.clone());
        self.mut_cache = cache;
    }

    fn new_super_version(&mut self, version: Arc<Version>) {
        self.super_version_id.fetch_add(1, Ordering::SeqCst);
        self.super_version = Arc::new(SuperVersion::new(
            self.tf_id,
            self.opts.clone(),
            CacheGroup {
                delta_mut_cache: self.delta_mut_cache.clone(),
                delta_immut_cache: self.delta_immut_cache.clone(),
                mut_cache: self.mut_cache.clone(),
                immut_cache: self.immut_cache.clone(),
            },
            version,
            self.super_version_id.load(Ordering::SeqCst),
        ))
    }

    /// Set new Version into current TsFamily,
    /// then create new SuperVersion, update seq_no
    pub fn new_version(&mut self, new_version: Version) {
        let version = Arc::new(new_version);
        self.new_super_version(version.clone());
        self.seq_no = version.last_seq;
        self.version = version;
    }

    pub async fn switch_to_immutable(&mut self) {
        self.super_version
            .caches
            .mut_cache
            .write()
            .switch_to_immutable();

        self.immut_cache.push(self.mut_cache.clone());
        self.mut_cache = Arc::from(RwLock::new(MemCache::new(
            self.tf_id,
            self.opts.max_memcache_size,
            self.seq_no,
            false,
        )));
        self.new_super_version(self.version.clone());
    }

    pub async fn switch_to_delta_immutable(&mut self) {
        self.delta_mut_cache.write().switch_to_immutable();
        self.delta_immut_cache.push(self.delta_mut_cache.clone());
        self.delta_mut_cache = Arc::new(RwLock::new(MemCache::new(
            self.tf_id,
            self.opts.max_memcache_size,
            self.seq_no,
            true,
        )));
        self.new_super_version(self.version.clone());
    }

    async fn wrap_delta_flush_req(&mut self, sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>) {
        if self.delta_mut_cache.read().is_empty() {
            return;
        }

        info!("delta memcache full or other,switch to immutable");
        self.switch_to_delta_immutable().await;
        let len = self.delta_immut_cache.len();
        let mut immut = vec![];
        for i in self.delta_immut_cache.iter() {
            if !i.read().flushed {
                immut.push(i.clone());
            }
        }
        self.delta_immut_cache = immut;

        if len != self.delta_immut_cache.len() {
            self.new_super_version(self.version.clone());
        }

        let mut req_mem = vec![];
        for i in self.delta_immut_cache.iter() {
            let read_i = i.read();
            if read_i.flushing {
                continue;
            }
            req_mem.push((self.tf_id, i.clone()));
        }
        if req_mem.is_empty() {
            return;
        }
        for i in req_mem.iter() {
            i.1.write().flushing = true;
        }
        FLUSH_REQ.lock().push(FlushReq {
            mems: req_mem,
            wait_req: 0,
        });
        info!(
            "delta flush_req send,now req queue len : {}",
            FLUSH_REQ.lock().len()
        );
        sender
            .send(FLUSH_REQ.clone())
            .expect("error send flush req to kvcore");
    }

    async fn wrap_flush_req(&mut self, sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>) {
        let len = self.immut_cache.len();
        let mut imut = vec![];
        for i in self.immut_cache.iter() {
            if !i.read().flushed {
                imut.push(i.clone());
            }
        }
        self.immut_cache = imut;

        if len != self.immut_cache.len() {
            self.new_super_version(self.version.clone());
        }

        self.immut_ts_min
            .store(self.mut_ts_max.load(Ordering::Relaxed), Ordering::Relaxed);
        let mut req_mem = vec![];
        for i in self.immut_cache.iter() {
            let read_i = i.read();
            if read_i.flushing {
                continue;
            }
            req_mem.push((self.tf_id, i.clone()));
        }

        if req_mem.len() < self.opts.max_immemcache_num as usize {
            return;
        }

        for i in req_mem.iter() {
            i.1.write().flushing = true;
        }

        self.wrap_delta_flush_req(sender.clone()).await;
        FLUSH_REQ.lock().push(FlushReq {
            mems: req_mem,
            wait_req: 0,
        });
        info!(
            "flush_req send,now req queue len : {}",
            FLUSH_REQ.lock().len()
        );
        sender
            .send(FLUSH_REQ.clone())
            .expect("error send flush req to kvcore");
    }

    pub async fn put_points(&self, seq: u64, points: &Vec<InMemPoint>) {
        for p in points.iter() {
            let sid = p.series_id();
            for f in p.fields().iter() {
                self.put_mutcache(&mut MemRaw {
                    seq,
                    ts: p.timestamp,
                    field_id: f.field_id(),
                    field_type: f.value_type,
                    val: &f.value,
                })
                .await;
            }
        }
    }

    pub async fn check_to_flush(&mut self, sender: UnboundedSender<Arc<Mutex<Vec<FlushReq>>>>) {
        if self.super_version.caches.mut_cache.read().is_full() {
            info!("mut_cache full,switch to immutable");
            self.switch_to_immutable().await;
            if self.immut_cache.len() >= self.opts.max_immemcache_num as usize {
                self.wrap_flush_req(sender.clone()).await;
            }
        }

        if self.super_version.caches.delta_mut_cache.read().is_full() {
            self.wrap_delta_flush_req(sender.clone()).await;
        }
    }

    // todo(Subsegment) : (&mut self) will case performance regression.we must get writeLock to get
    // version_set when we insert each point
    pub async fn put_mutcache(&self, raw: &mut MemRaw<'_>) {
        if raw.ts >= self.immut_ts_min.load(Ordering::Relaxed) {
            if raw.ts > self.mut_ts_max.load(Ordering::Relaxed) {
                self.mut_ts_max.store(raw.ts, Ordering::Relaxed);
            }
            let mem = self.super_version.caches.mut_cache.write();
            let _ = mem.insert_raw(raw);
        } else {
            let delta_mem = self.super_version.caches.delta_mut_cache.write();
            let _ = delta_mem.insert_raw(raw);
        }
    }

    pub async fn delete_cache(&self, time_range: &TimeRange) {
        self.mut_cache.read().delete_data(time_range);
        self.delta_mut_cache.read().delete_data(time_range);

        for memcache in self.immut_cache.iter() {
            memcache.read().delete_data(time_range);
        }
    }

    pub fn pick_compaction(&self) -> Option<CompactReq> {
        self.compact_picker.pick_compaction(self.version.clone())
    }

    pub fn tf_id(&self) -> TseriesFamilyId {
        self.tf_id
    }

    pub fn cache(&self) -> &Arc<RwLock<MemCache>> {
        &self.mut_cache
    }

    pub fn delta_cache(&self) -> &Arc<RwLock<MemCache>> {
        &self.delta_mut_cache
    }

    pub fn delta_immut_cache(&self) -> &Vec<Arc<RwLock<MemCache>>> {
        &self.delta_immut_cache
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

    pub fn options(&self) -> Arc<TseriesFamOpt> {
        self.opts.clone()
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, path::PathBuf, sync::Arc};

    use config::get_config;
    use models::{Timestamp, ValueType};
    use parking_lot::{Mutex, RwLock};
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::UnboundedReceiver;
    use trace::info;

    use crate::memcache::MemRaw;
    use crate::summary::SummaryTask;
    use crate::{
        compaction::{run_flush_memtable_job, FlushReq},
        context::GlobalContext,
        file_manager,
        kv_option::TseriesFamOpt,
        memcache::MemCache,
        summary::{CompactMeta, VersionEdit},
        tseries_family::{TimeRange, TseriesFamily, Version},
        tsm::TsmTombstone,
        version_set::VersionSet,
        TseriesFamilyId,
    };

    use super::{ColumnFile, LevelInfo};

    #[test]
    fn test_version_apply_version_edits_1() {
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
        let global_config = get_config("../config/config.toml");
        let tsf_opt = Arc::new(TseriesFamOpt::from(global_config));
        #[rustfmt::skip]
        let version = Version {
            ts_family_id: 1, ts_family_name: "test".to_string(),
            ts_family_opt: tsf_opt.clone(),
            last_seq: 1, max_level_ts: 3100,
            levels_info: [
                LevelInfo::init(0, tsf_opt.clone()),
                LevelInfo {
                    files: vec![
                        Arc::new(ColumnFile::new(3, 1, TimeRange::new(3001, 3100), 100, false, tsf_opt.clone())),
                    ],
                    tsf_id: 1, tsf_opt: tsf_opt.clone(),
                    level: 1, cur_size: 100, max_size: 1000,
                    time_range: TimeRange::new(3001, 3100),
                },
                LevelInfo {
                    files: vec![
                        Arc::new(ColumnFile::new(1, 2, TimeRange::new(1, 1000), 1000, false, tsf_opt.clone())),
                        Arc::new(ColumnFile::new(2, 2, TimeRange::new(1001, 2000), 1000, false, tsf_opt.clone())),
                    ],
                    tsf_id: 1, tsf_opt: tsf_opt.clone(),
                    level: 2, cur_size: 2000, max_size: 10000,
                    time_range: TimeRange::new(1, 2000),
                },
                LevelInfo::init(3, tsf_opt.clone()),
                LevelInfo::init(4, tsf_opt),
            ],
        };
        let mut version_edits = Vec::new();
        let mut ve = VersionEdit::new();
        #[rustfmt::skip]
        ve.add_file(
            CompactMeta {
                file_id: 4, file_size: 100, tsf_id: 1, level: 1,
                min_ts: 3051, max_ts: 3150, high_seq: 2, low_seq: 2,
                is_delta: false,
            },
            3100,
        );
        version_edits.push(ve);
        let mut ve = VersionEdit::new();
        ve.del_file(1, 3, false);
        version_edits.push(ve);
        let new_version = version.copy_apply_version_edits(version_edits, Some(3));

        assert_eq!(new_version.last_seq, 3);
        assert_eq!(new_version.max_level_ts, 3150);

        let lvl = new_version.levels_info.get(1).unwrap();
        assert_eq!(lvl.time_range, TimeRange::new(3051, 3150));
        assert_eq!(lvl.files.len(), 1);
        let col_file = lvl.files.first().unwrap();
        assert_eq!(col_file.time_range, TimeRange::new(3051, 3150));
    }

    #[test]
    fn test_version_apply_version_edits_2() {
        //! There is a Version with two levels:
        //! - Lv.0: [ ]
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
        let global_config = get_config("../config/config.toml");
        let tsf_opt = Arc::new(TseriesFamOpt::from(global_config));
        #[rustfmt::skip]
        let version = Version {
            ts_family_id: 1, ts_family_name: "test".to_string(),
            ts_family_opt: tsf_opt.clone(),
            last_seq: 1, max_level_ts: 3150,
            levels_info: [
                LevelInfo::init(0, tsf_opt.clone()),
                LevelInfo {
                    files: vec![
                        Arc::new(ColumnFile::new(3, 1, TimeRange::new(3001, 3100), 100, false, tsf_opt.clone())),
                        Arc::new(ColumnFile::new(4, 1, TimeRange::new(3051, 3150), 100, false, tsf_opt.clone())),
                    ],
                    tsf_id: 1, tsf_opt: tsf_opt.clone(),
                    level: 1, cur_size: 100, max_size: 1000,
                    time_range: TimeRange::new(3001, 3150),
                },
                LevelInfo {
                    files: vec![
                        Arc::new(ColumnFile::new(1, 2, TimeRange::new(1, 1000), 1000, false, tsf_opt.clone())),
                        Arc::new(ColumnFile::new(2, 2, TimeRange::new(1001, 2000), 1000, false, tsf_opt.clone())),
                    ],
                    tsf_id: 1, tsf_opt: tsf_opt.clone(),
                    level: 2, cur_size: 2000, max_size: 10000,
                    time_range: TimeRange::new(1, 2000),
                },
                LevelInfo::init(3, tsf_opt.clone()),
                LevelInfo::init(4, tsf_opt),
            ],
        };
        let mut version_edits = Vec::new();
        let mut ve = VersionEdit::new();
        #[rustfmt::skip]
        ve.add_file(
            CompactMeta {
                file_id: 5, file_size: 150, tsf_id: 1, level: 2,
                min_ts: 3001, max_ts: 3150, high_seq: 2, low_seq: 2,
                is_delta: false,
            },
            3150,
        );
        #[rustfmt::skip]
        ve.add_file(
            CompactMeta {
                file_id: 6, file_size: 2000, tsf_id: 1, level: 3,
                min_ts: 1, max_ts: 2000, high_seq: 2, low_seq: 2,
                is_delta: false,
            },
            3150,
        );
        version_edits.push(ve);
        let mut ve = VersionEdit::new();
        ve.del_file(1, 3, false);
        ve.del_file(1, 4, false);
        ve.del_file(2, 1, false);
        ve.del_file(2, 2, false);
        version_edits.push(ve);
        let new_version = version.copy_apply_version_edits(version_edits, Some(3));

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

    #[tokio::test]
    pub async fn test_tsf_delete() {
        let global_config = get_config("../config/config.toml");
        let tcfg = Arc::new(TseriesFamOpt::from(global_config));
        let tsf = TseriesFamily::new(
            0,
            "db".to_string(),
            MemCache::new(0, 500, 0, false),
            Arc::new(Version::new(
                0,
                "db".to_string(),
                tcfg.clone(),
                0,
                LevelInfo::init_levels(tcfg.clone()),
                0,
            )),
            tcfg.clone(),
        );

        tsf.put_mutcache(&mut MemRaw {
            seq: 0,
            ts: 0,
            field_id: 0,
            field_type: ValueType::Integer,
            val: 10_i32.to_be_bytes().as_slice(),
        })
        .await;
        assert_eq!(tsf.mut_cache.read().get(&0).unwrap().read().cells.len(), 1);
        tsf.delete_cache(&TimeRange {
            max_ts: 0,
            min_ts: 0,
        })
        .await;
        assert_eq!(tsf.mut_cache.read().get(&0).unwrap().read().cells.len(), 0);
    }

    async fn update_ts_family_version(
        version_set: Arc<RwLock<VersionSet>>,
        ts_family_id: TseriesFamilyId,
        mut summary_task_receiver: UnboundedReceiver<SummaryTask>,
    ) {
        let mut version_edits: Vec<VersionEdit> = Vec::new();
        let mut min_seq: u64 = 0;
        while let Some(summary_task) = summary_task_receiver.recv().await {
            for edit in summary_task.edits.into_iter() {
                if edit.tsf_id == ts_family_id {
                    version_edits.push(edit.clone());
                    if edit.has_seq_no {
                        min_seq = edit.seq_no;
                    }
                }
            }
        }
        let version_set = version_set.write();
        if let Some(ts_family) = version_set.get_tsfamily_by_tf_id(ts_family_id) {
            let mut ts_family = ts_family.write();
            let new_version = ts_family
                .version()
                .copy_apply_version_edits(version_edits, Some(min_seq));
            ts_family.new_version(new_version);
        }
    }

    #[tokio::test]
    pub async fn test_read_with_tomb() {
        let dir = PathBuf::from("db/tsm/test/0".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let dir = PathBuf::from("dev/db".to_string());
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mem = MemCache::new(0, 1000, 0, false);
        mem.insert_raw(&mut MemRaw {
            seq: 0,
            ts: 0,
            field_id: 0,
            field_type: ValueType::Integer,
            val: 10_i64.to_be_bytes().as_slice(),
        })
        .unwrap();
        let mem = Arc::new(RwLock::new(mem));
        let req_mem = vec![(0, mem)];
        let flush_seq = Arc::new(Mutex::new(vec![FlushReq {
            mems: req_mem,
            wait_req: 0,
        }]));

        let kernel = Arc::new(GlobalContext::new());
        let global_config = get_config("../config/config.toml");
        let mut cfg = TseriesFamOpt::from(global_config);
        cfg.tsm_dir = "db/tsm/test".to_string();
        let cfg = Arc::new(cfg);
        let (summary_task_sender, summary_task_receiver) = mpsc::unbounded_channel();
        let (compact_task_sender, compact_task_receiver) = mpsc::unbounded_channel();

        let version_set: Arc<RwLock<VersionSet>> =
            Arc::new(RwLock::new(VersionSet::new(cfg.clone(), HashMap::new())));
        version_set.write().create_db(&"test".to_string());
        let db = version_set.write().get_db(&"test".to_string()).unwrap();

        db.write()
            .add_tsfamily(0, 0, 0, cfg.clone(), summary_task_sender.clone());

        run_flush_memtable_job(
            flush_seq,
            kernel,
            version_set.clone(),
            summary_task_sender,
            compact_task_sender,
        )
        .await
        .unwrap();

        update_ts_family_version(version_set.clone(), 0, summary_task_receiver).await;

        let version_set = version_set.write();
        let tsf = version_set
            .get_tsfamily_by_name(&"test".to_string())
            .unwrap();
        let version = tsf.write().version();
        version.levels_info[1].read_column_file(
            tsf.write().tf_id(),
            0,
            &TimeRange {
                max_ts: 0,
                min_ts: 0,
            },
        );
        let file = version.levels_info[1].files[0].clone();

        let mut tombstone = TsmTombstone::open_for_write("dev/db", file.file_id).unwrap();
        tombstone.add_range(&[0], 0, 0).unwrap();
        tombstone.flush().unwrap();
        tombstone.load().unwrap();

        version.levels_info[1].read_column_file(
            0,
            0,
            &TimeRange {
                max_ts: 0,
                min_ts: 0,
            },
        );
    }
}
