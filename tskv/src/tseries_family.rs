use std::{
    borrow::BorrowMut,
    cell::{Ref, RefCell},
    ops::DerefMut,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use models::ValueType;
use tokio::sync::RwLock;

use crate::{
    kv_option::TseriesFamOpt,
    memcache::{DataType, MemCache},
    summary::CompactMeta,
};

#[derive(Default)]
pub struct TimeRange {
    pub max_ts: i64,
    pub min_ts: i64,
}

impl TimeRange {
    pub fn new(max_ts: i64, min_ts: i64) -> Self {
        Self { max_ts, min_ts }
    }
    pub fn overlaps(&self, range: TimeRange) -> bool {
        if self.min_ts > range.max_ts || self.max_ts < range.min_ts { false } else { true }
    }
}

pub struct ColumnFile {
    file_id: u64,
    being_compact: AtomicBool,
    deleted: AtomicBool,
    range: TimeRange, // file time range
    size: u64,        // file size
}

impl ColumnFile {
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn range(&self) -> &TimeRange {
        &self.range
    }
}

impl ColumnFile {
    pub fn mark_removed(&self) {
        self.deleted.store(true, Ordering::Release);
    }

    pub fn mark_compaction(&self) {
        self.being_compact.store(true, Ordering::Release);
    }

    pub fn is_pending_compaction(&self) -> bool {
        self.being_compact.load(Ordering::Acquire)
    }
}

#[derive(Default)]
pub struct LevelInfo {
    pub files: Vec<Arc<ColumnFile>>,
    pub level: u32,
    pub cur_size: u64,
    pub max_size: u64,
    pub ts_range: TimeRange,
}

impl LevelInfo {
    pub fn init(level: u32) -> Self {
        Self { files: Vec::new(),
               level,
               cur_size: 0,
               max_size: 0,
               ts_range: TimeRange { max_ts: 0, min_ts: 0 } }
    }
    pub fn apply(&mut self, delta: &CompactMeta) {
        self.files.push(Arc::new(ColumnFile { file_id: delta.file_id,
                                              being_compact: AtomicBool::new(false),
                                              deleted: AtomicBool::new(false),
                                              range: TimeRange::new(delta.ts_max,
                                                                    delta.ts_min),
                                              size: delta.file_size }));
        // todo: get file size
        // self.cur_size = ;
        if self.ts_range.max_ts < delta.ts_max {
            self.ts_range.max_ts = delta.ts_max;
        }
        if self.ts_range.min_ts > delta.ts_max {
            self.ts_range.min_ts = delta.ts_min;
        }
    }

    pub fn level(&self) -> u32 {
        self.level
    }
}

#[derive(Default)]
pub struct Version {
    pub id: u32,
    pub log_no: u64,
    pub name: String,
    pub levels_info: Vec<LevelInfo>,
}

impl Version {
    pub fn new(id: u32, log_no: u64, name: String, levels_info: Vec<LevelInfo>) -> Self {
        Self { id, log_no, name, levels_info }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn levels_info(&self) -> &Vec<LevelInfo> {
        &self.levels_info
    }

    // todo:
    pub fn get_ts_overlap(&self, level: u32, ts_min: i64, ts_max: i64) -> Vec<Arc<ColumnFile>> {
        vec![]
    }
}

pub struct SuperVersion {
    pub id: u32,
    pub mut_cache: Arc<RwLock<MemCache>>,
    pub immut_cache: Vec<Arc<RwLock<MemCache>>>,
    pub cur_version: Arc<Version>,
    pub opt: Arc<TseriesFamOpt>,
    pub version_id: u64,
}

impl SuperVersion {
    pub fn new(id: u32,
               mut_cache: Arc<RwLock<MemCache>>,
               immut_cache: Vec<Arc<RwLock<MemCache>>>,
               cur_version: Arc<Version>,
               opt: Arc<TseriesFamOpt>,
               version_id: u64)
               -> Self {
        Self { id, mut_cache, immut_cache, cur_version, opt, version_id }
    }
}

pub struct TseriesFamily {
    tf_id: u32,
    mut_cache: Arc<RwLock<MemCache>>,
    immut_cache: Vec<Arc<RwLock<MemCache>>>, // todo: need to del RwLock in memcache
    super_version: Arc<SuperVersion>,
    super_version_id: AtomicU64,
    version: Arc<Version>,
    opts: Arc<TseriesFamOpt>,
    // min seq_no keep in the tsfam memcache
    seq_no: u64,
}

// todo: cal ref count
impl TseriesFamily {
    pub fn new(tf_id: u32,
               name: String,
               cache: MemCache,
               version: Arc<Version>,
               opt: TseriesFamOpt)
               -> Self {
        let mm = Arc::new(RwLock::new(cache));
        let cf = Arc::new(opt);
        Self { tf_id,
               seq_no: version.log_no,
               mut_cache: mm.clone(),
               immut_cache: Default::default(),
               super_version: Arc::new(SuperVersion::new(tf_id,
                                                         mm,
                                                         Default::default(),
                                                         version.clone(),
                                                         cf.clone(),
                                                         0)),
               super_version_id: AtomicU64::new(0),
               version: version,
               opts: cf }
    }

    pub fn switch_memcache(&mut self, cache: Arc<RwLock<MemCache>>) {
        self.immut_cache.push(self.mut_cache.clone());
        self.super_version_id.fetch_add(1, Ordering::SeqCst);
        let vers = SuperVersion::new(self.tf_id,
                                     cache.clone(),
                                     self.immut_cache.clone(),
                                     self.version.clone(),
                                     self.opts.clone(),
                                     self.super_version_id.load(Ordering::SeqCst));
        self.super_version = Arc::new(vers);
        self.mut_cache = cache;
    }

    pub async fn put_mutcache(&self, fid: u64, val: &[u8], dtype: ValueType, seq: u64, ts: i64) {
        let mut mem = self.super_version.mut_cache.write().await;
        let _ = mem.insert_raw(seq, fid, ts, dtype, val);
    }
}
