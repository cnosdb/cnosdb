#![allow(dead_code)]

pub const MAX_MEMCACHE_SIZE: u64 = 100 * 1024 * 1024; //100M

pub struct Options {
    pub(crate) front_cpu: usize,
    pub(crate) back_cpu: usize,
    pub(crate) task_buffer_size: usize,
    pub(crate) lrucache: CacheConfig,
    pub(crate) wal: WalConfig,
    // pub(crate) write_batch: WriteBatchConfig,
    pub(crate) compact_conf: CompactConfig,
}

impl Options {
    //todo:
    pub fn from_env() -> Self {
        Self {
            ..Default::default()
        }
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            front_cpu: Default::default(),
            back_cpu: Default::default(),
            task_buffer_size: Default::default(),
            lrucache: Default::default(),
            wal: Default::default(),
            compact_conf: Default::default(),
        }
    }
}

#[derive(Default)]
pub struct CacheConfig {}

#[allow(dead_code)]
#[derive(Clone)]
pub struct WalConfig {
    pub enabled: bool,
    pub dir: String,
    pub sync: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: Default::default(),
            sync: true,
        }
    }
}

#[allow(dead_code)]
pub struct WriteBatchConfig {}
#[derive(Default)]
pub struct CompactConfig {}

pub struct TimeRange {}

pub struct QueryOption {
    timerange: TimeRange,
    db: String,
    table: String,
    series_id: u64,
}

#[derive(Clone, PartialEq)]
pub struct TseriesFamOpt {
    max_level: u32,
    max_bytes_for_level_base: u64,
    level_ratio: f64,
    target_file_size_base: u64,
    file_number_compact_trigger: u32,
    max_compact_size: u64,
}

impl Default for TseriesFamOpt {
    fn default() -> Self {
        Self {
            max_level: 6,
            max_bytes_for_level_base: 256 * 1024 * 1024,
            level_ratio: 16f64,
            target_file_size_base: 64 * 1024 * 1024,
            file_number_compact_trigger: 4,
            max_compact_size: 2 * 1024 * 1024 * 1024,
        }
    }
}

pub struct TseriesFamDesc {
    pub name: String,
    pub opt: TseriesFamOpt,
}

pub struct MemCacheOpt {
    tf_id: u32,
    max_size: u64,
    seq_no: u64,
}

impl Default for MemCacheOpt {
    fn default() -> Self {
        Self {
            tf_id: 0,
            max_size: 100 * 1024 * 1024,
            seq_no: 0,
        }
    }
}
