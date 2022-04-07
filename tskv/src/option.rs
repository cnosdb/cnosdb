#[allow(dead_code)]
pub struct Options {
    pub(crate) front_cpu: usize,
    pub(crate) back_cpu: usize,
    pub(crate) enable_wal: bool,
    pub(crate) task_buffer_size: usize,
    pub(crate) lrucache: CacheConfig,
    // pub(crate) write_batch: WriteBatchConfig,
    pub(crate) compact_conf: CompactConfig,
}

impl Options{
    //todo: 
    pub fn from_env() -> Self{
        Self{
            .. Default::default()
        }
    }

}

impl Default for Options {
    fn default() -> Self {
        Self {
            front_cpu: Default::default(),
            back_cpu: Default::default(),
            enable_wal: Default::default(),
            task_buffer_size: Default::default(),
            lrucache: Default::default(),
            compact_conf: Default::default(),
        }
    }
}
#[allow(dead_code)]
#[derive(Default)]
pub struct CacheConfig {}

#[allow(dead_code)]
pub struct WriteBatchConfig {}
#[derive(Default)]
pub struct CompactConfig {}

pub struct TimeRange {}

#[allow(dead_code)]
pub struct QueryOption {
    timerange: TimeRange,
    db: String,
    table: String,
    series_id: u64,
}
