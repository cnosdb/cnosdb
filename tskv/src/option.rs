pub struct Options {
    pub(crate) num_cpu: usize,
    pub(crate) task_buffer_size: usize,
    pub(crate) lrucache: CacheConfig,
    // pub(crate) write_batch: WriteBatchConfig,
    pub(crate) compact_conf: CompactConfig,
}

pub struct CacheConfig {}

pub struct WriteBatchConfig {}

pub struct CompactConfig {}

pub struct TimeRange {}
pub struct QueryOption {
    timerange: TimeRange,
    db: String,
    table: String,
    series_id: u64,
}
