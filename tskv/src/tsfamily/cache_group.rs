use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::{SeriesId, Timestamp};
use parking_lot::RwLock;

use crate::mem_cache::memcache::{MemCache, MemCacheStatistics};

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
