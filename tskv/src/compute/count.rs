use std::collections::HashSet;
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::{utils as model_utils, ColumnId, FieldId, SeriesId, Timestamp};
use tokio::runtime::Runtime;
use trace::trace;

use crate::tseries_family::{ColumnFile, SuperVersion};
use crate::tsm::{self, BlockMeta, TsmReader};
use crate::{Error, Result};

#[derive(PartialEq, Eq)]
pub enum TimeRangeCmp {
    /// A has no intersection with B
    Exclude,
    /// A includes B
    Include,
    /// A overlaps with B
    Intersect,
}

enum CountingObject {
    Field(FieldId),
    Series(SeriesId),
}

/// Compute pushed down aggregate:
///
/// `SELECT count(<column>) FROM <table> WHERE <time_range_predicates>`
pub async fn count_column_non_null_values(
    runtime: Arc<Runtime>,
    super_version: Arc<SuperVersion>,
    series_ids: Arc<Vec<SeriesId>>,
    column_id: Option<ColumnId>,
    time_ranges: Arc<TimeRanges>,
) -> Result<u64> {
    let column_files = Arc::new(super_version.column_files(&time_ranges));
    let mut jh_vec = Vec::with_capacity(series_ids.len());

    for series_id in series_ids.iter() {
        let count_object = if let Some(column_id) = column_id {
            let field_id = model_utils::unite_id(column_id, *series_id);
            CountingObject::Field(field_id)
        } else {
            CountingObject::Series(*series_id)
        };
        let sv_inner = super_version.clone();
        let cfs_inner = column_files.clone();

        jh_vec.push(runtime.spawn(count_non_null_values_inner(
            sv_inner,
            count_object,
            cfs_inner,
            time_ranges.clone(),
        )));
    }

    let mut count_sum = 0_u64;
    for jh in jh_vec {
        // JoinHandle returns JoinError if task was paniced.
        count_sum += jh.await.map_err(|e| Error::IO { source: e.into() })??;
    }

    Ok(count_sum)
}

/// Get count of non-null values in time ranges of a field.
async fn count_non_null_values_inner(
    super_version: Arc<SuperVersion>,
    counting_object: CountingObject,
    column_files: Arc<Vec<Arc<ColumnFile>>>,
    time_ranges: Arc<TimeRanges>,
) -> Result<u64> {
    let read_tasks = create_file_read_tasks(
        &super_version,
        &column_files,
        &counting_object,
        &time_ranges,
    )
    .await?;
    // TODO(zipper): Very big HashSet maybe slow insert.
    let (cached_timestamps, cached_time_range) = match counting_object {
        CountingObject::Field(field_id) => {
            get_field_timestamps_in_caches(&super_version, field_id, &time_ranges)
        }
        CountingObject::Series(series_id) => {
            get_series_timestamps_in_caches(&super_version, series_id, &time_ranges)
        }
    };

    let mut count = cached_timestamps.len() as u64;

    let mut grouped_tr = TimeRange::new(i64::MAX, i64::MIN);
    let mut grouped_reader_blk_metas: Vec<ReadTask> = Vec::new();
    for read_task in read_tasks {
        if grouped_reader_blk_metas.is_empty() || grouped_tr.overlaps(&read_task.time_range) {
            // There is no grouped blocks to decode, or still grouping blocks.
            grouped_tr.merge(&read_task.time_range);
        } else {
            grouped_tr = read_task.time_range;
            if grouped_reader_blk_metas.len() == 1 {
                // Only 1 grouped block, maybe no need to decode it.
                let reader_blk_metas = std::mem::take(&mut grouped_reader_blk_metas);
                let b = reader_blk_metas.first().unwrap();
                if cached_time_range.overlaps(&b.time_range) || b.time_range_intersected {
                    trace!(
                        "Length is 1 but cache overlapped, split it: {}",
                        &b.time_range
                    );
                    // Block overlaps with cached data or conditions, need to decode it to remove duplicates.
                    count += count_non_null_values_in_files(
                        reader_blk_metas,
                        &time_ranges,
                        &cached_timestamps,
                        &cached_time_range,
                    )
                    .await?;
                } else {
                    trace!(
                        "Length is 1 and cache doesn't overlapped, don't split: {}",
                        &b.time_range
                    );
                    count += b.block_meta.count() as u64;
                }
            } else {
                // There are grouped blocks which have overlapped time range, need to decode them.
                trace!("Length is {} split them", grouped_reader_blk_metas.len());
                let reader_blk_metas = std::mem::take(&mut grouped_reader_blk_metas);
                count += count_non_null_values_in_files(
                    reader_blk_metas,
                    &time_ranges,
                    &cached_timestamps,
                    &cached_time_range,
                )
                .await?;
            }
        }
        grouped_reader_blk_metas.push(read_task);
    }
    if !grouped_reader_blk_metas.is_empty() {
        trace!("Calculate the last {}", grouped_reader_blk_metas.len());
        count += count_non_null_values_in_files(
            grouped_reader_blk_metas,
            &time_ranges,
            &cached_timestamps,
            &cached_time_range,
        )
        .await?;
    }

    Ok(count)
}

/// Get timestamps of a field in time ranges from all mutable and immutable caches.
fn get_field_timestamps_in_caches(
    super_version: &SuperVersion,
    field_id: FieldId,
    time_ranges: &TimeRanges,
) -> (HashSet<Timestamp>, TimeRange) {
    let time_predicate = |ts| time_ranges.is_boundless() || time_ranges.contains(ts);
    let mut cached_timestamps: HashSet<i64> = HashSet::new();
    let mut cached_time_range = TimeRange::new(i64::MAX, i64::MIN);
    super_version.caches.read_field_data(
        field_id,
        time_predicate,
        |_| true,
        |d| {
            let ts = d.timestamp();
            cached_time_range.min_ts = cached_time_range.min_ts.min(ts);
            cached_time_range.max_ts = cached_time_range.max_ts.max(ts);
            cached_timestamps.insert(ts);
        },
    );

    (cached_timestamps, cached_time_range)
}

/// Get timestamps of series in time ranges from all mutable and immutable caches.
fn get_series_timestamps_in_caches(
    super_version: &SuperVersion,
    series_id: SeriesId,
    time_ranges: &TimeRanges,
) -> (HashSet<Timestamp>, TimeRange) {
    let time_predicate = |ts| time_ranges.is_boundless() || time_ranges.contains(ts);
    let mut cached_timestamps: HashSet<i64> = HashSet::new();
    let mut cached_time_range = TimeRange::new(i64::MAX, i64::MIN);
    super_version
        .caches
        .read_series_timestamps(&[series_id], time_predicate, |ts| {
            cached_time_range.min_ts = cached_time_range.min_ts.min(ts);
            cached_time_range.max_ts = cached_time_range.max_ts.max(ts);
            cached_timestamps.insert(ts);
        });

    (cached_timestamps, cached_time_range)
}

struct ReadTask {
    /// Reader for a file.
    tsm_reader: Arc<TsmReader>,
    /// BlockMeta in a file.
    block_meta: Arc<BlockMeta>,
    /// Time range by BlockMeta::time_range() .
    time_range: TimeRange,
    /// Is time_range is intersected with time range predicates.
    time_range_intersected: bool,
}

/// Filter block metas in files by time ranges, open files and then create file read tasks.
async fn create_file_read_tasks<'a>(
    super_version: &SuperVersion,
    files: &[Arc<ColumnFile>],
    counting_object: &CountingObject,
    time_ranges: &TimeRanges,
) -> Result<Vec<ReadTask>> {
    let mut read_tasks: Vec<ReadTask> = Vec::new();

    for cf in files {
        let path = cf.file_path();
        let reader = super_version.version.get_tsm_reader(&path).await?;
        let idx_meta_iter = match counting_object {
            CountingObject::Field(field_id) => {
                if !cf.contains_field_id(*field_id) {
                    continue;
                }
                reader.index_iterator_opt(*field_id)
            }
            CountingObject::Series(_series_ids) => reader.index_iterator(),
        };
        for idx in idx_meta_iter {
            if let CountingObject::Series(series_id) = counting_object {
                let (_, sid) = model_utils::split_id(idx.field_id());
                if *series_id != sid {
                    continue;
                }
            }
            for blk_meta in idx.block_iterator() {
                let blk_tr = blk_meta.time_range();
                let tr_cmp = if time_ranges.includes(&blk_tr) {
                    // Block is included by conditions, needn't to decode.
                    TimeRangeCmp::Include
                } else if time_ranges.overlaps(&blk_tr) {
                    TimeRangeCmp::Intersect
                } else {
                    TimeRangeCmp::Exclude
                };
                if tr_cmp != TimeRangeCmp::Exclude {
                    read_tasks.push(ReadTask {
                        tsm_reader: reader.clone(),
                        block_meta: Arc::new(blk_meta),
                        time_range: blk_tr,
                        time_range_intersected: tr_cmp == TimeRangeCmp::Intersect,
                    });
                }
            }
        }
    }

    read_tasks.sort_by(|a, b| a.time_range.cmp(&b.time_range));
    Ok(read_tasks)
}

/// Get count of non-null values in time ranges from grouped read tasks.
async fn count_non_null_values_in_files(
    reader_blk_metas: Vec<ReadTask>,
    time_ranges: &TimeRanges,
    cached_timestamps: &HashSet<Timestamp>,
    cached_time_range: &TimeRange,
) -> Result<u64> {
    let mut count = 0_u64;
    let mut ts_set: HashSet<Timestamp> = HashSet::with_capacity(tsm::MAX_BLOCK_VALUES as usize);
    for read_task in reader_blk_metas {
        let blk = read_task
            .tsm_reader
            .get_data_block(&read_task.block_meta)
            .await
            .unwrap();
        let timestamps = blk.ts();
        if cached_time_range.overlaps(&read_task.time_range) {
            trace!(
                "Overlapped with cache {} : {}",
                &cached_time_range,
                &read_task.time_range
            );
            // Time range of this DataBlock overlaps with cached timestamps,
            // need to find the difference and add to count directly.
            for ts in timestamps {
                if !cached_timestamps.contains(ts) {
                    ts_set.insert(*ts);
                }
            }
        } else {
            // Cached timestmaps not contains this DataBlock.
            for tr in time_ranges.time_ranges() {
                let low = match timestamps.binary_search(&tr.min_ts) {
                    Ok(l) => l,
                    Err(l) => {
                        if l == 0 {
                            // Lowest timestamp is smaller than the first timestamp.
                            0
                        } else if l < timestamps.len() {
                            if timestamps[l] >= tr.min_ts {
                                l
                            } else {
                                l + 1
                            }
                        } else {
                            // Lowest timestamp is greater than the last timestamp.
                            break;
                        }
                    }
                };
                let high = match timestamps.binary_search(&tr.max_ts) {
                    Ok(h) => h,
                    Err(h) => {
                        if h == 0 {
                            // Highest timestamp is smaller than the first timestamp.
                            continue;
                        } else if h < timestamps.len() {
                            if timestamps[h] <= tr.max_ts {
                                h
                            } else {
                                h - 1
                            }
                        } else {
                            // Highest timestamp is greater than the last timestamp.
                            timestamps.len() - 1
                        }
                    }
                };
                ts_set.extend(timestamps[low..=high].iter());
            }
        }
    }
    count += ts_set.len() as u64;

    Ok(count)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use memory_pool::{GreedyMemoryPool, MemoryPoolRef};
    use models::predicate::domain::TimeRanges;
    use models::{utils as model_utils, ColumnId, SeriesId};
    use parking_lot::RwLock;
    use tokio::runtime::Runtime;

    use crate::compaction::flush_tests::default_table_schema;
    use crate::compaction::test::write_data_blocks_to_column_file;
    use crate::compute::count::count_column_non_null_values;
    use crate::memcache::test::put_rows_to_cache;
    use crate::memcache::MemCache;
    use crate::tseries_family::test_tseries_family::build_version_by_column_files;
    use crate::tseries_family::{CacheGroup, SuperVersion};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::DataBlock;
    use crate::{Options, Result};

    struct TestHelper {
        runtime: Arc<Runtime>,
        super_version: Arc<SuperVersion>,
    }

    impl TestHelper {
        fn run(
            &self,
            series_ids: &[SeriesId],
            column_id: Option<ColumnId>,
            time_ranges: impl Into<TimeRanges>,
        ) -> Result<u64> {
            let time_ranges = Arc::new(time_ranges.into());
            self.runtime.block_on(count_column_non_null_values(
                self.runtime.clone(),
                self.super_version.clone(),
                Arc::new(series_ids.to_vec()),
                column_id,
                time_ranges,
            ))
        }
    }

    #[test]
    fn test_super_version_count_file() {
        let dir = "/tmp/test/ts_family/super_version_count_file";
        let mut global_config = config::get_config_for_test();
        global_config.storage.path = dir.to_string();

        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![10, 20, 30, 40], val: vec![1, 1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![40, 50, 60], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![70, 80, 90], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
        ];

        let opt = Arc::new(Options::from(&global_config));
        let database = Arc::new("dba".to_string());
        let ts_family_id = 1;
        let dir = opt.storage.tsm_dir(&database, 1);
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );

        let (_, files) = runtime.block_on(write_data_blocks_to_column_file(&dir, data));
        let version =
            build_version_by_column_files(opt.storage.clone(), database, ts_family_id, files);
        let pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::default());
        let test_helper = TestHelper {
            runtime,
            super_version: Arc::new(SuperVersion::new(
                ts_family_id,
                opt.storage.clone(),
                CacheGroup {
                    mut_cache: Arc::new(RwLock::new(MemCache::new(ts_family_id, 1, 2, 1, &pool))),
                    immut_cache: vec![],
                },
                Arc::new(version),
                1,
            )),
        };

        #[rustfmt::skip]
        let _skip_fmt = {
            assert_eq!(test_helper.run(&[1], None, (i64::MIN, i64::MAX)).unwrap(), 9);
            assert_eq!(test_helper.run(&[1, 2], None, (i64::MIN, i64::MAX)).unwrap(), 18);

            assert_eq!(test_helper.run(&[1], Some(1), (i64::MIN, i64::MAX)).unwrap(), 9);
            assert_eq!(test_helper.run(&[1, 2], Some(1), (i64::MIN, i64::MAX)).unwrap(), 18);

            assert_eq!(test_helper.run(&[1], Some(1), (0, 0)).unwrap(), 0);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 1)).unwrap(), 1);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 4)).unwrap(), 4);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 5)).unwrap(), 5);
            assert_eq!(test_helper.run(&[1], Some(1), (4, 4)).unwrap(), 1);
            assert_eq!(test_helper.run(&[1], Some(1), (4, 5)).unwrap(), 2);
            assert_eq!(test_helper.run(&[1], Some(1), (4, 7)).unwrap(), 4);
            assert_eq!(test_helper.run(&[1], Some(1), (10, 10)).unwrap(), 0);

            assert_eq!(test_helper.run(&[2], Some(1), (10, 40)).unwrap(), 4);
            assert_eq!(test_helper.run(&[2], Some(1), (40, 50)).unwrap(), 2);

            "skip_fmt"
        };
    }

    #[test]
    fn test_super_version_count_memcache() {
        let dir = "/tmp/test/ts_family/super_version_count_memcache";
        let mut global_config = config::get_config_for_test();
        global_config.storage.path = dir.to_string();

        let pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::default());
        #[rustfmt::skip]
        let cache_group = {
            let caches = vec![MemCache::new(1, 16, 2, 0, &pool), MemCache::new(1, 16, 2, 0, &pool), MemCache::new(1, 16, 2, 0, &pool)];
            // cache, sid, schema_id, schema, time_range, put_none
            put_rows_to_cache(&caches[0], 1, 1, default_table_schema(vec![1]), (1, 4), false);
            put_rows_to_cache(&caches[0], 2, 1, default_table_schema(vec![1]), (101, 104), false);
            put_rows_to_cache(&caches[1], 1, 1, default_table_schema(vec![1]), (4, 6), false);
            put_rows_to_cache(&caches[1], 2, 1, default_table_schema(vec![1]), (104, 106), false);
            put_rows_to_cache(&caches[2], 1, 1, default_table_schema(vec![1]), (7, 9), false);
            put_rows_to_cache(&caches[2], 2, 1, default_table_schema(vec![1]), (107, 109), false);
            put_rows_to_cache(&caches[2], 1, 1, default_table_schema(vec![1]), (11, 15), false);
            put_rows_to_cache(&caches[2], 2, 1, default_table_schema(vec![1]), (111, 115), false);
            let cache = MemCache::new(1, 16, 2, 0, &pool);
            put_rows_to_cache(&cache, 1, 1, default_table_schema(vec![1]), (11, 15), false);
            put_rows_to_cache(&cache, 2, 1, default_table_schema(vec![1]), (111, 115), false);
            CacheGroup {
                mut_cache: Arc::new(RwLock::new(cache)),
                immut_cache: caches.into_iter().map(|c| Arc::new(RwLock::new(c))).collect(),
            }
        };

        let opt = Arc::new(Options::from(&global_config));
        let database = Arc::new("dba".to_string());
        let ts_family_id = 1;

        let version =
            build_version_by_column_files(opt.storage.clone(), database, ts_family_id, vec![]);
        let test_helper = TestHelper {
            runtime: Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap(),
            ),
            super_version: Arc::new(SuperVersion::new(
                ts_family_id,
                opt.storage.clone(),
                cache_group,
                Arc::new(version),
                1,
            )),
        };

        #[rustfmt::skip]
        let _skip_fmt = {
            assert_eq!(test_helper.run(&[1], None, (i64::MIN, i64::MAX)).unwrap(), 14);
            assert_eq!(test_helper.run(&[1], None, (i64::MIN, i64::MAX)).unwrap(), 14);

            assert_eq!(test_helper.run(&[1], Some(1), (i64::MIN, i64::MAX)).unwrap(), 14);
            assert_eq!(test_helper.run(&[1, 2], Some(1), (i64::MIN, i64::MAX)).unwrap(), 28);
            assert_eq!(test_helper.run(&[1], Some(1), (0, 0)).unwrap(), 0);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 1)).unwrap(), 1);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 4)).unwrap(), 4);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 5)).unwrap(), 5);
            assert_eq!(test_helper.run(&[1], Some(1), (4, 4)).unwrap(), 1);
            assert_eq!(test_helper.run(&[1], Some(1), (4, 5)).unwrap(), 2);
            assert_eq!(test_helper.run(&[1], Some(1), (4, 7)).unwrap(), 4);
            assert_eq!(test_helper.run(&[1], Some(1), (8, 10)).unwrap(), 2);
            assert_eq!(test_helper.run(&[1], Some(1), (10, 10)).unwrap(), 0);

            assert_eq!(test_helper.run(&[2], Some(1), (101, 104)).unwrap(), 4);
            assert_eq!(test_helper.run(&[2], Some(1), (104, 105)).unwrap(), 2);
            "skip_fmt"
        };
    }

    #[test]
    fn test_super_version_count() {
        let dir = "/tmp/test/ts_family/super_version_count";
        let mut global_config = config::get_config_for_test();
        global_config.storage.path = dir.to_string();
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![2, 3, 101, 104], val: vec![1, 1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![5, 104, 106], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![8, 107, 109], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
        ];

        let pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::default());
        #[rustfmt::skip]
        let cache_group = {
            let caches = vec![MemCache::new(1, 16, 2, 0, &pool), MemCache::new(1, 16, 2, 0, &pool), MemCache::new(1, 16, 2, 0, &pool)];
            // cache, sid, schema_id, schema, time_range, put_none
            put_rows_to_cache(&caches[0], 1, 1, default_table_schema(vec![1]), (11, 15), false);
            put_rows_to_cache(&caches[1], 1, 1, default_table_schema(vec![1]), (21, 25), false);
            put_rows_to_cache(&caches[2], 1, 1, default_table_schema(vec![1]), (31, 35), false);

            put_rows_to_cache(&caches[0], 2, 1, default_table_schema(vec![1]), (12, 13), false);
            put_rows_to_cache(&caches[0], 2, 1, default_table_schema(vec![1]), (111, 115), false);
            put_rows_to_cache(&caches[1], 2, 1, default_table_schema(vec![1]), (22, 23), false);
            put_rows_to_cache(&caches[1], 2, 1, default_table_schema(vec![1]), (121, 125), false);
            put_rows_to_cache(&caches[2], 2, 1, default_table_schema(vec![1]), (32, 33), false);
            put_rows_to_cache(&caches[2], 2, 1, default_table_schema(vec![1]), (131, 135), false);
            let mut_cache = MemCache::new(1, 16, 2, 0, &pool);
            put_rows_to_cache(&mut_cache, 1, 1, default_table_schema(vec![1]), (31, 40), false);
            put_rows_to_cache(&mut_cache, 2, 1, default_table_schema(vec![1]), (36, 37), false);
            put_rows_to_cache(&mut_cache, 2, 1, default_table_schema(vec![1]), (131, 140), false);
            CacheGroup {
                mut_cache: Arc::new(RwLock::new(mut_cache)),
                immut_cache: caches.into_iter().map(|c| Arc::new(RwLock::new(c))).collect(),
            }
        };

        let opt = Arc::new(Options::from(&global_config));
        let database = Arc::new("dba".to_string());
        let ts_family_id = 1;
        let dir = opt.storage.tsm_dir(&database, 1);
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );

        let (_, files) = runtime.block_on(write_data_blocks_to_column_file(&dir, data));
        let version =
            build_version_by_column_files(opt.storage.clone(), database, ts_family_id, files);
        let test_helper = TestHelper {
            runtime,
            super_version: Arc::new(SuperVersion::new(
                ts_family_id,
                opt.storage.clone(),
                cache_group,
                Arc::new(version),
                1,
            )),
        };

        // # sum
        // sid=1: 29, sid=2: 37
        // # disk
        // sid=1 (9): 1, 2, 3, 4, 5, 6, 7, 8, 9
        // sid=2 (9):    2, 3,    5,       8, 101, 104, 106, 107, 109
        // # memory
        // sid=1 (20): 11, 12, 13, 14, 15, 21, 22, 23, 24, 25, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40
        // sid=2 (8):  12, 13,             22, 23,             32, 33,         36, 37,
        // sid=2 (20): 111, 112, 113, 114, 115, 121, 122, 123, 124, 125, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140

        #[rustfmt::skip]
        let _skip_fmt = {
            assert_eq!(test_helper.run(&[1], None, (i64::MIN, i64::MAX)).unwrap(), 29);
            assert_eq!(test_helper.run(&[1, 2], None, (i64::MIN, i64::MAX)).unwrap(), 66);

            assert_eq!(test_helper.run(&[1], Some(1), (i64::MIN, i64::MAX)).unwrap(), 29);
            assert_eq!(test_helper.run(&[1, 2], Some(1), (i64::MIN, i64::MAX)).unwrap(), 66);
            assert_eq!(test_helper.run(&[1], Some(1), (0, 0)).unwrap(), 0);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 1)).unwrap(), 1);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 10)).unwrap(), 9);
            assert_eq!(test_helper.run(&[1], Some(1), (1, 50)).unwrap(), 29);
            assert_eq!(test_helper.run(&[1], Some(1), (10, 20)).unwrap(), 5);
            assert_eq!(test_helper.run(&[1], Some(1), (10, 50)).unwrap(), 20);
            assert_eq!(test_helper.run(&[1], Some(1), (15, 21)).unwrap(), 2);
            assert_eq!(test_helper.run(&[1], Some(1), (40, 40)).unwrap(), 1);
            assert_eq!(test_helper.run(&[1], Some(1), (41, 41)).unwrap(), 0);

            assert_eq!(test_helper.run(&[2], Some(1), (105, 110)).unwrap(), 3);
            assert_eq!(test_helper.run(&[2], Some(1), (105, 121)).unwrap(), 9);
            assert_eq!(test_helper.run(&[2], Some(1), (115, 125)).unwrap(), 6);
            "skip_fmt"
        };
    }
}
