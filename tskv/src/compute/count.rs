use std::collections::HashSet;
use std::sync::Arc;

use models::{utils as model_utils, ColumnId, FieldId, SeriesId, Timestamp};
use trace::trace;

use crate::tseries_family::{ColumnFile, SuperVersion, TimeRangeCmp, Version};
use crate::tsm::{BlockMeta, IndexMeta, TsmReader};
use crate::{Error, Result, TimeRange};

/// Compute pushed down aggregate:
///
/// `SELECT count(<column>) FROM <table> WHERE <time_range_predicates>`
pub async fn count_column_non_null_values(
    super_version: Arc<SuperVersion>,
    series_ids: &[SeriesId],
    column_id: Option<ColumnId>,
    time_ranges: Arc<Vec<TimeRange>>,
) -> Result<u64> {
    let column_files = Arc::new(super_version.column_files(&time_ranges));

    if let Some(column_id) = column_id {
        trace!("Selecting count for column: {}", column_id);

        let mut jh_vec = Vec::with_capacity(series_ids.len());

        for series_id in series_ids {
            let field_id = model_utils::unite_id(column_id, *series_id);
            let sv_inner = super_version.clone();
            let cfs_inner = column_files.clone();
            let trs_inner = time_ranges.clone();

            jh_vec.push(tokio::spawn(async move {
                count_non_null_values_inner(
                    sv_inner,
                    CountingObject::Field(field_id),
                    &cfs_inner,
                    trs_inner,
                )
                .await
            }));
        }

        let mut count_sum = 0_u64;
        for jh in jh_vec {
            // JoinHandle returns JoinError if task was paniced.
            count_sum += jh.await.map_err(|e| Error::IO { source: e.into() })??;
        }

        Ok(count_sum)
    } else {
        trace!("Selecting count for column: time");

        count_non_null_values_inner(
            super_version,
            CountingObject::Series(series_ids),
            &column_files,
            time_ranges,
        )
        .await
    }
}

enum CountingObject<'a> {
    Field(FieldId),
    Series(&'a [SeriesId]),
}

/// Get count of non-null values in time ranges of a field.
async fn count_non_null_values_inner<'a>(
    super_version: Arc<SuperVersion>,
    counting_object: CountingObject<'a>,
    column_files: &[Arc<ColumnFile>],
    sorted_time_ranges: Arc<Vec<TimeRange>>,
) -> Result<u64> {
    let read_tasks = create_file_read_tasks(
        &super_version,
        column_files,
        &counting_object,
        &sorted_time_ranges,
    )
    .await?;
    let (cached_timestamps, cached_time_range) = match counting_object {
        CountingObject::Field(field_id) => {
            get_field_timestamps_in_caches(&super_version, field_id, &sorted_time_ranges)
        }
        CountingObject::Series(series_ids) => {
            get_series_timestamps_in_caches(&super_version, series_ids, &sorted_time_ranges)
        }
    };

    let mut count = cached_timestamps.len() as u64;
    let cached_timestamps = Arc::new(cached_timestamps);
    let cached_time_range = Arc::new(cached_time_range);

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
                        sorted_time_ranges.clone(),
                        cached_timestamps.clone(),
                        cached_time_range.clone(),
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
                    sorted_time_ranges.clone(),
                    cached_timestamps.clone(),
                    cached_time_range.clone(),
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
            sorted_time_ranges.clone(),
            cached_timestamps.clone(),
            cached_time_range.clone(),
        )
        .await?;
    }

    Ok(count)
}

/// Get timestamps of a field in time ranges from all mutable and immutable caches.
fn get_field_timestamps_in_caches(
    super_version: &SuperVersion,
    field_id: FieldId,
    time_ranges: &[TimeRange],
) -> (HashSet<Timestamp>, TimeRange) {
    let time_predicate = |ts| {
        time_ranges
            .iter()
            .any(|tr| tr.is_boundless() || tr.contains(ts))
    };
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
    series_ids: &[SeriesId],
    time_ranges: &[TimeRange],
) -> (HashSet<Timestamp>, TimeRange) {
    let time_predicate = |ts| {
        time_ranges
            .iter()
            .any(|tr| tr.is_boundless() || tr.contains(ts))
    };
    let mut cached_timestamps: HashSet<i64> = HashSet::new();
    let mut cached_time_range = TimeRange::new(i64::MAX, i64::MIN);
    super_version
        .caches
        .read_series_timestamps(series_ids, time_predicate, |ts| {
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
    counting_object: &CountingObject<'a>,
    time_ranges: &[TimeRange],
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
            CountingObject::Series(series_ids) => reader.index_iterator(),
        };
        for idx in idx_meta_iter {
            if let CountingObject::Series(series_ids) = counting_object {
                let (_, sid) = model_utils::split_id(idx.field_id());
                let mut idx_in_series = false;
                for series_id in *series_ids {
                    if *series_id == sid {
                        idx_in_series = true;
                        break;
                    }
                }
                if !idx_in_series {
                    continue;
                }
            }
            for blk_meta in idx.block_iterator() {
                let blk_tr = blk_meta.time_range();
                let mut tr_cmp = TimeRangeCmp::Exclude;
                for tr in time_ranges.iter() {
                    if tr.includes(&blk_tr) {
                        trace!("Included: {} for block {}", &tr, &blk_tr);
                        // Block is included by conditions, needn't to decode.
                        tr_cmp = TimeRangeCmp::Include;
                        break;
                    } else if tr.overlaps(&blk_tr) {
                        trace!("overlapped: {} for block {}", &tr, &blk_tr);
                        tr_cmp = TimeRangeCmp::Intersect;
                    }
                }
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
    time_ranges: Arc<Vec<TimeRange>>,
    cached_timestamps: Arc<HashSet<Timestamp>>,
    cached_time_range: Arc<TimeRange>,
) -> Result<u64> {
    let mut count = 0_u64;
    let mut ts_set: HashSet<Timestamp> = HashSet::new();
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
            for tr in time_ranges.iter() {
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
                                h - 1
                            } else {
                                h
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

    use config::get_config;
    use memory_pool::{GreedyMemoryPool, MemoryPoolRef};
    use models::utils as model_utils;
    use parking_lot::RwLock;

    use crate::compaction::flush_tests::default_table_schema;
    use crate::compaction::test::write_data_blocks_to_column_file;
    use crate::compute::count::count_column_non_null_values;
    use crate::memcache::test::put_rows_to_cache;
    use crate::memcache::MemCache;
    use crate::tseries_family::test_tseries_family::build_version_by_column_files;
    use crate::tseries_family::{CacheGroup, SuperVersion};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::DataBlock;
    use crate::Options;

    #[tokio::test]
    async fn test_super_version_count_file() {
        let dir = "/tmp/test/ts_family/super_version_count_file";
        let mut global_config = get_config("../config/config.toml");
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

        let (_, files) =
            write_data_blocks_to_column_file(&dir, data, ts_family_id, opt.clone()).await;
        let version =
            build_version_by_column_files(opt.storage.clone(), database, ts_family_id, files);
        let pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let super_version = Arc::new(SuperVersion::new(
            ts_family_id,
            opt.storage.clone(),
            CacheGroup {
                mut_cache: Arc::new(RwLock::new(MemCache::new(ts_family_id, 1, 1, &pool))),
                immut_cache: vec![],
            },
            Arc::new(version),
            1,
        ));

        #[rustfmt::skip]
        let _skip_fmt = {
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], None, Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 9);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1, 2], None, Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 18);

            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 9);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1, 2], Some(1), Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 18);

            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(0, 0).into()])).await.unwrap(), 0);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 1).into()])).await.unwrap(), 1);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 4).into()])).await.unwrap(), 4);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 5).into()])).await.unwrap(), 5);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(4, 4).into()])).await.unwrap(), 1);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(4, 5).into()])).await.unwrap(), 2);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(4, 7).into()])).await.unwrap(), 4);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(10, 10).into()])).await.unwrap(), 0);

            assert_eq!(count_column_non_null_values(super_version.clone(), &[2], Some(1), Arc::new(vec![(10, 40).into()])).await.unwrap(), 4);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[2], Some(1), Arc::new(vec![(40, 50).into()])).await.unwrap(), 2);

            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(
                vec![(2, 3).into(), (5, 5).into(), (8, 8).into()]
            )).await.unwrap(), 4);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(
                vec![(4, 5).into(), (6, 8).into()]
            )).await.unwrap(), 5);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(
                vec![(1, 3).into(), (2, 4).into(), (3, 7).into()]
            )).await.unwrap(), 7);

            assert_eq!(count_column_non_null_values(super_version, &[1, 2], Some(1), Arc::new(
                vec![(1, 3).into(), (2, 4).into(), (3, 7).into(),
                     (10, 30).into(), (20, 40).into(), (30, 70).into()]
            )).await.unwrap(), 14);
            "skip_fmt"
        };
    }

    #[tokio::test]
    async fn test_super_version_count_memcache() {
        let dir = "/tmp/test/ts_family/super_version_count_memcache";
        let mut global_config = get_config("../config/config.toml");
        global_config.storage.path = dir.to_string();

        let pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        #[rustfmt::skip]
        let cache_group = {
            let mut caches = vec![MemCache::new(1, 16, 0, &pool), MemCache::new(1, 16, 0, &pool), MemCache::new(1, 16, 0, &pool)];
            // cache, sid, schema_id, schema, time_range, put_none
            put_rows_to_cache(&mut caches[0], 1, 1, default_table_schema(vec![1]), (1, 4), false);
            put_rows_to_cache(&mut caches[0], 2, 1, default_table_schema(vec![1]), (101, 104), false);
            put_rows_to_cache(&mut caches[1], 1, 1, default_table_schema(vec![1]), (4, 6), false);
            put_rows_to_cache(&mut caches[1], 2, 1, default_table_schema(vec![1]), (104, 106), false);
            put_rows_to_cache(&mut caches[2], 1, 1, default_table_schema(vec![1]), (7, 9), false);
            put_rows_to_cache(&mut caches[2], 2, 1, default_table_schema(vec![1]), (107, 109), false);
            let mut mut_cache = MemCache::new(1, 16, 0, &pool);
            put_rows_to_cache(&mut mut_cache, 1, 1, default_table_schema(vec![1]), (11, 15), false);
            put_rows_to_cache(&mut mut_cache, 2, 1, default_table_schema(vec![1]), (111, 115), false);
            CacheGroup {
                mut_cache: Arc::new(RwLock::new(mut_cache)),
                immut_cache: caches.into_iter().map(|c| Arc::new(RwLock::new(c))).collect(),
            }
        };

        let opt = Arc::new(Options::from(&global_config));
        let database = Arc::new("dba".to_string());
        let ts_family_id = 1;

        let version =
            build_version_by_column_files(opt.storage.clone(), database, ts_family_id, vec![]);
        let super_version = Arc::new(SuperVersion::new(
            ts_family_id,
            opt.storage.clone(),
            cache_group,
            Arc::new(version),
            1,
        ));

        #[rustfmt::skip]
        let _skip_fmt = {
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], None, Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 14);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], None, Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 14);

            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 14);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1, 2], Some(1), Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 28);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(0, 0).into()])).await.unwrap(), 0);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 1).into()])).await.unwrap(), 1);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 4).into()])).await.unwrap(), 4);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 5).into()])).await.unwrap(), 5);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(4, 4).into()])).await.unwrap(), 1);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(4, 5).into()])).await.unwrap(), 2);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(4, 7).into()])).await.unwrap(), 4);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(10, 10).into()])).await.unwrap(), 0);

            assert_eq!(count_column_non_null_values(super_version.clone(), &[2], Some(1), Arc::new(vec![(101, 104).into()])).await.unwrap(), 4);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[2], Some(1), Arc::new(vec![(104, 105).into()])).await.unwrap(), 2);

            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(
                vec![(2, 3).into(), (5, 5).into(), (8, 8).into()]
            )).await.unwrap(), 4);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(
                vec![(4, 5).into(), (6, 8).into()]
            )).await.unwrap(), 5);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(
                vec![(1, 3).into(), (2, 4).into(), (3, 7).into()]
            )).await.unwrap(), 7);

            assert_eq!(count_column_non_null_values(super_version, &[1, 2], Some(1), Arc::new(
                vec![(1, 3).into(), (2, 4).into(), (3, 7).into(),
                     (101, 103).into(), (102, 104).into(), (103, 107).into()]
            )).await.unwrap(), 14);
            "skip_fmt"
        };
    }

    #[tokio::test]
    async fn test_super_version_count() {
        let dir = "/tmp/test/ts_family/super_version_count";
        let mut global_config = get_config("../config/config.toml");
        global_config.storage.path = dir.to_string();

        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![101, 102, 103, 104], val: vec![1, 1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![104, 105, 106], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (model_utils::unite_id(1, 1), vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
                (model_utils::unite_id(1, 2), vec![DataBlock::I64 { ts: vec![107, 108, 109], val: vec![1, 1, 1], enc: DataBlockEncoding::default() }]),
            ]),
        ];

        let pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        #[rustfmt::skip]
        let cache_group = {
            let mut caches = vec![MemCache::new(1, 16, 0, &pool), MemCache::new(1, 16, 0, &pool), MemCache::new(1, 16, 0, &pool)];
            // cache, sid, schema_id, schema, time_range, put_none
            put_rows_to_cache(&mut caches[0], 1, 1, default_table_schema(vec![1]), (11, 15), false);
            put_rows_to_cache(&mut caches[0], 2, 1, default_table_schema(vec![1]), (111, 115), false);
            put_rows_to_cache(&mut caches[1], 1, 1, default_table_schema(vec![1]), (21, 25), false);
            put_rows_to_cache(&mut caches[1], 2, 1, default_table_schema(vec![1]), (121, 125), false);
            put_rows_to_cache(&mut caches[2], 1, 1, default_table_schema(vec![1]), (31, 35), false);
            put_rows_to_cache(&mut caches[2], 2, 1, default_table_schema(vec![1]), (131, 135), false);
            let mut mut_cache = MemCache::new(1, 16, 0, &pool);
            put_rows_to_cache(&mut mut_cache, 1, 1, default_table_schema(vec![1]), (31, 40), false);
            put_rows_to_cache(&mut mut_cache, 2, 1, default_table_schema(vec![1]), (131, 140), false);
            CacheGroup {
                mut_cache: Arc::new(RwLock::new(mut_cache)),
                immut_cache: caches.into_iter().map(|c| Arc::new(RwLock::new(c))).collect(),
            }
        };

        let opt = Arc::new(Options::from(&global_config));
        let database = Arc::new("dba".to_string());
        let ts_family_id = 1;
        let dir = opt.storage.tsm_dir(&database, 1);

        let (_, files) =
            write_data_blocks_to_column_file(&dir, data, ts_family_id, opt.clone()).await;
        let version =
            build_version_by_column_files(opt.storage.clone(), database, ts_family_id, files);
        let super_version = Arc::new(SuperVersion::new(
            ts_family_id,
            opt.storage.clone(),
            cache_group,
            Arc::new(version),
            1,
        ));

        #[rustfmt::skip]
        let _skip_fmt = {
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], None, Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 29);
            assert_eq!(count_column_non_null_values(super_version.clone(), &[1, 2], None, Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 58);

            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 29);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1, 2], Some(1), Arc::new(vec![(i64::MIN, i64::MAX).into()])).await.unwrap(), 58);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(0, 0).into()])).await.unwrap(), 0);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 1).into()])).await.unwrap(), 1);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 10).into()])).await.unwrap(), 9);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(1, 50).into()])).await.unwrap(), 29);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(10, 20).into()])).await.unwrap(), 5);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(10, 50).into()])).await.unwrap(), 20);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(15, 21).into()])).await.unwrap(), 2);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(40, 40).into()])).await.unwrap(), 1);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[1], Some(1), Arc::new(vec![(41, 41).into()])).await.unwrap(), 0);

            // assert_eq!(count_column_non_null_values(super_version.clone(), &[2], Some(1), Arc::new(vec![(105, 110).into()])).await.unwrap(), 5);
            // assert_eq!(count_column_non_null_values(super_version.clone(), &[2], Some(1), Arc::new(vec![(105, 121).into()])).await.unwrap(), 11);
            // assert_eq!(count_column_non_null_values(super_version, &[2], Some(1), Arc::new(vec![(115, 125).into()])).await.unwrap(), 6);
            "skip_fmt"
        };
    }
}
