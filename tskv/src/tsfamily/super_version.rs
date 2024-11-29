use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use models::predicate::domain::{TimeRange, TimeRanges};
use models::{ColumnId, SeriesId};

use super::cache_group::CacheGroup;
use super::column_file::ColumnFile;
use super::version::Version;
use crate::error::TskvResult;
use crate::mem_cache::memcache::MemCacheStatistics;
use crate::tsm::page::PageMeta;
use crate::tsm::ColumnGroupID;
use crate::ColumnFileId;

#[derive(Debug)]
pub struct SuperVersion {
    pub ts_family_id: u32,
    pub caches: CacheGroup,
    pub version: Arc<Version>,
    pub version_number: u64,
}

impl SuperVersion {
    pub fn new(
        ts_family_id: u32,
        caches: CacheGroup,
        version: Arc<Version>,
        version_number: u64,
    ) -> Self {
        Self {
            ts_family_id,
            caches,
            version,
            version_number,
        }
    }

    pub fn column_files(&self, time_ranges: &TimeRanges) -> Vec<Arc<ColumnFile>> {
        let mut files = Vec::new();

        for lv in self.version.levels_info().iter() {
            if !time_ranges.overlaps(&lv.time_range) {
                continue;
            }
            for cf in lv.files.iter() {
                if time_ranges.overlaps(cf.time_range()) {
                    files.push(cf.clone());
                }
            }
        }
        files
    }

    pub async fn column_files_by_sid_and_time(
        &self,
        sids: &[SeriesId],
        time_ranges: &TimeRanges,
    ) -> TskvResult<Vec<Arc<ColumnFile>>> {
        let mut files = Vec::new();

        for lv in self.version.levels_info().iter() {
            if !time_ranges.overlaps(&lv.time_range) {
                continue;
            }
            for cf in lv.files.iter() {
                if time_ranges.overlaps(cf.time_range()) && cf.contains_any_series_id(sids).await? {
                    files.push(cf.clone());
                }
            }
        }
        Ok(files)
    }

    pub fn cache_group(&self) -> &CacheGroup {
        &self.caches
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_predicate: TimeRange,
    ) -> (
        BTreeMap<u64, MemCacheStatistics>,
        BTreeMap<ColumnFileId, BTreeMap<SeriesId, Vec<(ColumnGroupID, Vec<PageMeta>)>>>,
    ) {
        let cache = self.caches.cache_statistics(series_ids, time_predicate);
        let sts = self.version.statistics(series_ids, time_predicate).await;
        (cache, sts)
    }

    pub async fn add_tombstone(
        &self,
        series_ids: &[SeriesId],
        column_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> TskvResult<()> {
        let column_files = self
            .column_files_by_sid_and_time(series_ids, &TimeRanges::new(vec![*time_range]))
            .await?;

        let mut file_series_map = HashMap::new();
        for column_file in column_files.iter() {
            let mut valid_series = Vec::new();
            for sid in series_ids {
                if column_file.maybe_contains_series_id(*sid).await? {
                    valid_series.push(*sid);
                }
            }
            if !valid_series.is_empty() {
                file_series_map.insert(column_file.file_id(), (column_file.clone(), valid_series));
            }
        }

        for (_, (column_file, valid_series)) in file_series_map {
            self.version
                .remove_tsm_reader_cache(column_file.file_path())
                .await;

            let mut columns = Vec::new();
            for sid in valid_series {
                for column_id in column_ids {
                    columns.push((sid, *column_id));
                }
            }

            if !columns.is_empty() {
                column_file.add_tombstone(&columns, time_range).await?;
            }
        }
        Ok(())
    }
}
