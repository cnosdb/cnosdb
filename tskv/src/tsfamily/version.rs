use std::cmp::max;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use cache::{AsyncCache, ShardedAsyncCache};
use models::predicate::domain::TimeRange;
use models::{SeriesId, Timestamp};
use tokio::sync::RwLock as TokioRwLock;
use trace::error;
use utils::BloomFilter;

use super::column_file::ColumnFile;
use super::level_info::LevelInfo;
use crate::error::TskvResult;
use crate::kv_option::StorageOptions;
use crate::summary::{CompactMeta, VersionEdit};
use crate::tsm::page::PageMeta;
use crate::tsm::reader::TsmReader;
use crate::tsm::ColumnGroupID;
use crate::{ColumnFileId, VnodeId};

#[derive(Debug)]
pub struct Version {
    ts_family_id: VnodeId,
    owner: Arc<String>,
    storage_opt: Arc<StorageOptions>,
    /// The max seq_no of write batch in wal flushed to column file.
    last_seq: u64,
    /// The max timestamp of write batch in wal flushed to column file.
    max_level_ts: i64,
    levels_info: [LevelInfo; 5],
    tsm_reader_cache: Arc<ShardedAsyncCache<String, Arc<TsmReader>>>,
}

impl Version {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ts_family_id: VnodeId,
        owner: Arc<String>,
        storage_opt: Arc<StorageOptions>,
        last_seq: u64,
        levels_info: [LevelInfo; 5],
        max_level_ts: i64,
        tsm_reader_cache: Arc<ShardedAsyncCache<String, Arc<TsmReader>>>,
    ) -> Self {
        Self {
            ts_family_id,
            owner,
            storage_opt,
            last_seq,
            max_level_ts,
            levels_info,
            tsm_reader_cache,
        }
    }

    /// Creates new Version using current Version and `VersionEdit`s.
    pub fn copy_apply_version_edits(
        &self,
        ve: VersionEdit,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) -> Version {
        let mut added_files: Vec<Vec<CompactMeta>> = vec![vec![]; 5];
        let mut deleted_files: Vec<HashSet<ColumnFileId>> = vec![HashSet::new(); 5];
        if !ve.add_files.is_empty() {
            ve.add_files.into_iter().for_each(|f| {
                added_files[f.level as usize].push(f);
            });
        }
        if !ve.del_files.is_empty() {
            ve.del_files.into_iter().for_each(|f| {
                deleted_files[f.level as usize].insert(f.file_id);
            });
        }

        let mut new_levels = LevelInfo::init_levels(
            self.owner.clone(),
            self.ts_family_id,
            self.storage_opt.clone(),
        );
        let weak_tsm_reader_cache = Arc::downgrade(&self.tsm_reader_cache);
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if deleted_files[file.level() as usize].contains(&file.file_id()) {
                    file.mark_deleted();
                    continue;
                }
                new_levels[level.level as usize].push_column_file(file.clone());
            }
            for file in added_files[level.level as usize].iter() {
                let series_filter = file_metas.remove(&file.file_id).unwrap_or_else(|| {
                    error!("missing bloom filter for file_id: {}", file.file_id);
                    Arc::new(BloomFilter::default())
                });
                new_levels[level.level as usize].push_compact_meta(
                    file,
                    TokioRwLock::new(Some(series_filter)),
                    weak_tsm_reader_cache.clone(),
                );
            }
            new_levels[level.level as usize].update_time_range();
        }

        let mut new_version = Self {
            last_seq: max(self.last_seq, ve.seq_no),
            ts_family_id: self.ts_family_id,
            owner: self.owner.clone(),
            storage_opt: self.storage_opt.clone(),
            max_level_ts: self.max_level_ts,
            levels_info: new_levels,
            tsm_reader_cache: self.tsm_reader_cache.clone(),
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
                max_ts = file.time_range().max_ts.max(max_ts);
            }
        }

        self.max_level_ts = max_ts;
    }

    pub fn tf_id(&self) -> VnodeId {
        self.ts_family_id
    }

    pub fn owner(&self) -> Arc<String> {
        self.owner.clone()
    }

    pub fn levels_info(&self) -> &[LevelInfo; 5] {
        &self.levels_info
    }

    pub fn storage_opt(&self) -> Arc<StorageOptions> {
        self.storage_opt.clone()
    }

    // todo:
    pub fn get_ts_overlap(&self, _level: u32, _ts_min: i64, _ts_max: i64) -> Vec<Arc<ColumnFile>> {
        vec![]
    }

    pub async fn get_tsm_reader(&self, path: impl AsRef<Path>) -> TskvResult<Arc<TsmReader>> {
        let path = path.as_ref().display().to_string();
        let tsm_reader = match self.tsm_reader_cache.get(&path).await {
            Some(val) => val,
            None => match self.tsm_reader_cache.get(&path).await {
                Some(val) => val,
                None => {
                    let tsm_reader = Arc::new(TsmReader::open(&path).await?);
                    self.tsm_reader_cache.insert(path, tsm_reader.clone()).await;
                    tsm_reader
                }
            },
        };
        Ok(tsm_reader)
    }

    pub fn max_level_ts(&self) -> i64 {
        self.max_level_ts
    }

    pub fn tsm_reader_cache(&self) -> &Arc<ShardedAsyncCache<String, Arc<TsmReader>>> {
        &self.tsm_reader_cache
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_predicate: TimeRange,
    ) -> BTreeMap<ColumnFileId, BTreeMap<SeriesId, Vec<(ColumnGroupID, Vec<PageMeta>)>>> {
        let mut result = BTreeMap::new();
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if file.is_deleted() || !file.overlap(&time_predicate) {
                    continue;
                }
                let reader = self.get_tsm_reader(file.file_path()).await.unwrap();
                let fid = reader.file_id();
                let sts = reader.statistics(series_ids, time_predicate).await.unwrap();
                result.insert(fid, sts);
            }
        }
        result
    }

    pub async fn remove_tsm_reader_cache(&self, path: impl AsRef<Path>) {
        let path = path.as_ref().display().to_string();
        self.tsm_reader_cache.remove(&path).await;
    }

    pub async fn unmark_compacting_files(&self, files_ids: &HashSet<ColumnFileId>) {
        if files_ids.is_empty() {
            return;
        }
        for level in self.levels_info.iter() {
            for file in level.files.iter() {
                if files_ids.contains(&file.file_id()) {
                    let mut compacting = file.write_lock_compacting().await;
                    *compacting = false;
                }
            }
        }
    }

    #[cfg(test)]
    pub fn levels_info_mut(&mut self) -> &mut [LevelInfo; 5] {
        &mut self.levels_info
    }

    #[cfg(test)]
    pub fn inner(&self) -> Self {
        Self {
            ts_family_id: self.ts_family_id,
            owner: self.owner.clone(),
            storage_opt: self.storage_opt.clone(),
            last_seq: self.last_seq,
            max_level_ts: self.max_level_ts,
            levels_info: self.levels_info.clone(),
            tsm_reader_cache: self.tsm_reader_cache.clone(),
        }
    }
}
