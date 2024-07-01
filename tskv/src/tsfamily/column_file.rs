use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use cache::{AsyncCache, ShardedAsyncCache};
use models::predicate::domain::TimeRange;
use models::{ColumnId, FieldId, SeriesId};
use tokio::sync::RwLock as TokioRwLock;
use trace::{debug, error, info};
use utils::BloomFilter;

use crate::error::TskvResult;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::summary::CompactMeta;
use crate::tsm::reader::TsmReader;
use crate::tsm::TsmTombstone;
use crate::{tsm, ColumnFileId, LevelId};

#[derive(Debug)]
pub struct ColumnFile {
    file_id: ColumnFileId,
    level: LevelId,
    is_delta: bool,
    time_range: TimeRange,
    size: u64,
    series_id_filter: TokioRwLock<Option<Arc<BloomFilter>>>,
    deleted: AtomicBool,
    compacting: AtomicBool,

    path: PathBuf,
    tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TsmReader>>>,
}

impl ColumnFile {
    pub fn with_compact_data(
        meta: &CompactMeta,
        path: impl AsRef<Path>,
        series_id_filter: TokioRwLock<Option<Arc<BloomFilter>>>,
        tsm_reader_cache: Weak<ShardedAsyncCache<String, Arc<TsmReader>>>,
    ) -> Self {
        Self {
            file_id: meta.file_id,
            level: meta.level,
            is_delta: meta.is_delta,
            time_range: TimeRange::new(meta.min_ts, meta.max_ts),
            size: meta.file_size,
            series_id_filter,
            deleted: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            path: path.as_ref().into(),
            tsm_reader_cache,
        }
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

    pub fn file_path(&self) -> &PathBuf {
        &self.path
    }

    pub fn tombstone_path(&self) -> PathBuf {
        let mut path = self.path.clone();
        path.set_extension(tsm::TOMBSTONE_FILE_SUFFIX);
        path
    }

    pub fn overlap(&self, time_range: &TimeRange) -> bool {
        self.time_range.overlaps(time_range)
    }

    pub async fn maybe_contains_series_id(&self, series_id: SeriesId) -> TskvResult<bool> {
        let bloom_filter = self.load_bloom_filter().await?;
        let res = bloom_filter.maybe_contains(&series_id.to_be_bytes());
        Ok(res)
    }

    pub async fn load_bloom_filter(&self) -> TskvResult<Arc<BloomFilter>> {
        {
            if let Some(filter) = self.series_id_filter.read().await.as_ref() {
                return Ok(filter.clone());
            }
        }
        let mut filter_w = self.series_id_filter.write().await;
        if let Some(filter) = filter_w.as_ref() {
            return Ok(filter.clone());
        }
        let bloom_filter = if let Some(tsm_reader_cache) = self.tsm_reader_cache.upgrade() {
            let reader = match tsm_reader_cache
                .get(&format!("{}", self.path.display()))
                .await
            {
                Some(r) => r,
                None => {
                    let reader = TsmReader::open(&self.path).await?;
                    let reader = Arc::new(reader);
                    tsm_reader_cache
                        .insert(self.path.display().to_string(), reader.clone())
                        .await;
                    reader
                }
            };
            reader.footer().series().bloom_filter().clone()
        } else {
            TsmReader::open(&self.path)
                .await?
                .footer()
                .series()
                .bloom_filter()
                .clone()
        };
        let bloom_filter = Arc::new(bloom_filter);
        filter_w.replace(bloom_filter.clone());
        Ok(bloom_filter)
    }

    pub async fn contains_any_series_id(&self, series_ids: &[SeriesId]) -> TskvResult<bool> {
        for series_id in series_ids {
            if self.maybe_contains_series_id(*series_id).await? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn contains_any_field_id(&self, _series_ids: &[FieldId]) -> bool {
        unimplemented!("contains_any_field_id")
    }

    pub async fn add_tombstone(
        &self,
        series_id: SeriesId,
        column_id: ColumnId,
        time_range: &TimeRange,
    ) -> TskvResult<()> {
        let dir = self.path.parent().expect("file has parent");
        // TODO flock tombstone file.
        let mut tombstone = TsmTombstone::open(dir, self.file_id).await?;
        tombstone
            .add_range(&[(series_id, column_id)], time_range)
            .await?;
        tombstone.flush().await?;
        Ok(())
    }
}

impl ColumnFile {
    pub fn is_deleted(&self) -> bool {
        self.deleted.load(Ordering::Acquire)
    }

    pub fn mark_deleted(&self) {
        self.deleted.store(true, Ordering::Release);
    }

    pub fn is_compacting(&self) -> bool {
        self.compacting.load(Ordering::Acquire)
    }

    pub fn mark_compacting(&self) -> bool {
        self.compacting
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    pub fn unmark_compacting(&self) {
        self.compacting.store(false, Ordering::Release);
    }
}

impl Drop for ColumnFile {
    fn drop(&mut self) {
        debug!(
            "Removing tsm file {} and it's tombstone if exists.",
            self.file_id
        );
        if self.is_deleted() {
            let path = self.file_path();
            if let Some(cache) = self.tsm_reader_cache.upgrade() {
                let k = format!("{}", path.display());
                tokio::spawn(async move {
                    cache.remove(&k).await;
                });
            }
            if let Err(e) = std::fs::remove_file(path) {
                error!(
                    "Failed to remove tsm file {} at '{}': {e}",
                    self.file_id,
                    path.display()
                );
            } else {
                info!("Removed tsm file {} at '{}", self.file_id, path.display());
            }

            let tombstone_path = self.tombstone_path();
            if LocalFileSystem::try_exists(&tombstone_path) {
                if let Err(e) = std::fs::remove_file(&tombstone_path) {
                    error!(
                        "Failed to remove tsm tombstone '{}': {e}",
                        tombstone_path.display()
                    );
                } else {
                    info!("Removed tsm tombstone '{}", tombstone_path.display());
                }
            }
        }
    }
}

#[cfg(test)]
impl ColumnFile {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_id: ColumnFileId,
        level: LevelId,
        time_range: TimeRange,
        size: u64,
        is_delta: bool,
        path: impl AsRef<Path>,
    ) -> Self {
        Self {
            file_id,
            level,
            is_delta,
            time_range,
            size,
            series_id_filter: TokioRwLock::new(Some(Arc::new(BloomFilter::default()))),
            deleted: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            path: path.as_ref().into(),
            tsm_reader_cache: Weak::new(),
        }
    }

    pub fn set_series_id_filter(
        &mut self,
        series_id_filter: TokioRwLock<Option<Arc<BloomFilter>>>,
    ) {
        self.series_id_filter = series_id_filter;
    }
}
