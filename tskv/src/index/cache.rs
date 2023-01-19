use lru::LruCache;
use models::{SeriesId, SeriesKey};

use std::{
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
};

pub struct SeriesKeyInfo {
    pub key: SeriesKey,
    pub hash: u64,
    pub id: SeriesId,
}
pub struct ForwardIndexCache {
    id_map: LruCache<SeriesId, Arc<SeriesKeyInfo>>,
    hash_map: LruCache<u64, Arc<SeriesKeyInfo>>,
}

impl ForwardIndexCache {
    pub fn new(size: usize) -> Self {
        Self {
            id_map: LruCache::new(NonZeroUsize::new(size).unwrap()),
            hash_map: LruCache::new(NonZeroUsize::new(size).unwrap()),
        }
    }

    pub fn add(&mut self, info: SeriesKeyInfo) {
        let id = info.id;
        let hash = info.hash;
        let info_ref = Arc::new(info);

        self.id_map.put(id, info_ref.clone());
        self.hash_map.put(hash, info_ref);
    }

    pub fn del(&mut self, id: SeriesId, hash: u64) {
        self.id_map.pop(&id);
        self.hash_map.pop(&hash);
    }

    pub fn get_series_id_by_key(&mut self, key: &SeriesKey) -> Option<SeriesId> {
        let hash = key.hash();

        if let Some(info) = self.hash_map.get(&hash) {
            if info.key.eq(key) {
                return Some(info.id);
            }
        }

        None
    }

    pub fn get_series_key_by_id(&mut self, id: SeriesId) -> Option<SeriesKey> {
        self.id_map.get(&id).map(|info| info.key.clone())
    }
}
