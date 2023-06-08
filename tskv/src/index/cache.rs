use std::sync::Arc;

use lru_cache::ShardedCache;
use models::{SeriesId, SeriesKey};

#[derive(Debug)]
pub struct SeriesKeyInfo {
    pub key: SeriesKey,
    pub hash: u64,
    pub id: SeriesId,
}
pub struct ForwardIndexCache {
    id_map: ShardedCache<SeriesId, Arc<SeriesKeyInfo>>,
    hash_map: ShardedCache<u64, Arc<SeriesKeyInfo>>,
}

impl ForwardIndexCache {
    pub fn new(size: usize) -> Self {
        Self {
            id_map: ShardedCache::with_capacity(size),
            hash_map: ShardedCache::with_capacity(size),
        }
    }

    pub fn add(&self, info: SeriesKeyInfo) {
        let id = info.id;
        let hash = info.hash;
        let info_ref = Arc::new(info);

        self.id_map.insert(id, info_ref.clone());
        self.hash_map.insert(hash, info_ref);
    }

    pub fn del(&self, id: SeriesId, hash: u64) {
        self.id_map.remove(&id);
        self.hash_map.remove(&hash);
    }

    pub fn get_series_id_by_key(&self, key: &SeriesKey) -> Option<SeriesId> {
        let hash = key.hash();

        if let Some(info) = self.hash_map.get(&hash) {
            if info.key.eq(key) {
                return Some(info.id);
            }
        }

        None
    }

    pub fn get_series_key_by_id(&self, id: SeriesId) -> Option<SeriesKey> {
        self.id_map.get(&id).map(|info| info.key.clone())
    }
}
