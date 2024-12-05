use std::collections::{BTreeMap, HashMap};
use std::ops::{BitAnd, BitOr, RangeBounds};
use std::sync::Arc;

use cache::{ShardedSyncCache, SyncCache};
use maplit::{btreemap, hashmap};
use models::{SeriesId, SeriesKey};

use super::ts_index::{encode_inverted_index_key, encode_series_key};
use super::{IndexResult, IndexStorageSnafu};
use crate::index::ts_index::encode_series_id_key;

#[derive(Debug)]
pub struct IndexCache {
    pub read_cache: ForwardCache,
    pub write_cache: IndexMemCache,
}

impl IndexCache {
    pub fn new(read_cache_size: usize) -> Self {
        Self {
            read_cache: ForwardCache::new(read_cache_size),
            write_cache: IndexMemCache::new(),
        }
    }

    pub fn write(&mut self, id: SeriesId, key: SeriesKey) {
        self.write_cache.add(id, key);
    }

    pub fn cache(&self, id: SeriesId, key: SeriesKey) {
        self.read_cache.add(id, key);
    }

    pub fn del(&mut self, id: SeriesId, key: &SeriesKey) {
        let hash = key.hash();
        self.write_cache.del(id, key);
        self.read_cache.del(id, hash);
    }

    pub fn get_series_id_by_key(&self, key: &SeriesKey) -> Option<SeriesId> {
        if let Some(id) = self.write_cache.get_series_id_by_key(key) {
            return Some(id);
        }

        if let Some(id) = self.read_cache.get_series_id_by_key(key) {
            return Some(id);
        }

        None
    }

    pub fn get_series_key_by_id(&self, id: SeriesId) -> Option<SeriesKey> {
        if let Some(key) = self.write_cache.get_series_key_by_id(id) {
            return Some(key);
        }

        if let Some(key) = self.read_cache.get_series_key_by_id(id) {
            return Some(key);
        }

        None
    }
}

#[derive(Debug)]
pub struct SeriesKeyInfo {
    pub key: SeriesKey,
    pub hash: u64,
    pub id: SeriesId,
}
#[derive(Debug)]
pub struct ForwardCache {
    id_map: ShardedSyncCache<SeriesId, Arc<SeriesKeyInfo>>,
    hash_map: ShardedSyncCache<u64, Arc<SeriesKeyInfo>>,
}

impl ForwardCache {
    fn new(size: usize) -> Self {
        Self {
            id_map: ShardedSyncCache::create_lru_sharded_cache(size),
            hash_map: ShardedSyncCache::create_lru_sharded_cache(size),
        }
    }

    fn add(&self, id: SeriesId, key: SeriesKey) {
        let info = Arc::new(SeriesKeyInfo {
            hash: key.hash(),
            key,
            id,
        });

        let hash = info.hash;
        self.id_map.insert(id, info.clone());
        self.hash_map.insert(hash, info);
    }

    fn del(&self, id: SeriesId, hash: u64) {
        self.id_map.remove(&id);
        self.hash_map.remove(&hash);
    }

    fn get_series_id_by_key(&self, key: &SeriesKey) -> Option<SeriesId> {
        let hash = key.hash();

        if let Some(info) = self.hash_map.get(&hash) {
            if info.key.eq(key) {
                return Some(info.id);
            }
        }

        None
    }

    fn get_series_key_by_id(&self, id: SeriesId) -> Option<SeriesKey> {
        self.id_map.get(&id).map(|info| info.key.clone())
    }
}

// id_map: SeriesId -> SeriesKeyInfo
// key_map: SeriesKey -> SeriesKeyInfo
// inverted: table -> tag_name -> tag_value -> id_list(RoaringBitmap)
#[derive(Debug)]
#[allow(clippy::type_complexity)]
pub struct IndexMemCache {
    id_map: HashMap<SeriesId, Arc<SeriesKeyInfo>>,
    key_map: HashMap<Vec<u8>, Arc<SeriesKeyInfo>>,
    inverted: HashMap<String, HashMap<Vec<u8>, BTreeMap<Vec<u8>, roaring::RoaringBitmap>>>,
}

impl IndexMemCache {
    fn new() -> Self {
        Self {
            id_map: HashMap::new(),
            key_map: HashMap::new(),
            inverted: HashMap::new(),
        }
    }

    fn add_inverted(&mut self, tab: &str, name: &[u8], value: &[u8], id: SeriesId) {
        if let Some(item) = self.inverted.get_mut(tab) {
            if let Some(item) = item.get_mut(name) {
                if let Some(item) = item.get_mut(value) {
                    item.insert(id);
                } else {
                    let mut rb = roaring::RoaringBitmap::new();
                    rb.insert(id);
                    item.insert(value.to_vec(), rb);
                }
            } else {
                let mut rb = roaring::RoaringBitmap::new();
                rb.insert(id);
                item.insert(name.to_vec(), btreemap! {value.to_vec()=>rb});
            }
        } else {
            let mut rb = roaring::RoaringBitmap::new();
            rb.insert(id);

            self.inverted.insert(
                tab.to_string(),
                hashmap! {name.to_vec()=>btreemap!{value.to_vec()=>rb}},
            );
        }
    }

    fn del_inverted(&mut self, tab: &str, name: &[u8], value: &[u8], id: SeriesId) {
        if let Some(item) = self.inverted.get_mut(tab) {
            if let Some(item) = item.get_mut(name) {
                if let Some(item) = item.get_mut(value) {
                    item.remove(id);
                }
            }
        }
    }

    fn add(&mut self, id: SeriesId, key: SeriesKey) {
        for tag in key.tags() {
            self.add_inverted(key.table(), &tag.key, &tag.value, id);
        }

        if key.tags().is_empty() {
            self.add_inverted(key.table(), &[], &[], id);
        }

        let hash = key.hash();
        let key_buf = encode_series_key(&key.table, &key.tags);
        let info = Arc::new(SeriesKeyInfo { hash, key, id });
        self.id_map.insert(id, info.clone());
        self.key_map.insert(key_buf, info);
    }

    fn del(&mut self, id: SeriesId, key: &SeriesKey) {
        for tag in key.tags() {
            self.del_inverted(key.table(), &tag.key, &tag.value, id);
        }

        if key.tags().is_empty() {
            self.del_inverted(key.table(), &[], &[], id);
        }

        let key_buf = encode_series_key(&key.table, &key.tags);
        self.id_map.remove(&id);
        self.key_map.remove(&key_buf);
    }

    fn get_series_id_by_key(&self, key: &SeriesKey) -> Option<SeriesId> {
        let key_buf = encode_series_key(&key.table, &key.tags);
        if let Some(info) = self.key_map.get(&key_buf) {
            return Some(info.id);
        }

        None
    }

    fn get_series_key_by_id(&self, id: SeriesId) -> Option<SeriesKey> {
        self.id_map.get(&id).map(|info| info.key.clone())
    }

    pub fn get_inverted_by_range(
        &self,
        tab: &str,
        tag_key: &str,
        range: impl RangeBounds<Vec<u8>>,
    ) -> roaring::RoaringBitmap {
        let _ = range;
        let tag_map = self
            .inverted
            .get(tab)
            .and_then(|item| item.get(tag_key.as_bytes()));
        let mut bitmap = roaring::RoaringBitmap::new();
        if let Some(bt) = tag_map {
            for (_, rb) in bt.range(range) {
                bitmap = bitmap.bitor(rb);
            }
        }
        bitmap
    }

    pub fn get_inverted_by_tags(&self, tab: &str, tags: &[models::Tag]) -> roaring::RoaringBitmap {
        if tags.is_empty() {
            let mut bitmap = roaring::RoaringBitmap::new();
            if let Some(item) = self.inverted.get(tab) {
                for (_, tags_map) in item.iter() {
                    for (_, vals_map) in tags_map.iter() {
                        bitmap = bitmap.bitor(vals_map);
                    }
                }

                return bitmap;
            } else {
                return roaring::RoaringBitmap::new();
            }
        }

        let mut bitmap = roaring::RoaringBitmap::new();
        for tag in tags {
            let rb = self
                .inverted
                .get(tab)
                .and_then(|item| item.get(&tag.key))
                .and_then(|item| item.get(&tag.value));
            if let Some(rb) = rb {
                if bitmap.is_empty() {
                    bitmap = rb.clone();
                } else {
                    bitmap = bitmap.bitand(rb)
                }
            } else {
                return roaring::RoaringBitmap::new();
            }
        }

        bitmap
    }

    pub async fn flush(&mut self, storage: &super::engine2::IndexEngine2) -> IndexResult<()> {
        // flush forward index
        let mut writer = storage.writer_txn()?;
        for (id, key) in self.id_map.iter() {
            trace::debug!("--- Index flush new series id:{}, key: {}", id, key.key);
            let key_buf = encode_series_key(key.key.table(), key.key.tags());
            // storage.set(&key_buf, &id.to_be_bytes())?;
            // storage.set(&encode_series_id_key(*id), &key.key.encode())?;

            storage.txn_write(&key_buf, &id.to_be_bytes(), &mut writer)?;
            storage.txn_write(&encode_series_id_key(*id), &key.key.encode(), &mut writer)?;
        }

        // flush inverted index
        for (tab, tags) in self.inverted.iter() {
            for (tag_key, values) in tags {
                for (tag_val, rb) in values {
                    let key = encode_inverted_index_key(tab, tag_key, tag_val);
                    //storage.merge_rb(&key, rb)?;
                    storage.txn_merge_rb(&key, rb, &mut writer)?;
                }
            }
        }
        writer
            .commit()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
        storage.flush()?;

        self.id_map.clear();
        self.key_map.clear();
        self.inverted.clear();

        Ok(())
    }
}
