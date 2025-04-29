use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;

use rand::RngCore;

use crate::lru_cache::LruWrap;
use crate::sync_cache::SyncCacheWrap;
use crate::{per_shard, shard, SyncCache, NUM_SHARDS};

#[derive(Debug)]
pub struct ShardedSyncCache<K, V>
where
    K: Debug + Hash + Send + Sync,
    V: Debug + Clone + Send + Sync,
{
    shard: Vec<Box<dyn SyncCache<K = K, V = V>>>,
}
unsafe impl<K: Debug + Send + Hash + Sync, V: Debug + Send + Sync + Clone> Send
    for ShardedSyncCache<K, V>
{
}
unsafe impl<K: Debug + Send + Hash + Sync, V: Debug + Send + Sync + Clone> Sync
    for ShardedSyncCache<K, V>
{
}

impl<K: Debug + Hash + Eq + 'static + Send + Sync, V: Debug + Clone + Send + Sync + 'static>
    ShardedSyncCache<K, V>
{
    pub fn create_lru_sharded_cache(capacity: usize) -> ShardedSyncCache<K, V> {
        let per_shard = per_shard(capacity, NUM_SHARDS);
        let shard = (0..NUM_SHARDS)
            .map(|_| {
                Box::new(SyncCacheWrap::new(LruWrap::new(per_shard)))
                    as Box<dyn SyncCache<K = K, V = V>>
            })
            .collect();
        Self { shard }
    }

    pub fn create_lru_sharded_cache_unbounded() -> ShardedSyncCache<K, V> {
        let shard = (0..NUM_SHARDS)
            .map(|_| {
                Box::new(SyncCacheWrap::new(LruWrap::unbounded()))
                    as Box<dyn SyncCache<K = K, V = V>>
            })
            .collect();
        Self { shard }
    }

    pub fn pop_shard(&self, key: &K) -> Option<(K, V)> {
        let index = shard(key, self.shard.len());
        self.shard.get(index)?.pop()
    }

    pub fn shard(&self, key: &K) -> Option<&dyn SyncCache<K = K, V = V>> {
        let index = shard(key, self.shard.len());
        self.shard.get(index).map(|s| s.as_ref())
    }
}

impl<K, V> SyncCache for ShardedSyncCache<K, V>
where
    K: Debug + Hash + Send + Sync,
    V: Debug + Clone + Send + Sync,
{
    type K = K;
    type V = V;

    fn insert(&self, key: Self::K, value: Self::V) -> Option<Self::V> {
        let index = shard(&key, self.shard.len());
        self.shard.get(index).and_then(|a| a.insert(key, value))
    }

    fn get(&self, key: &Self::K) -> Option<Self::V> {
        let index = shard(&key, self.shard.len());
        self.shard.get(index).and_then(|a| a.get(key))
    }

    fn remove(&self, key: &Self::K) -> Option<Self::V> {
        let index = shard(&key, self.shard.len());
        self.shard.get(index).and_then(|a| a.remove(key))
    }

    fn pop(&self) -> Option<(Self::K, Self::V)> {
        let none_empty_shard = self
            .shard
            .iter()
            .filter(|s| s.get_usage().ne(&0))
            .collect::<Vec<_>>();

        if none_empty_shard.is_empty() {
            None
        } else {
            let shard_len = none_empty_shard.len();
            let index = rand::rng().next_u64() as usize % shard_len;
            self.shard.get(index)?.pop()
        }
    }

    fn set_capacity(&self, capacity: NonZeroUsize) {
        // FIXME: Cannot set a precise capacity freely (such as 1000, will be 63 * 16)
        let per_shard = capacity.get().div_ceil(self.shard.len());
        let per_shard =
            NonZeroUsize::new(per_shard).unwrap_or(unsafe { NonZeroUsize::new_unchecked(1) });

        self.shard.iter().for_each(|a| a.set_capacity(per_shard));
    }

    fn get_capacity(&self) -> usize {
        self.shard.iter().map(|a| a.get_capacity()).sum()
    }

    fn get_usage(&self) -> usize {
        self.shard.iter().map(|a| a.get_usage()).sum()
    }
    fn clear(&self) {
        self.shard.iter().for_each(|c| {
            c.clear();
        })
    }
}
