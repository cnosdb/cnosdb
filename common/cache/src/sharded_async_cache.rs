use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use futures::future::join_all;
use rand::RngCore;
use tokio::sync::{Mutex, MutexGuard};

use crate::async_cache::AsyncCache;
use crate::lru_cache::LruWrap;
use crate::{shard, Cache, NUM_SHARDS};

#[derive(Debug)]
pub struct ShardedAsyncCache<K, V>
where
    K: Debug + Hash,
    V: Debug + Clone,
{
    shard: Vec<Mutex<Box<dyn Cache<K = K, V = V>>>>,
}

impl<K, V> ShardedAsyncCache<K, V>
where
    K: Debug + Hash + Eq + 'static + Send + Sync,
    V: Debug + Clone + 'static + Send + Sync,
{
    pub fn create_lru_sharded_cache(capacity: usize) -> ShardedAsyncCache<K, V> {
        let per_shard = crate::per_shard(capacity, NUM_SHARDS);
        let shard = (0..NUM_SHARDS)
            .map(|_| Mutex::new(Box::new(LruWrap::new(per_shard)) as Box<dyn Cache<K = K, V = V>>))
            .collect::<Vec<Mutex<Box<dyn Cache<K = K, V = V>>>>>();
        Self { shard }
    }

    pub async fn pop_shard(&self, key: &K) -> Option<(K, V)> {
        let index = shard(key, self.shard.len());
        self.shard.get(index)?.lock().await.pop()
    }

    pub async fn lock_shard(
        &self,
        key: &K,
    ) -> Option<MutexGuard<'_, Box<dyn Cache<K = K, V = V>>>> {
        let index = shard(&key, self.shard.len());
        Some(self.shard.get(index)?.lock().await)
    }
}

#[async_trait]
impl<K, V> AsyncCache for ShardedAsyncCache<K, V>
where
    K: Debug + Hash + Send + Sync,
    V: Debug + Clone + Send + Sync,
{
    type K = K;
    type V = V;

    async fn insert(&self, key: Self::K, value: Self::V) -> Option<Self::V> {
        let index = shard(&key, self.shard.len());
        self.shard.get(index)?.lock().await.insert(key, value)
    }

    async fn get(&self, key: &Self::K) -> Option<Self::V> {
        let index = shard(&key, self.shard.len());
        self.shard.get(index)?.lock().await.get(key)
    }

    async fn remove(&self, key: &Self::K) -> Option<Self::V> {
        let index = shard(&key, self.shard.len());
        self.shard.get(index)?.lock().await.remove(key)
    }

    async fn pop(&self) -> Option<(Self::K, Self::V)> {
        let index = rand::rng().next_u64() as usize % self.shard.len();
        self.shard.get(index)?.lock().await.pop()
    }

    async fn set_capacity(&self, capacity: NonZeroUsize) {
        let per_shard = crate::per_shard(capacity.get(), self.shard.len());
        let futures = self
            .shard
            .iter()
            .map(|a| async { a.lock().await.set_capacity(per_shard) });
        join_all(futures).await;
    }

    async fn get_capacity(&self) -> usize {
        let futures = self
            .shard
            .iter()
            .map(|a| async { a.lock().await.get_capacity() });
        join_all(futures).await.iter().sum()
    }

    async fn get_usage(&self) -> usize {
        let futures = self
            .shard
            .iter()
            .map(|a| async { a.lock().await.get_usage() });
        join_all(futures).await.iter().sum()
    }

    async fn clear(&self) {
        let futures = self.shard.iter().map(|a| async { a.lock().await.clear() });
        join_all(futures).await;
    }
}
