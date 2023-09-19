use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::Cache;

#[async_trait]
pub trait AsyncCache: Debug {
    type K;
    type V: Clone;
    async fn insert(&self, key: Self::K, value: Self::V) -> Option<Self::V>;
    async fn get(&self, key: &Self::K) -> Option<Self::V>;
    async fn remove(&self, key: &Self::K) -> Option<Self::V>;
    async fn pop(&self) -> Option<(Self::K, Self::V)>;
    async fn set_capacity(&self, capacity: NonZeroUsize);
    async fn get_capacity(&self) -> usize;
    async fn get_usage(&self) -> usize;
    async fn clear(&self);
}

#[derive(Debug)]
pub struct AsyncCacheWrap<K, V> {
    cache: Mutex<Box<dyn Cache<K = K, V = V>>>,
}
impl<K, V> AsyncCacheWrap<K, V> {
    pub fn new(cache: impl Cache<K = K, V = V> + 'static) -> AsyncCacheWrap<K, V> {
        Self {
            cache: Mutex::new(Box::new(cache)),
        }
    }
}

#[async_trait]
impl<K, V> AsyncCache for AsyncCacheWrap<K, V>
where
    K: Debug + Hash + Send + Sync,
    V: Debug + Clone + Send + Sync,
{
    type K = K;
    type V = V;

    async fn insert(&self, key: Self::K, value: Self::V) -> Option<Self::V> {
        self.cache.lock().await.insert(key, value)
    }

    async fn get(&self, key: &Self::K) -> Option<Self::V> {
        self.cache.lock().await.get(key)
    }

    async fn remove(&self, key: &Self::K) -> Option<Self::V> {
        self.cache.lock().await.remove(key)
    }

    async fn pop(&self) -> Option<(Self::K, Self::V)> {
        self.cache.lock().await.pop()
    }

    async fn set_capacity(&self, capacity: NonZeroUsize) {
        self.cache.lock().await.set_capacity(capacity)
    }

    async fn get_capacity(&self) -> usize {
        self.cache.lock().await.get_capacity()
    }

    async fn get_usage(&self) -> usize {
        self.cache.lock().await.get_usage()
    }
    async fn clear(&self) {
        self.cache.lock().await.clear()
    }
}
