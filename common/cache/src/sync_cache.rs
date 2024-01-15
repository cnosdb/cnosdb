use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;

use parking_lot::Mutex;

use crate::Cache;

pub trait SyncCache: Debug {
    type K;
    type V: Clone;
    fn insert(&self, key: Self::K, value: Self::V) -> Option<Self::V>;
    fn get(&self, key: &Self::K) -> Option<Self::V>;
    fn remove(&self, key: &Self::K) -> Option<Self::V>;
    fn pop(&self) -> Option<(Self::K, Self::V)>;
    fn set_capacity(&self, capacity: NonZeroUsize);
    fn get_capacity(&self) -> usize;
    fn get_usage(&self) -> usize;
    fn clear(&self);
}

#[derive(Debug)]
pub struct SyncCacheWrap<K, V>
where
    K: Debug + Hash + Sync + Send,
    V: Debug + Clone + Sync + Send,
{
    cache: Mutex<Box<dyn Cache<K = K, V = V>>>,
}
impl<K, V> SyncCacheWrap<K, V>
where
    K: Debug + Hash + Sync + Send,
    V: Debug + Clone + Sync + Send,
{
    pub fn new(cache: impl Cache<K = K, V = V> + 'static) -> SyncCacheWrap<K, V> {
        Self {
            cache: Mutex::new(Box::new(cache)),
        }
    }
}

impl<K, V> SyncCache for SyncCacheWrap<K, V>
where
    K: Debug + Hash + Sync + Send,
    V: Debug + Clone + Sync + Send,
{
    type K = K;
    type V = V;

    fn insert(&self, key: Self::K, value: Self::V) -> Option<Self::V> {
        self.cache.lock().insert(key, value)
    }

    fn get(&self, key: &Self::K) -> Option<Self::V> {
        self.cache.lock().get(key)
    }

    fn remove(&self, key: &Self::K) -> Option<Self::V> {
        self.cache.lock().remove(key)
    }

    fn pop(&self) -> Option<(Self::K, Self::V)> {
        self.cache.lock().pop()
    }

    fn set_capacity(&self, capacity: NonZeroUsize) {
        self.cache.lock().set_capacity(capacity)
    }

    fn get_capacity(&self) -> usize {
        self.cache.lock().get_capacity()
    }

    fn get_usage(&self) -> usize {
        self.cache.lock().get_usage()
    }
    fn clear(&self) {
        self.cache.lock().clear()
    }
}
