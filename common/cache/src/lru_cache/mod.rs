#![allow(dead_code)]

use std::fmt::Debug;
use std::hash::Hash;
use std::num::NonZeroUsize;

use lru::LruCache;

use crate::Cache;

#[derive(Debug)]
pub struct LruWrap<K, V>
where
    K: Debug + Hash + Eq + Sync + Send,
    V: Sync + Send,
{
    pub cache: LruCache<K, V>,
}

impl<K: Debug + Hash + Eq + Sync + Send, V: Sync + Send> LruWrap<K, V> {
    pub fn new(cap: NonZeroUsize) -> LruWrap<K, V> {
        Self {
            cache: LruCache::new(cap),
        }
    }

    pub fn unbounded() -> LruWrap<K, V> {
        Self {
            cache: LruCache::unbounded(),
        }
    }
}

impl<K: Debug + Hash + Eq + Sync + Send, V: Clone + Debug + Sync + Send> Cache for LruWrap<K, V> {
    type K = K;
    type V = V;

    fn insert(&mut self, key: Self::K, value: Self::V) -> Option<Self::V> {
        self.cache.put(key, value)
    }

    fn get(&mut self, key: &Self::K) -> Option<Self::V> {
        self.cache.get(key).cloned()
    }

    fn remove(&mut self, key: &Self::K) -> Option<Self::V> {
        self.cache.pop(key)
    }

    fn pop(&mut self) -> Option<(Self::K, Self::V)> {
        self.cache.pop_lru()
    }

    fn set_capacity(&mut self, capacity: NonZeroUsize) {
        self.cache.resize(capacity)
    }

    fn get_capacity(&self) -> usize {
        self.cache.cap().into()
    }

    fn get_usage(&self) -> usize {
        self.cache.len()
    }

    fn clear(&mut self) {
        self.cache.clear()
    }
}
