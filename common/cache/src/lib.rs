mod async_cache;
mod circular_kvcache;
mod lru_cache;
mod sharded_async_cache;
mod sharded_sync_cache;
mod sync_cache;

use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

pub use crate::async_cache::*;
pub use crate::circular_kvcache::*;
pub use crate::lru_cache::*;
pub use crate::sharded_async_cache::*;
pub use crate::sharded_sync_cache::*;
pub use crate::sync_cache::*;

pub type AfterRemovedFnMut<K, V> = Box<dyn FnMut(&K, &mut V) + Send + Sync>;

pub(crate) const NUM_SHARD_BITS: usize = 4;
pub(crate) const NUM_SHARDS: usize = 1 << NUM_SHARD_BITS;

pub(crate) fn per_shard(capacity: usize, shard_len: usize) -> NonZeroUsize {
    // FIXME: Cannot set a precise capacity freely (such as 1000, will be 63 * 16)
    let per_shard = capacity.div_ceil(shard_len);
    NonZeroUsize::new(per_shard).unwrap_or(unsafe { NonZeroUsize::new_unchecked(1) })
}

pub(crate) fn shard<K: Hash>(key: &K, shard_len: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let res = hasher.finish() as usize % shard_len;
    debug_assert!(res <= shard_len);
    res
}

pub trait Cache: Debug + Sync + Send {
    type K;
    type V: Clone;
    /// Insert a key-value pair,
    /// If the key already exists in the cache,
    /// then it updates the key's value and returns the old value.
    /// Otherwise, None is returned.
    fn insert(&mut self, key: Self::K, value: Self::V) -> Option<Self::V>;

    fn get(&mut self, key: &Self::K) -> Option<Self::V>;
    fn remove(&mut self, key: &Self::K) -> Option<Self::V>;
    /// Removes and returns the key and value corresponding to the least recently
    /// used item or `None` if the cache is empty.
    ///
    /// # Example
    ///
    /// ```
    /// use cache::{Cache, LruWrap};
    /// use std::num::NonZeroUsize;
    /// let mut cache = LruWrap::new(NonZeroUsize::new(2).unwrap());
    ///
    /// cache.insert(2, "a");
    /// cache.insert(3, "b");
    /// cache.insert(4, "c");
    /// cache.get(&3);
    ///
    /// assert_eq!(cache.pop(), Some((4, "c")));
    /// assert_eq!(cache.pop(), Some((3, "b")));
    /// assert_eq!(cache.pop(), None);
    /// assert_eq!(cache.get_usage(), 0);
    /// ```
    fn pop(&mut self) -> Option<(Self::K, Self::V)>;
    fn set_capacity(&mut self, capacity: NonZeroUsize);
    fn get_capacity(&self) -> usize;
    fn get_usage(&self) -> usize;
    fn clear(&mut self);
}
