#![allow(dead_code)]

pub mod asynchronous;

use std::borrow::Borrow;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::Arc;

use parking_lot::Mutex;
use utils::BkdrHasher;

use crate::cache::{Cache, KeyPtr};
use crate::AfterRemovedFnMut;

const NUM_SHARD_BITS: usize = 4;
const NUM_SHARDS: usize = 1 << NUM_SHARD_BITS;

#[derive(Debug)]
pub struct ShardedCache<K: Display, V: Debug> {
    shard: [Arc<Mutex<Cache<K, V>>>; NUM_SHARDS],
}

impl<K, V> Default for ShardedCache<K, V>
where
    K: Eq + Hash + Display,
    V: Debug,
{
    fn default() -> Self {
        Self::with_capacity(128)
    }
}

impl<K, V> ShardedCache<K, V>
where
    K: Eq + Hash + Display,
    V: Debug,
{
    pub fn with_capacity(capacity: usize) -> ShardedCache<K, V> {
        // FIXME: Cannot set a precise capacity freely (such as 1000, will be 63 * 16)
        let per_shard = (capacity + (NUM_SHARDS - 1)) / NUM_SHARDS;
        Self {
            shard: unsafe {
                let shard = MaybeUninit::<[Arc<Mutex<Cache<K, V>>>; NUM_SHARDS]>::uninit();
                let mut shard = shard.assume_init();
                for e in shard.iter_mut() {
                    ptr::write(e, Arc::new(Mutex::new(Cache::with_capacity(per_shard))));
                }
                shard
            },
        }
    }

    fn shard<Q>(k: &Q) -> usize
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut hasher = BkdrHasher::new();
        k.hash(&mut hasher);
        let hash = hasher.finish();
        (hash % NUM_SHARDS as u64) as usize
    }

    pub fn insert(&self, k: K, v: V) -> Option<&V> {
        self.insert_opt(k, v, 1, None)
    }

    pub fn insert_opt(
        &self,
        k: K,
        v: V,
        charge: usize,
        after_removed: Option<AfterRemovedFnMut<K, V>>,
    ) -> Option<&V> {
        self.shard[Self::shard(&k)]
            .lock()
            .insert_and_return_value(k, v, charge, after_removed)
            .map(|v| unsafe { &(*v).v })
    }

    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shard[Self::shard(k)]
            .lock()
            .get_value(k)
            .map(|v| unsafe { &(*v).v })
    }

    pub fn get_mut<Q>(&self, k: &Q) -> Option<&mut V>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shard[Self::shard(k)]
            .lock()
            .get_value_mut(k)
            .map(|v| unsafe { &mut (*v).v })
    }

    pub fn remove<Q>(&self, k: &Q)
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shard[Self::shard(k)].lock().remove(k)
    }

    pub fn prune(&self) {
        for s in self.shard.iter() {
            s.lock().prune();
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tokio::spawn;

    use crate::sharded::ShardedCache;

    const CACHE_SIZE: usize = 1000;

    struct ShardedCacheTester {
        cache: ShardedCache<i32, i32>,
        deleted_keys: Arc<RwLock<Vec<i32>>>,
        deleted_values: Arc<RwLock<Vec<i32>>>,
    }

    impl ShardedCacheTester {
        fn new() -> Self {
            Self {
                cache: ShardedCache::with_capacity(CACHE_SIZE),
                deleted_keys: Arc::new(RwLock::new(Vec::new())),
                deleted_values: Arc::new(RwLock::new(Vec::new())),
            }
        }

        fn get_or_default(&mut self, key: i32) -> i32 {
            if let Some(v) = self.cache.get(&key) {
                *v
            } else {
                -1
            }
        }

        fn insert(&mut self, key: i32, value: i32) {
            Self::insert_charge(self, key, value, 1);
        }

        fn insert_charge(&mut self, key: i32, value: i32, charge: i32) {
            self.cache.insert_opt(
                key,
                value,
                charge as usize,
                Some(Box::new(fn_deleter(
                    self.deleted_keys.clone(),
                    self.deleted_values.clone(),
                ))),
            );
        }

        fn insert_and_return_default(&mut self, key: i32, value: i32) -> i32 {
            if let Some(v) = self.cache.insert_opt(
                key,
                value,
                1,
                Some(Box::new(fn_deleter(
                    self.deleted_keys.clone(),
                    self.deleted_values.clone(),
                ))),
            ) {
                *v
            } else {
                -1
            }
        }

        fn remove(&mut self, key: i32) {
            self.cache.remove(&key);
        }
    }

    fn fn_deleter(
        keys: Arc<RwLock<Vec<i32>>>,
        values: Arc<RwLock<Vec<i32>>>,
    ) -> impl FnMut(&i32, &mut i32) {
        move |k, v| {
            keys.write().push(*k);
            values.write().push(*v);
        }
    }

    #[test]
    fn test_hit_and_miss() {
        let mut ct = ShardedCacheTester::new();
        assert_eq!(-1, ct.get_or_default(100));

        ct.insert(100, 101);
        assert_eq!(ct.get_or_default(100), 101);
        assert_eq!(ct.get_or_default(200), -1);
        assert_eq!(ct.get_or_default(300), -1);

        ct.insert(200, 201);
        assert_eq!(ct.get_or_default(100), 101);
        assert_eq!(ct.get_or_default(200), 201);
        assert_eq!(ct.get_or_default(300), -1);

        ct.insert(100, 102);
        assert_eq!(ct.get_or_default(100), 102);
        assert_eq!(ct.get_or_default(200), 201);
        assert_eq!(ct.get_or_default(300), -1);

        assert_eq!(ct.deleted_keys.read().len(), 1);
        assert_eq!(ct.deleted_keys.read()[0], 100);
        assert_eq!(ct.deleted_values.read()[0], 101);
    }

    #[tokio::test]
    async fn test_multi_threads() {
        let lru = Arc::new(ShardedCache::<&str, i32>::with_capacity(1));

        let lru_2 = lru.clone();
        let jh = spawn(async move {
            assert_eq!(lru_2.insert("One", 2), Some(&2));
            assert_eq!(lru_2.get(&"One"), Some(&2));

            assert_eq!(lru_2.insert("One", 1,), Some(&1));
            assert_eq!(lru_2.get(&"One"), Some(&1));

            lru_2.remove(&"One");
            assert_eq!(lru_2.get(&"One"), None);

            assert_eq!(lru_2.insert("One", 1,), Some(&1));
        });
        jh.await.unwrap();

        assert_eq!(lru.get(&"One"), Some(&1));
    }

    #[test]
    fn test_remove() {
        let mut ct = ShardedCacheTester::new();
        ct.remove(200);
        assert_eq!(0, ct.deleted_keys.read().len());

        ct.insert(100, 101);
        ct.insert(200, 201);
        ct.remove(100);
        assert_eq!(ct.get_or_default(100), -1);
        assert_eq!(ct.get_or_default(200), 201);
        assert_eq!(ct.deleted_keys.read().len(), 1);
        assert_eq!(ct.deleted_keys.read()[0], 100);
        assert_eq!(ct.deleted_values.read()[0], 101);

        ct.remove(100);
        assert_eq!(ct.get_or_default(100), -1);
        assert_eq!(ct.get_or_default(200), 201);
        assert_eq!(ct.deleted_keys.read().len(), 1);
    }

    #[test]
    fn test_eviction_policy() {
        let mut ct = ShardedCacheTester::new();
        ct.insert(100, 101);
        ct.insert(200, 201);
        ct.insert(300, 301);

        assert_eq!(ct.cache.get(&300), Some(&301));

        for i in 0..CACHE_SIZE + 100 {
            let i1 = i as i32;
            ct.insert(1000 + i1, 2000 + i1);
            assert_eq!(ct.get_or_default(1000 + i1), 2000 + i1);
            assert_eq!(ct.get_or_default(100), 101);
        }

        assert_eq!(ct.get_or_default(100), 101);
        assert_eq!(ct.get_or_default(200), -1);
        assert_eq!(ct.get_or_default(300), -1);
    }

    #[test]
    fn test_use_exceeds_cache_size() {
        let mut ct = ShardedCacheTester::new();
        let mut v = Vec::new();
        for i in 0..CACHE_SIZE + 100 {
            let i1 = i as i32;
            v.push(ct.insert_and_return_default(1000 + i1, 2000 + i1));
        }
        for (idx, val) in v.iter().enumerate().skip(100) {
            assert_eq!(ct.get_or_default(1000 + idx as i32), *val);
        }
    }

    #[test]
    fn test_heavy_entries() {
        let mut ct = ShardedCacheTester::new();
        const LIGHT: i32 = 1;
        const HEAVY: i32 = 10;
        let mut added = 0;
        let mut index = 0;
        while added < 2 * CACHE_SIZE {
            let weight = if (index & 1) == 1 { LIGHT } else { HEAVY };
            ct.insert_charge(index, 1000 + index, weight);
            added += weight as usize;
            index += 1;
        }

        let mut cached_weight = 0;
        for i in 0..index {
            let weight = if i & 1 == 1 { LIGHT } else { HEAVY };
            let r = ct.get_or_default(i);
            if r >= 0 {
                cached_weight += weight;
                assert_eq!(1000 + i, r);
            }
        }

        assert!(cached_weight <= CACHE_SIZE as i32 + CACHE_SIZE as i32 / 10);
    }

    #[test]
    fn test_prune() {
        let mut ct = ShardedCacheTester::new();
        ct.insert(1, 100);
        ct.insert(2, 200);
        assert_eq!(ct.cache.get(&1), Some(&100));

        ct.cache.prune();
        assert_eq!(ct.get_or_default(1), -1);
        assert_eq!(ct.get_or_default(2), -1);
    }

    #[test]
    fn test_zero_size_cache() {
        let mut ct = ShardedCacheTester::new();
        ct.cache = ShardedCache::with_capacity(0);
        ct.insert(1, 100);
        assert_eq!(ct.get_or_default(1), -1);
    }
}
