use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::mem::{self, MaybeUninit};
use std::ptr::{self, NonNull};
use std::sync::Arc;

use parking_lot::Mutex;
use utils::BkdrHasher;

const NUM_SHARD_BITS: usize = 4;
const NUM_SHARDS: usize = 1 << NUM_SHARD_BITS;

pub type AfterRemovedFnMut<K, V> = Box<dyn FnMut(&K, &V) + Send + Sync>;

#[derive(Debug)]
pub struct ShardedCache<K, V> {
    shard: [Arc<Mutex<Cache<K, V>>>; NUM_SHARDS],
}

impl<K, V> Default for ShardedCache<K, V>
where
    K: Eq + Hash,
{
    fn default() -> Self {
        Self::with_capacity(1000)
    }
}

impl<K, V> ShardedCache<K, V>
where
    K: Eq + Hash,
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

#[derive(Debug)]
struct Cache<K, V> {
    /// Cache capacity
    capacity: usize,
    /// Used of cache capacity
    usage: usize,
    /// Dummy node for head
    head: *mut Entry<K, V>,
    /// Dummy node for tail
    tail: *mut Entry<K, V>,
    /// HashMap for storinf and searching
    table: HashMap<KeyPtr<K>, NonNull<Entry<K, V>>>,
}

unsafe impl<K: Send, V: Send> Send for Cache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for Cache<K, V> {}

#[allow(dead_code)]
impl<K, V> Cache<K, V>
where
    K: Eq + Hash,
{
    pub fn with_capacity(capacity: usize) -> Cache<K, V> {
        let head = Box::into_raw(Box::new(Entry::<K, V>::null()));
        let tail = Box::into_raw(Box::new(Entry::<K, V>::null()));

        unsafe {
            (*head).next = tail;
            (*tail).prev = head;
        }

        Self {
            capacity,
            usage: 0,
            head,
            tail,
            table: HashMap::with_capacity(capacity),
        }
    }

    /// Insert a key-value pair, then return reference of v.
    ///
    /// If capacity of Cache is 0, return None.
    pub fn insert(&mut self, k: K, v: V) -> Option<&V> {
        self.insert_opt(k, v, 1, None)
    }

    /// Insert a key-value pair with specified charge against the total cache capacity,
    /// then return reference of v.
    ///
    /// If charge > capacity, return None.
    pub fn insert_opt(
        &mut self,
        k: K,
        v: V,
        charge: usize,
        after_remove: Option<AfterRemovedFnMut<K, V>>,
    ) -> Option<&V> {
        self.insert_and_return_value(k, v, charge, after_remove)
            .map(|v| unsafe { &(*v).v })
    }

    /// Returns a reference to the value corresponding to the key.
    ///
    /// If key exists in cache, move it to head.
    pub fn get<Q>(&mut self, k: &Q) -> Option<&V>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get_value(k).map(|v| unsafe { &(*v).v })
    }

    /// Returns a mutable reference to the value corresponding to the key.
    ///
    /// If key exists in cache, move it to head.
    pub fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get_value_mut(k).map(|v| unsafe { &mut (*v).v })
    }

    /// Removes a key from the map, release the usage using charge of the value.
    pub fn remove<Q>(&mut self, k: &Q)
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(ent) = self.table.remove(k) {
            let ptr = ent.as_ptr();
            self.unset_entry(ptr);
            self.usage = self.usage.saturating_sub(unsafe { (*ptr).charge });
            self.after_removed(ptr);
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    fn insert_and_return_value(
        &mut self,
        k: K,
        v: V,
        charge: usize,
        after_remove: Option<AfterRemovedFnMut<K, V>>,
    ) -> Option<*const Value<K, V>> {
        if self.capacity < charge {
            return None;
        }
        let mut entry = Entry::new(k, v, charge, after_remove);
        match self.table.get_mut(&entry.as_key_ptr()) {
            Some(curr_ent) => {
                let current_ent_ptr = curr_ent.as_ptr();
                let curr_v_ptr = unsafe { &mut curr_ent.as_mut().v };
                mem::swap(&mut entry.v, curr_v_ptr);
                self.unset_entry(current_ent_ptr);
                self.set_entry(current_ent_ptr);
                self.after_removed(&mut entry as *mut Entry<K, V>);

                Some(unsafe { curr_v_ptr.assume_init_ref() })
            }
            None => {
                let lru = self.head;
                let capacity = self.capacity.saturating_sub(entry.charge);
                if self.usage > capacity {
                    unsafe {
                        while self.usage > capacity && (*lru).next != self.tail {
                            let old_key_ptr = (*(*self.tail).prev).as_key_ptr();
                            if let Some(old_ent) = self.table.remove(&old_key_ptr) {
                                let old_ent_ptr = old_ent.as_ptr();
                                self.unset_entry(old_ent_ptr);
                                self.usage = self.usage.saturating_sub((*old_ent_ptr).charge);
                                self.after_removed(old_ent_ptr);
                            }
                        }
                    }
                }
                self.usage += entry.charge;

                let entry_ptr = Box::into_raw(Box::new(entry));
                self.set_entry(entry_ptr);
                let key_ptr = unsafe { (*entry_ptr).k.as_ptr() };
                let entry_v = unsafe { NonNull::new_unchecked(entry_ptr) };
                let entry_ptr = entry_v.as_ptr();
                self.table.insert(KeyPtr { p: key_ptr }, entry_v);
                Some(unsafe { (*entry_ptr).v.assume_init_ref() })
            }
        }
    }

    fn get_value<Q>(&mut self, k: &Q) -> Option<*const Value<K, V>>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.table.get_mut(k) {
            Some(ent) => {
                let ptr = ent.as_ptr();
                self.unset_entry(ptr);
                self.set_entry(ptr);
                Some(unsafe { (*ptr).v.as_ptr() })
            }
            None => None,
        }
    }

    fn get_value_mut<Q>(&mut self, k: &Q) -> Option<*mut Value<K, V>>
    where
        KeyPtr<K>: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        match self.table.get_mut(k) {
            Some(ent) => {
                let ptr = ent.as_ptr();
                self.unset_entry(ptr);
                self.set_entry(ptr);
                Some(unsafe { (*ptr).v.as_mut_ptr() })
            }
            None => None,
        }
    }

    fn set_entry(&mut self, ent: *mut Entry<K, V>) {
        unsafe {
            (*ent).next = (*self.head).next;
            (*(*self.head).next).prev = ent;
            (*ent).prev = self.head;
            (*self.head).next = ent;
        }
    }

    fn unset_entry(&mut self, ent: *mut Entry<K, V>) {
        unsafe {
            (*(*ent).next).prev = (*ent).prev;
            (*(*ent).prev).next = (*ent).next;
        }
    }

    fn prune(&mut self) {
        let lru = self.head;
        unsafe {
            while (*lru).next != self.tail {
                let ptr = (*lru).next;
                self.table.remove(&(*ptr).as_key_ptr());
                self.unset_entry(ptr);
                self.usage = self.usage.saturating_sub((*ptr).charge);
                self.after_removed(ptr);
            }
        }
    }

    fn after_removed(&mut self, ent: *mut Entry<K, V>) {
        unsafe {
            let k = (*ent).k.assume_init_ref();
            let v = (*ent).v.assume_init_mut();
            if let Some(f) = v.after_removed.as_mut() {
                f(k, &v.v);
            }
        }
    }
}

impl<K, V> Drop for Cache<K, V> {
    fn drop(&mut self) {
        self.table.drain().for_each(|(_, ent)| unsafe {
            let mut ent = *Box::from_raw(ent.as_ptr());
            ptr::drop_in_place(ent.k.as_mut_ptr());
            ptr::drop_in_place(ent.v.as_mut_ptr());
        });
        let _head = unsafe { *Box::from_raw(self.head) };
        let _tail = unsafe { *Box::from_raw(self.tail) };
    }
}

struct Entry<K, V> {
    k: MaybeUninit<K>,
    v: MaybeUninit<Value<K, V>>,
    charge: usize,

    next: *mut Entry<K, V>,
    prev: *mut Entry<K, V>,
}

impl<K, V> Entry<K, V> {
    pub fn null() -> Entry<K, V> {
        Entry {
            k: MaybeUninit::uninit(),
            v: MaybeUninit::uninit(),
            charge: 0,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

impl<K, V> Entry<K, V> {
    pub fn new(
        k: K,
        v: V,
        charge: usize,
        after_removed: Option<AfterRemovedFnMut<K, V>>,
    ) -> Entry<K, V> {
        Entry {
            k: MaybeUninit::new(k),
            v: MaybeUninit::new(Value { v, after_removed }),
            charge,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }

    fn as_key_ptr(&self) -> KeyPtr<K> {
        KeyPtr { p: self.k.as_ptr() }
    }
}

impl<K: Debug, V: Debug> Debug for Entry<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("k", &self.k)
            .field("v", &self.v)
            .field("charge", &self.charge)
            .field("next", &self.next)
            .field("prev", &self.prev)
            .finish()
    }
}

pub struct KeyPtr<T> {
    p: *const T,
}

impl<T: Eq> Eq for KeyPtr<T> {}

impl<T: PartialEq> PartialEq for KeyPtr<T> {
    fn eq(&self, other: &KeyPtr<T>) -> bool {
        unsafe { (*self.p).eq(&*other.p) }
    }
}

impl<T: Hash> Hash for KeyPtr<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe {
            (*self.p).hash(state);
        }
    }
}

impl<T: Display> Display for KeyPtr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { (*self.p).fmt(f) }
    }
}

impl<T: Debug> Debug for KeyPtr<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyPtr")
            .field("p", unsafe { &(*self.p) })
            .finish()
    }
}

impl<T> Borrow<T> for KeyPtr<T> {
    fn borrow(&self) -> &T {
        unsafe { &*self.p }
    }
}

impl<T> Borrow<[T]> for KeyPtr<Vec<T>> {
    fn borrow(&self) -> &[T] {
        unsafe { &*self.p }
    }
}

impl Borrow<str> for KeyPtr<String> {
    fn borrow(&self) -> &str {
        unsafe { &*self.p }
    }
}

impl<T: ?Sized> Borrow<T> for KeyPtr<Box<T>> {
    fn borrow(&self) -> &T {
        unsafe { &*self.p }
    }
}

pub struct Value<K, V> {
    v: V,
    after_removed: Option<AfterRemovedFnMut<K, V>>,
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use parking_lot::RwLock;
    use tokio::spawn;

    use crate::{Cache, ShardedCache};

    #[test]
    fn test_cache_basic() {
        let mut lru: Cache<&str, u32> = Cache::with_capacity(2);
        assert_eq!(lru.insert("One", 2), Some(&2));
        assert_eq!(lru.len(), 1);
        assert_eq!(lru.get(&"One"), Some(&2));

        assert_eq!(lru.insert("One", 1,), Some(&1));
        assert_eq!(lru.get(&"One"), Some(&1));
        assert_eq!(lru.len(), 1);

        assert_eq!(lru.insert("Two", 2), Some(&2));
        assert_eq!(lru.len(), 2);

        assert_eq!(lru.get(&"One"), Some(&1));
        assert_eq!(lru.get(&"Two"), Some(&2));

        assert_eq!(lru.insert("Three", 3), Some(&3));
        assert_eq!(lru.get(&"Three"), Some(&3));
        assert_eq!(lru.len(), 2);
        assert_eq!(lru.get(&"One"), None);

        lru.remove(&"Three");
        assert_eq!(lru.len(), 1);
        assert_eq!(lru.get(&"Three"), None);
    }

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
    ) -> impl FnMut(&i32, &i32) {
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
