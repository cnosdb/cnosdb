extern crate core;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hasher;
use std::mem::MaybeUninit;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::sync::Arc;

use parking_lot::RwLock;

use utils::BkdrHasher;

pub trait NodeVal<T> {
    fn get_value(&self) -> &T;
}

type NodeValPtr<T> = Arc<RefCell<dyn NodeVal<T>>>;

pub trait Cache<T> {
    /// Insert a mapping from key->value into the cache and assign it the specified charge
    /// against the total cache capacity.
    ///
    /// Return a handle that corresponds to the mapping. the caller must call
    /// `release(handle)` when the returned mapping is no longer needed.
    ///
    /// When the inserted entry is no longer needed, the key and value will be passed to
    /// `deleter`.
    fn insert(
        &mut self,
        key: String,
        value: T,
        charge: usize,
        deleter: Box<dyn FnMut(&String, &T)>,
    ) -> NodeValPtr<T>;

    /// If the cache has no mapping for `key`, returns `None`. Otherwise, return a handle
    /// that corresponds to the mapping. The caller must call `release(handle)` when the
    /// returned mapping is no longer needed.
    fn lookup(&self, key: &String) -> Option<NodeValPtr<T>>;

    /// Release a mapping returned by a previous `insert` or `lookup`.
    /// REQUIRES: `handle` must not have been released yet.
    /// REQUIRES: `handle` must have been returned by a method on this instance.
    fn release(&mut self, handle: NodeValPtr<T>);

    /// If the cache contains entry for the key, erase it. Note that the underlying entry
    /// will be kept around until all existing handles to it have been released.
    fn erase(&mut self, key: &String);

    /// Return a new numeric id. May be used by multiple clients who are sharing the same
    /// cache to partition the key space. Typically the client will allocate a new id at
    /// startup and prepend the id to its cache keys.
    fn new_id(&self) -> u64;

    /// Remove all cache entries that are not actively in use. Memory-constrained
    /// applications may wish to call this method to reduce memory usage.
    fn prune(&mut self);

    /// Return an estimate of the combined charges of all elements stored in the cache.
    fn total_charge(&self) -> usize;
}

struct LRUNode<T: Default + Debug + Send + Sync> {
    key: Box<[u8]>,
    value: T,
    charge: usize,
    delft: Option<Box<dyn FnMut(&String, &T)>>,
    next: *mut LRUNode<T>,
    prev: *mut LRUNode<T>,
}

// type LRUNodePtr<T> = Rc<RefCell<LRUNode<T>>>;
type LRUNodePtr<T> = Arc<RefCell<LRUNode<T>>>;

impl<T> Drop for LRUNode<T>
where
    T: Default + Debug + Send + Sync,
{
    fn drop(&mut self) {
        let key = self.key();
        if let Some(ref mut deleter) = self.delft {
            (deleter)(&key, &self.value);
        }
    }
}

impl<T> Default for LRUNode<T>
where
    T: Default + Debug + Send + Sync,
{
    fn default() -> Self {
        LRUNode {
            key: Vec::new().into_boxed_slice(),
            value: T::default(),
            charge: 0,
            delft: None,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }
}

impl<T> LRUNode<T>
where
    T: Default + Debug + Send + Sync,
{
    fn new(key: Box<[u8]>, value: T, charge: usize, deleter: Box<dyn FnMut(&String, &T)>) -> Self {
        Self {
            key,
            value,
            charge,
            delft: Some(deleter),
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }

    fn key(&self) -> String {
        let key = unsafe { String::from_utf8_unchecked(self.key.to_vec()) };
        key
    }
}

impl<T> NodeVal<T> for LRUNode<T>
where
    T: Default + Debug + Send + Sync,
{
    fn get_value(&self) -> &T {
        &self.value
    }
}

struct HashData<T: Default + Debug + Send + Sync> {
    usage: usize,
    lru: *mut LRUNode<T>,
    in_use: *mut LRUNode<T>,
    table: HashMap<String, LRUNodePtr<T>>,
}

struct LRUCache<T: Default + Debug + Send + Sync + 'static> {
    capacity: usize,
    inner_data: RwLock<HashData<T>>,
}

impl<T> Drop for LRUCache<T>
where
    T: Default + Debug + Send + Sync + 'static,
{
    fn drop(&mut self) {
        let mutex_data = self.inner_data.write();
        Self::drop_dummy_node(mutex_data.lru);
        Self::drop_dummy_node(mutex_data.in_use);
    }
}

impl<T> LRUCache<T>
where
    T: Default + Debug + Send + Sync + 'static,
{
    fn new(capacity: usize) -> Self {
        let mutex_data = HashData {
            usage: 0,
            lru: Self::create_dummy_node(),
            in_use: Self::create_dummy_node(),
            table: HashMap::default(),
        };

        Self {
            capacity,
            inner_data: RwLock::new(mutex_data),
        }
    }

    fn create_dummy_node() -> *mut LRUNode<T> {
        unsafe {
            let n = Box::into_raw(Box::new(LRUNode::default()));
            (*n).next = n;
            (*n).prev = n;
            n
        }
    }

    fn drop_dummy_node(n: *mut LRUNode<T>) {
        assert!(!n.is_null());
        unsafe {
            let _ = *Box::from_raw(n);
        }
    }

    fn inc_ref(list: *mut LRUNode<T>, h: &LRUNodePtr<T>) {
        if Arc::strong_count(h) == 1 {
            let p = h.borrow_mut().deref_mut() as *mut LRUNode<T>;
            Self::lru_remove(p);
            Self::lru_append(list, p);
        }
    }

    fn dec_ref(list: *mut LRUNode<T>, h: NodeValPtr<T>) {
        let c = Arc::strong_count(&h);
        if c == 2 {
            let p = h.borrow_mut().deref_mut() as *mut dyn NodeVal<T> as *mut LRUNode<T>;
            Self::lru_remove(p);
            Self::lru_append(list, p);
        }
    }

    fn lru_remove(h: *mut LRUNode<T>) {
        unsafe {
            (*(*h).next).prev = (*h).prev;
            (*(*h).prev).next = (*h).next;
        }
    }

    fn lru_append(list: *mut LRUNode<T>, h: *mut LRUNode<T>) {
        unsafe {
            (*h).next = list;
            (*h).prev = (*list).prev;
            (*(*h).prev).next = h;
            (*(*h).next).prev = h;
        }
    }

    fn finish_erase(mutex_data: &mut HashData<T>, e: LRUNodePtr<T>) {
        mutex_data.usage -= e.borrow().charge;
        Self::lru_remove(e.borrow_mut().deref_mut() as *mut LRUNode<T>);
        Self::dec_ref(mutex_data.lru, e);
    }
}

impl<T: Default + Debug + Send + Sync + 'static> Cache<T> for LRUCache<T> {
    fn insert(
        &mut self,
        key: String,
        value: T,
        charge: usize,
        deleter: Box<dyn FnMut(&String, &T)>,
    ) -> NodeValPtr<T> {
        let mut mutex_data = self.inner_data.write();

        let b = Vec::from(key.clone()).into_boxed_slice();
        let mut e = LRUNode::new(b, value, charge, deleter);

        let r = if self.capacity > 0 {
            let r = Arc::new(RefCell::new(e));
            Self::lru_append(mutex_data.in_use, r.clone().borrow_mut().deref_mut());
            mutex_data.usage += charge;
            if let Some(old) = mutex_data.table.insert(key, r.clone()) {
                Self::finish_erase(&mut mutex_data, old);
            };
            r
        } else {
            e.next = ptr::null_mut();
            Arc::new(RefCell::new(e))
        };

        let lru: *mut LRUNode<T> = mutex_data.lru;
        unsafe {
            while mutex_data.usage > self.capacity && (*lru).next != lru {
                let old: *mut LRUNode<T> = (*lru).next;
                if let Some(old) = mutex_data.table.remove(&(*old).key()) {
                    assert_eq!(Arc::strong_count(&old), 1);
                    Self::finish_erase(&mut mutex_data, old);
                }
            }
        }

        r
    }

    fn lookup(&self, key: &String) -> Option<NodeValPtr<T>> {
        let mutex_data = self.inner_data.read();
        match mutex_data.table.get(key) {
            Some(e) => {
                Self::inc_ref(mutex_data.in_use, e);
                Some(e.clone())
            }
            None => None,
        }
    }

    fn release(&mut self, handle: NodeValPtr<T>) {
        let mutex_data = self.inner_data.write();
        Self::dec_ref(mutex_data.lru, handle);
    }

    fn erase(&mut self, key: &String) {
        let mut mutex_data = self.inner_data.write();
        if let Some(p) = mutex_data.table.remove(key) {
            Self::finish_erase(&mut mutex_data, p);
        }
    }

    fn new_id(&self) -> u64 {
        0 // Dummy return value which never get used.
    }

    fn prune(&mut self) {
        let mut mutex_data = self.inner_data.write();
        let lru = mutex_data.lru;
        unsafe {
            while (*lru).next != lru {
                let e = (*lru).next;
                let p = mutex_data.table.remove(&(*e).key()).expect("Key not found");
                Self::finish_erase(&mut mutex_data, p);
            }
        }
    }

    fn total_charge(&self) -> usize {
        let mutex_data = self.inner_data.read();
        mutex_data.usage
    }
}

const NUM_SHARD_BITS: usize = 4;
const NUM_SHARDS: usize = 1 << NUM_SHARD_BITS;

pub struct ShardedLRUCache<T: Default + Debug + Send + Sync + 'static> {
    shard: [LRUCache<T>; NUM_SHARDS],
    last_id: RwLock<u64>,
}

impl<T: Default + Debug + Send + Sync + 'static> ShardedLRUCache<T> {
    pub fn new(capacity: usize) -> Self {
        let per_shard = (capacity + (NUM_SHARDS - 1)) / NUM_SHARDS;
        Self {
            shard: unsafe {
                let shard = MaybeUninit::<[LRUCache<T>; NUM_SHARDS]>::uninit();
                let mut shard = shard.assume_init();
                for e in shard.iter_mut() {
                    ptr::write(e, LRUCache::new(per_shard));
                }
                shard
            },
            last_id: RwLock::new(0),
        }
    }
}

fn shard(str: &str) -> usize {
    let mut hasher = BkdrHasher::new();
    hasher.write(str.as_bytes());
    let hash = hasher.finish();
    (hash % NUM_SHARDS as u64) as usize
}

impl<T: Default + Debug + Send + Sync + 'static> Cache<T> for ShardedLRUCache<T> {
    fn insert(
        &mut self,
        key: String,
        value: T,
        charge: usize,
        deleter: Box<dyn FnMut(&String, &T)>,
    ) -> NodeValPtr<T> {
        self.shard[shard(&key)].insert(key, value, charge, deleter)
    }

    fn lookup(&self, key: &String) -> Option<NodeValPtr<T>> {
        self.shard[shard(&key)].lookup(key)
    }

    fn release(&mut self, handle: NodeValPtr<T>) {
        let key = unsafe {
            let h = handle.borrow().deref() as *const dyn NodeVal<T> as *const LRUNode<T>;
            (*h).key()
        };
        self.shard[shard(&key)].release(handle);
    }

    fn erase(&mut self, key: &String) {
        self.shard[shard(&key)].erase(key)
    }

    fn new_id(&self) -> u64 {
        let mut l = self.last_id.write();
        *l += 1;
        *l
    }

    fn prune(&mut self) {
        for s in self.shard.iter_mut() {
            s.prune()
        }
    }

    fn total_charge(&self) -> usize {
        self.shard.iter().fold(0, |acc, s| acc + s.total_charge())
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use crate::lru_cache::{Cache, NodeValPtr, ShardedLRUCache};

    struct TesterCache {
        cache: Box<dyn Cache<i32>>,
        inserted_keys: Vec<i32>,
        deleted_keys: Rc<RefCell<Vec<i32>>>,
        deleted_values: Rc<RefCell<Vec<i32>>>,
    }

    const CACHE_SIZE: usize = 1000;

    impl TesterCache {
        fn new() -> Self {
            let keys = Rc::new(RefCell::new(Vec::new()));
            let values = Rc::new(RefCell::new(Vec::new()));
            Self {
                cache: Box::new(ShardedLRUCache::new(CACHE_SIZE)),
                inserted_keys: Vec::new(),
                deleted_keys: keys,
                deleted_values: values,
            }
        }

        fn lookup(&mut self, key: i32) -> i32 {
            let k = self.gen_key(key);
            if let Some(handle) = self.cache.lookup(&k) {
                let result = *handle.borrow().get_value() as i32;
                self.cache.release(handle);
                result
            } else {
                -1
            }
        }

        fn insert(&mut self, key: i32, value: i32) {
            Self::insert_charge(self, key, value, 1);
        }

        fn insert_charge(&mut self, key: i32, value: i32, charge: i32) {
            let k = self.gen_key(key);
            let h = self.cache.insert(
                k,
                value,
                charge as usize,
                Box::new(fn_deleter(
                    self.deleted_keys.clone(),
                    self.deleted_values.clone(),
                )),
            );
            self.cache.release(h);
        }

        fn insert_and_return_handle(&mut self, key: i32, value: i32) -> NodeValPtr<i32> {
            let k = self.gen_key(key);
            self.cache.insert(
                k,
                value,
                1,
                Box::new(fn_deleter(
                    self.deleted_keys.clone(),
                    self.deleted_values.clone(),
                )),
            )
        }

        fn erase(&mut self, key: i32) {
            let k = self.gen_key(key);
            self.cache.erase(&k);
        }

        fn gen_key(&mut self, k: i32) -> String {
            self.inserted_keys.push(k);
            String::from(k.to_string())
        }
    }

    fn fn_deleter(
        keys: Rc<RefCell<Vec<i32>>>,
        values: Rc<RefCell<Vec<i32>>>,
    ) -> impl FnMut(&String, &i32) {
        move |k, v| {
            let key = k.parse::<i32>().unwrap();
            keys.borrow_mut().push(key);
            values.borrow_mut().push(*v);
        }
    }

    #[test]
    fn hit_and_miss() {
        let mut ct = TesterCache::new();
        assert_eq!(-1, ct.lookup(100));

        ct.insert(100, 101);
        assert_eq!(101, ct.lookup(100));
        assert_eq!(-1, ct.lookup(200));
        assert_eq!(-1, ct.lookup(300));

        ct.insert(200, 201);
        assert_eq!(101, ct.lookup(100));
        assert_eq!(201, ct.lookup(200));
        assert_eq!(-1, ct.lookup(300));

        ct.insert(100, 102);
        assert_eq!(102, ct.lookup(100));
        assert_eq!(201, ct.lookup(200));
        assert_eq!(-1, ct.lookup(300));

        assert_eq!(1, ct.deleted_keys.borrow().len());
        assert_eq!(100, ct.deleted_keys.borrow()[0]);
        assert_eq!(101, ct.deleted_values.borrow()[0]);
    }

    #[test]
    fn erase() {
        let mut ct = TesterCache::new();
        ct.erase(200);
        assert_eq!(0, ct.deleted_keys.borrow().len());

        ct.insert(100, 101);
        ct.insert(200, 201);
        ct.erase(100);
        assert_eq!(-1, ct.lookup(100));
        assert_eq!(201, ct.lookup(200));
        assert_eq!(1, ct.deleted_keys.borrow().len());
        assert_eq!(100, ct.deleted_keys.borrow()[0]);
        assert_eq!(101, ct.deleted_values.borrow()[0]);

        ct.erase(100);
        assert_eq!(-1, ct.lookup(100));
        assert_eq!(201, ct.lookup(200));
        assert_eq!(1, ct.deleted_keys.borrow().len());
    }

    #[test]
    fn entries_are_pinned() {
        let mut ct = TesterCache::new();
        ct.insert(100, 101);

        let key = 100;
        let s = key.to_string();

        let h1 = ct.cache.lookup(&s).expect("lookup() should return Some");
        assert_eq!(101, *h1.borrow().get_value());

        ct.insert(100, 102);
        let h2 = ct.cache.lookup(&s).expect("lookup() should return Some");
        assert_eq!(102, *h2.borrow().get_value());
        assert_eq!(0, ct.deleted_keys.borrow().len());

        ct.cache.release(h1);
        assert_eq!(1, ct.deleted_keys.borrow().len());
        assert_eq!(100, ct.deleted_keys.borrow()[0]);
        assert_eq!(101, ct.deleted_values.borrow()[0]);

        ct.erase(100);
        assert_eq!(-1, ct.lookup(100));
        assert_eq!(1, ct.deleted_keys.borrow().len());

        ct.cache.release(h2);
        assert_eq!(2, ct.deleted_keys.borrow().len());
        assert_eq!(100, ct.deleted_keys.borrow()[1]);
        assert_eq!(102, ct.deleted_values.borrow()[1]);
    }

    #[test]
    fn eviction_policy() {
        let mut ct = TesterCache::new();
        ct.insert(100, 101);
        ct.insert(200, 201);
        ct.insert(300, 301);

        let s = 300.to_string();

        let h = ct.cache.lookup(&s).expect("lookup() should return Some");

        for i in 0..CACHE_SIZE + 100 {
            let i1 = i as i32;
            ct.insert(1000 + i1, 2000 + i1);
            assert_eq!(2000 + i1, ct.lookup(1000 + i1));
            assert_eq!(101, ct.lookup(100));
        }

        assert_eq!(101, ct.lookup(100));
        assert_eq!(-1, ct.lookup(200));
        assert_eq!(301, ct.lookup(300));
        ct.cache.release(h);
    }

    #[test]
    fn use_exceeds_cache_size() {
        let mut ct = TesterCache::new();
        let mut v = Vec::new();
        for i in 0..CACHE_SIZE + 100 {
            let i1 = i as i32;
            v.push(ct.insert_and_return_handle(1000 + i1, 2000 + i1));
        }
        for i in 0..v.len() {
            let i1 = i as i32;
            assert_eq!(2000 + i1, ct.lookup(1000 + i1));
        }
        for h in v {
            ct.cache.release(h);
        }
    }

    #[test]
    fn heavy_entries() {
        let mut ct = TesterCache::new();
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
            let r = ct.lookup(i);
            if r >= 0 {
                cached_weight += weight;
                assert_eq!(1000 + i, r);
            }
        }

        assert!(cached_weight <= CACHE_SIZE as i32 + CACHE_SIZE as i32 / 10);
    }

    #[test]
    fn new_id() {
        let ct = TesterCache::new();
        let a = ct.cache.new_id();
        let b = ct.cache.new_id();
        assert!(a != b);
    }

    #[test]
    fn prune() {
        let mut ct = TesterCache::new();
        ct.insert(1, 100);
        ct.insert(2, 200);

        let s = 1.to_string();
        let h = ct.cache.lookup(&s).expect("lookup() should return Some");
        ct.cache.prune();
        ct.cache.release(h);

        assert_eq!(100, ct.lookup(1));
        assert_eq!(-1, ct.lookup(2));
    }

    #[test]
    fn zero_size_cache() {
        let mut ct = TesterCache::new();
        ct.cache = Box::new(ShardedLRUCache::new(0));
        ct.insert(1, 100);
        assert_eq!(-1, ct.lookup(1));
    }
}
