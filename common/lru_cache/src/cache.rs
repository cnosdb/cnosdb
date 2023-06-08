#![allow(dead_code)]

use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{Debug, Display};
use std::hash::{Hash, Hasher};
use std::mem::{self, MaybeUninit};
use std::ptr::{self, NonNull};

use tracing::trace;

use crate::AfterRemovedFnMut;

#[derive(Debug)]
pub struct Cache<K: Display, V: Debug> {
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

unsafe impl<K: Send + Display, V: Send + Debug> Send for Cache<K, V> {}
unsafe impl<K: Sync + Display, V: Sync + Debug> Sync for Cache<K, V> {}

impl<K, V> Cache<K, V>
where
    K: Eq + Hash + Display,
    V: Debug,
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
            unsafe {
                trace!(
                    "Removing LRU cache entry '{}' at{:x} by key, value: {:?}",
                    (*ptr).k.assume_init_ref(),
                    ptr as usize,
                    (*ptr).v.assume_init_ref().v
                )
            }
            Self::on_remove_entry(ptr);
        }
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn insert_and_return_value(
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
                unsafe {
                    trace!(
                        "Insert to 'some' entry '{}' at {:x}, value: {:?}, removed value: {:?}",
                        (*current_ent_ptr).k.assume_init_ref(),
                        current_ent_ptr as usize,
                        (*current_ent_ptr).v.assume_init_ref().v,
                        entry.v.assume_init_ref().v
                    );
                }
                Self::on_remove_entry(&mut entry as *mut Entry<K, V>);

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
                                trace!(
                                    "Insert to 'none' entry exceed, remove old entry '{}' at {:x}, value: {:?}",
                                    (*old_ent_ptr).k.assume_init_ref(),
                                    old_ent_ptr as usize,
                                    (*old_ent_ptr).v.assume_init_ref().v
                                );
                                Self::on_remove_entry(old_ent_ptr);
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

                unsafe {
                    trace!(
                        "Insert to 'none' entry: '{}' at {:x}, value: {:?}",
                        (*entry_ptr).k.assume_init_ref(),
                        entry_ptr as usize,
                        (*entry_ptr).v.assume_init_ref().v
                    );
                }

                self.table.insert(KeyPtr { p: key_ptr }, entry_v);
                Some(unsafe { (*entry_ptr).v.assume_init_ref() })
            }
        }
    }

    pub fn get_value<Q>(&mut self, k: &Q) -> Option<*const Value<K, V>>
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

    pub fn get_value_mut<Q>(&mut self, k: &Q) -> Option<*mut Value<K, V>>
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

    pub fn prune(&mut self) {
        let lru = self.head;
        unsafe {
            while (*lru).next != self.tail {
                let ptr = (*lru).next;
                self.table.remove(&(*ptr).as_key_ptr());
                self.unset_entry(ptr);
                self.usage = self.usage.saturating_sub((*ptr).charge);
                Self::on_remove_entry(ptr);
            }
        }
    }
}

impl<K: Display, V: Debug> Cache<K, V> {
    fn on_remove_entry(ent: *mut Entry<K, V>) {
        unsafe {
            let k = (*ent).k.assume_init_ref();
            let v = (*ent).v.assume_init_mut();
            if let Some(f) = v.after_removed.as_mut() {
                trace!(
                    "Running after removed callback for LRU cache entry '{}' and value: {:?}",
                    k,
                    v.v
                );
                f(k, &mut v.v);
            } else {
                (*ent).free()
            }
        }
    }
}

impl<K: Display, V: Debug> Drop for Cache<K, V> {
    fn drop(&mut self) {
        self.table
            .drain()
            .for_each(|(_, ent)| Self::on_remove_entry(ent.as_ptr()));
        let _head = unsafe { *Box::from_raw(self.head) };
        let _tail = unsafe { *Box::from_raw(self.tail) };
    }
}

pub struct Entry<K: Display, V: Debug> {
    k: MaybeUninit<K>,
    v: MaybeUninit<Value<K, V>>,
    charge: usize,

    next: *mut Entry<K, V>,
    prev: *mut Entry<K, V>,
}

impl<K: Display, V: Debug> Entry<K, V> {
    pub fn null() -> Entry<K, V> {
        Entry {
            k: MaybeUninit::uninit(),
            v: MaybeUninit::uninit(),
            charge: 0,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        }
    }

    pub fn new(
        k: K,
        v: V,
        charge: usize,
        after_removed: Option<AfterRemovedFnMut<K, V>>,
    ) -> Entry<K, V> {
        let entry = Entry {
            k: MaybeUninit::new(k),
            v: MaybeUninit::new(Value { v, after_removed }),
            charge,
            next: ptr::null_mut(),
            prev: ptr::null_mut(),
        };
        trace!(
            "New LRU cache entry at {:x}",
            &entry as *const Entry<K, V> as usize
        );
        entry
    }

    fn as_key_ptr(&self) -> KeyPtr<K> {
        KeyPtr { p: self.k.as_ptr() }
    }

    fn free(&mut self) {
        unsafe {
            trace!(
                "Dropping LRU cacne entry (1) at {:x}",
                self as *const Entry<K, V> as usize
            );
            trace!(
                "Dropping LRU cache entry (2) '{}' at {:x}, value: {:?}",
                self.k.assume_init_ref(),
                self as *const Entry<K, V> as usize,
                self.v.assume_init_ref().v,
            );

            ptr::drop_in_place(self.k.as_mut_ptr());
            ptr::drop_in_place(self.v.as_mut_ptr());
        }
    }
}

pub struct KeyPtr<T> {
    pub p: *const T,
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

pub struct Value<K: Display, V: Debug> {
    pub v: V,
    pub after_removed: Option<AfterRemovedFnMut<K, V>>,
}

#[cfg(test)]
mod test {
    use crate::cache::Cache;

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
}
