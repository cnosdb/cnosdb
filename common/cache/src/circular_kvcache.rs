#![allow(dead_code, clippy::map_entry)]

use std::collections::HashMap;
use std::fmt::Debug;

#[derive(Debug)]
struct CircularBuffer<T> {
    buffer: Vec<T>,
    head: usize,
    tail: usize,
}

impl<T> CircularBuffer<T>
where
    T: Default + Clone + std::cmp::Ord,
{
    fn new(size: usize) -> Self {
        CircularBuffer {
            buffer: vec![Default::default(); size],
            head: 0,
            tail: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    fn is_full(&self) -> bool {
        (self.tail + 1) % self.buffer.len() == self.head
    }

    fn take(&mut self, index: usize) -> T {
        std::mem::take(&mut self.buffer[index])
    }

    fn get_tail(&self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        Some(self.buffer[self.tail].clone())
    }

    fn push(&mut self, item: T) {
        self.tail = (self.tail + 1) % self.buffer.len();
        if self.tail == self.head {
            self.head = (self.head + 1) % self.buffer.len();
        }
        self.buffer[self.tail] = item;
    }

    fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }
        let item = self.take(self.head);
        self.head = (self.head + 1) % self.buffer.len();
        Some(item)
    }

    pub fn del_befor(&mut self, key: T) -> Vec<T> {
        let mut list = vec![];
        let mut current = self.head;
        while current != self.tail {
            if self.buffer[current] < key {
                list.push(self.take(current));

                self.head = (self.head + 1) % self.buffer.len();

                current = self.head;
            } else {
                break;
            }
        }

        list
    }

    pub fn del_after(&mut self, key: T) -> Vec<T> {
        let mut list = vec![];
        let mut current = self.tail;
        while current != self.head {
            if self.buffer[current] >= key {
                list.push(self.take(current));

                if self.tail == 0 {
                    self.tail = self.buffer.len() - 1;
                } else {
                    self.tail = (self.tail - 1) % self.buffer.len();
                }

                current = self.tail;
            } else {
                break;
            }
        }

        list
    }
}

#[derive(Debug)]
pub struct CircularKVCache<K, V> {
    cache: HashMap<K, V>,
    buffer: CircularBuffer<K>,
}

impl<K: Eq + std::hash::Hash, V> CircularKVCache<K, V>
where
    K: Default + Clone + Debug + Send + Sync + std::cmp::Ord,
{
    pub fn new(capacity: usize) -> Self {
        CircularKVCache {
            cache: HashMap::new(),
            buffer: CircularBuffer::new(capacity),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        self.cache.get(key)
    }

    pub fn last(&self) -> Option<&V> {
        if let Some(key) = self.buffer.get_tail() {
            self.cache.get(&key)
        } else {
            None
        }
    }

    pub fn del_befor(&mut self, key: K) {
        let items = self.buffer.del_befor(key);
        for item in items {
            self.cache.remove(&item);
        }
    }

    pub fn del_after(&mut self, key: K) {
        let items = self.buffer.del_after(key);
        for item in items {
            self.cache.remove(&item);
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        if self.cache.contains_key(&key) {
            self.cache.insert(key, value);
            return;
        }

        if self.buffer.is_full() {
            if let Some(oldest_key) = self.buffer.pop() {
                self.cache.remove(&oldest_key);
            }
        }

        self.buffer.push(key.clone());
        self.cache.insert(key, value);
    }
}

#[cfg(test)]
mod test {
    use crate::circular_kvcache::CircularKVCache;

    #[test]
    fn test_circular_kvcache() {
        let mut cache: CircularKVCache<u64, u64> = CircularKVCache::new(8);
        for i in 1..11 {
            cache.put(i, i * 1000);
        }

        assert_eq!(Some(10000), cache.last().copied());
        assert_eq!(None, cache.get(&1).copied());
        assert_eq!(Some(5000), cache.get(&5).copied());

        cache.del_after(9);
        cache.del_befor(5);
        assert_eq!(Some(8000), cache.last().copied());
        assert_eq!(None, cache.get(&4).copied());
        assert_eq!(None, cache.get(&9).copied());
        assert_eq!(Some(5000), cache.get(&5).copied());
    }
}
