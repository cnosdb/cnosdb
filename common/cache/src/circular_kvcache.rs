#![allow(
    dead_code,
    clippy::map_entry,
    clippy::inherent_to_string,
    clippy::needless_range_loop
)]

use std::collections::HashMap;
use std::fmt::{Debug, Display};

/// head: pointer to the oldest item, pop will remove this item
/// tail: pointer to the newest item, push will add item behind this
#[derive(Debug)]
struct CircularBuffer<T> {
    buffer: Vec<T>,
    head: usize,
    tail: usize,
    empty: bool,
}

impl<T> CircularBuffer<T>
where
    T: Default + Clone + std::cmp::Ord + Display,
{
    fn new(size: usize) -> Self {
        CircularBuffer {
            buffer: vec![Default::default(); size],
            head: 0,
            tail: 0,
            empty: true,
        }
    }

    fn is_empty(&self) -> bool {
        self.empty
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
        if self.is_empty() {
            self.empty = false;
            self.buffer[self.tail] = item;
            return;
        }

        self.tail = (self.tail + 1) % self.buffer.len();
        if self.tail == self.head {
            self.head = (self.head + 1) % self.buffer.len();
        }
        self.buffer[self.tail] = item;
    }

    /// Pop the oldest item（the head pointer item）
    fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        let item = self.take(self.head);
        if self.head == self.tail {
            self.empty = true;
        } else {
            self.head = (self.head + 1) % self.buffer.len();
        }

        Some(item)
    }

    /// Drains all items before the given key from the buffer
    /// and leaves the default value, return all drained items.
    /// This needs to be pushed in ascending order
    pub fn del_before(&mut self, key: T) -> Vec<T> {
        let mut list = vec![];
        let mut current = self.head;
        loop {
            if self.is_empty() {
                break;
            }

            if self.buffer[current] < key {
                list.push(self.take(current));
                if self.head == self.tail {
                    self.empty = true;
                    break;
                }

                self.head = (self.head + 1) % self.buffer.len();
                current = self.head;
            } else {
                break;
            }
        }

        list
    }

    /// Drains all items after the given key from the buffer
    /// and leaves the default value, return all drained items.
    /// This needs to be pushed in ascending order
    pub fn del_after(&mut self, key: T) -> Vec<T> {
        let mut list = vec![];
        let mut current = self.tail;
        loop {
            if self.is_empty() {
                break;
            }

            if self.buffer[current] >= key {
                list.push(self.take(current));
                if self.head == self.tail {
                    self.empty = true;
                    break;
                }

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

    fn to_string(&self) -> String {
        if self.is_empty() {
            return "[ ]".to_string();
        }

        use std::fmt::Write;
        let mut s = String::with_capacity(16);
        write!(s, "[ ").unwrap();

        let mut index = self.head;
        loop {
            write!(s, "{} ", self.buffer[index]).unwrap();
            if index == self.tail {
                break;
            }

            index = (index + 1) % self.buffer.len();
        }

        s.push(']');

        s
    }
}

#[derive(Debug)]
pub struct CircularKVCache<K, V> {
    cache: HashMap<K, V>,
    buffer: CircularBuffer<K>,
}

impl<K: Eq + std::hash::Hash, V> CircularKVCache<K, V>
where
    K: Default + Clone + Display + Send + Sync + std::cmp::Ord,
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

    pub fn del_before(&mut self, key: K) {
        let items = self.buffer.del_before(key);
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
    use crate::circular_kvcache::{CircularBuffer, CircularKVCache};

    #[test]
    fn test_ring_buf() {
        {
            // Test initial state
            let buf: CircularBuffer<usize> = CircularBuffer::new(8);
            assert_eq!(buf.buffer, vec![0_usize; 8]);
            assert_eq!(buf.head, 0);
            assert_eq!(buf.tail, 0);
        }

        {
            // Test push
            let capacity = 8_usize;
            let cases = vec![
                (1..=2, "[ 1 2 ]"),
                (1..=7, "[ 1 2 3 4 5 6 7 ]"),
                (1..=8, "[ 1 2 3 4 5 6 7 8 ]"),
                (1..=9, "[ 2 3 4 5 6 7 8 9 ]"),
            ];
            for (range, expected_result) in cases.into_iter() {
                let mut buf: CircularBuffer<usize> = CircularBuffer::new(capacity);
                range.for_each(|i| buf.push(i));
                assert_eq!(buf.to_string(), expected_result);
            }
        }
        {
            // Test pop
            let capacity = 8_usize;
            let cases = vec![
                (1..=1, 1, vec![Some(1)], "[ ]"),
                (1..=7, 1, vec![Some(1)], "[ 2 3 4 5 6 7 ]"),
                (1..=8, 1, vec![Some(1)], "[ 2 3 4 5 6 7 8 ]"),
                (1..=9, 1, vec![Some(2)], "[ 3 4 5 6 7 8 9 ]"),
            ];
            for (range, pop_count, pop_values, expected_result) in cases.into_iter() {
                let mut buf: CircularBuffer<usize> = CircularBuffer::new(capacity);
                range.for_each(|i| buf.push(i));

                for pop_i in 0..pop_count {
                    assert_eq!(buf.pop(), pop_values[pop_i]);
                }
                assert_eq!(buf.to_string(), expected_result);
            }
        }

        {
            // Test push
            {
                let mut buf: CircularBuffer<usize> = CircularBuffer::new(8);
                buf.push(1);
                buf.push(2);
                assert_eq!(buf.to_string(), "[ 1 2 ]");
            }
            {
                let mut buf: CircularBuffer<usize> = CircularBuffer::new(8);
                for i in 1..=8 {
                    buf.push(i);
                }
                assert_eq!(buf.to_string(), "[ 1 2 3 4 5 6 7 8 ]");
            }
        }

        {
            // Test pop
            {
                let mut buf: CircularBuffer<usize> = CircularBuffer::new(8);
                for i in 1..=8 {
                    buf.push(i);
                }
                buf.pop();
                assert_eq!(buf.to_string(), "[ 2 3 4 5 6 7 8 ]");
            }
        }

        {
            // Test delete before
            {
                let mut buf: CircularBuffer<usize> = CircularBuffer::new(8);
                for i in 1..=8 {
                    buf.push(i);
                }
                buf.del_before(5);
                assert_eq!(buf.to_string(), "[ 5 6 7 8 ]");

                buf.del_before(10);
                assert_eq!(buf.to_string(), "[ ]");

                buf.push(10);
                buf.push(11);
                assert_eq!(buf.to_string(), "[ 10 11 ]");

                buf.pop();
                assert_eq!(buf.to_string(), "[ 11 ]");

                buf.pop();
                assert_eq!(buf.to_string(), "[ ]");
            }
        }

        {
            // Test delete after
            {
                let mut buf: CircularBuffer<usize> = CircularBuffer::new(8);
                for i in 1..=8 {
                    buf.push(i);
                }

                let res = buf.del_after(5);
                assert_eq!(res, vec![8, 7, 6, 5]);
                assert_eq!(buf.to_string(), "[ 1 2 3 4 ]");

                let res = buf.del_after(1);
                assert_eq!(res, vec![4, 3, 2, 1]);
                assert_eq!(buf.to_string(), "[ ]");

                buf.push(10);
                buf.push(11);
                assert_eq!(buf.to_string(), "[ 10 11 ]");

                buf.pop();
                assert_eq!(buf.to_string(), "[ 11 ]");

                buf.pop();
                assert_eq!(buf.to_string(), "[ ]");
            }
        }
    }

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
        cache.del_before(5);
        assert_eq!(Some(8000), cache.last().copied());
        assert_eq!(None, cache.get(&4).copied());
        assert_eq!(None, cache.get(&9).copied());
        assert_eq!(Some(5000), cache.get(&5).copied());
    }
}
