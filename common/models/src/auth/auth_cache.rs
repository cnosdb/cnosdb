use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    expires_at: Option<Instant>,
}

impl<V> CacheEntry<V> {
    fn new(value: V, ttl: Option<Duration>) -> Self {
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
        CacheEntry { value, expires_at }
    }

    fn is_expired(&self) -> bool {
        self.expires_at
            .map(|expires_at| expires_at <= Instant::now())
            .unwrap_or(false)
    }
}

#[derive(Clone)]
pub struct AuthCache<K, V> {
    ttl: Option<Duration>,
    cache: Arc<Mutex<lru::LruCache<K, CacheEntry<V>>>>,
}

impl<K, V> AuthCache<K, V>
where
    K: Eq + std::hash::Hash,
{
    pub fn new(capacity: usize, ttl: Option<Duration>) -> Self {
        let cache_cap = unsafe { NonZeroUsize::new_unchecked(capacity) };
        AuthCache {
            ttl,
            cache: Arc::new(Mutex::new(lru::LruCache::new(cache_cap))),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        self.cache.lock().put(key, CacheEntry::new(value, self.ttl));
    }

    pub fn get<Q>(&self, key: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
        V: Clone,
    {
        let mut cache = self.cache.lock();
        if let Some(entry) = cache.get(key) {
            if !entry.is_expired() {
                return Some(entry.value.clone());
            }
        }
        None
    }

    pub fn remove<Q>(&self, key: &Q)
    where
        K: std::borrow::Borrow<Q>,
        Q: std::hash::Hash + Eq + ?Sized,
    {
        let mut cache = self.cache.lock();
        cache.pop(key);
    }

    pub fn clear(&self) {
        let mut cache = self.cache.lock();
        cache.clear();
    }
}

#[test]
fn test() {
    let cache: AuthCache<&str, &str> = AuthCache::new(2, Some(Duration::from_secs(5)));

    cache.insert("k1", "v1");
    cache.insert("k2", "v2");
    assert_eq!("v1", cache.get(&"k1").unwrap());
    assert_eq!("v2", cache.get(&"k2").unwrap());
    cache.remove(&"k2");
    assert!(cache.get(&"k2").is_none());
    assert_eq!("v1", cache.get(&"k1").unwrap());

    std::thread::sleep(Duration::from_secs(3));

    assert_eq!("v1", cache.get(&"k1").unwrap());
    assert!(cache.get(&"k2").is_none());

    std::thread::sleep(Duration::from_secs(3));

    assert!(cache.get(&"k1").is_none());
    assert!(cache.get(&"k2").is_none());

    cache.insert("k1", "v1");
    assert_eq!("v1", cache.get(&"k1").unwrap());
    cache.clear();
    assert!(cache.get(&"k1").is_none());
}
