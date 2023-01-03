use std::fs;
use std::io;
use std::ops::Range;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use radixdb;
use radixdb::store;
use radixdb::store::BlobStore;
use trace::debug;

use super::IndexError;
use super::IndexResult;

#[derive(Debug)]
pub struct IndexEngine {
    dir: PathBuf,

    db: radixdb::RadixTree<store::PagedFileStore>,
    store: store::PagedFileStore,
}

impl IndexEngine {
    pub fn new(path: impl AsRef<Path>) -> IndexResult<Self> {
        let path = path.as_ref();
        let _ = fs::create_dir_all(path);
        debug!("Creating index engine : {:?}", &path);

        let db_path = path.join("index.db");
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&db_path)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        let store = store::PagedFileStore::new(file, 1024 * 1024)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;
        let db = radixdb::RadixTree::try_load(store.clone(), store.last_id())
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(Self {
            db,
            store,
            dir: path.into(),
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> IndexResult<()> {
        self.db
            .try_insert(key, value)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }

    // /// Create a double-ended iterator over tuples of keys and values,
    // /// where the keys fall within the specified range.
    // ///
    // /// by range syntax like `..`, `a..`, `..b`, `..=c`, `d..e`, or `f..=g`.
    // pub fn range<K, R>(
    //     &self,
    //     range: R,
    // ) -> impl Iterator<Item = Result<(Vec<u8>, Vec<u8>), sled::Error>>
    // where
    //     K: AsRef<[u8]>,
    //     R: RangeBounds<K>,
    // {
    //     self.db
    //         .range(range)
    //         .map(|e| e.map(|(k, v)| (k.to_vec(), v.to_vec())))
    // }

    pub fn get(&self, key: &[u8]) -> IndexResult<Option<Vec<u8>>> {
        let val = self
            .db
            .try_get(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        match val {
            Some(v) => {
                let data = self.load(&v)?;

                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    pub fn load(&self, val: &radixdb::node::Value<store::PagedFileStore>) -> IndexResult<Vec<u8>> {
        let blob = val
            .load(&self.store)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(blob.to_vec())
    }

    pub fn modify(&mut self, key: &[u8], id: u32, add: bool) -> IndexResult<()> {
        let mut rb = match self.get(key)? {
            Some(val) => roaring::RoaringBitmap::deserialize_from(&*val)
                .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?,

            None => roaring::RoaringBitmap::new(),
        };

        if add {
            rb.insert(id);
        } else {
            rb.remove(id);
        }

        let mut bytes = vec![];
        rb.serialize_into(&mut bytes)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        self.set(key, &bytes)?;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> IndexResult<()> {
        self.db
            .try_remove(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }

    pub fn prefix<'a>(
        &'a self,
        key: &'a [u8],
    ) -> IndexResult<radixdb::node::KeyValueIter<store::PagedFileStore>> {
        self.db
            .try_scan_prefix(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })
    }

    pub fn exist(&self, key: &[u8]) -> IndexResult<bool> {
        let result = self
            .db
            .try_contains_key(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(result)
    }

    pub fn flush(&mut self) -> IndexResult<()> {
        let id = self
            .db
            .try_reattach()
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        self.store
            .sync()
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }
}

mod test {
    use models::utils::now_timestamp;
    use parking_lot::RwLock;
    use std::sync::{self, atomic::AtomicU64, Arc};
    use tokio::time::{self, Duration};

    use super::IndexEngine;

    #[tokio::test]
    async fn test_engine() {
        let mut engine = IndexEngine::new("aaa_test_index").unwrap();
        // engine.set(b"key1", b"v11111").unwrap();
        // engine.set(b"key2", b"v22222").unwrap();
        // engine.set(b"key3", b"v33333").unwrap();
        // engine.set(b"key4", b"v44444").unwrap();
        // engine.set(b"key5", b"v55555").unwrap();

        engine.set(b"key3", b"v333334").unwrap();
        engine.flush().unwrap();

        println!("=== {:?}", engine.get(b"key"));
        println!("=== {:?}", engine.get(b"key1"));
        println!("=== {:?}", engine.get(b"key2"));
        println!("=== {:?}", engine.get(b"key3"));
        println!("=== {:?}", engine.delete(b"key3"));
        println!("=== {:?}", engine.get(b"key3"));
    }

    #[tokio::test]
    async fn test_engine_write_perf() {
        let mut engine = IndexEngine::new("aaa_test_index").unwrap();

        let mut begin = now_timestamp() / 1000000;
        for i in 1..10001 {
            let key = format!("key012345678901234567890123456789_{}", i);
            let val = format!("val012345678901234567890123456789_{}", i);
            engine.set(key.as_bytes(), val.as_bytes()).unwrap();
            if i % 100000 == 0 {
                engine.flush().unwrap();

                let end = now_timestamp() / 1000000;
                println!("{}  : time {}", i, end - begin);
                begin = end;
            }
        }
    }

    async fn test_engine_read_perf() {
        let engine = IndexEngine::new("aaa_test_index").unwrap();
        let engine = Arc::new(engine);

        let atomic = Arc::new(AtomicU64::new(0));

        for _ in 0..8 {
            //tokio::spawn(random_read(engine.clone(), atomic.clone()));
            let parm = (engine.clone(), atomic.clone());
            std::thread::spawn(|| random_read(parm.0, parm.1));
        }

        time::sleep(Duration::from_secs(3)).await;
    }

    fn engine_iter(engine: Arc<IndexEngine>) {
        let it = engine.prefix("key".as_bytes()).unwrap();
        for item in it {
            let item = item.unwrap();
            let key = std::str::from_utf8(item.0.as_ref()).unwrap();
            let val = engine.load(&item.1).unwrap();
            let val = std::str::from_utf8(&val).unwrap();

            println!("{}: {}", key, val)
        }
    }

    fn random_read(engine: Arc<IndexEngine>, count: Arc<AtomicU64>) {
        for i in 1..10000000 {
            let random: i32 = rand::Rng::gen_range(&mut rand::thread_rng(), 1..=10000000);

            let key = format!("key012345678901234567890123456789_{}", random);

            engine.get(key.as_bytes()).unwrap();

            let total = count.fetch_add(1, sync::atomic::Ordering::SeqCst);
            if total % 100000 == 0 {
                println!("read total: {}; time: {}", total, now_timestamp() / 1000000);
            }
        }
    }
}
