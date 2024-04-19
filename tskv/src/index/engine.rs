use std::fs;
use std::ops::RangeBounds;
use std::path::Path;

use radixdb;
use radixdb::store;
use radixdb::store::BlobStore;
use trace::debug;

use super::{IndexError, IndexResult};

#[derive(Debug)]
pub struct IndexEngine {
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
            .open(db_path)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        let store = store::PagedFileStore::new(file, 1024 * 1024)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;
        let db = radixdb::RadixTree::try_load(store.clone(), store.last_id())
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(Self { db, store })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> IndexResult<()> {
        self.db
            .try_insert(key, value)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }

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

    pub fn get_rb(&self, key: &[u8]) -> IndexResult<Option<roaring::RoaringBitmap>> {
        if let Some(data) = self.get(key)? {
            let rb = roaring::RoaringBitmap::deserialize_from(&*data)
                .map_err(|e| IndexError::RoaringBitmap { source: e })?;

            Ok(Some(rb))
        } else {
            Ok(None)
        }
    }

    pub fn load_rb(
        &self,
        val: &radixdb::node::Value<store::PagedFileStore>,
    ) -> IndexResult<roaring::RoaringBitmap> {
        let data = self.load(val)?;

        let rb = roaring::RoaringBitmap::deserialize_from(&*data)
            .map_err(|e| IndexError::RoaringBitmap { source: e })?;

        Ok(rb)
    }

    pub fn build_revert_index(&self, key: &[u8], id: u32, add: bool) -> IndexResult<Vec<u8>> {
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

        Ok(bytes)
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

    pub fn combine(&mut self, tree: radixdb::RadixTree) -> IndexResult<()> {
        self.db
            .try_outer_combine_with(&tree, radixdb::node::DetachConverter, |a, b| {
                a.set(Some(b.downcast()));
                Ok(())
            })
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }

    pub fn exist(&self, key: &[u8]) -> IndexResult<bool> {
        let result = self
            .db
            .try_contains_key(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(result)
    }

    pub fn range(&self, range: impl RangeBounds<Vec<u8>>) -> RangeKeyValIter {
        RangeKeyValIter::new_iterator(
            copy_bound(range.start_bound()),
            copy_bound(range.end_bound()),
            self.db.try_iter(),
        )
    }

    pub fn prefix<'a>(
        &'a self,
        key: &'a [u8],
    ) -> IndexResult<radixdb::node::KeyValueIter<store::PagedFileStore>> {
        self.db
            .try_scan_prefix(key)
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })
    }

    pub fn flush(&mut self) -> IndexResult<()> {
        let _id = self
            .db
            .try_reattach()
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        self.store
            .sync()
            .map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;

        Ok(())
    }
}

fn copy_bound(bound: std::ops::Bound<&Vec<u8>>) -> std::ops::Bound<Vec<u8>> {
    match bound {
        std::ops::Bound::Included(val) => std::ops::Bound::Included(val.to_vec()),
        std::ops::Bound::Excluded(val) => std::ops::Bound::Excluded(val.to_vec()),
        std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
    }
}
pub struct RangeKeyValIter {
    start: std::ops::Bound<Vec<u8>>,
    end: std::ops::Bound<Vec<u8>>,

    iter: radixdb::node::KeyValueIter<store::PagedFileStore>,
}

impl RangeKeyValIter {
    pub fn new_iterator(
        start: std::ops::Bound<Vec<u8>>,
        end: std::ops::Bound<Vec<u8>>,
        iter: radixdb::node::KeyValueIter<store::PagedFileStore>,
    ) -> Self {
        Self { iter, start, end }
    }
}

impl Iterator for RangeKeyValIter {
    type Item = IndexResult<(
        radixdb::node::IterKey,
        radixdb::node::Value<store::PagedFileStore>,
    )>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.iter.next() {
                None => {
                    return None;
                }

                Some(item) => match item {
                    Err(e) => {
                        return Some(Err(IndexError::IndexStroage { msg: e.to_string() }));
                    }

                    Ok(item) => match &self.start {
                        std::ops::Bound::Included(start) => match &self.end {
                            std::ops::Bound::Included(end) => {
                                if item.0.as_ref() >= start.as_slice()
                                    && item.0.as_ref() <= end.as_slice()
                                {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() > end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Excluded(end) => {
                                if item.0.as_ref() >= start.as_slice()
                                    && item.0.as_ref() < end.as_slice()
                                {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() >= end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Unbounded => {
                                if item.0.as_ref() >= start.as_slice() {
                                    return Some(Ok(item));
                                }
                            }
                        },

                        std::ops::Bound::Excluded(start) => match &self.end {
                            std::ops::Bound::Included(end) => {
                                if item.0.as_ref() > start.as_slice()
                                    && item.0.as_ref() <= end.as_slice()
                                {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() > end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Excluded(end) => {
                                if item.0.as_ref() > start.as_slice()
                                    && item.0.as_ref() < end.as_slice()
                                {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() >= end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Unbounded => {
                                if item.0.as_ref() > start.as_slice() {
                                    return Some(Ok(item));
                                }
                            }
                        },

                        std::ops::Bound::Unbounded => match &self.end {
                            std::ops::Bound::Included(end) => {
                                if item.0.as_ref() <= end.as_slice() {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() > end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Excluded(end) => {
                                if item.0.as_ref() < end.as_slice() {
                                    return Some(Ok(item));
                                } else if item.0.as_ref() >= end.as_slice() {
                                    return None;
                                }
                            }
                            std::ops::Bound::Unbounded => {
                                return Some(Ok(item));
                            }
                        },
                    },
                },
            }
        }
    }
}

#[cfg(test)]
mod test {

    use super::IndexEngine;

    #[tokio::test]
    async fn test_engine() {
        let mut engine = IndexEngine::new("/tmp/test/1").unwrap();
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
}
