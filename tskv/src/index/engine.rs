use std::fs;
use std::ops::{BitAnd, BitOr, RangeBounds};
use std::path::Path;

use radixdb;
use radixdb::store;
use radixdb::store::BlobStore;
use snafu::ResultExt;

use super::{IndexResult, IndexStorageSnafu, RoaringBitmapSnafu};

#[derive(Debug)]
pub struct IndexEngine {
    db: radixdb::RadixTree<store::PagedFileStore>,
    store: store::PagedFileStore,
}

impl IndexEngine {
    #[allow(clippy::suspicious_open_options)]
    pub fn new(path: impl AsRef<Path>) -> IndexResult<Self> {
        let path = path.as_ref();
        let _ = fs::create_dir_all(path);
        trace::debug!("Creating index engine : {:?}", &path);

        let db_path = path.join("index.db");
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(db_path)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        let store = store::PagedFileStore::new(file, 1024 * 1024)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
        let db = radixdb::RadixTree::try_load(store.clone(), store.last_id())
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(Self { db, store })
    }

    #[allow(dead_code)]
    pub fn copy_to_engine2(&self, engine2: &super::engine2::IndexEngine2) -> IndexResult<u64> {
        let mut key_count: u64 = 0;
        let iter = self.db.try_iter();
        let mut writer = engine2.writer_txn()?;
        for item in iter {
            let item = item.map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
            let key = item.0;
            let val = self.load(&item.1)?;

            key_count += 1;
            engine2.txn_write(&key, &val, &mut writer)?;
        }

        writer
            .commit()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
        engine2.flush()?;

        Ok(key_count)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> IndexResult<()> {
        self.db
            .try_insert(key, value)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> IndexResult<Option<Vec<u8>>> {
        let val = self
            .db
            .try_get(key)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

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
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(blob.to_vec())
    }

    pub fn get_rb(&self, key: &[u8]) -> IndexResult<Option<roaring::RoaringBitmap>> {
        if let Some(data) = self.get(key)? {
            let rb =
                roaring::RoaringBitmap::deserialize_from(&*data).context(RoaringBitmapSnafu)?;

            Ok(Some(rb))
        } else {
            Ok(None)
        }
    }

    fn load_rb(
        &self,
        val: &radixdb::node::Value<store::PagedFileStore>,
    ) -> IndexResult<roaring::RoaringBitmap> {
        let data = self.load(val)?;

        let rb = roaring::RoaringBitmap::deserialize_from(&*data).context(RoaringBitmapSnafu)?;

        Ok(rb)
    }

    pub fn modify(&mut self, key: &[u8], id: u32, add: bool) -> IndexResult<()> {
        let mut rb = match self.get(key)? {
            Some(val) => roaring::RoaringBitmap::deserialize_from(&*val)
                .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?,

            None => roaring::RoaringBitmap::new(),
        };

        if add {
            rb.insert(id);
        } else {
            rb.remove(id);
        }

        let mut bytes = vec![];
        rb.serialize_into(&mut bytes)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        self.set(key, &bytes)?;

        Ok(())
    }

    pub fn merge_rb(&mut self, key: &[u8], rb: &roaring::RoaringBitmap) -> IndexResult<()> {
        let value = match self.get(key)? {
            Some(val) => roaring::RoaringBitmap::deserialize_from(&*val)
                .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?,

            None => roaring::RoaringBitmap::new(),
        };

        let mut bytes = vec![];
        let value = value.bitor(rb);
        value
            .serialize_into(&mut bytes)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        self.set(key, &bytes)?;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> IndexResult<()> {
        self.db
            .try_remove(key)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(())
    }

    pub fn del_prefix(&mut self, prefix: &[u8]) -> IndexResult<()> {
        self.db
            .try_remove_prefix(prefix)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(())
    }

    pub fn exist(&self, key: &[u8]) -> IndexResult<bool> {
        let result = self
            .db
            .try_contains_key(key)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(result)
    }

    pub fn prefix<'a>(
        &'a self,
        key: &'a [u8],
    ) -> IndexResult<radixdb::node::KeyValueIter<store::PagedFileStore>> {
        self.db
            .try_scan_prefix(key)
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())
    }

    pub fn flush(&mut self) -> IndexResult<()> {
        let _id = self
            .db
            .try_reattach()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        self.store
            .sync()
            .map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;

        Ok(())
    }

    //---------------------------------------------------------------------
    pub fn get_series_id_by_range(
        &self,
        range: impl RangeBounds<Vec<u8>>,
    ) -> IndexResult<roaring::RoaringBitmap> {
        // process equal special situation
        if let std::ops::Bound::Included(start) = range.start_bound() {
            if let std::ops::Bound::Included(end) = range.end_bound() {
                if start == end {
                    if let Some(rb) = self.get_rb(start)? {
                        return Ok(rb);
                    };
                    return Ok(roaring::RoaringBitmap::new());
                }
            }
        }

        let mut bitmap = roaring::RoaringBitmap::new();
        let iter = RangeKeyValIter::new_iterator(
            copy_bound(range.start_bound()),
            copy_bound(range.end_bound()),
            self.db.try_iter(),
        );
        for item in iter {
            let item = item?;
            let rb = self.load_rb(&item.1)?;
            bitmap = bitmap.bitor(rb);
        }

        Ok(bitmap)
    }

    pub fn get_series_id_by_tags(
        &self,
        tab: &str,
        tags: &[models::Tag],
    ) -> IndexResult<roaring::RoaringBitmap> {
        let mut bitmap = roaring::RoaringBitmap::new();
        if tags.is_empty() {
            let prefix = format!("{}.", tab);
            let it = self.prefix(prefix.as_bytes())?;
            for val in it {
                let val = val.map_err(|e| IndexStorageSnafu { msg: e.to_string() }.build())?;
                let rb = self.load_rb(&val.1)?;

                bitmap = bitmap.bitor(rb);
            }
        } else {
            let key = super::ts_index::encode_inverted_index_key(tab, &tags[0].key, &tags[0].value);
            if let Some(rb) = self.get_rb(&key)? {
                bitmap = rb;
            }

            for tag in &tags[1..] {
                let key = super::ts_index::encode_inverted_index_key(tab, &tag.key, &tag.value);
                if let Some(rb) = self.get_rb(&key)? {
                    bitmap = bitmap.bitand(rb);
                } else {
                    return Ok(roaring::RoaringBitmap::new());
                }
            }
        }

        Ok(bitmap)
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
                        return Some(Err(IndexStorageSnafu { msg: e.to_string() }.build()));
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
