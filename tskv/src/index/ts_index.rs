use std::collections::HashMap;
use std::fmt;
use std::ops::{BitAnd, BitOr, Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use bytes::BufMut;
use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use models::predicate::domain::{utf8_from, Domain, Range};
use models::tag::TagFromParts;
use models::{utils, SeriesKey, Tag};
use tokio::sync::RwLock;
use trace::{debug, info};

use super::binlog::{IndexBinlog, SeriesKeyBlock};
use super::cache::{ForwardIndexCache, SeriesKeyInfo};
use super::{IndexEngine, IndexError, IndexResult};
use crate::file_system::file_manager;
use crate::index::binlog::{BinlogReader, BinlogWriter};
use crate::index::ts_index::fmt::Debug;
use crate::{byte_utils, file_utils};

const SERIES_ID_PREFIX: &str = "_id_";
const SERIES_KEY_PREFIX: &str = "_key_";
const AUTO_INCR_ID_KEY: &str = "_auto_incr_id";

pub struct TSIndex {
    path: PathBuf,
    incr_id: AtomicU32,
    write_count: AtomicU32,

    binlog: Arc<RwLock<IndexBinlog>>,
    storage: Arc<RwLock<IndexEngine>>,
    forward_cache: ForwardIndexCache,
}

impl TSIndex {
    pub async fn new(path: impl AsRef<Path>) -> IndexResult<Self> {
        let path = path.as_ref();

        let binlog = IndexBinlog::new(path).await?;
        let storage = IndexEngine::new(path)?;

        let incr_id = match storage.get(AUTO_INCR_ID_KEY.as_bytes())? {
            Some(data) => byte_utils::decode_be_u32(&data),
            None => 0,
        };

        let mut ts_index = Self {
            binlog: Arc::new(RwLock::new(binlog)),
            storage: Arc::new(RwLock::new(storage)),
            incr_id: AtomicU32::new(incr_id),
            write_count: AtomicU32::new(0),
            path: path.into(),
            forward_cache: ForwardIndexCache::new(1_000_000),
        };

        ts_index.recover().await?;
        info!("index {:?} incr id start at:{}", path, incr_id);

        Ok(ts_index)
    }

    async fn recover(&mut self) -> IndexResult<()> {
        let path = self.path.clone();
        let files = file_manager::list_file_names(&path);
        for filename in files.iter() {
            if let Ok(file_id) = file_utils::get_index_binlog_file_id(filename) {
                let tmp_file = BinlogWriter::open(file_id, path.join(filename)).await?;
                let mut reader_file = BinlogReader::new(file_id, tmp_file.file.into()).await?;

                info!("index recover file: {:?}", path.join(filename));
                self.recover_from_file(&mut reader_file).await?;
            }
        }

        Ok(())
    }

    async fn recover_from_file(&mut self, reader_file: &mut BinlogReader) -> IndexResult<()> {
        let mut max_id = 0;
        while let Some(block) = reader_file.next_block().await? {
            if block.data_len > 0 {
                // add series
                let series_key = SeriesKey::decode(&block.data)
                    .map_err(|e| IndexError::DecodeSeriesKey { msg: e.to_string() })?;

                let mut storage_w = self.storage.write().await;
                let id = block.series_id;
                let key_buf = encode_series_key(series_key.table(), series_key.tags());
                storage_w.set(&key_buf, &id.to_be_bytes())?;
                storage_w.set(&encode_series_id_key(id), &block.data)?;
                for tag in series_key.tags() {
                    let key = encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
                    storage_w.modify(&key, id, true)?;
                }
                if series_key.tags().is_empty() {
                    let key = encode_inverted_index_key(series_key.table(), &[], &[]);
                    storage_w.modify(&key, id, true)?;
                }

                if max_id < block.series_id {
                    max_id = block.series_id
                }
            } else {
                // delete series
                self.del_series_id_from_engine(block.series_id).await?;
            }
        }
        self.incr_id.store(max_id, Ordering::Relaxed);

        let id_bytes = self.incr_id.load(Ordering::Relaxed).to_be_bytes();
        self.storage
            .write()
            .await
            .set(AUTO_INCR_ID_KEY.as_bytes(), &id_bytes)?;
        self.storage.write().await.flush()?;
        reader_file.advance_read_offset(0).await?;

        Ok(())
    }

    pub async fn get_series_id(&self, series_key: &SeriesKey) -> IndexResult<Option<u32>> {
        if let Some(id) = self.forward_cache.get_series_id_by_key(series_key) {
            return Ok(Some(id));
        }

        let key_buf = encode_series_key(series_key.table(), series_key.tags());
        if let Some(val) = self.storage.read().await.get(&key_buf)? {
            let id = byte_utils::decode_be_u32(&val);

            let info = SeriesKeyInfo {
                id,
                key: series_key.clone(),
                hash: series_key.hash(),
            };
            self.forward_cache.add(info);

            return Ok(Some(id));
        }

        Ok(None)
    }

    pub async fn get_series_key(&self, sid: u32) -> IndexResult<Option<SeriesKey>> {
        if let Some(key) = self.forward_cache.get_series_key_by_id(sid) {
            return Ok(Some(key));
        }

        let series_key = self.storage.read().await.get(&encode_series_id_key(sid))?;
        if let Some(res) = series_key {
            let key = SeriesKey::decode(&res)
                .map_err(|e| IndexError::DecodeSeriesKey { msg: e.to_string() })?;

            let info = SeriesKeyInfo {
                id: sid,
                key: key.clone(),
                hash: key.hash(),
            };
            self.forward_cache.add(info);

            return Ok(Some(key));
        }

        Ok(None)
    }

    pub async fn add_series_if_not_exists(&self, series_key: &SeriesKey) -> IndexResult<u32> {
        // generate series id; prepare data
        let id = self.incr_id.fetch_add(1, Ordering::Relaxed) + 1;
        let key_buf = encode_series_key(series_key.table(), series_key.tags());
        let encode = series_key.encode();
        let block = SeriesKeyBlock {
            ts: utils::now_timestamp_nanos(),
            series_id: id,
            data_len: encode.len() as u32,
            data: encode,
        };

        let block_encode = block.encode();
        self.binlog.write().await.write(&block_encode).await?;

        // is already exist and store forward index
        {
            let mut storage_w = self.storage.write().await;
            if let Some(val) = storage_w.get(&key_buf)? {
                return Ok(byte_utils::decode_be_u32(&val));
            }

            storage_w.set(&key_buf, &id.to_be_bytes())?;
            storage_w.set(&encode_series_id_key(id), &block.data)?;
        }

        // then inverted index
        for tag in series_key.tags() {
            let key = encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
            self.storage.write().await.modify(&key, id, true)?;
        }
        if series_key.tags().is_empty() {
            let key = encode_inverted_index_key(series_key.table(), &[], &[]);
            self.storage.write().await.modify(&key, id, true)?;
        }

        let _ = self.check_to_flush(false).await;

        Ok(id)
    }

    async fn check_to_flush(&self, force: bool) -> IndexResult<()> {
        let count = self.write_count.fetch_add(1, Ordering::Relaxed);
        if !force && count < 20000 {
            return Ok(());
        }

        let mut storage_w = self.storage.write().await;
        let id_bytes = self.incr_id.load(Ordering::Relaxed).to_be_bytes();
        storage_w.set(AUTO_INCR_ID_KEY.as_bytes(), &id_bytes)?;
        storage_w.flush()?;

        let current_id;
        {
            let mut binlog_w = self.binlog.write().await;
            binlog_w.advance_write_offset(0).await?;
            current_id = binlog_w.current_write_file_id();
        }

        let log_dir = self.path.clone();
        let files = file_manager::list_file_names(&log_dir);
        for filename in files.iter() {
            if let Ok(file_id) = file_utils::get_index_binlog_file_id(filename) {
                if current_id != file_id {
                    let _ = std::fs::remove_file(log_dir.join(filename));
                }
            }
        }

        self.write_count.store(0, Ordering::Relaxed);

        Ok(())
    }

    pub async fn del_series_info(&self, sid: u32) -> IndexResult<()> {
        // first write binlog
        let block = SeriesKeyBlock {
            ts: utils::now_timestamp_nanos(),
            series_id: sid,
            data_len: 0,
            data: vec![],
        };
        self.binlog.write().await.write(&block.encode()).await?;

        // then delete forward index and inverted index
        self.del_series_id_from_engine(sid).await?;

        let _ = self.check_to_flush(false).await;

        Ok(())
    }

    async fn del_series_id_from_engine(&self, sid: u32) -> IndexResult<()> {
        let mut storage_w = self.storage.write().await;
        let series_key = match self.forward_cache.get_series_key_by_id(sid) {
            Some(k) => Some(k),
            None => match storage_w.get(&encode_series_id_key(sid))? {
                Some(res) => {
                    let key = SeriesKey::decode(&res)
                        .map_err(|e| IndexError::DecodeSeriesKey { msg: e.to_string() })?;
                    Some(key)
                }
                None => None,
            },
        };
        let _ = storage_w.delete(&encode_series_id_key(sid));
        if let Some(series_key) = series_key {
            self.forward_cache.del(sid, series_key.hash());
            let key_buf = encode_series_key(series_key.table(), series_key.tags());
            let _ = storage_w.delete(&key_buf);
            for tag in series_key.tags() {
                let key = encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
                storage_w.modify(&key, sid, false)?;
            }
        }

        Ok(())
    }

    pub async fn get_series_ids_by_domains(
        &self,
        tab: &str,
        tag_domains: &HashMap<String, Domain>,
    ) -> IndexResult<Vec<u32>> {
        debug!("pushed tags: {:?}", tag_domains);
        let mut series_ids = vec![];
        for (k, v) in tag_domains.iter() {
            let rb = self.get_series_ids_by_domain(tab, k, v).await?;
            series_ids.push(rb);
        }

        debug!("filter scan all series_ids: {:?}", series_ids);

        let result = series_ids
            .into_iter()
            .reduce(|p, c| p.bitand(c))
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();

        Ok(result)
    }

    pub async fn get_series_id_list(&self, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u32>> {
        let res = self.get_series_id_bitmap(tab, tags).await?.iter().collect();
        Ok(res)
    }

    pub async fn get_series_id_bitmap(
        &self,
        tab: &str,
        tags: &[Tag],
    ) -> IndexResult<roaring::RoaringBitmap> {
        let mut bitmap = roaring::RoaringBitmap::new();
        let storage_r = self.storage.read().await;
        if tags.is_empty() {
            let prefix = format!("{}.", tab);
            let it = storage_r.prefix(prefix.as_bytes())?;
            for val in it {
                let val = val.map_err(|e| IndexError::IndexStroage { msg: e.to_string() })?;
                let rb = storage_r.load_rb(&val.1)?;

                bitmap = bitmap.bitor(rb);
            }
        } else {
            let key = encode_inverted_index_key(tab, &tags[0].key, &tags[0].value);
            if let Some(rb) = storage_r.get_rb(&key)? {
                bitmap = rb;
            }

            for tag in &tags[1..] {
                let key = encode_inverted_index_key(tab, &tag.key, &tag.value);
                if let Some(rb) = storage_r.get_rb(&key)? {
                    bitmap = bitmap.bitand(rb);
                } else {
                    return Ok(roaring::RoaringBitmap::new());
                }
            }
        }

        Ok(bitmap)
    }

    pub async fn get_series_ids_by_domain(
        &self,
        tab: &str,
        tag_key: &str,
        v: &Domain,
    ) -> IndexResult<roaring::RoaringBitmap> {
        let mut bitmap = roaring::RoaringBitmap::new();
        match v {
            Domain::Range(range_set) => {
                let storage_r = self.storage.read().await;
                for (_, range) in range_set.low_indexed_ranges().into_iter() {
                    let key_range = filter_range_to_index_key_range(tab, tag_key, range);
                    let (is_equal, equal_key) = is_equal_value(&key_range);
                    if is_equal {
                        if let Some(rb) = storage_r.get_rb(&equal_key)? {
                            bitmap = bitmap.bitor(rb);
                        };

                        continue;
                    }

                    // Search the sid list corresponding to qualified tags in the range
                    let iter = storage_r.range(key_range);
                    for item in iter {
                        let item = item?;
                        let rb = storage_r.load_rb(&item.1)?;
                        bitmap = bitmap.bitor(rb);
                    }
                }
            }
            Domain::Equtable(val) => {
                let storage_r = self.storage.read().await;
                if val.is_white_list() {
                    // Contains the given value
                    for entry in val.entries().into_iter() {
                        let index_key = tag_value_to_index_key(tab, tag_key, entry.value());
                        if let Some(rb) = storage_r.get_rb(&index_key)? {
                            bitmap = bitmap.bitor(rb);
                        };
                    }
                } else {
                    // Does not contain a given value, that is, a value other than a given value
                    // TODO will not deal with this situation for the time being
                    bitmap = self.get_series_id_bitmap(tab, &[]).await?;
                }
            }
            Domain::None => {
                // Normally, it will not go here unless no judgment is made at the ColumnDomains level
                // If you go here, you will directly return an empty series, because the tag condition in the map is' and '
                return Ok(roaring::RoaringBitmap::new());
            }
            Domain::All => {
                // Normally, it will not go here unless no judgment is made at the ColumnDomains level
                // The current tag is not filtered, all series are obtained, and the next tag is processed
                bitmap = self.get_series_id_bitmap(tab, &[]).await?;
            }
        };

        Ok(bitmap)
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub async fn flush(&self) -> IndexResult<()> {
        self.check_to_flush(true).await?;

        Ok(())
    }
}

impl Debug for TSIndex {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

pub fn is_equal_value(range: &impl RangeBounds<Vec<u8>>) -> (bool, Vec<u8>) {
    if let std::ops::Bound::Included(start) = range.start_bound() {
        if let std::ops::Bound::Included(end) = range.end_bound() {
            if start == end {
                return (true, start.clone());
            }
        }
    }

    (false, vec![])
}

pub fn filter_range_to_index_key_range(
    tab: &str,
    tag_key: &str,
    range: &Range,
) -> impl RangeBounds<Vec<u8>> {
    let start_bound = range.start_bound();
    let end_bound = range.end_bound();

    // Convert ScalarValue value to inverted index key
    let generate_index_key = |v: &ScalarValue| tag_value_to_index_key(tab, tag_key, v);

    // Convert the tag value in Bound to the inverted index key
    let translate_bound = |bound: Bound<&ScalarValue>, is_lower: bool| match bound {
        Bound::Unbounded => {
            let buf = if is_lower {
                encode_inverted_min_index_key(tab, tag_key.as_bytes())
            } else {
                encode_inverted_max_index_key(tab, tag_key.as_bytes())
            };
            Bound::Included(buf)
        }
        Bound::Included(v) => Bound::Included(generate_index_key(v)),
        Bound::Excluded(v) => Bound::Excluded(generate_index_key(v)),
    };

    (
        translate_bound(start_bound, true),
        translate_bound(end_bound, false),
    )
}

pub fn tag_value_to_index_key(tab: &str, tag_key: &str, v: &ScalarValue) -> Vec<u8> {
    // Tag can only be of string type
    assert_eq!(DataType::Utf8, v.get_datatype());

    // Convert a string to an inverted index key
    let generate_index_key = |tag_val| {
        let tag = Tag::from_parts(tag_key, tag_val);
        encode_inverted_index_key(tab, &tag.key, &tag.value)
    };

    unsafe { utf8_from(v).map(generate_index_key).unwrap_unchecked() }
}

pub fn encode_series_id_key(id: u32) -> Vec<u8> {
    let len = SERIES_ID_PREFIX.len() + 4;
    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(SERIES_ID_PREFIX.as_bytes());
    buf.extend_from_slice(&id.to_be_bytes());

    buf
}

pub fn encode_inverted_max_index_key(tab: &str, tag_key: &[u8]) -> Vec<u8> {
    tab.as_bytes()
        .iter()
        .chain(".".as_bytes())
        .chain(tag_key)
        .chain(">".as_bytes())
        .cloned()
        .collect()
}

pub fn encode_inverted_min_index_key(tab: &str, tag_key: &[u8]) -> Vec<u8> {
    tab.as_bytes()
        .iter()
        .chain(".".as_bytes())
        .chain(tag_key)
        .chain("<".as_bytes())
        .cloned()
        .collect()
}

// tab.tag=val
pub fn encode_inverted_index_key(tab: &str, tag_key: &[u8], tag_val: &[u8]) -> Vec<u8> {
    tab.as_bytes()
        .iter()
        .chain(".".as_bytes())
        .chain(tag_key)
        .chain("=".as_bytes())
        .chain(tag_val)
        .cloned()
        .collect()
}

pub fn encode_series_key(tab: &str, tags: &[Tag]) -> Vec<u8> {
    let mut len = SERIES_KEY_PREFIX.len() + 2 + tab.len();
    for tag in tags.iter() {
        len += 2 + tag.key.len();
        len += 2 + tag.value.len();
    }

    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(SERIES_KEY_PREFIX.as_bytes());
    buf.extend_from_slice(&(tab.len() as u16).to_be_bytes());
    buf.extend_from_slice(tab.as_bytes());
    for tag in tags.iter() {
        buf.extend_from_slice(&(tag.key.len() as u16).to_be_bytes());
        buf.extend_from_slice(&tag.key);

        buf.extend_from_slice(&(tag.value.len() as u16).to_be_bytes());
        buf.extend_from_slice(&tag.value);
    }

    buf
}

pub fn decode_series_id_list(data: &[u8]) -> IndexResult<Vec<u32>> {
    if data.len() % 4 != 0 {
        return Err(IndexError::DecodeSeriesIDList);
    }

    let count = data.len() / 4;
    let mut list: Vec<u32> = Vec::with_capacity(count);
    for i in 0..count {
        let id = byte_utils::decode_be_u32(&data[i * 4..]);
        list.push(id);
    }

    Ok(list)
}

pub fn encode_series_id_list(list: &[u32]) -> Vec<u8> {
    let mut data: Vec<u8> = Vec::with_capacity(list.len() * 4);
    for i in list {
        data.put_u32(*i);
    }

    data
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use models::schema::ExternalTableSchema;
    use models::{SeriesKey, Tag};

    use super::TSIndex;

    #[tokio::test]
    async fn test_index() {
        let series_key1 = SeriesKey {
            id: 0,
            db: "db_test".to_string(),
            table: "table_test".to_string(),

            tags: vec![
                Tag::new(b"loc".to_vec(), b"bj".to_vec()),
                Tag::new(b"host".to_vec(), b"h1".to_vec()),
            ],
        };

        let series_key2 = SeriesKey {
            id: 0,
            db: "db_test".to_string(),
            table: "table_test".to_string(),

            tags: vec![
                Tag::new(b"loc".to_vec(), b"bj".to_vec()),
                Tag::new(b"host".to_vec(), b"h2".to_vec()),
            ],
        };

        let series_key3 = SeriesKey {
            id: 0,
            db: "db_test".to_string(),
            table: "table_test".to_string(),

            tags: vec![
                Tag::new(b"loc".to_vec(), b"bj".to_vec()),
                Tag::new(b"host".to_vec(), b"h3".to_vec()),
            ],
        };

        let ts_index = TSIndex::new(PathBuf::from("/tmp/test".to_string()))
            .await
            .unwrap();

        let id = ts_index
            .add_series_if_not_exists(&series_key1)
            .await
            .unwrap();
        println!("series_key1 id: {}", id);

        let id = ts_index
            .add_series_if_not_exists(&series_key2)
            .await
            .unwrap();
        println!("series_key2 id: {}", id);

        let id = ts_index
            .add_series_if_not_exists(&series_key3)
            .await
            .unwrap();
        println!("series_key3 id: {}", id);

        // get ...
        let id = ts_index.get_series_id(&series_key1).await.unwrap();
        println!("get series_key1 id: {:?}", id);

        let list = ts_index
            .get_series_id_list("table_test", &[Tag::new(b"host".to_vec(), b"h2".to_vec())])
            .await
            .unwrap();
        println!("get series id list h2: {:?}", list);

        ts_index.del_series_info(1).await.unwrap();

        let list = ts_index
            .get_series_id_list("table_test", &[])
            .await
            .unwrap();
        println!("get series id list all table: {:?}", list);
    }

    #[test]
    fn test_serde() {
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("priority", DataType::UInt8, false),
        ]);

        let schema = ExternalTableSchema {
            tenant: "cnosdb".to_string(),
            db: "hello".to_string(),
            name: "world".to_string(),
            file_compression_type: "test".to_string(),
            file_type: "1".to_string(),
            location: "2".to_string(),
            target_partitions: 3,
            table_partition_cols: vec![("4".to_string(), DataType::UInt8)],
            has_header: true,
            delimiter: 5,
            schema,
        };

        let ans_inter = serde_json::to_string(&schema).unwrap();
        let ans = serde_json::from_str::<ExternalTableSchema>(&ans_inter).unwrap();

        assert_eq!(ans, schema);
    }
}
