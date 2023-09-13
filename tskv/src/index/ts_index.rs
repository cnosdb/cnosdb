use std::collections::HashMap;
use std::fmt;
use std::ops::{BitAnd, BitOr, Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::BufMut;
use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use models::predicate::domain::{utf8_from, Domain, Range};
use models::tag::TagFromParts;
use models::{utils, SeriesKey, Tag};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::RwLock;
use trace::{debug, error, info};

use super::binlog::{AddSeries, DeleteSeries, IndexBinlog, IndexBinlogBlock};
use super::cache::{ForwardIndexCache, SeriesKeyInfo};
use super::{IndexEngine, IndexError, IndexResult};
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::index::binlog::{BinlogReader, BinlogWriter};
use crate::index::ts_index::fmt::Debug;
use crate::{byte_utils, file_utils};

const SERIES_ID_PREFIX: &str = "_id_";
const SERIES_KEY_PREFIX: &str = "_key_";
const AUTO_INCR_ID_KEY: &str = "_auto_incr_id";

/// Used to maintain forward and inverted indexes
///
/// # Example
///
/// The following is an index relationship diagram
///
/// In the following example, there are two records whose series keys are SeriesKey1 and SeriesKey2
///
/// SeriesKey1: Table1 T1=1a,T2=2a,T3=3a
///
/// SeriesKey2: Table1 T1=1b,T2=2b,T3=3a
///
///
/// ```text
/// SeriesKey1
/// ┌────────┐
/// │Table1  │◄───────────────────────── ┌──────────┐
/// ├──┬──┬──┤                           │SeriesId-1│
/// │T1│T2│T3│ ────────────────────────► └──────────┘
/// │1a│2a│3a│                            ▲  ▲  ▲
/// └┬─┴┬─┴─┬┘                            │  │  │
///  │  │   │                             │  │  │
///  │  │   └─────────────────────────────┘  │  │
///  │  │                                    │  │
///  │  └────────────────────────────────────┘  │
///  │                                          │
///  └──────────────────────────────────────────┘
///
///     
/// ┌────────┐
/// │Table1  │◄───────────────────────── ┌──────────┐
/// ├──┬──┬──┤                           │SeriesId-2│
/// │T1│T2│T3│ ────────────────────────► └──────────┘
/// │1b│2b│3a│                            ▲  ▲  ▲
/// └┬─┴┬─┴─┬┘                            │  │  │
///  │  │   │                             │  │  │
///  │  │   └─────────────────────────────┘  │  │
///  │  │                                    │  │
///  │  └────────────────────────────────────┘  │
///  │                                          │
///  └──────────────────────────────────────────┘
///
/// point1 with SeriesKey1(Table1 T1=1a,T2=2a,T3=3a), the generated indexes are
///     _key_Table1_T1_1a_T2_2a_T3_3a -> SeriesId-1
///     Table1.T1=1a -> SeriesId-1                    ──┐  
///     Table1.T2=2a -> SeriesId-1                      │---- Inverted index, used to filter data based on tag value
///     Table1.T3=3a -> SeriesId-1                    ──┘
///     _id_SeriesId-1 -> SeriesKey1                  ------- Used to construct the result
///
/// point2 with SeriesKey1(Table1 T1=1b,T2=2b,T3=3a), the generated indexes are
///     _key_Table1_T1_1b_T2_2b_T3_3a -> SeriesId-2
///     Table1.T1=1b -> SeriesId-2
///     Table1.T2=2b -> SeriesId-2
///     Table1.T3=3a -> SeriesId-2
///     _id_SeriesId-2 -> SeriesKey2
/// ```
pub struct TSIndex {
    path: PathBuf,
    incr_id: AtomicU32,
    write_count: AtomicU32,

    binlog: Arc<RwLock<IndexBinlog>>,
    storage: Arc<RwLock<IndexEngine>>,
    forward_cache: ForwardIndexCache,
    binlog_change_sender: UnboundedSender<()>,
}

impl TSIndex {
    pub async fn new(path: impl AsRef<Path>) -> IndexResult<Arc<Self>> {
        let path = path.as_ref();

        let binlog = IndexBinlog::new(path).await?;
        let storage = IndexEngine::new(path)?;

        let incr_id = match storage.get(AUTO_INCR_ID_KEY.as_bytes())? {
            Some(data) => byte_utils::decode_be_u32(&data),
            None => 0,
        };

        let (binlog_change_sender, binlog_change_reciver) = unbounded_channel();

        let mut ts_index = Self {
            binlog: Arc::new(RwLock::new(binlog)),
            storage: Arc::new(RwLock::new(storage)),
            incr_id: AtomicU32::new(incr_id),
            write_count: AtomicU32::new(0),
            path: path.into(),
            forward_cache: ForwardIndexCache::new(1_000_000),
            binlog_change_sender,
        };

        ts_index.recover().await?;
        let ts_index = Arc::new(ts_index);
        run_index_job(ts_index.clone(), binlog_change_reciver);
        info!(
            "Recovered index dir '{}', incr_id start at: {incr_id}",
            path.display()
        );

        Ok(ts_index)
    }

    async fn recover(&mut self) -> IndexResult<()> {
        let path = self.path.clone();
        let files = file_manager::list_file_names(&path);
        for filename in files.iter() {
            if let Ok(file_id) = file_utils::get_index_binlog_file_id(filename) {
                let file_path = path.join(filename);
                info!("Recovering index binlog: '{}'", file_path.display());
                let tmp_file = BinlogWriter::open(file_id, &file_path).await?;
                let mut reader_file = BinlogReader::new(file_id, tmp_file.file.into()).await?;
                self.recover_from_file(&mut reader_file).await?;
            }
        }

        Ok(())
    }

    async fn recover_from_file(&mut self, reader_file: &mut BinlogReader) -> IndexResult<()> {
        let mut max_id = self.incr_id.load(Ordering::Relaxed);
        while let Some(block) = reader_file.next_block().await? {
            match block {
                IndexBinlogBlock::Add(block) => {
                    // add series
                    let series_key = SeriesKey::decode(block.data())
                        .map_err(|e| IndexError::DecodeSeriesKey { msg: e.to_string() })?;

                    let mut storage_w = self.storage.write().await;
                    let id = block.series_id();
                    let key_buf = encode_series_key(series_key.table(), series_key.tags());
                    storage_w.set(&key_buf, &id.to_be_bytes())?;
                    storage_w.set(&encode_series_id_key(id), block.data())?;
                    for tag in series_key.tags() {
                        let key =
                            encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
                        storage_w.modify(&key, id, true)?;
                    }
                    if series_key.tags().is_empty() {
                        let key = encode_inverted_index_key(series_key.table(), &[], &[]);
                        storage_w.modify(&key, id, true)?;
                    }

                    if max_id < block.series_id() {
                        max_id = block.series_id()
                    }
                }
                IndexBinlogBlock::Delete(block) => {
                    // delete series
                    self.del_series_id_from_engine(block.series_id()).await?;
                }
                IndexBinlogBlock::Update(_) => {
                    return Err(IndexError::NotImplemented {
                        err: "Update series".to_string(),
                    })
                }
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

    pub async fn add_series_if_not_exists(
        &self,
        series_keys: Vec<SeriesKey>,
    ) -> IndexResult<Vec<u32>> {
        let mut ids = Vec::with_capacity(series_keys.len());
        let mut blocks_data = Vec::new();
        for series_key in series_keys.into_iter() {
            let encode = series_key.encode();
            let key_buf = encode_series_key(series_key.table(), series_key.tags());
            {
                let mut storage_w = self.storage.write().await;
                if let Some(val) = storage_w.get(&key_buf)? {
                    ids.push(byte_utils::decode_be_u32(&val));
                    continue;
                }
                let id = self.incr_id.fetch_add(1, Ordering::Relaxed) + 1;
                storage_w.set(&key_buf, &id.to_be_bytes())?;
                let block =
                    IndexBinlogBlock::Add(AddSeries::new(utils::now_timestamp_nanos(), id, encode));
                ids.push(id);
                blocks_data.push(block);
            }
        }
        self.binlog.write().await.write_blocks(&blocks_data).await?;
        self.binlog_change_sender
            .send(())
            .map_err(|e| IndexError::IndexStroage {
                msg: format!("Send binlog change failed, err: {}", e),
            })?;

        Ok(ids)
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
        let block = IndexBinlogBlock::Delete(DeleteSeries::new(sid));

        self.binlog.write().await.write_blocks(&[block]).await?;

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
        debug!("Index get sids: pushed tag_domains: {:?}", tag_domains);
        let mut series_ids = vec![];
        for (k, v) in tag_domains.iter() {
            let rb = self.get_series_ids_by_domain(tab, k, v).await?;
            series_ids.push(rb);
        }

        debug!(
            "Index get sids: filter scan result series_ids: {:?}",
            series_ids
        );

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

pub fn run_index_job(ts_index: Arc<TSIndex>, mut binlog_change_reciver: UnboundedReceiver<()>) {
    tokio::spawn(async move {
        let path = ts_index.path.clone();
        let mut handle_file = HashMap::new();
        while (binlog_change_reciver.recv().await).is_some() {
            let files = file_manager::list_file_names(&path);
            for filename in files.iter() {
                if let Ok(file_id) = file_utils::get_index_binlog_file_id(filename) {
                    let file_path = path.join(filename);
                    let file = match file_manager::open_file(&file_path).await {
                        Ok(f) => f,
                        Err(e) => {
                            error!(
                                "Open index binlog file '{}' failed, err: {}",
                                file_path.display(),
                                e
                            );
                            continue;
                        }
                    };

                    if file.len() <= *handle_file.get(&file_id).unwrap_or(&0) {
                        continue;
                    }

                    let mut reader_file = match BinlogReader::new(file_id, file.into()).await {
                        Ok(r) => r,
                        Err(e) => {
                            error!(
                                "Open index binlog file '{}' failed, err: {}",
                                file_path.display(),
                                e
                            );
                            continue;
                        }
                    };
                    while let Ok(Some(block)) = reader_file.next_block().await {
                        if reader_file.pos() <= *handle_file.get(&file_id).unwrap_or(&0) {
                            continue;
                        }

                        match block {
                            IndexBinlogBlock::Add(block) => {
                                let series_key = match SeriesKey::decode(block.data()) {
                                    Ok(key) => key,
                                    Err(e) => {
                                        error!("Decode series key failed, err: {}", e);
                                        continue;
                                    }
                                };
                                // is already exist and store forward index
                                {
                                    let mut storage_w;
                                    loop {
                                        match ts_index.storage.try_write() {
                                            Ok(stroage) => {
                                                storage_w = stroage;
                                                break;
                                            }
                                            Err(_) => {
                                                info!("Waiting for index storage write lock");
                                                tokio::time::sleep(Duration::from_secs(5)).await;
                                            }
                                        }
                                    }
                                    if let Err(e) = storage_w
                                        .set(&encode_series_id_key(block.series_id()), block.data())
                                    {
                                        error!("Set series key failed, err: {}", e);
                                    }
                                }

                                // then inverted index
                                for tag in series_key.tags() {
                                    let key = encode_inverted_index_key(
                                        series_key.table(),
                                        &tag.key,
                                        &tag.value,
                                    );
                                    if let Err(e) = ts_index.storage.write().await.modify(
                                        &key,
                                        block.series_id(),
                                        true,
                                    ) {
                                        error!("Modify inverted index failed, err: {}", e);
                                    }
                                }
                                if series_key.tags().is_empty() {
                                    let key =
                                        encode_inverted_index_key(series_key.table(), &[], &[]);
                                    if let Err(e) = ts_index.storage.write().await.modify(
                                        &key,
                                        block.series_id(),
                                        true,
                                    ) {
                                        error!("Modify inverted index failed, err: {}", e);
                                    }
                                }

                                let _ = ts_index.check_to_flush(false).await;
                                handle_file.insert(file_id, reader_file.pos());
                            }
                            IndexBinlogBlock::Delete(block) => {
                                // delete series
                                let _ = ts_index.del_series_id_from_engine(block.series_id()).await;
                                let _ = ts_index.check_to_flush(false).await;
                                handle_file.insert(file_id, reader_file.pos());
                            }
                            IndexBinlogBlock::Update(_) => {
                                error!("This feature is not implemented: Update series");
                            }
                        }
                    }
                }
            }
        }
    });
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use models::schema::ExternalTableSchema;
    use models::{SeriesId, SeriesKey, Tag};

    use super::TSIndex;

    /// ( sid, database, table, [(tag_key, tag_value)] )
    type SeriesKeyDesc<'a> = (SeriesId, &'a str, &'a str, Vec<(&'a str, &'a str)>);

    fn build_series_keys(series_keys_desc: &[SeriesKeyDesc<'_>]) -> Vec<SeriesKey> {
        let mut series_keys = Vec::with_capacity(series_keys_desc.len());
        for (sid, db, table, tags) in series_keys_desc {
            series_keys.push(SeriesKey {
                id: *sid,
                tags: tags
                    .iter()
                    .map(|(k, v)| Tag::new(k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                    .collect(),
                table: table.to_string(),
                db: db.to_string(),
            });
        }
        series_keys
    }

    #[tokio::test]
    async fn test_index() {
        let dir = "/tmp/test/ts_index/1";
        let _ = std::fs::remove_dir_all(dir);
        let database = "db_test";
        let mut max_sid = 0;
        {
            // Generic tests.
            #[rustfmt::skip]
            let series_keys_desc: Vec<SeriesKeyDesc> = vec![
                (0, database, "table_test", vec![("loc", "bj"), ("host", "h1")]),
                (0, database, "table_test", vec![("loc", "bj"), ("host", "h2")]),
                (0, database, "table_test", vec![("loc", "bj"), ("host", "h3")]),
                (0, database, "ma", vec![("ta", "a1"), ("tb", "b1")]),
                (0, database, "ma", vec![("ta", "a1"), ("tb", "b1")]),
                (0, database, "table_test", vec![("loc", "nj"), ("host", "h1")]),
                (0, database, "table_test", vec![("loc", "nj"), ("host", "h2")]),
                (0, database, "table_test", vec![("loc", "nj"), ("host", "h3")]),
            ];
            let series_keys = build_series_keys(&series_keys_desc);
            // Test build_series_key()
            assert_eq!(series_keys_desc.len(), series_keys.len());
            for ((id, db, table, tags), series_key) in
                series_keys_desc.iter().zip(series_keys.iter())
            {
                assert_eq!(*id, series_key.id);
                assert_eq!(*db, &series_key.db);
                assert_eq!(*table, &series_key.table);
                assert_eq!(tags.len(), series_key.tags.len());
                for ((k, v), tag) in tags.iter().zip(series_key.tags.iter()) {
                    assert_eq!(k.as_bytes(), tag.key.as_slice());
                    assert_eq!(v.as_bytes(), tag.value.as_slice());
                }
            }

            let ts_index = TSIndex::new(dir).await.unwrap();
            // Insert series into index.
            let mut series_keys_sids = Vec::with_capacity(series_keys_desc.len());
            for (i, series_key) in series_keys.iter().enumerate() {
                let sid = ts_index
                    .add_series_if_not_exists(vec![series_key.clone()])
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let last_key = ts_index.get_series_key(sid[0]).await.unwrap().unwrap();
                assert_eq!(series_key.to_string(), last_key.to_string());

                max_sid = max_sid.max(sid[0]);
                println!("test_index#1: series {i} - '{series_key}' - id: {:?}", sid);
                series_keys_sids.push(sid[0]);
            }

            // Test get series from index
            for (i, series_key) in series_keys.iter().enumerate() {
                let sid = ts_index.get_series_id(series_key).await.unwrap().unwrap();
                assert_eq!(series_keys_sids[i], sid);

                let list = ts_index
                    .get_series_id_list(&series_key.table, &series_key.tags)
                    .await
                    .unwrap();
                assert_eq!(vec![sid], list);
            }

            // Test query series list
            let query_t = "table_test";
            let query_k = b"host".to_vec();
            let query_v = b"h2".to_vec();
            let mut list_1 = ts_index
                .get_series_id_list(query_t, &[Tag::new(query_k.clone(), query_v.clone())])
                .await
                .unwrap();
            list_1.sort();
            let mut list_2 = Vec::with_capacity(list_1.len());
            for (i, series_key) in series_keys.iter().enumerate() {
                if series_key.table == query_t
                    && series_key
                        .tags
                        .iter()
                        .any(|t| t.key == query_k && t.value == query_v)
                {
                    list_2.push(series_keys_sids[i]);
                }
            }
            list_2.sort();
            assert_eq!(list_1, list_2);

            // Test delete series
            ts_index.del_series_info(series_keys_sids[1]).await.unwrap();
            assert_eq!(ts_index.get_series_id(&series_keys[1]).await.unwrap(), None);
            let list = ts_index.get_series_id_list(query_t, &[]).await.unwrap();
            assert_eq!(list.len(), 5);
        }

        {
            // Test re-open, query and insert.
            let ts_index = TSIndex::new(dir).await.unwrap();
            let list = ts_index
                .get_series_id_list("table_test", &[])
                .await
                .unwrap();
            assert_eq!(list.len(), 5);

            #[rustfmt::skip]
            let series_keys_desc: Vec<SeriesKeyDesc> = vec![
                (0, database, "table_test", vec![("loc", "dj"), ("host", "h1")]),
                (0, database, "table_test", vec![("loc", "dj"), ("host", "h2")]),
                (0, database, "ma", vec![("ta", "a2"), ("tb", "b2")]),
                (0, database, "ma", vec![("ta", "a2"), ("tb", "b2")]),
                (0, database, "table_test", vec![("loc", "xj"), ("host", "h1")]),
                (0, database, "table_test", vec![("loc", "xj"), ("host", "h2")]),
            ];
            let series_keys = build_series_keys(&series_keys_desc);

            // Test insert after re-open.
            let prev_max_sid = max_sid;
            for (i, series_key) in series_keys.iter().enumerate() {
                let sid = ts_index
                    .add_series_if_not_exists(vec![series_key.clone()])
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let last_key = ts_index.get_series_key(sid[0]).await.unwrap().unwrap();
                assert_eq!(series_key.to_string(), last_key.to_string());

                assert!(sid[0] > prev_max_sid);
                max_sid = max_sid.max(sid[0]);
                println!("test_index#2: series {i} - '{series_key}' - id: {:?}", sid);
            }
        }

        // Test re-open, do not insert and then re-open.
        let ts_index = TSIndex::new(dir).await.unwrap();
        drop(ts_index);
        let ts_index = TSIndex::new(dir).await.unwrap();
        #[rustfmt::skip]
        let series_keys_desc: Vec<SeriesKeyDesc> = vec![
            (0, database, "table_test", vec![("loc", "dbj"), ("host", "h1")]),
            (0, database, "table_test", vec![("loc", "dnj"), ("host", "h2")]),
            (0, database, "table_test", vec![("loc", "xbj"), ("host", "h1")]),
            (0, database, "table_test", vec![("loc", "xnj"), ("host", "h2")]),
        ];
        let series_keys = build_series_keys(&series_keys_desc);
        let prev_max_sid = max_sid;
        for (i, series_key) in series_keys.iter().enumerate() {
            let sid = ts_index
                .add_series_if_not_exists(vec![series_key.clone()])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            let last_key = ts_index.get_series_key(sid[0]).await.unwrap().unwrap();
            assert_eq!(series_key.to_string(), last_key.to_string());

            assert!(sid[0] > prev_max_sid);
            println!("test_index#3: series {i} - '{series_key}' - id: {:?}", sid);
        }
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
