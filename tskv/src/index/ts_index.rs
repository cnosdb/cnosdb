use std::collections::{HashMap, HashSet};
use std::ops::{BitAnd, BitOr, Bound, RangeBounds};
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::scalar::ScalarValue;
use models::predicate::domain::{utf8_from, ColumnDomains, Domain, Range};
use models::schema::tskv_table_schema::TskvTableSchema;
use models::{tag, SeriesId, SeriesKey, Tag, TagKey, TagValue};
use snafu::{OptionExt, ResultExt};
use tokio::sync::RwLock;

use super::cache::IndexCache;
use super::{DecodeSeriesKeySnafu, IndexEngine, IndexResult};
use crate::error::{ColumnNotFoundSnafu, IndexErrSnafu};
use crate::index::SeriesAlreadyExistsSnafu;
use crate::{byte_utils, TskvError, UpdateSetValue};

const SERIES_ID_PREFIX: &str = "_id_";
const SERIES_KEY_PREFIX: &str = "_key_";
const TOMBSTONE_PREFIX: &str = "_tomb_";
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
    incr_id: AtomicU32,
    write_count: AtomicU32,

    storage: IndexEngine,
    cache: IndexCache,
}

impl TSIndex {
    pub async fn new(path: impl AsRef<Path>, cap: u64) -> IndexResult<Arc<RwLock<Self>>> {
        let path = path.as_ref();
        let storage = IndexEngine::new(path)?;

        let incr_id = match storage.get(AUTO_INCR_ID_KEY.as_bytes())? {
            Some(data) => byte_utils::decode_be_u32(&data),
            None => 0,
        };

        let ts_index = Self {
            storage,
            incr_id: AtomicU32::new(incr_id),
            write_count: AtomicU32::new(0),
            cache: IndexCache::new(cap as usize),
        };

        trace::info!(
            "Open index dir '{:?}', incr id start at: {:?}",
            path,
            ts_index.incr_id,
        );

        Ok(Arc::new(RwLock::new(ts_index)))
    }

    pub async fn add_series_for_rebuild(
        &mut self,
        id: SeriesId,
        key: &SeriesKey,
    ) -> IndexResult<()> {
        let key_buf = encode_series_key(key.table(), key.tags());
        if self.storage.exist(&key_buf)? {
            return Ok(());
        }

        if self.incr_id.load(Ordering::Relaxed) <= id {
            self.incr_id.store(id + 1, Ordering::Relaxed);
        }

        self.cache.write(id, key.clone());
        Ok(())
    }

    pub async fn add_series_if_not_exists(
        &mut self,
        series_keys: Vec<SeriesKey>,
    ) -> IndexResult<Vec<(SeriesId, SeriesKey)>> {
        let mut ids = Vec::with_capacity(series_keys.len());
        for series_key in series_keys.into_iter() {
            let key_buf = encode_series_key(series_key.table(), series_key.tags());
            if let Some(id) = self.cache.get_series_id_by_key(&series_key) {
                ids.push((id, series_key));
                continue;
            }
            if let Some(val) = self.storage.get(&key_buf)? {
                let id = byte_utils::decode_be_u32(&val);
                ids.push((id, series_key.clone()));
                self.cache.cache(id, series_key);
                continue;
            }
            let id = self.incr_id.fetch_add(1, Ordering::Relaxed) + 1;
            ids.push((id, series_key.clone()));

            // write index memcache
            trace::debug!("Index add new series id:{}, key: {}", id, series_key);
            self.cache.write(id, series_key);

            let _ = self.check_to_flush(false).await;
        }

        Ok(ids)
    }

    pub async fn get_series_id(&self, series_key: &SeriesKey) -> IndexResult<Option<u32>> {
        if let Some(id) = self.cache.get_series_id_by_key(series_key) {
            return Ok(Some(id));
        }

        let key_buf = encode_series_key(series_key.table(), series_key.tags());
        if let Some(val) = self.storage.get(&key_buf)? {
            let id = byte_utils::decode_be_u32(&val);
            self.cache.cache(id, series_key.clone());

            return Ok(Some(id));
        }

        Ok(None)
    }

    pub async fn get_series_key(&self, sid: SeriesId) -> IndexResult<Option<SeriesKey>> {
        if let Some(key) = self.cache.get_series_key_by_id(sid) {
            return Ok(Some(key));
        }

        let series_key = self.storage.get(&encode_series_id_key(sid))?;
        if let Some(res) = series_key {
            let key = SeriesKey::decode(&res)
                .map_err(|e| DecodeSeriesKeySnafu { msg: e.to_string() }.build())?;

            self.cache.cache(sid, key.clone());

            return Ok(Some(key));
        }

        Ok(None)
    }

    async fn add_tombstone_series(
        &mut self,
        id: SeriesId,
        series_key: &SeriesKey,
    ) -> IndexResult<()> {
        let key_buf = encode_tombstone_series_key(series_key.table(), series_key.tags());
        trace::debug!("Index add tombstone id:{}, key: {}", id, series_key);
        self.storage.set(&key_buf, &id.to_be_bytes())
    }

    pub async fn clear_tombstone_series(&mut self) -> IndexResult<()> {
        trace::info!("Index clear all tombstone");
        self.storage.del_prefix(TOMBSTONE_PREFIX.as_bytes())
    }

    pub async fn get_tombstone_series_id(
        &self,
        series_key: &SeriesKey,
    ) -> IndexResult<Option<SeriesId>> {
        let key_buf = encode_tombstone_series_key(series_key.table(), series_key.tags());
        if let Some(val) = self.storage.get(&key_buf)? {
            let id = byte_utils::decode_be_u32(&val);
            return Ok(Some(id));
        }

        Ok(None)
    }

    pub async fn del_series_info(&mut self, sid: u32) -> IndexResult<()> {
        let series_key = self.get_series_key(sid).await?;
        let _ = self.storage.delete(&encode_series_id_key(sid));
        if let Some(series_key) = series_key {
            self.cache.del(sid, &series_key);
            let key_buf = encode_series_key(series_key.table(), series_key.tags());
            let _ = self.storage.delete(&key_buf);
            for tag in series_key.tags() {
                let key = encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
                self.storage.modify(&key, sid, false)?;
            }
            if series_key.tags().is_empty() {
                let key = encode_inverted_index_key(series_key.table(), &[], &[]);
                self.storage.modify(&key, sid, false)?;
            }

            trace::debug!("Index delete series id:{}, key: {}", sid, series_key);
        }

        let _ = self.check_to_flush(false).await;

        Ok(())
    }

    /// 记录更新series key的binlog，并且删除缓存中的sid -> series key的映射
    pub async fn update_series_key(
        &mut self,
        old_series_keys: Vec<SeriesKey>,
        new_series_keys: Vec<SeriesKey>,
        sids: Vec<SeriesId>,
        _recovering: bool,
    ) -> IndexResult<()> {
        for ((old_series, new_series), sid) in old_series_keys
            .iter()
            .zip(new_series_keys.iter())
            .zip(sids.iter())
        {
            trace::debug!(
                "Index update series id:{}, key: {} -> {}",
                sid,
                old_series,
                new_series
            );
            self.del_series_info(*sid).await?;
            self.add_tombstone_series(*sid, old_series).await?;

            self.cache.write(*sid, new_series.clone());

            let _ = self.check_to_flush(false).await;
        }

        Ok(())
    }

    async fn check_to_flush(&mut self, force: bool) -> IndexResult<()> {
        let count = self.write_count.fetch_add(1, Ordering::Relaxed);
        if !force && count < 10000 {
            return Ok(());
        }

        let id_bytes = self.incr_id.load(Ordering::Relaxed).to_be_bytes();
        self.storage.set(AUTO_INCR_ID_KEY.as_bytes(), &id_bytes)?;
        self.cache.write_cache.flush(&mut self.storage).await?;
        self.storage.flush()?;

        self.write_count.store(0, Ordering::Relaxed);

        Ok(())
    }

    /// if tags == [] return all
    pub async fn get_series_id_list(&self, tab: &str, tags: &[Tag]) -> IndexResult<Vec<SeriesId>> {
        let rb = self.get_series_id_bitmap(tab, tags).await?;

        Ok(rb.iter().collect())
    }

    async fn get_series_id_bitmap(
        &self,
        tab: &str,
        tags: &[Tag],
    ) -> IndexResult<roaring::RoaringBitmap> {
        let cache_rb = self.cache.write_cache.get_inverted_by_tags(tab, tags);
        let engine_rb = self.storage.get_series_id_by_tags(tab, tags)?;

        Ok(cache_rb.bitor(&engine_rb))
    }

    /// 获取所有匹配的旧的series key及其更新后的series key，以及对应的series id
    /// (old_series_keys, new_series_keys, sids)
    pub async fn prepare_update_tags_value(
        &self,
        new_tags: &[UpdateSetValue<TagKey, TagValue>],
        matched_series: &[SeriesKey],
        check_conflict: bool,
    ) -> IndexResult<(Vec<SeriesKey>, Vec<SeriesKey>, Vec<SeriesId>)> {
        // Find all matching series ids
        let mut ids = vec![];
        let mut old_keys = vec![];
        for key in matched_series {
            if let Some(sid) = self.get_series_id(key).await? {
                ids.push(sid);
                old_keys.push(key.clone());
            }
        }

        let mut new_keys = vec![];
        let mut new_keys_set = HashSet::new();
        for key in &old_keys {
            // modify tag value
            let mut old_tags = key
                .tags
                .iter()
                .map(|Tag { key, value }| (key, Some(value.as_ref())))
                .collect::<HashMap<_, _>>();

            // 更新 tag value
            for UpdateSetValue { key, value } in new_tags {
                old_tags.insert(key, value.as_deref());
            }
            // 移除值为 Null 的 tag
            let mut tags = old_tags
                .iter()
                .flat_map(|(key, value)| value.map(|val| Tag::new(key.to_vec(), val.to_vec())))
                .collect::<Vec<_>>();

            tag::sort_tags(&mut tags);

            let new_key = SeriesKey {
                tags,
                table: key.table.clone(),
            };

            if check_conflict && self.get_series_id(&new_key).await?.is_some() {
                trace::warn!("Series already exists: {:?}", new_key);
                return Err(SeriesAlreadyExistsSnafu {
                    key: key.to_string(),
                }
                .build());
            }

            // TODO 去重
            new_keys.push(new_key);
        }

        new_keys_set.extend(new_keys.clone());

        // 检查新生成的series key是否有重复key
        if new_keys_set.len() != new_keys.len() {
            return Err(SeriesAlreadyExistsSnafu {
                key: "new series keys".to_string(),
            }
            .build());
        }

        Ok((old_keys, new_keys, ids))
    }

    pub async fn get_series_ids_by_domains(
        &self,
        table_schema: &TskvTableSchema,
        tag_domains: &ColumnDomains<String>,
    ) -> Result<Vec<SeriesId>, TskvError> {
        let tab = table_schema.name.as_str();
        if tag_domains.is_all() {
            // Match all records
            trace::debug!("pushed tags filter is All.");
            return self
                .get_series_id_list(tab, &[])
                .await
                .context(IndexErrSnafu);
        }

        if let Some(domains) = tag_domains.domains() {
            trace::debug!("Index get sids: pushed tag_domains: {:?}", domains);
            let mut series_ids = vec![];
            for (k, v) in domains.iter() {
                let id = table_schema
                    .column(k)
                    .context(ColumnNotFoundSnafu {
                        column: k.to_string(),
                    })?
                    .id
                    .to_string();

                let rb = self
                    .get_series_ids_by_domain(tab, &id, v)
                    .await
                    .context(IndexErrSnafu)?;
                series_ids.push(rb);
            }

            trace::debug!(
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
        } else {
            // Does not match any record, return null
            trace::debug!("pushed tags filter is None.");
            Ok(vec![])
        }
    }

    async fn get_series_ids_by_domain(
        &self,
        tab: &str,
        tag_key: &str,
        v: &Domain,
    ) -> IndexResult<roaring::RoaringBitmap> {
        let mut bitmap = roaring::RoaringBitmap::new();
        match v {
            Domain::Range(range_set) => {
                for (_, range) in range_set.low_indexed_ranges().into_iter() {
                    let cache_rb = self.cache.write_cache.get_inverted_by_range(
                        tab,
                        tag_key,
                        filter_range_to_value_range(range),
                    );
                    bitmap = bitmap.bitor(cache_rb);

                    // Search the sid list corresponding to qualified tags in the range
                    let key_range = filter_range_to_index_key_range(tab, tag_key, range);
                    let engine_rb = self.storage.get_series_id_by_range(key_range)?;
                    bitmap = bitmap.bitor(engine_rb);
                }
            }
            Domain::Equtable(val) => {
                if val.is_white_list() {
                    // Contains the given value
                    for entry in val.entries().into_iter() {
                        let tags = vec![Tag {
                            key: tag_key.as_bytes().to_vec(),
                            value: scalar_value_to_tag_value(entry.value()),
                        }];

                        let cache_rb = self.cache.write_cache.get_inverted_by_tags(tab, &tags);
                        bitmap = bitmap.bitor(cache_rb);

                        let engine_rb = self.storage.get_series_id_by_tags(tab, &tags)?;
                        bitmap = bitmap.bitor(engine_rb);
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

    pub async fn flush(&mut self) -> IndexResult<()> {
        self.check_to_flush(true).await?;

        Ok(())
    }
}

impl std::fmt::Debug for TSIndex {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

pub fn filter_range_to_index_key_range(
    tab: &str,
    tag_key: &str,
    range: &Range,
) -> impl RangeBounds<Vec<u8>> {
    let start_bound = range.start_bound();
    let end_bound = range.end_bound();

    // Convert ScalarValue value to inverted index key
    let generate_index_key = |v: &ScalarValue| {
        let tag_value = scalar_value_to_tag_value(v);
        encode_inverted_index_key(tab, tag_key.as_bytes(), &tag_value)
    };

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

pub fn filter_range_to_value_range(range: &Range) -> impl RangeBounds<Vec<u8>> {
    let start_bound = range.start_bound();
    let end_bound = range.end_bound();

    // Convert ScalarValue value to inverted index key
    let get_value = |v: &ScalarValue| scalar_value_to_tag_value(v);

    // Convert the tag value in Bound to the inverted index key
    let translate_bound = |bound: Bound<&ScalarValue>| match bound {
        Bound::Unbounded => Bound::Unbounded,

        Bound::Included(v) => Bound::Included(get_value(v)),
        Bound::Excluded(v) => Bound::Excluded(get_value(v)),
    };

    (translate_bound(start_bound), translate_bound(end_bound))
}

pub fn scalar_value_to_tag_value(v: &ScalarValue) -> Vec<u8> {
    // Tag can only be of string type
    assert_eq!(DataType::Utf8, v.get_datatype());
    unsafe { utf8_from(v).unwrap_unchecked().into() }
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
    encode_series_key_with_prefix(SERIES_KEY_PREFIX, tab, tags)
}

pub fn encode_tombstone_series_key(tab: &str, tags: &[Tag]) -> Vec<u8> {
    encode_series_key_with_prefix(TOMBSTONE_PREFIX, tab, tags)
}

fn encode_series_key_with_prefix(prefix: &str, tab: &str, tags: &[Tag]) -> Vec<u8> {
    let mut len = prefix.len() + 2 + tab.len();
    for tag in tags.iter() {
        len += 2 + tag.key.len();
        len += 2 + tag.value.len();
    }

    let mut buf = Vec::with_capacity(len);
    buf.extend_from_slice(prefix.as_bytes());
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use models::schema::external_table_schema::ExternalTableSchema;
    use models::{SeriesId, SeriesKey, Tag};

    use super::TSIndex;
    use crate::UpdateSetValue;

    /// ( sid, database, table, [(tag_key, tag_value)] )
    type SeriesKeyDesc<'a> = (SeriesId, &'a str, &'a str, Vec<(&'a str, &'a str)>);

    fn build_series_keys(series_keys_desc: &[SeriesKeyDesc<'_>]) -> Vec<SeriesKey> {
        let mut series_keys = Vec::with_capacity(series_keys_desc.len());
        for (_, _, table, tags) in series_keys_desc {
            series_keys.push(SeriesKey {
                tags: tags
                    .iter()
                    .map(|(k, v)| Tag::new(k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                    .collect(),
                table: table.to_string(),
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
            for ((_, _, table, tags), series_key) in series_keys_desc.iter().zip(series_keys.iter())
            {
                assert_eq!(*table, &series_key.table);
                assert_eq!(tags.len(), series_key.tags.len());
                for ((k, v), tag) in tags.iter().zip(series_key.tags.iter()) {
                    assert_eq!(k.as_bytes(), tag.key.as_slice());
                    assert_eq!(v.as_bytes(), tag.value.as_slice());
                }
            }

            let ts_index = TSIndex::new(dir, 10000).await.unwrap();
            let mut ts_index = ts_index.write().await;
            // Insert series into index.
            let mut series_keys_sids = Vec::with_capacity(series_keys_desc.len());
            for (i, series_key) in series_keys.iter().enumerate() {
                let sid = ts_index
                    .add_series_if_not_exists(vec![series_key.clone()])
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(100)).await;
                let last_key = ts_index.get_series_key(sid[0].0).await.unwrap().unwrap();
                assert_eq!(series_key.to_string(), last_key.to_string());

                max_sid = max_sid.max(sid[0].0);
                println!("test_index#1: series {i} - '{series_key}' - id: {:?}", sid);
                series_keys_sids.push(sid[0].0);
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

            // flush just for later test re-open
            ts_index.flush().await.unwrap();
        }

        {
            // Test re-open, query and insert.
            let ts_index = TSIndex::new(dir, 10000).await.unwrap();
            let mut ts_index = ts_index.write().await;
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
                let last_key = ts_index.get_series_key(sid[0].0).await.unwrap().unwrap();
                assert_eq!(series_key.to_string(), last_key.to_string());
                println!("test_index#2: series {i} - '{series_key}' - id: {:?}", sid);

                assert!(sid[0].0 > prev_max_sid);
                max_sid = max_sid.max(sid[0].0);
            }

            // flush just for later test re-open
            ts_index.flush().await.unwrap();
        }

        // Test re-open, do not insert and then re-open.
        let ts_index = TSIndex::new(dir, 10000).await.unwrap();
        drop(ts_index);
        let ts_index = TSIndex::new(dir, 10000).await.unwrap();
        let mut ts_index = ts_index.write().await;
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
            let last_key = ts_index.get_series_key(sid[0].0).await.unwrap().unwrap();
            assert_eq!(series_key.to_string(), last_key.to_string());
            println!("test_index#3: series {i} - '{series_key}' - id: {:?}", sid);

            assert!(sid[0].0 > prev_max_sid);
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

    #[tokio::test]
    async fn test_update_tags_value() {
        let table_name = "table";
        let dir = "/tmp/test/cnosdb/ts_index/update_tags_value";
        let _ = std::fs::remove_dir_all(dir);

        let ts_index = TSIndex::new(dir, 10000).await.unwrap();
        let mut ts_index = ts_index.write().await;

        let tags1 = vec![
            Tag::new("station".as_bytes().to_vec(), "a0".as_bytes().to_vec()),
            Tag::new("region".as_bytes().to_vec(), "SX".as_bytes().to_vec()),
        ];
        let waiting_update_tags = vec![
            Tag::new("station".as_bytes().to_vec(), "a1".as_bytes().to_vec()),
            Tag::new("region".as_bytes().to_vec(), "SN".as_bytes().to_vec()),
        ];
        let tags3 = vec![Tag::new(
            "region".as_bytes().to_vec(),
            "HB".as_bytes().to_vec(),
        )];
        let tags4 = vec![Tag::new(
            "station".as_bytes().to_vec(),
            "a1".as_bytes().to_vec(),
        )];

        let series_keys = [tags1, waiting_update_tags.clone(), tags3, tags4]
            .into_iter()
            .map(|tags| SeriesKey {
                tags,
                table: table_name.to_string(),
            })
            .collect::<Vec<_>>();

        // 添加series
        let sids = ts_index
            .add_series_if_not_exists(series_keys.clone())
            .await
            .unwrap();

        assert_eq!(sids.len(), 4);

        // 校验写入成功
        for (sid, expected_series) in sids.iter().zip(series_keys.iter()) {
            let series = ts_index.get_series_key(sid.0).await.unwrap().unwrap();
            assert_eq!(expected_series, &series)
        }

        let matched_series = SeriesKey {
            tags: waiting_update_tags,
            table: table_name.to_string(),
        };
        let (old_series_keys, new_series_keys, matched_sids) = ts_index
            .prepare_update_tags_value(
                &[UpdateSetValue {
                    key: "station".as_bytes().to_vec(),
                    value: Some("a2".as_bytes().to_vec()),
                }],
                &[matched_series.clone()],
                true,
            )
            .await
            .unwrap();
        ts_index
            .update_series_key(old_series_keys, new_series_keys, matched_sids, false)
            .await
            .unwrap();

        // Wait for binlog to be consumed
        tokio::time::sleep(Duration::from_secs(1)).await;

        // 校验修改成功
        for (sid, expected_series) in sids.iter().zip(series_keys.iter()) {
            let series = ts_index.get_series_key(sid.0).await.unwrap().unwrap();
            if series != matched_series {
                continue;
            }

            for (expected_tag, tag) in expected_series.tags.iter().zip(series.tags.iter()) {
                if expected_tag.key == "station".as_bytes().to_vec() {
                    assert_eq!("a2".as_bytes().to_vec(), tag.value);
                } else {
                    assert_eq!(expected_tag.key, tag.key);
                }
            }
        }
    }
}
