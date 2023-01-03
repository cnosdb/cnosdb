use std::borrow::Borrow;
use std::collections::HashSet;
use std::hash::Hash;
use std::mem::size_of;
use std::ops::{Bound, Index, RangeBounds};
use std::path::{self, Path, PathBuf};
use std::string::FromUtf8Error;
use std::{collections::HashMap, sync::Arc};

use bytes::BufMut;
use chrono::format::format;
use datafusion::prelude::Column;
use datafusion::scalar::ScalarValue;
use lazy_static::__Deref;
use models::predicate::domain::{utf8_from, Domain, Marker, Range, ValueEntry};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use sled::Error;
use snafu::ResultExt;

use crate::index::utils::{encode_inverted_max_index_key, encode_inverted_min_index_key};
use crate::Error::IndexErr;
use config::Config;
use datafusion::arrow::datatypes::{DataType, ToByteSlice};
use datafusion::parquet::data_type::AsBytes;
use models::codec::Encoding;
use models::{
    tag::TagFromParts, utils, ColumnId, FieldId, FieldInfo, SeriesId, SeriesKey, Tag, ValueType,
};
use protos::models::Point;
use trace::{debug, error, info, warn};

use super::utils::{decode_series_id_list, encode_inverted_index_key, encode_series_id_list};
use super::*;
use super::{errors, IndexEngine, IndexError, IndexResult};

const SERIES_KEY_PREFIX: &str = "_series_key_";
const TABLE_SCHEMA_PREFIX: &str = "_table_schema_";
const DATABASE_SCHEMA_PREFIX: &str = "_database_schema_";

#[derive(Debug, Clone)]
pub struct IndexConfig {
    pub path: String,
}

impl From<&Config> for IndexConfig {
    fn from(config: &Config) -> Self {
        Self {
            path: config.storage.path.clone(),
        }
    }
}

pub fn index_manger(path: impl AsRef<Path>) -> &'static Arc<RwLock<DbIndexMgr>> {
    static INSTANCE: OnceCell<Arc<RwLock<DbIndexMgr>>> = OnceCell::new();
    INSTANCE.get_or_init(|| Arc::new(RwLock::new(DbIndexMgr::new(path))))
}

#[derive(Debug)]
pub struct DbIndexMgr {
    base_path: PathBuf,
    //db_name -> DBIndex
    indexs: HashMap<String, Arc<DBIndex>>,
}

impl DbIndexMgr {
    pub fn new(index_base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: index_base_path.as_ref().into(),
            indexs: HashMap::new(),
        }
    }

    pub fn get_db_index(&mut self, name: &str) -> Arc<DBIndex> {
        let index = self.indexs.get(name);
        match index {
            None => {
                let index = Arc::new(DBIndex::new(self.base_path.join(name)));
                self.indexs.insert(name.to_string(), index.clone());
                index
            }
            Some(index) => index.clone(),
        }
    }

    pub fn remove_db_index(&mut self, db_name: &str) {
        self.indexs.remove(db_name);
    }
}

#[derive(Debug)]
pub struct DBIndex {
    path: PathBuf,
    storage: IndexEngine,
    //The u32 comes from split(SeriesKey.hash())
    series_cache: RwLock<HashMap<u32, Vec<SeriesKey>>>,
}

impl DBIndex {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        let storage = IndexEngine::new(path);
        Self {
            storage,
            series_cache: RwLock::new(HashMap::new()),
            path: path.into(),
        }
    }

    pub fn get_sid_from_cache(&self, info: &Point) -> IndexResult<Option<u64>> {
        let series_key = SeriesKey::from_flatbuffer(info)
            .map_err(|e| IndexError::FieldType { msg: e.to_string() })?;
        let (hash_id, _) = utils::split_id(series_key.hash());

        if let Some(keys) = self.series_cache.read().get(&hash_id) {
            if let Some(k) = keys.iter().find(|key| series_key.eq(key)) {
                let id = k.id();
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    pub fn add_series_if_not_exists(&self, info: &Point) -> IndexResult<u64> {
        let mut series_key = SeriesKey::from_flatbuffer(info)
            .map_err(|e| IndexError::FieldType { msg: e.to_string() })?;

        let (hash_id, _) = utils::split_id(series_key.hash());
        let stroage_key = format!("{}{}", SERIES_KEY_PREFIX, hash_id);

        // load index first from cache,or else from storage and than cache it!
        let mut keys: &mut Vec<SeriesKey> = &mut vec![];
        let mut series_cache = self.series_cache.write();
        match series_cache.get_mut(&hash_id) {
            Some(v) => keys = v,
            None => {
                if let Some(data) = self.storage.get(stroage_key.as_bytes())? {
                    if let Ok(v) = bincode::deserialize(&data) {
                        series_cache.insert(hash_id, v);
                        keys = series_cache.get_mut(&hash_id).unwrap();
                    }
                }
            }
        }

        //if exist return series_id
        if let Some(k) = keys.iter().find(|key| series_key.eq(key)) {
            return Ok(k.id());
        }
        //if not exist add it!
        let id = utils::unite_id(hash_id as u64, self.storage.incr_id()?);
        series_key.set_id(id);
        keys.push(series_key.clone());
        self.storage
            .set(stroage_key.as_bytes(), &bincode::serialize(&keys).unwrap())?;
        drop(series_cache);

        for tag in series_key.tags() {
            let key = encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
            self.storage.push(&key, id.to_be_bytes().as_ref())?;
        }
        Ok(id)
    }

    pub fn flush(&self) -> IndexResult<()> {
        self.storage.flush();
        Ok(())
    }

    pub fn get_series_key(&self, sid: u64) -> IndexResult<Option<SeriesKey>> {
        let (hash_id, _) = utils::split_id(sid);
        let stroage_key = format!("{}{}", SERIES_KEY_PREFIX, hash_id);

        // load index first from cache,or else from storage and than cache it!
        let lock = self.series_cache.read();
        let res = lock
            .get(&hash_id)
            .map(|keys| keys.iter().find(|k| k.id() == sid).cloned());
        drop(lock);
        match res {
            Some(res) => Ok(res),
            None => {
                if let Some(data) = self.storage.get(stroage_key.as_bytes())? {
                    if let Ok(v) = bincode::deserialize::<Vec<SeriesKey>>(&data) {
                        let res = v.clone();
                        self.series_cache.write().entry(hash_id).or_insert(v);
                        let res = res.iter().find(|k| k.id() == sid);
                        if let Some(k) = res {
                            return Ok(Some(k.clone()));
                        }
                    }
                    return Err(IndexError::IndexStroage {
                        msg: "deserialize failed".to_string(),
                    });
                }
                Ok(None)
            }
        }
    }

    pub fn del_series_info(&self, sid: u64) -> IndexResult<()> {
        let (hash_id, _) = utils::split_id(sid);
        self.series_cache.write().remove(&hash_id);

        let stroage_key = format!("{}{}", SERIES_KEY_PREFIX, hash_id);
        if let Some(data) = self.storage.get(stroage_key.as_bytes())? {
            if let Ok(keys) = bincode::deserialize::<Vec<SeriesKey>>(&data) {
                if let Some(key) = keys.iter().find(|key| key.id() == sid) {
                    for tag in key.tags() {
                        let key = encode_inverted_index_key(key.table(), &tag.key, &tag.value);
                        if let Some(data) = self.storage.get(&key)? {
                            let mut id_list = decode_series_id_list(&data)?;
                            // TODO binary search by low 40 bits
                            id_list.retain(|x| *x != sid);
                            // if let Ok(index) = id_list.binary_search(&sid) {
                            //     id_list.remove(index);
                            // }

                            let data = encode_series_id_list(&id_list);
                            self.storage.set(&key, &data)?;
                        }
                    }

                    let keys: Vec<&SeriesKey> = keys.iter().filter(|k| k.id() != sid).collect();
                    self.storage
                        .set(stroage_key.as_bytes(), &bincode::serialize(&keys).unwrap())?;
                }
            }
        }

        Ok(())
    }

    pub fn get_series_ids_by_domains(
        &self,
        tab: &str,
        tag_domains: &HashMap<String, Domain>,
    ) -> IndexResult<Vec<u64>> {
        debug!("pushed tags: {:?}", tag_domains);

        let mut series_ids: Vec<Vec<u64>> = vec![vec![]; tag_domains.len()];

        for (idx, (tag_key, v)) in tag_domains.iter().enumerate() {
            series_ids[idx] = self.get_series_ids_by_domain(tab, tag_key, v)?;
        }

        debug!("filter scan all series_ids: {:?}", series_ids);

        let result = series_ids
            .into_iter()
            // The relationship between multiple tags is 'and', so use and
            .reduce(|p, c| utils::and_u64(&p, &c))
            .unwrap_or_default();

        Ok(result)
    }

    pub fn get_series_id_list(&self, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u64>> {
        let mut result: Vec<u64> = vec![];
        if tags.is_empty() {
            let mut it = self.storage.prefix(format!("{}.", tab).as_bytes());
            for kv in it.by_ref() {
                if let Ok(kv) = kv {
                    let id_list = decode_series_id_list(&kv.1)?;
                    result = utils::or_u64(&result, &id_list);
                } else {
                    return Err(IndexError::IndexStroage {
                        msg: format!("scan prefix: {}", tab),
                    });
                }
            }

            return Ok(result);
        }

        let key = encode_inverted_index_key(tab, &tags[0].key, &tags[0].value);
        if let Some(data) = self.storage.get(&key)? {
            result = decode_series_id_list(&data)?;
            if tags.len() == 1 {
                return Ok(result);
            }
        }

        for tag in &tags[1..] {
            let key = encode_inverted_index_key(tab, &tag.key, &tag.value);
            if let Some(data) = self.storage.get(&key)? {
                let id_list = decode_series_id_list(&data)?;
                result = utils::and_u64(&result, &id_list);
            } else {
                return Ok(vec![]);
            }
        }

        Ok(result)
    }

    pub fn get_series_ids_by_domain(
        &self,
        tab: &str,
        tag_key: &str,
        v: &Domain,
    ) -> IndexResult<Vec<u64>> {
        let mut series_ids: Vec<u64> = vec![];

        match v {
            Domain::Range(range_set) => {
                for (_, range) in range_set.low_indexed_ranges().into_iter() {
                    let key_range = filter_range_to_index_key_range(tab, tag_key, range);

                    // Search the sid list corresponding to qualified tags in the range
                    let iter = self.storage.range(key_range).collect::<Vec<_>>();

                    // Save all sids
                    for kv in iter {
                        let (idx_key, ori_sid_list) = kv?;
                        let sid_list = decode_series_id_list(&ori_sid_list)?;
                        series_ids = utils::or_u64(&series_ids, &sid_list);
                    }

                    debug!("range scan series_ids[{}]: {:?}", tag_key, series_ids);
                }
            }
            Domain::Equtable(val) => {
                if val.is_white_list() {
                    // Contains the given value
                    for entry in val.entries().into_iter() {
                        let index_key = tag_value_to_index_key(tab, tag_key, entry.value());

                        if let Some(data) = self.storage.get(&index_key)? {
                            let id_list = decode_series_id_list(&data)?;
                            series_ids = utils::or_u64(&series_ids, &id_list);
                        };
                    }
                } else {
                    // Does not contain a given value, that is, a value other than a given value
                    // TODO will not deal with this situation for the time being
                    series_ids = self.get_series_id_list(tab, &[])?;
                }
            }
            Domain::None => {
                // Normally, it will not go here unless no judgment is made at the ColumnDomains level
                // If you go here, you will directly return an empty series, because the tag condition in the map is' and '
                return Ok(vec![]);
            }
            Domain::All => {
                // Normally, it will not go here unless no judgment is made at the ColumnDomains level
                // The current tag is not filtered, all series are obtained, and the next tag is processed
                series_ids = self.get_series_id_list(tab, &[])?;
            }
        };

        Ok(series_ids)
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
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

#[cfg(test)]
mod test {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use models::schema::ExternalTableSchema;

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
            table_partition_cols: vec!["4".to_string()],
            has_header: true,
            delimiter: 5,
            schema,
        };

        let ans_inter = serde_json::to_string(&schema).unwrap();
        let ans = serde_json::from_str::<ExternalTableSchema>(&ans_inter).unwrap();

        assert_eq!(ans, schema);
    }
}
