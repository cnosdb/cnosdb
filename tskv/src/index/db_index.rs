use std::borrow::Borrow;
use std::collections::HashSet;
use std::mem::size_of;
use std::ops::Index;
use std::path::{self, Path, PathBuf};
use std::string::FromUtf8Error;
use std::{collections::HashMap, sync::Arc};

use bytes::BufMut;
use chrono::format::format;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use snafu::ResultExt;
use tracing::info;

use crate::Error::IndexErr;
use config::Config;
use datafusion::arrow::datatypes::ToByteSlice;
use models::schema::{ColumnType, TableFiled, TableSchema};
use models::{utils, FieldId, FieldInfo, SeriesId, SeriesKey, Tag, ValueType};
use protos::models::Point;
use trace::warn;

use super::utils::{decode_series_id_list, encode_inverted_index_key, encode_series_id_list};
use super::*;
use super::{errors, IndexEngine, IndexError, IndexResult};

const SERIES_KEY_PREFIX: &str = "_series_key_";
const TABLE_SCHEMA_PREFIX: &str = "_table_schema_";
const TIME_STAMP_NAME: &str = "time";

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
    indexs: HashMap<String, Arc<DBIndex>>,
}

impl DbIndexMgr {
    pub fn new(index_base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: index_base_path.as_ref().into(),
            indexs: HashMap::new(),
        }
    }

    pub fn get_db_index(&mut self, db: &String) -> Arc<DBIndex> {
        let index = self
            .indexs
            .entry(db.clone())
            .or_insert_with(|| Arc::new(DBIndex::new(self.base_path.join(db))));

        index.clone()
    }
}

#[derive(Debug)]
pub struct DBIndex {
    path: PathBuf,
    storage: IndexEngine,
    series_cache: RwLock<HashMap<u32, Vec<SeriesKey>>>,
    table_schema: RwLock<HashMap<String, TableSchema>>,
}

impl From<&str> for DBIndex {
    fn from(path: &str) -> Self {
        DBIndex::new(path)
    }
}

impl DBIndex {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        Self {
            storage: IndexEngine::new(path),
            series_cache: RwLock::new(HashMap::new()),
            table_schema: RwLock::new(HashMap::new()),
            path: path.into(),
        }
    }

    pub fn get_sid_from_cache(&self, info: &Point) -> IndexResult<Option<u64>> {
        let series_key = SeriesKey::from_flatbuffer(info).map_err(|e| IndexError::FieldType)?;
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
        let mut series_key = SeriesKey::from_flatbuffer(info).map_err(|e| IndexError::FieldType)?;
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

    pub fn check_field_type_from_cache(&self, series_id: u64, info: &Point) -> IndexResult<()> {
        let table_name = unsafe { String::from_utf8_unchecked(info.table().unwrap().to_vec()) };
        if let Some(schema) = self.table_schema.read().get(&table_name) {
            for field in info.fields().unwrap() {
                let field_name = String::from_utf8(field.name().unwrap().to_vec()).unwrap();
                if let Some(v) = schema.fields.get(&field_name) {
                    if field.type_().0 != v.column_type.field_type() as i32 {
                        return Err(IndexError::FieldType);
                    }
                } else {
                    return Err(IndexError::NotFoundField);
                }
            }
            for tag in info.tags().unwrap() {
                let tag_name: String = String::from_utf8(tag.key().unwrap().to_vec()).unwrap();
                if let Some(v) = schema.fields.get(&tag_name) {
                    if ColumnType::Tag != v.column_type {
                        return Err(IndexError::FieldType);
                    }
                } else {
                    return Err(IndexError::NotFoundField);
                }
            }
            // info.set_schema_id(self.table_schema_id(info.table()));
            Ok(())
        } else {
            Err(IndexError::NotFoundField)
        }
    }

    pub fn check_field_type_or_else_add(&self, series_id: u64, info: &Point) -> IndexResult<()> {
        //load schema first from cache,or else from storage and than cache it!
        let mut schema = &mut TableSchema::default();
        let table_name = unsafe { String::from_utf8_unchecked(info.table().unwrap().to_vec()) };
        let mut fields = self.table_schema.write();
        let mut new_schema = false;
        match fields.get_mut(&table_name) {
            Some(fields) => schema = fields,
            None => {
                new_schema = true;
                schema.name = table_name.clone();
                let key = format!("{}{}", TABLE_SCHEMA_PREFIX, table_name);
                if let Some(data) = self.storage.get(key.as_bytes())? {
                    if let Ok(list) = bincode::deserialize(&data) {
                        fields.insert(table_name.clone(), list);
                        schema = fields.get_mut(&table_name).unwrap();
                    }
                }
            }
        }

        let mut schema_change = false;
        let mut check_fn = |field: &mut TableFiled| -> IndexResult<()> {
            let codec = match schema.fields.get(&field.name) {
                None => 0,
                Some(v) => v.codec,
            };
            field.codec = codec;

            match schema.fields.get(&field.name) {
                Some(v) => {
                    if field.column_type != v.column_type {
                        return Err(IndexError::FieldType);
                    }
                }
                None => {
                    schema_change = true;
                    field.id = (schema.fields.len() + 1) as u64;
                    schema.fields.insert(field.name.clone(), field.clone());
                }
            }
            Ok(())
        };
        //check timestamp
        check_fn(&mut TableFiled::new_with_default(
            TIME_STAMP_NAME.to_string(),
            ColumnType::Time,
        ))?;

        //check tags
        for tag in info.tags().unwrap() {
            let tag_key = unsafe { String::from_utf8_unchecked(tag.key().unwrap().to_vec()) };
            check_fn(&mut TableFiled::new_with_default(tag_key, ColumnType::Tag))?
        }

        //check fields
        for field in info.fields().unwrap() {
            let field_name = unsafe { String::from_utf8_unchecked(field.name().unwrap().to_vec()) };
            check_fn(&mut TableFiled::new_with_default(
                field_name,
                ColumnType::from_i32(field.type_().0),
            ))?
        }
        //schema changed store it
        if new_schema {
            schema.schema_id = 0;
        } else if schema_change {
            schema.schema_id += 1;
        }
        let data = bincode::serialize(schema).unwrap();
        let key = format!("{}{}", TABLE_SCHEMA_PREFIX, &table_name);
        self.storage.set(key.as_bytes(), &data)?;
        Ok(())
    }

    pub fn get_table_schema(&self, tab: &str) -> IndexResult<Option<TableSchema>> {
        if let Some(fields) = self.table_schema.read().get(tab) {
            return Ok(Some(fields.clone()));
        }

        let key = format!("{}{}", TABLE_SCHEMA_PREFIX, tab);
        if let Some(data) = self.storage.get(key.as_bytes())? {
            if let Ok(list) = bincode::deserialize::<TableSchema>(&data) {
                //todo: remove copy
                self.table_schema
                    .write()
                    .insert(tab.to_string(), list.clone());
                return Ok(Some(list));
            }
        }

        Ok(None)
    }

    pub fn get_table_schema_by_series_id(
        &self,
        series_id: SeriesId,
    ) -> IndexResult<Option<TableSchema>> {
        match self.get_series_key(series_id) {
            Ok(Some(key)) => match self.get_table_schema(key.table()) {
                Ok(None) => {
                    warn!(
                        "Schema for table ('{}') not found, get empty schema",
                        key.table()
                    );
                    Ok(None)
                }
                other => other,
            },
            Ok(None) => {
                warn!(
                    "Table for series id('{}') not found, get empty schema",
                    series_id
                );
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    pub fn del_table_schema(&self, tab: &String) -> IndexResult<()> {
        self.table_schema.write().remove(tab);

        let key = format!("{}{}", TABLE_SCHEMA_PREFIX, tab);
        self.storage.delete(key.as_bytes())?;

        Ok(())
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

    pub fn get_series_id_list(&self, tab: &str, tags: &[Tag]) -> IndexResult<Vec<u64>> {
        let mut result: Vec<u64> = vec![];
        if tags.is_empty() {
            info!("{:?}", format!("{}.", tab).as_bytes());
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

    pub fn create_table(&self, schema: &TableSchema) -> IndexResult<()> {
        let data = bincode::serialize(schema).unwrap();
        let key = format!("{}{}", TABLE_SCHEMA_PREFIX, schema.name);
        self.table_schema
            .write()
            .insert(schema.name.clone(), schema.clone());
        self.storage.set(key.as_bytes(), &data)?;
        self.flush()?;
        Ok(())
    }
}
