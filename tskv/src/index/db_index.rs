use std::borrow::Borrow;
use std::collections::HashSet;
use std::mem::size_of;
use std::ops::Index;
use std::path::{self, Path, PathBuf};
use std::{collections::HashMap, sync::Arc};

use super::utils::{decode_series_id_list, encode_inverted_index_key, encode_series_id_list};
use super::*;

use bytes::BufMut;
use chrono::format::format;
use config::Config;
use models::{utils, FieldId, FieldInfo, SeriesId, SeriesInfo, SeriesKey, Tag, ValueType};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use trace::warn;

use super::{errors, IndexEngine, IndexError, IndexResult};

const SERIES_KEY_PREFIX: &str = "_series_key_";
const TABLE_SCHEMA_PREFIX: &str = "_table_schema_";

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
    indexs: HashMap<String, Arc<RwLock<DBIndex>>>,
}

impl DbIndexMgr {
    pub fn new(index_base_path: impl AsRef<Path>) -> Self {
        Self {
            base_path: index_base_path.as_ref().into(),
            indexs: HashMap::new(),
        }
    }

    pub fn get_db_index(&mut self, db: &String) -> Arc<RwLock<DBIndex>> {
        let index = self
            .indexs
            .entry(db.clone())
            .or_insert_with(|| Arc::new(RwLock::new(DBIndex::new(self.base_path.join(db)))));

        index.clone()
    }
}

#[derive(Debug)]
pub struct DBIndex {
    path: PathBuf,

    storage: IndexEngine,
    series_cache: HashMap<u32, Vec<SeriesKey>>,

    schema_id: HashMap<String, u32>,
    table_schema: HashMap<String, Vec<FieldInfo>>,
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
            series_cache: HashMap::new(),
            table_schema: HashMap::new(),
            schema_id: HashMap::new(),

            path: path.into(),
        }
    }

    pub fn get_from_cache(&self, info: &mut SeriesInfo) -> Option<u64> {
        let series_key = SeriesKey::from(info.borrow());
        let (hash_id, _) = utils::split_id(series_key.hash());
        let stroage_key = format!("{}{}", SERIES_KEY_PREFIX, hash_id);

        if let Some(keys) = self.series_cache.get(&hash_id) {
            if let Some(k) = keys.iter().find(|key| series_key.eq(key)) {
                let id = k.id();
                if let Ok(()) = self.chech_field_type_from_cache(id, info) {
                    return Some(id);
                }
            }
        }

        None
    }

    pub fn add_series_if_not_exists(&mut self, info: &mut SeriesInfo) -> errors::IndexResult<u64> {
        let mut series_key = SeriesKey::from(info.borrow());
        let (hash_id, _) = utils::split_id(series_key.hash());
        let stroage_key = format!("{}{}", SERIES_KEY_PREFIX, hash_id);

        // load index first from cache,or else from storage and than cache it!
        let mut keys: &mut Vec<SeriesKey> = &mut vec![];
        match self.series_cache.get_mut(&hash_id) {
            Some(v) => keys = v,
            None => {
                if let Some(data) = self.storage.get(stroage_key.as_bytes())? {
                    if let Ok(v) = bincode::deserialize(&data) {
                        self.series_cache.insert(hash_id, v);
                        keys = self.series_cache.get_mut(&hash_id).unwrap();
                    }
                }
            }
        }

        let mut found = false;
        let mut id = 0_u64;
        //if exist return series_id
        if let Some(k) = keys.iter().find(|key| series_key.eq(key)) {
            id = k.id();
            found = true;
        }

        if found {
            self.check_field_type_or_else_add(id, info)?;
            return Ok(id);
        }

        //if not exist add it!
        let id = utils::unite_id(hash_id as u64, self.storage.incr_id()?);
        series_key.set_id(id);
        for tag in series_key.tags() {
            let key = encode_inverted_index_key(series_key.table(), &tag.key, &tag.value);
            self.storage.push(&key, id.to_be_bytes().as_ref())?;
        }

        keys.push(series_key);
        self.storage
            .set(stroage_key.as_bytes(), &bincode::serialize(&keys).unwrap())?;

        self.check_field_type_or_else_add(id, info)?;

        Ok(id)
    }

    fn chech_field_type_from_cache(
        &self,
        series_id: u64,
        info: &mut SeriesInfo,
    ) -> IndexResult<()> {
        if let Some(schema) = self.table_schema.get(info.table()) {
            for field in info.field_infos() {
                if let Some(v) = schema.iter().find(|item| field.eq(item)) {
                    if field.value_type() == v.value_type() {
                        field.set_field_id(utils::unite_id(v.field_id(), series_id));
                    } else {
                        return Err(IndexError::FieldType);
                    }
                } else {
                    return Err(IndexError::NotFoundField);
                }
            }

            for it in schema.iter() {
                match info.field_infos().iter().find(|item| it.eq(item)) {
                    Some(v) => {}
                    None => {
                        if !it.is_tag() {
                            info.push_field_fill(it.clone())
                        }
                    }
                }
            }

            for tag in info.tags() {
                let mut field = FieldInfo::from(tag);
                if let Some(v) = schema.iter().find(|item| field.eq(item)) {
                    if field.value_type() == v.value_type() {
                        field.set_field_id(utils::unite_id(v.field_id(), series_id));
                    } else {
                        return Err(IndexError::FieldType);
                    }
                } else {
                    return Err(IndexError::NotFoundField);
                }
            }

            info.set_schema_id(self.table_schema_id(info.table()));

            Ok(())
        } else {
            Err(IndexError::NotFoundField)
        }
    }

    fn check_field_type_or_else_add(
        &mut self,
        series_id: u64,
        info: &mut SeriesInfo,
    ) -> IndexResult<()> {
        //load schema first from cache,or else from storage and than cache it!
        let mut schema: &mut Vec<FieldInfo> = &mut vec![];
        match self.table_schema.get_mut(info.table()) {
            Some(fields) => schema = fields,
            None => {
                let key = format!("{}{}", TABLE_SCHEMA_PREFIX, info.table());
                if let Some(data) = self.storage.get(key.as_bytes())? {
                    if let Ok(list) = bincode::deserialize(&data) {
                        self.table_schema.insert(info.table().to_string(), list);
                        schema = self.table_schema.get_mut(info.table()).unwrap();
                    }
                }
            }
        }

        let mut schema_change = false;
        let mut check_fn = |field: &mut FieldInfo| -> IndexResult<()> {
            match schema.iter().find(|item| field.eq(item)) {
                Some(v) => {
                    if field.value_type() != v.value_type() {
                        return Err(IndexError::FieldType);
                    }

                    field.set_field_id(utils::unite_id(v.field_id(), series_id));
                }
                None => {
                    schema_change = true;

                    let index = (schema.len() + 1) as u64;

                    let mut clone = field.clone();
                    clone.set_field_id(index);
                    schema.push(clone);

                    field.set_field_id(utils::unite_id(index, series_id));
                }
            }
            Ok(())
        };

        //check fields
        for field in info.field_infos() {
            check_fn(field)?
        }

        //check tags
        for tag in info.tags() {
            check_fn(&mut FieldInfo::from(tag))?
        }

        for it in schema.iter() {
            match info.field_infos().iter().find(|item| it.eq(item)) {
                Some(v) => {}
                None => {
                    if !it.is_tag() {
                        info.push_field_fill(it.clone())
                    }
                }
            }
        }

        //schema changed store it
        if schema_change {
            let data = bincode::serialize(schema).unwrap();
            let key = format!("{}{}", TABLE_SCHEMA_PREFIX, info.table());
            self.storage.set(key.as_bytes(), &data)?;

            info.set_schema_id(self.incr_schema_id(info.table()));
        } else {
            info.set_schema_id(self.table_schema_id(info.table()));
        }

        Ok(())
    }

    pub fn get_table_schema(&mut self, tab: &String) -> IndexResult<Option<Vec<FieldInfo>>> {
        if let Some(fields) = self.table_schema.get(tab) {
            return Ok(Some(fields.to_vec()));
        }

        let key = format!("{}{}", TABLE_SCHEMA_PREFIX, tab);
        if let Some(data) = self.storage.get(key.as_bytes())? {
            if let Ok(list) = bincode::deserialize::<Vec<FieldInfo>>(&data) {
                let clone = list.to_vec();
                self.table_schema.insert(tab.to_string(), list);
                return Ok(Some(clone));
            }
        }

        Ok(None)
    }

    pub fn get_table_schema_by_series_id(
        &mut self,
        series_id: SeriesId,
    ) -> IndexResult<Option<Vec<FieldInfo>>> {
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

    pub fn table_schema_id(&self, tab: &String) -> u32 {
        if let Some(v) = self.schema_id.get(tab) {
            return *v;
        }

        return 1;
    }

    pub fn incr_schema_id(&mut self, tab: &String) -> u32 {
        let v = self.schema_id.entry(tab.clone()).or_insert(1);

        *v = *v + 1;

        return *v;
    }

    pub async fn del_table_schema(&mut self, tab: &String) -> IndexResult<()> {
        self.table_schema.remove(tab);

        let key = format!("{}{}", TABLE_SCHEMA_PREFIX, tab);
        self.storage.delete(key.as_bytes())?;

        Ok(())
    }

    pub async fn flush(&mut self) -> IndexResult<()> {
        self.storage.flush();
        Ok(())
    }

    pub fn get_series_key(&mut self, sid: u64) -> IndexResult<Option<SeriesKey>> {
        let (hash_id, _) = utils::split_id(sid);
        let stroage_key = format!("{}{}", SERIES_KEY_PREFIX, hash_id);

        // load index first from cache,or else from storage and than cache it!
        let mut keys: &mut Vec<SeriesKey> = &mut vec![];
        if let Some(v) = self.series_cache.get_mut(&hash_id) {
            keys = v;
        } else if let Some(data) = self.storage.get(stroage_key.as_bytes())? {
            if let Ok(v) = bincode::deserialize(&data) {
                self.series_cache.insert(hash_id, v);
                keys = self.series_cache.get_mut(&hash_id).unwrap();
            }
        }

        if let Some(k) = keys.iter().find(|key| key.id() == sid) {
            return Ok(Some(k.clone()));
        }

        Ok(None)
    }

    pub async fn del_series_info(&mut self, sid: u64) -> IndexResult<()> {
        let (hash_id, _) = utils::split_id(sid);
        self.series_cache.remove(&hash_id);

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

    pub async fn get_series_id_list(&self, tab: &String, tags: &Vec<Tag>) -> IndexResult<Vec<u64>> {
        let mut result: Vec<u64> = vec![];
        if tags.is_empty() {
            let mut it = self.storage.prefix(format!("{}.", tab).as_bytes());
            loop {
                if let Some(kv) = it.next() {
                    if let Ok(kv) = kv {
                        let id_list = decode_series_id_list(&kv.1)?;
                        result = utils::or_u64(&result, &id_list);
                    } else {
                        return Err(IndexError::IndexStroage {
                            msg: format!("scan prefix: {}", tab),
                        });
                    }
                } else {
                    break;
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
}
