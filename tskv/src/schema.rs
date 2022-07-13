//! Schemas:
//! - Database #1
//!     - Table #1
//!         - Column #1
//!         - Column #2
//!     - Table #2
//!         - Column #3
//!         - Column #4

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
};

use crossbeam::channel::{self, Receiver};
use futures::lock::{Mutex, MutexGuard};
use protos::{
    models as fb_models,
    schema_service::{Column, Database, Table},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use snafu::ResultExt;

use crate::{
    direct_io::File,
    error::{self, Error, Result},
    file_manager, file_utils, kv_option,
};

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct DatabaseSchema {
    pub id: u64,
    pub name: String,
    pub tables: BTreeMap<String, TableSchema>,
}

impl DatabaseSchema {
    pub fn new(id: u64, name: String) -> Self {
        Self { id, name, tables: BTreeMap::new() }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct TableSchema {
    pub id: u64,
    pub name: String,
    pub columns: BTreeMap<String, ColumnSchema>,
}

impl TableSchema {
    pub fn new(id: u64, name: String) -> Self {
        Self { id, name, columns: BTreeMap::new() }
    }

    pub fn add_column(&mut self, col: &Column) {
        self.columns
            .insert(col.name.clone(), ColumnSchema::try_from(col).expect("column is invalid"));
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ColumnSchema {
    pub id: u64,
    pub name: String,
    pub column_type: ColumnType,
}

impl ColumnSchema {
    pub fn new(id: u64, name: String, column_type: ColumnType) -> Self {
        Self { id, name, column_type }
    }
}

impl TryFrom<&Column> for ColumnSchema {
    type Error = Box<dyn std::error::Error>;

    fn try_from(c: &Column) -> Result<Self, Self::Error> {
        Ok(Self { id: c.id,
                  name: c.name.clone(),
                  column_type: ColumnType::try_from(c.column_type)? })
    }
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, Eq, PartialEq)]
pub enum ColumnType {
    I64    = 1,
    U64    = 2,
    F64    = 3,
    Bool   = 4,
    String = 5,
    Time   = 6,
    Tag    = 7,
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::I64 => "i64",
            Self::U64 => "u64",
            Self::F64 => "f64",
            Self::Bool => "bool",
            Self::String => "string",
            Self::Time => "time",
            Self::Tag => "tag",
        }
    }
}

impl TryFrom<u32> for ColumnType {
    type Error = Box<dyn std::error::Error>;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::I64 as u32 => Ok(Self::I64),
            x if x == Self::U64 as u32 => Ok(Self::U64),
            x if x == Self::F64 as u32 => Ok(Self::F64),
            x if x == Self::Bool as u32 => Ok(Self::Bool),
            x if x == Self::String as u32 => Ok(Self::String),
            x if x == Self::Time as u32 => Ok(Self::Time),
            x if x == Self::Tag as u32 => Ok(Self::Tag),
            _ => Err("invalid column value".into()),
        }
    }
}

impl std::fmt::Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.as_str();

        write!(f, "{}", s)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Data {
    schema: BTreeMap<String, DatabaseSchema>,

    next_datbase_id: AtomicU64,
    next_table_id: AtomicU64,
    next_column_id: AtomicU64,
    index: AtomicU64,
}

impl Data {
    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).context(error::EncodeSnafu)
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        bincode::deserialize::<Data>(data).context(error::DecodeSnafu)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        // Get file and check if new file
        let mut new_file = false;
        let file = if file_manager::try_exists(path) {
            let f = file_manager::get_file_manager().open_file(path)?;
            if f.len() == 0 {
                new_file = true;
            }
            f
        } else {
            new_file = true;
            file_manager::get_file_manager().create_file(path)?
        };
        if !new_file {
            let mut buf = vec![0_u8; file.len() as usize];
            file.read_at(0, &mut buf[..]).context(error::ReadFileSnafu)?;

            Self::decode(&buf)
        } else {
            let data = Self::default();
            file.write_at(0, &data.encode()?).context(error::WriteFileSnafu)?;
            Ok(data)
        }
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        // TODO: check file using
        let file = file_manager::get_file_manager().create_file(path)?;
        file.write_at(0, &self.encode()?).context(error::WriteFileSnafu)?;
        Ok(())
    }

    pub fn database(&self, database: String) -> Option<DatabaseSchema> {
        None
    }

    /// Creates a new database.
    pub fn create_database(&mut self, database: String) -> Result<()> {
        match self.schema.get(&database) {
            Some(_) => Ok(()),
            None => {
                self.schema.insert(database.clone(),
                                   DatabaseSchema { id: self.next_datbase_id
                                                            .fetch_add(1, Ordering::SeqCst),
                                                    name: database.clone(),
                                                    tables: BTreeMap::new() });

                Ok(())
            },
        }
    }

    /// Removes a database by name.
    pub fn drop_database(&mut self, database: String) -> Result<()> {
        self.schema.remove(&database);
        Ok(())
    }

    pub fn create_table(&mut self, database: String, table: TableSchema) -> Result<()> {
        self.schema
            .get_mut(&database)
            .ok_or_else(|| Error::DatabaseNotFound { database: database.clone() })
            .map(|db| {
                db.tables.insert(table.name.clone(), table);
            })
    }

    pub fn drop_table(&mut self, database: String, table: String) -> Result<()> {
        self.schema
            .get_mut(&database)
            .ok_or_else(|| Error::DatabaseNotFound { database: database.clone() })
            .map(|db| {
                db.tables.remove(&table);
            })
    }
}

impl Default for Data {
    fn default() -> Self {
        Self { schema: Default::default(),
               next_datbase_id: 1.into(),
               next_table_id: 1.into(),
               next_column_id: 1.into(),
               index: 1.into() }
    }
}

/// Memory storage for schemas.
pub struct SchemaStore {
    config: Arc<kv_option::SchemaStoreConfig>,
    data: Arc<Mutex<Data>>,
    changed: Receiver<()>,
}

impl SchemaStore {
    pub fn new(config: kv_option::SchemaStoreConfig) -> Self {
        let config = Arc::new(config);
        let (last, seq) =
            match file_utils::get_max_sequence_file_name(PathBuf::from(config.dir.clone()),
                                                         file_utils::get_schema_file_id)
            {
                Some((file, seq)) => (file, seq),
                None => {
                    let seq = 1;
                    (file_utils::make_schema_file(&config.dir.clone(), seq), seq)
                },
            };

        let current_dir_path = PathBuf::from(config.dir.clone());
        if !file_manager::try_exists(&current_dir_path) {
            std::fs::create_dir_all(&current_dir_path).unwrap();
        }
        let current_file_path = current_dir_path.join(last);
        let data = Data::load(current_file_path).unwrap();
        let data = Arc::new(Mutex::new(data));

        let (tx, rx) = channel::bounded(1);

        Self { config, data, changed: rx }
    }

    pub async fn get_database(&self, database: &String) -> Option<DatabaseSchema> {
        let data = self.data.lock().await;
        data.schema.get(database).cloned()
    }
}

#[cfg(test)]
mod test {
    use super::SchemaStore;
    use crate::kv_option::SchemaStoreConfig;
    use serial_test::serial;

    const DIR: &str = "/tmp/test/meta";

    #[tokio::test]
    #[serial]
    async fn test() {
        let config = SchemaStoreConfig { dir: DIR.to_string() };
        let store = SchemaStore::new(config);
    }
}
