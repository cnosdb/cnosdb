use std::{collections::HashMap, sync::Arc};

use models::schema::{DatabaseSchema, TableSchema};
use parking_lot::RwLock;
use spi::catalog::MetadataError;
use spi::catalog::Result;

use tskv::engine::EngineRef;

pub type UserCatalogRef = Arc<UserCatalog>;

pub struct UserCatalog {
    engine: EngineRef,
    /// DBName -> DB
    schemas: RwLock<HashMap<String, Arc<Database>>>,
}

impl UserCatalog {
    pub fn new(engine: EngineRef) -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            engine,
        }
    }

    pub fn deregister_schema(&self, db_name: &str) -> Result<()> {
        let mut schema = self.schemas.write();
        match schema.get(db_name) {
            None => {
                return Err(MetadataError::DatabaseNotExists {
                    database_name: db_name.to_string(),
                })
            }
            Some(db) => {
                let tables = db.table_names();
                for i in tables {
                    db.deregister_table(&i)?;
                }
            }
        }
        self.engine
            .drop_database(db_name)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })?;
        schema.remove(db_name);
        Ok(())
    }

    pub fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    // get db_schema
    pub fn schema(&self, name: &str) -> Option<Arc<Database>> {
        let schemas = self.schemas.read();
        return if let Some(v) = schemas.get(name) {
            Some(v.clone())
        } else {
            drop(schemas);
            match self.engine.get_db_schema(name) {
                None => return None,
                Some(schema) => {
                    let mut schemas = self.schemas.write();
                    schemas.insert(
                        name.to_string(),
                        Arc::new(Database::new(name.to_string(), self.engine.clone(), schema)),
                    );
                    let v = schemas.get(name).unwrap();
                    return Some(v.clone());
                }
            }
        };
    }

    pub fn register_schema(
        &self,
        name: &str,
        schema: Arc<Database>,
    ) -> Result<Option<Arc<Database>>> {
        let mut schemas = self.schemas.write();
        self.engine
            .create_database(&schema.database_schema)
            .map_err(|_| MetadataError::DatabaseAlreadyExists {
                database_name: schema.database_schema.name.clone(),
            })?;

        Ok(schemas.insert(name.into(), schema))
    }
}

pub struct Database {
    db_name: String,
    engine: EngineRef,
    // table_name -> TableRef
    tables: RwLock<HashMap<String, TableSchema>>,
    database_schema: DatabaseSchema,
}

impl Database {
    pub fn new(db: String, engine: EngineRef, database_schema: DatabaseSchema) -> Self {
        Self {
            db_name: db,
            tables: RwLock::new(HashMap::new()),
            engine,
            database_schema,
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read();
        tables.keys().cloned().collect()
    }

    pub fn table(&self, name: &str) -> Option<TableSchema> {
        // table schema may be changed after write, so get from storage engine directly
        // {
        //     let tables = self.tables.read();
        //     if let Some(v) = tables.get(name) {
        //         return Some(v.clone());
        //     }
        // }

        let mut tables = self.tables.write();
        if let Ok(Some(schema)) = self.engine.get_table_schema(&self.db_name, name) {
            tables.insert(name.to_owned(), schema.clone());
            return Some(schema);
        }

        // get external table
        if let Some(v) = tables.get(name) {
            return Some(v.clone());
        }

        None
    }

    pub fn register_table(&self, name: String, table: TableSchema) -> Result<Option<TableSchema>> {
        if self.table_exist(name.as_str()) {
            return Err(MetadataError::TableAlreadyExists { table_name: name });
        }
        self.engine
            .create_table(&table)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })?;
        let mut tables = self.tables.write();
        Ok(tables.insert(name, table))
    }

    pub fn deregister_table(&self, name: &str) -> Result<Option<TableSchema>> {
        let mut tables = self.tables.write();

        self.engine
            .drop_table(&self.db_name, name)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })?;

        Ok(tables.remove(name))
    }

    pub fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read();
        tables.contains_key(name)
    }
}
