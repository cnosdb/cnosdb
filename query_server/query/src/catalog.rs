use std::{collections::HashMap, sync::Arc};

use coordinator::service::CoordinatorRef;

use models::{
    meta_data::DatabaseInfo,
    schema::{DatabaseSchema, TableColumn, TableSchema},
};
use parking_lot::RwLock;

use spi::catalog::Result;
use spi::catalog::{MetadataError, DEFAULT_CATALOG};

use tskv::engine::EngineRef;

pub type UserCatalogRef = Arc<UserCatalog>;

pub struct UserCatalog {
    engine: EngineRef,
    coord: CoordinatorRef,
    /// DBName -> DB
    schemas: RwLock<HashMap<String, Arc<Database>>>,
}

impl UserCatalog {
    pub fn new(engine: EngineRef, coord: CoordinatorRef) -> Self {
        Self {
            schemas: RwLock::new(HashMap::new()),
            engine,
            coord,
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
                let tables = db.table_names()?;
                for table in tables {
                    db.deregister_table(&table)?;
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

    pub fn schema_names(&self) -> Result<Vec<String>> {
        self.engine
            .list_databases()
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })
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
                        Arc::new(Database::new(
                            name.to_string(),
                            self.engine.clone(),
                            self.coord.clone(),
                            schema,
                        )),
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

        let info = DatabaseInfo {
            name: name.to_string(),
            shard: schema.database_schema.config.shard_num_or_default() as u32,
            ttl: schema.database_schema.config.ttl_or_default().time_stamp(),
            vnode_duration: schema
                .database_schema
                .config
                .vnode_duration_or_default()
                .time_stamp(),
            replications: schema.database_schema.config.replica_or_default() as u32,
            buckets: vec![],

            tables: HashMap::new(),
        };

        let tenant = &DEFAULT_CATALOG.to_string();
        let meta_client = self
            .coord
            .tenant_meta(&tenant)
            .ok_or(MetadataError::InternalError {
                error_msg: format!("can't found tenant {}", tenant),
            })?;

        meta_client
            .create_db(&info)
            .map_err(|err| MetadataError::InternalError {
                error_msg: err.to_string(),
            })?;

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
    coord: CoordinatorRef,
    // table_name -> TableRef
    tables: RwLock<HashMap<String, TableSchema>>,
    database_schema: DatabaseSchema,
}

impl Database {
    pub fn new(
        db: String,
        engine: EngineRef,
        coord: CoordinatorRef,
        database_schema: DatabaseSchema,
    ) -> Self {
        Self {
            db_name: db,
            tables: RwLock::new(HashMap::new()),
            engine,
            coord,
            database_schema,
        }
    }

    pub fn table_names(&self) -> Result<Vec<String>> {
        self.engine
            .list_tables(&self.db_name)
            .map_err(|_| MetadataError::DatabaseNotExists {
                database_name: self.db_name.clone(),
            })
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

        None
    }

    pub fn register_table(&self, name: String, table: TableSchema) -> Result<Option<TableSchema>> {
        let mut tables = self.tables.write();
        if tables.contains_key(name.as_str()) {
            return Err(MetadataError::TableAlreadyExists { table_name: name });
        }

        self.engine
            .create_table(&table)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })?;
        Ok(tables.insert(name, table))
    }

    pub fn deregister_table(&self, name: &str) -> Result<Option<TableSchema>> {
        let mut tables = self.tables.write();

        let res = tables.remove(name);

        self.engine
            .drop_table(&self.db_name, name)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })?;

        Ok(res)
    }

    pub fn table_add_column(&self, table: &str, column: TableColumn) -> Result<()> {
        let _lock = self.tables.write();
        self.engine
            .add_table_column(&self.db_name, table, column)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })
    }

    pub fn table_drop_column(&self, table: &str, column: &str) -> Result<()> {
        let _lock = self.tables.write();
        self.engine
            .drop_table_column(&self.db_name, table, column)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })
    }

    pub fn table_alter_column(
        &self,
        table: &str,
        column: &str,
        new_column: TableColumn,
    ) -> Result<()> {
        let _lock = self.tables.write();
        self.engine
            .change_table_column(&self.db_name, table, column, new_column)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })
    }
}
