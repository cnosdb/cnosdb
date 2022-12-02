use std::{collections::HashMap, sync::Arc};

use coordinator::service::CoordinatorRef;

use models::schema::{DatabaseSchema, TableColumn, TableSchema};
use parking_lot::RwLock;

use spi::catalog::MetadataError;
use spi::catalog::Result;

use tskv::engine::EngineRef;

pub type UserCatalogRef = Arc<UserCatalog>;

pub struct UserCatalog {
    engine: EngineRef,
    coord: CoordinatorRef,
}

impl UserCatalog {
    pub fn new(engine: EngineRef, coord: CoordinatorRef) -> Self {
        Self { engine, coord }
    }

    pub fn deregister_schema(&self, tenant: &str, db_name: &str) -> Result<()> {
        let tables =
            self.engine
                .list_tables(tenant, db_name)
                .map_err(|e| MetadataError::External {
                    message: format!("{}", e),
                })?;
        for table in tables {
            self.engine
                .drop_table(tenant, db_name, &table)
                .map_err(|e| MetadataError::External {
                    message: format!("{}", e),
                })?;
        }
        self.engine
            .drop_database(tenant, db_name)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })?;
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
    pub fn schema(&self, tenant: &str, database: &str) -> Option<Arc<Database>> {
        self.engine.get_db_schema(tenant, database).map(|schema| {
            Arc::new(Database::new(
                database.to_string(),
                self.engine.clone(),
                self.coord.clone(),
                schema,
            ))
        })
    }

    pub fn register_schema(
        &self,
        tenant: &str,
        _database: &str,
        schema: Arc<Database>,
    ) -> Result<()> {
        let meta_client = self
            .coord
            .tenant_meta(tenant)
            .ok_or(MetadataError::InternalError {
                error_msg: format!("can't found tenant {}", tenant),
            })?;

        meta_client
            .create_db(&schema.database_schema)
            .map_err(|err| MetadataError::InternalError {
                error_msg: err.to_string(),
            })?;

        self.engine
            .create_database(&schema.database_schema)
            .map_err(|_| MetadataError::DatabaseAlreadyExists {
                database_name: schema.database_schema.database_name().to_string(),
            })?;

        Ok(())
    }
}

pub struct Database {
    db_name: String,
    engine: EngineRef,
    _coord: CoordinatorRef,
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
            _coord: coord,
            database_schema,
        }
    }

    pub fn table_names(&self) -> Result<Vec<String>> {
        self.engine
            .list_tables(self.database_schema.tenant_name(), &self.db_name)
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
        if let Ok(Some(schema)) =
            self.engine
                .get_table_schema(self.database_schema.tenant_name(), &self.db_name, name)
        {
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
            .drop_table(self.database_schema.tenant_name(), &self.db_name, name)
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })?;

        Ok(res)
    }

    pub fn table_add_column(&self, table: &str, column: TableColumn) -> Result<()> {
        let _lock = self.tables.write();
        self.engine
            .add_table_column(
                self.database_schema.tenant_name(),
                &self.db_name,
                table,
                column,
            )
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })
    }

    pub fn table_drop_column(&self, table: &str, column: &str) -> Result<()> {
        let _lock = self.tables.write();
        self.engine
            .drop_table_column(
                self.database_schema.tenant_name(),
                &self.db_name,
                table,
                column,
            )
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
            .change_table_column(
                self.database_schema.tenant_name(),
                &self.db_name,
                table,
                column,
                new_column,
            )
            .map_err(|e| MetadataError::External {
                message: format!("{}", e),
            })
    }
}
