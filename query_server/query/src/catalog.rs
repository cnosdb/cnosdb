use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
    error::{DataFusionError, Result},
};
use models::schema::{DatabaseSchema, TableSchema};
use parking_lot::RwLock;
use spi::catalog::TableRef;

use tskv::engine::EngineRef;

use crate::table::ClusterTable;
pub type UserCatalogRef = Arc<UserCatalog>;

pub struct UserCatalog {
    engine: EngineRef,
    /// DBName -> DB
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
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
                return Err(DataFusionError::Execution(
                    "database not exists".to_string(),
                ))
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
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        schema.remove(db_name);
        Ok(())
    }
}

impl CatalogProvider for UserCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    // get db_schema
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
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
                        Arc::new(UserSchema::new(
                            name.to_string(),
                            self.engine.clone(),
                            schema,
                        )),
                    );
                    let v = schemas.get(name).unwrap();
                    return Some(v.clone());
                }
            }
        };
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut schemas = self.schemas.write();
        let schema_opt = schema.as_any().downcast_ref::<UserSchema>();
        let user_schema = match schema_opt {
            None => {
                return Err(DataFusionError::Execution(
                    "failed to register schema".to_string(),
                ))
            }
            Some(v) => v,
        };
        self.engine
            .create_database(&user_schema.database_schema)
            .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;

        Ok(schemas.insert(name.into(), schema))
    }
}

pub struct UserSchema {
    db_name: String,
    engine: EngineRef,
    tables: RwLock<HashMap<String, TableRef>>,
    database_schema: DatabaseSchema,
}

impl UserSchema {
    pub fn new(db: String, engine: EngineRef, database_schema: DatabaseSchema) -> Self {
        Self {
            db_name: db,
            tables: RwLock::new(HashMap::new()),
            engine,
            database_schema,
        }
    }
}

impl SchemaProvider for UserSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read();
        tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        // table schema may be changed after write, so get from storage engine directly
        // {
        //     let tables = self.tables.read();
        //     if let Some(v) = tables.get(name) {
        //         return Some(v.clone());
        //     }
        // }

        let mut tables = self.tables.write();
        if let Ok(Some(schema)) = self.engine.get_table_schema(&self.db_name, name) {
            let table = Arc::new(ClusterTable::new(self.engine.clone(), schema));
            tables.insert(name.to_owned(), table.clone());
            return Some(table);
        }

        None
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "The table {} already exists",
                name
            )));
        }
        let mut tables = self.tables.write();
        let table_schema = table.as_any().downcast_ref::<TableSchema>();
        let cluster_table = match table_schema {
            None => table,
            Some(schema) => {
                self.engine
                    .create_table(schema)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let cluster_table = ClusterTable::new(self.engine.clone(), schema.clone());
                Arc::new(cluster_table)
            }
        };
        Ok(tables.insert(name, cluster_table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write();

        self.engine
            .drop_table(&self.db_name, name)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(tables.remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read();
        tables.contains_key(name)
    }
}
