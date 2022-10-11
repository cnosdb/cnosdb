use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
    error::{DataFusionError, Result},
};
use models::schema::{TableFiled, TableSchema, TIME_FIELD};
use parking_lot::RwLock;

use tskv::engine::EngineRef;

use crate::table::ClusterTable;
pub type CatalogRef = Arc<dyn CatalogProvider>;
pub type SchemaRef = Arc<dyn SchemaProvider>;
pub type UserCatalogRef = Arc<UserCatalog>;
pub type TableRef = Arc<dyn TableProvider>;

pub struct UserCatalog {
    engine: EngineRef,
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
        self.engine
            .drop_database(db_name)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut schema = self.schemas.write();
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

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        {
            let schemas = self.schemas.read();
            if let Some(v) = schemas.get(name) {
                return Some(v.clone());
            }
        }

        let mut schemas = self.schemas.write();
        let v = schemas
            .entry(name.to_owned())
            .or_insert_with(|| Arc::new(UserSchema::new(name.to_owned(), self.engine.clone())));

        Some(v.clone())
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut schemas = self.schemas.write();
        Ok(schemas.insert(name.into(), schema))
    }
}

pub struct UserSchema {
    db_name: String,
    engine: EngineRef,
    tables: RwLock<HashMap<String, TableRef>>,
}

impl UserSchema {
    pub fn new(db: String, engine: EngineRef) -> Self {
        Self {
            db_name: db,
            tables: RwLock::new(HashMap::new()),
            engine,
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
        {
            let tables = self.tables.read();
            if let Some(v) = tables.get(name) {
                return Some(v.clone());
            }
        }

        let mut tables = self.tables.write();
        if let Ok(Some(v)) = self.engine.get_table_schema(&self.db_name, name) {
            let mut fields = BTreeMap::new();
            let codec = match v.fields.get(TIME_FIELD) {
                None => 0,
                Some(v) => v.codec,
            };
            // system field (time)
            let time_field = TableFiled::time_field(codec);
            fields.insert(time_field.name.clone(), time_field);

            for item in v.fields {
                let field = item.1;
                fields.insert(field.name.clone(), field);
            }
            let schema = TableSchema::new(self.db_name.clone(), name.to_owned(), fields);
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
                self.engine.create_table(schema);
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
