use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
    error::{DataFusionError, Result},
};
use parking_lot::RwLock;

pub struct IsiphoCatalog {
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl IsiphoCatalog {
    pub fn new() -> Self {
        Self { schemas: RwLock::new(HashMap::new()) }
    }
}

impl CatalogProvider for IsiphoCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read();
        schemas.get(name).cloned()
    }

    fn register_schema(&self,
                       name: &str,
                       schema: Arc<dyn SchemaProvider>)
                       -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut schemas = self.schemas.write();
        Ok(schemas.insert(name.into(), schema))
    }
}

pub struct IsiphoSchema {
    tables: RwLock<HashMap<String, Arc<dyn TableProvider>>>,
}

impl IsiphoSchema {
    pub fn new() -> Self {
        Self { tables: RwLock::new(HashMap::new()) }
    }
}

impl Default for IsiphoSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaProvider for IsiphoSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let tables = self.tables.read();
        tables.keys().cloned().collect()
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        println!("this is SchemaProvider::table {}", name);
        let tables = self.tables.read();
        tables.get(name).cloned()
    }

    fn register_table(&self,
                      name: String,
                      table: Arc<dyn TableProvider>)
                      -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!("The table {} already exists", name)));
        }
        let mut tables = self.tables.write();
        Ok(tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let mut tables = self.tables.write();
        Ok(tables.remove(name))
    }

    fn table_exist(&self, name: &str) -> bool {
        let tables = self.tables.read();
        tables.contains_key(name)
    }
}
