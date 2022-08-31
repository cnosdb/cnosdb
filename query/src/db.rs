use datafusion::arrow::datatypes::DataType;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::TableReference;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::sql::planner::ContextProvider;
use futures::TryStreamExt;
use parking_lot::RwLock;
use tskv::engine::EngineRef;

use crate::meta::{LocalMeta, MetaRef};
use crate::{context::IsiphoSessionCtx, exec::Executor};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DatabaseRule {
    pub name: String,
}
impl DatabaseRule {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn db_name(&self) -> &str {
        self.name.as_str()
    }
}

#[allow(dead_code)]
pub struct Db {
    rule: RwLock<Arc<DatabaseRule>>,
    exec: Arc<Executor>,
    metadata: MetaRef,
}

impl Db {
    pub fn new(engine: EngineRef) -> Self {
        Self {
            rule: RwLock::new(Arc::new(DatabaseRule::new("default".to_string()))),
            exec: Arc::new(Executor::new(1)), //todo: add to config
            metadata: Arc::new(LocalMeta::new_with_default(engine)), //todo: add to config
        }
    }
    pub fn new_query_context(&self, tenant: String) -> IsiphoSessionCtx {
        let catalog = self.metadata.catalog();
        self.exec.new_execution_config().build(tenant, catalog)
    }
    fn metadata(&self) -> MetaRef {
        self.metadata.clone()
    }
    pub async fn run_query(&self, query: &str) -> Option<Vec<RecordBatch>> {
        let ctx = self.new_query_context("test".to_string());
        let task = ctx.inner().task_ctx();
        let frame = ctx.inner().sql(query).await.unwrap();
        let plan = frame.create_physical_plan().await.unwrap();
        let stream = self.exec.run(plan, task).unwrap().stream();
        let result: Vec<_> = stream.try_collect().await.ok()?;
        Some(result)
    }
}

impl ContextProvider for Db {
    fn get_table_provider(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        match self.metadata.get_table_provider(name) {
            Ok(table) => Ok(table),
            Err(_) => {
                let catalog_name = self.metadata.catalog_name();
                let schema_name = self.metadata.schema_name();
                let resolved_name = name.resolve(&catalog_name, &schema_name);
                Err(DataFusionError::Plan(format!(
                    "failed to resolve user:{}  db: {}, table: {}",
                    resolved_name.catalog, resolved_name.schema, resolved_name.table
                )))
            }
        }
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        todo!()
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        todo!()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        todo!()
    }
}
