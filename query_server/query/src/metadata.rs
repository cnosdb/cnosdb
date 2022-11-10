use std::any::Any;

use crate::catalog::{Database, UserCatalog, UserCatalogRef};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::{
    error::DataFusionError,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource},
    sql::{planner::ContextProvider, TableReference},
};

use models::schema::TableSchema;
use spi::query::execution::Output;

use datafusion::arrow::record_batch::RecordBatch;

use crate::table::ClusterTable;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::provider_as_source;
use models::schema::DatabaseSchema;
use spi::catalog::{
    MetaData, MetaDataRef, MetadataError, Result, DEFAULT_CATALOG, DEFAULT_DATABASE,
};
use spi::query::function::FuncMetaManagerRef;
use std::sync::Arc;
use tskv::engine::EngineRef;

/// remote meta
pub struct RemoteCatalogMeta {}

/// local meta
#[derive(Clone)]
pub struct LocalCatalogMeta {
    catalog_name: String,
    database_name: String,
    engine: EngineRef,
    catalog: UserCatalogRef,
    func_manager: FuncMetaManagerRef,
}

impl LocalCatalogMeta {
    pub fn new_with_default(engine: EngineRef, func_manager: FuncMetaManagerRef) -> Result<Self> {
        let meta = Self {
            catalog_name: DEFAULT_CATALOG.to_string(),
            database_name: DEFAULT_DATABASE.to_string(),
            engine: engine.clone(),
            catalog: Arc::new(UserCatalog::new(engine)),
            func_manager,
        };
        if let Err(e) = meta.create_database(
            &meta.database_name,
            DatabaseSchema::new(&meta.database_name),
        ) {
            match e {
                MetadataError::DatabaseAlreadyExists { .. } => {}
                _ => return Err(e),
            }
        };
        Ok(meta)
    }
}

impl MetaData for LocalCatalogMeta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_catalog(&self, catalog_name: &str) -> Arc<dyn MetaData + Send + Sync> {
        let mut metadata = self.clone();
        metadata.catalog_name = catalog_name.to_string();

        Arc::new(metadata)
    }

    fn with_database(&self, database: &str) -> Arc<dyn MetaData + Send + Sync> {
        let mut metadata = self.clone();
        metadata.database_name = database.to_string();

        Arc::new(metadata)
    }

    //todo: local mode dont support multi-tenant

    fn catalog_name(&self) -> String {
        self.catalog_name.clone()
    }

    fn schema_name(&self) -> String {
        self.database_name.clone()
    }

    fn table(&self, table: TableReference) -> Result<TableSchema> {
        let catalog_name = self.catalog_name();
        let schema_name = self.schema_name();
        let name = table.resolve(&catalog_name, &schema_name);
        // note: local mod dont support multiple catalog use DEFAULT_CATALOG
        // let catalog_name = name.catalog;
        let schema = match self.catalog.schema(name.schema) {
            None => {
                return Err(MetadataError::DatabaseNotExists {
                    database_name: name.schema.to_string(),
                });
            }
            Some(s) => s,
        };
        let table = match schema.table(name.table) {
            None => {
                return Err(MetadataError::TableNotExists {
                    table_name: name.table.to_string(),
                });
            }
            Some(t) => t,
        };
        Ok(table)
    }

    fn function(&self) -> FuncMetaManagerRef {
        self.func_manager.clone()
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        let table: TableReference = name.into();
        let name = table.resolve(self.catalog_name.as_str(), self.database_name.as_str());
        let schema = self.catalog.schema(name.schema);
        if let Some(db) = schema {
            return db.deregister_table(name.table).map(|_| ());
        }

        Err(MetadataError::DatabaseNotExists {
            database_name: name.schema.to_string(),
        })
    }

    fn drop_database(&self, name: &str) -> Result<()> {
        self.catalog.deregister_schema(name).map(|_| ())
    }

    fn create_table(&self, name: &str, table_schema: TableSchema) -> Result<()> {
        let table: TableReference = name.into();
        let table_ref = table.resolve(self.catalog_name.as_str(), self.database_name.as_str());

        self.catalog
            .schema(table_ref.schema)
            .ok_or_else(|| MetadataError::DatabaseNotExists {
                database_name: table_ref.schema.to_string(),
            })?
            // Currently the SchemaProvider creates a temporary table
            .register_table(table.table().to_owned(), table_schema)
            .map(|_| ())
    }

    fn create_database(&self, name: &str, database: DatabaseSchema) -> Result<()> {
        let user_schema = Database::new(name.to_string(), self.engine.clone(), database);
        self.catalog
            .register_schema(name, Arc::new(user_schema))
            .map(|_| ())
    }

    fn database_names(&self) -> Vec<String> {
        self.catalog.schema_names()
    }

    fn describe_database(&self, name: &str) -> Result<Output> {
        match self.engine.get_db_schema(name) {
            None => Err(MetadataError::DatabaseNotExists {
                database_name: name.to_string(),
            }),
            Some(db_cfg) => {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("TTL", DataType::Utf8, false),
                    Field::new("SHARD", DataType::Utf8, false),
                    Field::new("VNODE_DURATION", DataType::Utf8, false),
                    Field::new("REPLICA", DataType::Utf8, false),
                    Field::new("PRECISION", DataType::Utf8, false),
                ]));

                let ttl = db_cfg.config.ttl.to_string();
                let shard = db_cfg.config.shard_num.to_string();
                let vnode_duration = db_cfg.config.vnode_duration.to_string();
                let replica = db_cfg.config.replica.to_string();
                let precision = db_cfg.config.precision.to_string();

                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec![ttl.as_str()])),
                        Arc::new(StringArray::from(vec![shard.as_str()])),
                        Arc::new(StringArray::from(vec![vnode_duration.as_str()])),
                        Arc::new(StringArray::from(vec![replica.as_str()])),
                        Arc::new(StringArray::from(vec![precision.as_str()])),
                    ],
                )
                .unwrap();

                let batches = vec![Arc::new(batch)];

                Ok(Output::StreamData(stream_from_batches(batches)))
            }
        }
    }

    fn show_databases(&self) -> Result<Output> {
        let dbs = self.engine.list_databases();

        match dbs {
            Ok(databases) => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "Database",
                    DataType::Utf8,
                    false,
                )]));

                let batch =
                    RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(databases))])
                        .unwrap();

                let batches = vec![Arc::new(batch)];

                Ok(Output::StreamData(stream_from_batches(batches)))
            }
            Err(err) => Err(MetadataError::InternalError {
                error_msg: err.to_string(),
            }),
        }
    }

    fn show_tables(&self, name: &Option<String>) -> Result<Output> {
        let database_name = match name {
            None => self.database_name.as_str(),
            Some(v) => v.as_str(),
        };

        match self.catalog.schema(database_name) {
            None => Err(MetadataError::DatabaseNotExists {
                database_name: database_name.to_string(),
            }),
            Some(_db_cfgs) => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "Table",
                    DataType::Utf8,
                    false,
                )]));

                match self.engine.list_tables(database_name) {
                    Ok(tables) => {
                        let batch =
                            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(tables))])
                                .unwrap();

                        let batches = vec![Arc::new(batch)];

                        Ok(Output::StreamData(stream_from_batches(batches)))
                    }
                    Err(err) => Err(MetadataError::InternalError {
                        error_msg: err.to_string(),
                    }),
                }
            }
        }
    }
}

pub struct MetadataProvider {
    meta: MetaDataRef,
}

impl MetadataProvider {
    #[inline(always)]
    pub fn new(meta: MetaDataRef) -> Self {
        Self { meta }
    }
}
impl ContextProvider for MetadataProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<Arc<dyn TableSource>> {
        match self.meta.table(name) {
            Ok(table) => {
                // todo: we need a DataSourceManager to get engine and build table provider
                let local_catalog_meta = self
                    .meta
                    .as_any()
                    .downcast_ref::<LocalCatalogMeta>()
                    .ok_or_else(|| DataFusionError::Plan("failed to get meta data".to_string()))?;
                match table {
                    TableSchema::TsKvTableSchema(schema) => Ok(provider_as_source(Arc::new(
                        ClusterTable::new(local_catalog_meta.engine.clone(), schema),
                    ))),
                    TableSchema::ExternalTableSchema(schema) => {
                        let table_path = ListingTableUrl::parse(&schema.location)?;
                        let options = schema.table_options()?;
                        let config = ListingTableConfig::new(table_path)
                            .with_listing_options(options)
                            .with_schema(Arc::new(schema.schema));
                        Ok(provider_as_source(Arc::new(ListingTable::try_new(config)?)))
                    }
                }
            }
            Err(_) => {
                let catalog_name = self.meta.catalog_name();
                let schema_name = self.meta.schema_name();
                let resolved_name = name.resolve(&catalog_name, &schema_name);
                Err(DataFusionError::Plan(format!(
                    "failed to resolve user:{}  db: {}, table: {}",
                    resolved_name.catalog, resolved_name.schema, resolved_name.table
                )))
            }
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.meta.function().udf(name).ok()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.meta.function().udaf(name).ok()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        // TODO
        None
    }
}

pub fn stream_from_batches(batches: Vec<Arc<RecordBatch>>) -> SendableRecordBatchStream {
    let dummy_metrics = ExecutionPlanMetricsSet::new();
    let mem_metrics = MemTrackingMetrics::new(&dummy_metrics, 0);
    let stream = SizedRecordBatchStream::new(batches[0].schema(), batches, mem_metrics);
    Box::pin(stream)
}
