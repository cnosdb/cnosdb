use crate::catalog::{UserCatalog, UserCatalogRef, UserSchema};
use datafusion::arrow::array::{BooleanArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::catalog::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::{
    datasource::DefaultTableSource,
    error::DataFusionError,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource},
    sql::{planner::ContextProvider, TableReference},
};
use models::codec::Encoding;
use models::schema::ColumnType;
use models::ValueType;
use spi::query::execution::Output;

use datafusion::arrow::record_batch::RecordBatch;

use models::schema::DatabaseSchema;
use snafu::ResultExt;
use spi::catalog::{
    CatalogRef, ExternalSnafu, MetaData, MetaDataRef, MetadataError, Result, DEFAULT_CATALOG,
    DEFAULT_DATABASE,
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
                MetadataError::External { ref source } => {
                    if !source.to_string().eq(&format!(
                        "Execution error: database '{}' already exists",
                        DEFAULT_DATABASE
                    )) {
                        return Err(e);
                    }
                }
                _ => return Err(e),
            }
        };
        Ok(meta)
    }
}

impl MetaData for LocalCatalogMeta {
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

    fn table_provider(&self, table: TableReference) -> Result<Arc<dyn TableSource>> {
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
        Ok(Arc::new(DefaultTableSource::new(table.clone())))
    }

    fn catalog(&self) -> CatalogRef {
        self.catalog.clone()
    }

    fn function(&self) -> FuncMetaManagerRef {
        self.func_manager.clone()
    }

    fn drop_table(&self, name: &str) -> Result<()> {
        let table: TableReference = name.into();
        let name = table.resolve(self.catalog_name.as_str(), self.database_name.as_str());
        let schema = self.catalog.schema(name.schema);
        if let Some(db) = schema {
            return db
                .deregister_table(name.table)
                .map(|_| ())
                .context(ExternalSnafu);
        }

        Err(MetadataError::DatabaseNotExists {
            database_name: name.schema.to_string(),
        })
    }

    fn drop_database(&self, name: &str) -> Result<()> {
        self.catalog
            .deregister_schema(name)
            .map(|_| ())
            .context(ExternalSnafu)
    }

    fn create_table(&self, name: &str, table_provider: Arc<dyn TableProvider>) -> Result<()> {
        let table: TableReference = name.into();
        let table_ref = table.resolve(self.catalog_name.as_str(), self.database_name.as_str());

        self.catalog
            .schema(table_ref.schema)
            .ok_or_else(|| MetadataError::DatabaseNotExists {
                database_name: table_ref.schema.to_string(),
            })?
            // Currently the SchemaProvider creates a temporary table
            .register_table(table.table().to_owned(), table_provider)
            .map(|_| ())
            .context(ExternalSnafu)
    }

    fn create_database(&self, name: &str, database: DatabaseSchema) -> Result<()> {
        let user_schema = UserSchema::new(name.to_string(), self.engine.clone(), database);
        self.catalog
            .register_schema(name, Arc::new(user_schema))
            .map(|_| ())
            .context(ExternalSnafu)?;
        Ok(())
    }

    fn show_database(&self) -> Result<Output> {
        let dbs = self.catalog.schema_names();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "Database",
            DataType::Utf8,
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(dbs))]).unwrap();

        let batches = vec![Arc::new(batch)];

        Ok(Output::StreamData(stream_from_batches(batches)))
    }

    fn show_table(&self, name: &str) -> Result<Output> {
        let mut database_name = name;
        if database_name == "" {
            database_name = self.database_name.as_str()
        }

        match self.catalog.schema(database_name) {
            None => {
                return Err(MetadataError::DatabaseNotExists {
                    database_name: database_name.to_string(),
                })
            }
            Some(db_cfgs) => {
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "Table",
                    DataType::Utf8,
                    false,
                )]));

                let batch = RecordBatch::try_new(
                    schema,
                    vec![Arc::new(StringArray::from(db_cfgs.table_names()))],
                )
                .unwrap();

                let batches = vec![Arc::new(batch)];

                Ok(Output::StreamData(stream_from_batches(batches)))
            }
        }
    }

    fn describe_database(&self, name: &str) -> Result<Output> {
        match self.engine.get_db_schema(name) {
            None => {
                return Err(MetadataError::DatabaseNotExists {
                    database_name: name.to_string(),
                })
            }
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

    fn describe_table(&self, table: &str) -> Result<Output> {
        let table_name;
        let database_name;

        if table.contains(".") {
            (database_name, table_name) = table.split_once('.').unwrap();
        } else {
            table_name = table;
            database_name = self.database_name.as_str();
        }

        match self
            .engine
            .get_table_schema(database_name, table_name)
            .unwrap()
        {
            None => {
                return Err(MetadataError::TableNotExists {
                    table_name: table.to_string(),
                })
            }
            Some(table_schema) => {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("fieldname", DataType::Utf8, false),
                    Field::new("type", DataType::Utf8, false),
                    Field::new("istag", DataType::Boolean, false),
                    Field::new("compression", DataType::Utf8, false),
                ]));
                // fieldname    type        istag       compression
                //      time    Time,       No          codec
                //      c1      String      No          codec
                //      c2      uint64      No          default
                let columns = table_schema.columns().clone();

                let mut name_column = vec![];
                let mut type_column = vec![];
                let mut tags = vec![];
                let mut compressions = vec![];

                for item in &columns {
                    let field_name = item.name.as_str();
                    let field_type;
                    let mut tag = false;
                    let compression;
                    match item.column_type {
                        ColumnType::Tag => {
                            field_type = "STRING";
                            tag = true;
                        }
                        ColumnType::Time => field_type = "TIMESTAMP",
                        ColumnType::Field(ValueType::Float) => field_type = "DOUBLE",
                        ColumnType::Field(ValueType::Integer) => field_type = "BIGINT",
                        ColumnType::Field(ValueType::Unsigned) => field_type = "UNSIGNED",
                        ColumnType::Field(ValueType::String) => field_type = "STRING",
                        ColumnType::Field(ValueType::Boolean) => field_type = "BOOLEAN",
                        ColumnType::Field(ValueType::Unknown) => field_type = "UNKNOW",
                    }

                    match Encoding::from(item.codec) {
                        Encoding::Default => compression = "Default",
                        Encoding::Null => compression = "Null",
                        Encoding::Delta => compression = "Delta",
                        Encoding::Quantile => compression = "Quantile",
                        Encoding::Gzip => compression = "Gzip",
                        Encoding::Bzip => compression = "Bzip",
                        Encoding::Gorilla => compression = "Gorilla",
                        Encoding::Snappy => compression = "Snappy",
                        Encoding::Zstd => compression = "Zstd",
                        Encoding::Zlib => compression = "Zlib",
                        Encoding::BitPack => compression = "BitPack",
                        Encoding::Unknown => compression = "Unknown",
                    }
                    name_column.push(field_name);
                    type_column.push(field_type);
                    tags.push(tag);
                    compressions.push(compression);
                }

                let batch = RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(name_column)),
                        Arc::new(StringArray::from(type_column)),
                        Arc::new(BooleanArray::from(tags)),
                        Arc::new(StringArray::from(compressions)),
                    ],
                )
                .unwrap();

                let batches = vec![Arc::new(batch)];

                Ok(Output::StreamData(stream_from_batches(batches)))
            }
        }
    }

    fn database_names(&self) -> Vec<String> {
        self.catalog.schema_names()
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
        match self.meta.table_provider(name) {
            Ok(table) => Ok(table),
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
