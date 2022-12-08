use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::common::SizedRecordBatchStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MemTrackingMetrics};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::{
    error::DataFusionError,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource},
    sql::{planner::ContextProvider, TableReference},
};

use coordinator::service::CoordinatorRef;
use models::auth::user::UserDesc;
use models::schema::{TableSchema, Tenant};

use datafusion::arrow::record_batch::RecordBatch;
use meta::meta_client::MetaError;

use crate::table::ClusterTable;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::provider_as_source;

use spi::query::function::FuncMetaManagerRef;
use std::sync::Arc;

use crate::function::simple_func_manager::SimpleFunctionMetadataManager;

/// remote meta
pub struct RemoteCatalogMeta {}

pub trait ContextProviderExtension: ContextProvider {
    fn get_user(&self, name: &str) -> Result<UserDesc, MetaError>;
    fn get_tenant(&self, name: &str) -> Result<Tenant, MetaError>;
}

pub struct MetadataProvider {
    tenant: String,
    database: String,
    coord: CoordinatorRef,
    func_manager: FuncMetaManagerRef,
}

impl MetadataProvider {
    pub fn new(
        coord: CoordinatorRef,
        func_manager: SimpleFunctionMetadataManager,
        tenant: String,
        database: String,
    ) -> Self {
        Self {
            coord,
            tenant,
            database,
            func_manager: Arc::new(func_manager),
        }
    }
}

impl ContextProviderExtension for MetadataProvider {
    fn get_user(&self, name: &str) -> Result<UserDesc, MetaError> {
        self.coord
            .meta_manager()
            .user_manager()
            .user(name)?
            .ok_or_else(|| MetaError::UserNotFound {
                user: name.to_string(),
            })
    }

    fn get_tenant(&self, name: &str) -> Result<Tenant, MetaError> {
        self.coord
            .meta_manager()
            .tenant_manager()
            .tenant(name)?
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: name.to_string(),
            })
    }
}

impl ContextProvider for MetadataProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<Arc<dyn TableSource>> {
        let name = name.resolve(&self.tenant, &self.database);
        let client = self
            .coord
            .meta_manager()
            .tenant_manager()
            .tenant_meta(name.catalog)
            .ok_or_else(|| {
                DataFusionError::External(Box::new(MetaError::TenantNotFound {
                    tenant: name.catalog.to_string(),
                }))
            })?;
        match client
            .get_table_schema(name.schema, name.table)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            Some(table) => match table {
                TableSchema::TsKvTableSchema(schema) => Ok(provider_as_source(Arc::new(
                    ClusterTable::new(self.coord.clone(), schema),
                ))),
                TableSchema::ExternalTableSchema(schema) => {
                    let table_path = ListingTableUrl::parse(&schema.location)?;
                    let options = schema.table_options()?;
                    let config = ListingTableConfig::new(table_path)
                        .with_listing_options(options)
                        .with_schema(Arc::new(schema.schema));
                    Ok(provider_as_source(Arc::new(ListingTable::try_new(config)?)))
                }
            },
            None => Err(DataFusionError::Plan(format!(
                "failed to resolve tenant:{}  db: {}, table: {}",
                name.catalog, name.schema, name.table
            ))),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.func_manager.udf(name).ok()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.func_manager.udaf(name).ok()
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
