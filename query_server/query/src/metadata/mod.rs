mod cluster_schema_provider;
mod information_schema_provider;

use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::{
    error::DataFusionError,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource},
    sql::{planner::ContextProvider, TableReference},
};
use models::auth::user::UserDesc;
use models::schema::{TableSchema, Tenant};

use spi::query::session::IsiphoSessionCtx;

use crate::dispatcher::query_tracker::QueryTracker;
use crate::table::ClusterTable;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::provider_as_source;

use meta::error::MetaError;
use spi::query::function::FuncMetaManagerRef;
use std::sync::Arc;

use crate::function::simple_func_manager::SimpleFunctionMetadataManager;

use self::information_schema_provider::InformationSchemaProvider;

/// remote meta
pub struct RemoteCatalogMeta {}

pub trait ContextProviderExtension: ContextProvider {
    fn get_user(&self, name: &str) -> Result<UserDesc, MetaError>;
    fn get_tenant(&self, name: &str) -> Result<Tenant, MetaError>;
}

pub struct MetadataProvider {
    session: IsiphoSessionCtx,
    coord: CoordinatorRef,
    func_manager: FuncMetaManagerRef,
    information_schema_provider: InformationSchemaProvider,
}

impl MetadataProvider {
    pub fn new(
        coord: CoordinatorRef,
        func_manager: SimpleFunctionMetadataManager,
        query_tracker: Arc<QueryTracker>,
        session: IsiphoSessionCtx,
    ) -> Self {
        Self {
            coord,
            session,
            func_manager: Arc::new(func_manager),
            information_schema_provider: InformationSchemaProvider::new(query_tracker),
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
        let name = name.resolve(self.session.tenant(), self.session.default_database());

        let table_name = name.table;
        let database_name = name.schema;

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

        // process INFORMATION_SCHEMA
        if database_name.eq_ignore_ascii_case(self.information_schema_provider.name()) {
            let mem_table = self
                .information_schema_provider
                .table(self.session.user(), table_name, client)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(provider_as_source(mem_table));
        }

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
