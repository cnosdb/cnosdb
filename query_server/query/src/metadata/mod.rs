mod cluster_schema_provider;
mod information_schema_provider;
mod usage_schema_provider;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::provider_as_source;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::{ResolvedTableReference, TableReference};
use meta::error::MetaError;
use meta::MetaClientRef;
use models::auth::user::UserDesc;
use models::schema::{TableSchema, TableSourceAdapter, Tenant, DEFAULT_CATALOG};
use parking_lot::RwLock;
use spi::query::function::FuncMetaManagerRef;
use spi::query::session::SessionCtx;

use self::cluster_schema_provider::ClusterSchemaProvider;
use self::information_schema_provider::InformationSchemaProvider;
use crate::data_source::table_provider::tskv::ClusterTable;
use crate::dispatcher::query_tracker::QueryTracker;
use crate::function::simple_func_manager::SimpleFunctionMetadataManager;
use crate::metadata::usage_schema_provider::UsageSchemaProvider;

pub const CLUSTER_SCHEMA: &str = "CLUSTER_SCHEMA";
pub const INFORMATION_SCHEMA: &str = "INFORMATION_SCHEMA";
pub use usage_schema_provider::{USAGE_SCHEMA, USAGE_SCHEMA_VNODE_DISK_STORAGE};

/// remote meta
pub struct RemoteCatalogMeta {}

#[async_trait]
pub trait ContextProviderExtension: ContextProvider {
    async fn get_user(&self, name: &str) -> Result<UserDesc, MetaError>;
    async fn get_tenant(&self, name: &str) -> Result<Tenant, MetaError>;
    /// Clear the access record and return the content before clearing
    fn reset_access_databases(&self) -> DatabaseSet;
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<TableSourceAdapter>;
}

pub struct MetadataProvider {
    session: SessionCtx,
    config_options: ConfigOptions,
    coord: CoordinatorRef,
    meta_client: MetaClientRef,
    func_manager: FuncMetaManagerRef,
    information_schema_provider: InformationSchemaProvider,
    cluster_schema_provider: ClusterSchemaProvider,
    usage_schema_provider: UsageSchemaProvider,
    access_databases: RwLock<DatabaseSet>,
}

impl MetadataProvider {
    pub fn new(
        coord: CoordinatorRef,
        meta_client: MetaClientRef,
        func_manager: SimpleFunctionMetadataManager,
        query_tracker: Arc<QueryTracker>,
        session: SessionCtx,
        default_meta: MetaClientRef,
    ) -> Self {
        Self {
            coord,
            // TODO refactor
            config_options: session.inner().state().config_options().clone(),
            session,
            meta_client,
            func_manager: Arc::new(func_manager),
            information_schema_provider: InformationSchemaProvider::new(query_tracker),
            cluster_schema_provider: ClusterSchemaProvider::new(),
            usage_schema_provider: UsageSchemaProvider::new(default_meta),
            access_databases: Default::default(),
        }
    }

    fn process_system_table_source(
        &self,
        tenant_name: &str,
        database_name: &str,
        table_name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableSource>>> {
        // process INFORMATION_SCHEMA
        if database_name.eq_ignore_ascii_case(self.information_schema_provider.name()) {
            let mem_table = futures::executor::block_on(self.information_schema_provider.table(
                self.session.user(),
                table_name,
                self.meta_client.clone(),
            ))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(Some(provider_as_source(mem_table)));
        }

        // process USAGE_SCHEMA
        if database_name.eq_ignore_ascii_case(self.usage_schema_provider.name()) {
            let table_provider = self
                .usage_schema_provider
                .table(
                    table_name,
                    self.session.user(),
                    self.coord.clone(),
                    self.meta_client.clone(),
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Ok(Some(provider_as_source(table_provider)));
        }

        // process CNOSDB(sys tenant) -> CLUSTER_SCHEMA
        if tenant_name.eq_ignore_ascii_case(DEFAULT_CATALOG)
            && database_name.eq_ignore_ascii_case(self.cluster_schema_provider.name())
        {
            let mem_table = futures::executor::block_on(self.cluster_schema_provider.table(
                self.session.user(),
                table_name,
                self.coord.meta_manager(),
            ))
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(Some(provider_as_source(mem_table)));
        }

        Ok(None)
    }

    fn build_df_data_source(
        &self,
        name: &ResolvedTableReference<'_>,
    ) -> datafusion::common::Result<Arc<dyn TableSource>> {
        let tenant_name = name.catalog.as_ref();
        let database_name = name.schema.as_ref();
        let table_name = name.table.as_ref();

        if let Some(source) =
            self.process_system_table_source(tenant_name, database_name, table_name)?
        {
            return Ok(source);
        }

        let df_table_source = match self
            .meta_client
            .get_table_schema(database_name, table_name)
            .map_err(|e| DataFusionError::External(Box::new(e)))?
        {
            Some(table) => match table {
                TableSchema::TsKvTableSchema(schema) => {
                    provider_as_source(Arc::new(ClusterTable::new(self.coord.clone(), schema)))
                }
                TableSchema::ExternalTableSchema(schema) => {
                    let table_path = ListingTableUrl::parse(&schema.location)?;
                    let options = schema.table_options()?;
                    let config = ListingTableConfig::new(table_path)
                        .with_listing_options(options)
                        .with_schema(Arc::new(schema.schema.clone()));
                    provider_as_source(Arc::new(ListingTable::try_new(config)?))
                }
            },

            None => {
                return Err(DataFusionError::Plan(format!(
                    "failed to resolve tenant:{}  db: {}, table: {}",
                    name.catalog, name.schema, name.table
                )))
            }
        };

        Ok(df_table_source)
    }
}

#[async_trait::async_trait]
impl ContextProviderExtension for MetadataProvider {
    async fn get_user(&self, name: &str) -> Result<UserDesc, MetaError> {
        self.coord
            .meta_manager()
            .user_manager()
            .user(name)
            .await?
            .ok_or_else(|| MetaError::UserNotFound {
                user: name.to_string(),
            })
    }

    async fn get_tenant(&self, name: &str) -> Result<Tenant, MetaError> {
        self.coord
            .meta_manager()
            .tenant_manager()
            .tenant(name)
            .await?
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: name.to_string(),
            })
    }

    fn reset_access_databases(&self) -> DatabaseSet {
        let result = self.access_databases.read().clone();
        self.access_databases.write().reset();
        result
    }

    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<TableSourceAdapter> {
        let name = name.resolve(self.session.tenant(), self.session.default_database());

        let table_name = name.table.as_ref();
        let database_name = name.schema.as_ref();
        let tenant_name = name.catalog.as_ref();
        let tenant_id = *self.session.tenant_id();

        // Cannot query across tenants
        if self.session.tenant() != tenant_name {
            return Err(DataFusionError::Plan(format!(
                "Tenant conflict, the current connection's tenant is {}",
                self.session.tenant()
            )));
        }

        // save access table
        self.access_databases
            .write()
            .push_table(database_name, table_name);

        let df_table_source = self.build_df_data_source(&name)?;

        Ok(TableSourceAdapter::new(
            df_table_source,
            tenant_id,
            tenant_name,
            database_name,
            table_name,
        ))
    }
}

impl ContextProvider for MetadataProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion::error::Result<Arc<dyn TableSource>> {
        Ok(self.get_table_source(name)?.inner())
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

    fn options(&self) -> &ConfigOptions {
        // TODO refactor
        &self.config_options
    }
}

#[derive(Default, Clone)]
pub struct DatabaseSet {
    dbs: HashMap<String, TableSet>,
}

impl DatabaseSet {
    pub fn reset(&mut self) {
        self.dbs.clear();
    }

    pub fn push_table(&mut self, db: impl Into<String>, tbl: impl Into<String>) {
        self.dbs
            .entry(db.into())
            .or_insert_with(TableSet::default)
            .push_table(tbl);
    }

    pub fn dbs(&self) -> Vec<&String> {
        self.dbs.keys().collect()
    }

    pub fn table_set(&self, db_name: &str) -> Option<&TableSet> {
        self.dbs.get(db_name)
    }
}

#[derive(Default, Clone)]
pub struct TableSet {
    tables: HashSet<String>,
}

impl TableSet {
    pub fn push_table(&mut self, tbl: impl Into<String>) {
        self.tables.insert(tbl.into());
    }
}
