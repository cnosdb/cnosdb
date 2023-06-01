use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource};
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
pub use information_schema_provider::{
    DATABASES_DATABASE_NAME, DATABASES_PERCISION, DATABASES_REPLICA, DATABASES_SHARD,
    DATABASES_TENANT_NAME, DATABASES_TTL, DATABASES_VNODE_DURATION, INFORMATION_SCHEMA_DATABASES,
    INFORMATION_SCHEMA_TABLES, TABLES_TABLE_DATABASE, TABLES_TABLE_ENGINE, TABLES_TABLE_NAME,
    TABLES_TABLE_OPTIONS, TABLES_TABLE_TENANT, TABLES_TABLE_TYPE,
};
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::user::UserDesc;
use models::object_reference::{Resolve, ResolvedTable};
use models::schema::{Precision, Tenant, DEFAULT_CATALOG};
use parking_lot::RwLock;
use spi::query::function::FuncMetaManagerRef;
use spi::query::session::SessionCtx;
pub use usage_schema_provider::{USAGE_SCHEMA, USAGE_SCHEMA_VNODE_DISK_STORAGE};

pub use self::base_table::BaseTableProvider;
use self::cluster_schema_provider::ClusterSchemaProvider;
use self::information_schema_provider::InformationSchemaProvider;
use crate::data_source::table_source::{TableHandle, TableSourceAdapter};
use crate::dispatcher::query_tracker::QueryTracker;
use crate::metadata::usage_schema_provider::UsageSchemaProvider;

mod base_table;
mod cluster_schema_provider;
mod information_schema_provider;
mod usage_schema_provider;

pub const CLUSTER_SCHEMA: &str = "CLUSTER_SCHEMA";
pub const INFORMATION_SCHEMA: &str = "INFORMATION_SCHEMA";

/// remote meta
pub struct RemoteCatalogMeta {}

#[async_trait]
pub trait ContextProviderExtension: ContextProvider {
    async fn get_user(&self, name: &str) -> Result<UserDesc, MetaError>;
    async fn get_tenant(&self, name: &str) -> Result<Tenant, MetaError>;
    /// Clear the access record and return the content before clearing
    fn reset_access_databases(&self) -> DatabaseSet;
    fn get_db_precision(&self, name: &str) -> Result<Precision, MetaError>;
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion::common::Result<Arc<TableSourceAdapter>>;
}

pub type TableHandleProviderRef = Arc<dyn TableHandleProvider + Send + Sync>;

pub trait TableHandleProvider {
    fn build_table_handle(&self, ddatabase_name: &str, table_name: &str) -> DFResult<TableHandle>;
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
    // tskv/external
    current_session_table_provider: TableHandleProviderRef,
}

impl MetadataProvider {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        coord: CoordinatorRef,
        meta_client: MetaClientRef,
        current_session_table_provider: TableHandleProviderRef,
        default_table_provider: TableHandleProviderRef,
        func_manager: FuncMetaManagerRef,
        query_tracker: Arc<QueryTracker>,
        session: SessionCtx,
    ) -> Self {
        Self {
            current_session_table_provider,
            coord,
            // TODO refactor
            config_options: session.inner().state().config_options().clone(),
            session,
            meta_client,
            func_manager,
            information_schema_provider: InformationSchemaProvider::new(query_tracker),
            cluster_schema_provider: ClusterSchemaProvider::new(),
            usage_schema_provider: UsageSchemaProvider::new(default_table_provider),
            access_databases: Default::default(),
        }
    }

    fn process_system_table_source(
        &self,
        tenant_name: &str,
        database_name: &str,
        table_name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        // process INFORMATION_SCHEMA
        if database_name.eq_ignore_ascii_case(self.information_schema_provider.name()) {
            let mem_table = self
                .information_schema_provider
                .table(self.session.user(), table_name, self.meta_client.clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(Some(mem_table));
        }

        // process USAGE_SCHEMA
        if database_name.eq_ignore_ascii_case(self.usage_schema_provider.name()) {
            let table_provider = self
                .usage_schema_provider
                .table(&self.session, table_name)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Ok(Some(table_provider));
        }

        // process CNOSDB(sys tenant) -> CLUSTER_SCHEMA
        if tenant_name.eq_ignore_ascii_case(DEFAULT_CATALOG)
            && database_name.eq_ignore_ascii_case(self.cluster_schema_provider.name())
        {
            let mem_table = self
                .cluster_schema_provider
                .table(self.session.user(), table_name, self.coord.meta_manager())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            return Ok(Some(mem_table));
        }

        Ok(None)
    }

    fn build_table_handle(&self, name: &ResolvedTable) -> datafusion::common::Result<TableHandle> {
        let tenant_name = name.tenant();
        let database_name = name.database();
        let table_name = name.table();

        if let Some(source) =
            self.process_system_table_source(tenant_name, database_name, table_name)?
        {
            return Ok(source.into());
        }

        self.current_session_table_provider
            .build_table_handle(database_name, table_name)
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

    fn get_db_precision(&self, name: &str) -> Result<Precision, MetaError> {
        let precision = *self
            .meta_client
            .get_db_schema(name)?
            .ok_or(MetaError::DatabaseNotFound {
                database: name.to_string(),
            })?
            .config
            .precision_or_default();
        Ok(precision)
    }

    fn get_table_source(
        &self,
        table_ref: TableReference,
    ) -> datafusion::common::Result<Arc<TableSourceAdapter>> {
        let name = table_ref
            .clone()
            .resolve_object(self.session.tenant(), self.session.default_database())?;

        let table_name = name.table();
        let database_name = name.database();
        let tenant_name = name.tenant();

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

        let table_handle = self.build_table_handle(&name)?;

        Ok(Arc::new(TableSourceAdapter::try_new(
            table_ref.to_owned_reference(),
            database_name,
            table_name,
            table_handle,
        )?))
    }
}

impl ContextProvider for MetadataProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> datafusion::error::Result<Arc<dyn TableSource>> {
        Ok(self.get_table_source(name)?)
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
