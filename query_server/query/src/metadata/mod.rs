use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use cluster_schema_provider::{CLUSTER_SCHEMA_TENANTS, CLUSTER_SCHEMA_USERS};
use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::config::ConfigOptions;
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, TableSource, WindowUDF};
use datafusion::physical_expr::var_provider::is_system_variables;
use datafusion::sql::planner::ContextProvider;
use datafusion::sql::TableReference;
use datafusion::variable::{VarProvider, VarType};
pub use information_schema_provider::{
    COLUMNS_COLUMN_NAME, COLUMNS_COLUMN_TYPE, COLUMNS_COMPRESSION_CODEC, COLUMNS_DATABASE_NAME,
    COLUMNS_DATA_TYPE, COLUMNS_TABLE_NAME, DATABASES_DATABASE_NAME, DATABASES_MAX_CACHE_READERS,
    DATABASES_MAX_MEMCACHE_SIZE, DATABASES_MEMCACHE_PARTITIONS, DATABASES_PRECISION,
    DATABASES_REPLICA, DATABASES_SHARD, DATABASES_STRICT_WRITE, DATABASES_TENANT_NAME,
    DATABASES_TTL, DATABASES_VNODE_DURATION, DATABASES_WAL_MAX_FILE_SIZE, DATABASES_WAL_SYNC,
    INFORMATION_SCHEMA_COLUMNS, INFORMATION_SCHEMA_DATABASES, INFORMATION_SCHEMA_QUERIES,
    INFORMATION_SCHEMA_TABLES, TABLES_TABLE_DATABASE, TABLES_TABLE_ENGINE, TABLES_TABLE_NAME,
    TABLES_TABLE_OPTIONS, TABLES_TABLE_TENANT, TABLES_TABLE_TYPE,
};
use meta::error::MetaError;
use meta::model::MetaClientRef;
use models::auth::user::UserDesc;
use models::object_reference::{Resolve, ResolvedTable};
use models::schema::database_schema::Precision;
use models::schema::tenant::Tenant;
use models::schema::{DEFAULT_CATALOG, DEFAULT_DATABASE};
use parking_lot::RwLock;
use spi::query::function::FuncMetaManagerRef;
use spi::query::session::SessionCtx;

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

pub const CLUSTER_SCHEMA: &str = "cluster_schema";
pub const INFORMATION_SCHEMA: &str = "INFORMATION_SCHEMA";
pub const USAGE_SCHEMA: &str = "usage_schema";

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
    fn database_table_exist(
        &self,
        _database: &str,
        _table: Option<&ResolvedTable>,
    ) -> Result<(), MetaError> {
        Ok(())
    }
}

pub type TableHandleProviderRef = Arc<dyn TableHandleProvider + Send + Sync>;
pub type VarProviderRef = Arc<dyn VarProvider + Send + Sync>;

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
            config_options: session.inner().config_options().clone(),
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
        // usage_schema records usage information.
        // Under the cnosdb tenant, the table in usage_schema
        // corresponds to the actual kv table,
        // and any operation can be performed with permission.
        // Under other tenants, the usage_schema table is
        // a view of the kv table and can only be queried.
        if !tenant_name.eq(DEFAULT_CATALOG)
            && database_name.eq_ignore_ascii_case(self.usage_schema_provider.name())
        {
            let table_provider = self
                .usage_schema_provider
                .table(&self.session, table_name)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Ok(Some(table_provider));
        }

        // process CNOSDB(sys tenant) -> CLUSTER_SCHEMA
        if tenant_name.eq_ignore_ascii_case(DEFAULT_CATALOG)
            && database_name.eq_ignore_ascii_case(self.cluster_schema_provider.name())
            && (table_name.eq_ignore_ascii_case(CLUSTER_SCHEMA_TENANTS)
                || table_name.eq_ignore_ascii_case(CLUSTER_SCHEMA_USERS))
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
            .user(name)
            .await?
            .ok_or_else(|| MetaError::UserNotFound {
                user: name.to_string(),
            })
    }

    async fn get_tenant(&self, name: &str) -> Result<Tenant, MetaError> {
        self.coord
            .meta_manager()
            .tenant(name)
            .await?
            .ok_or_else(|| MetaError::TenantNotFound {
                tenant: name.to_string(),
            })
    }

    fn reset_access_databases(&self) -> DatabaseSet {
        let mut access_databases = self.access_databases.write();
        let res = access_databases.clone();
        access_databases.reset();
        res
    }

    fn get_db_precision(&self, name: &str) -> Result<Precision, MetaError> {
        let db_schema =
            self.meta_client
                .get_db_schema(name)?
                .ok_or_else(|| MetaError::DatabaseNotFound {
                    database: name.to_string(),
                })?;

        if db_schema.is_hidden() {
            return Err(MetaError::DatabaseNotFound {
                database: name.to_string(),
            });
        }

        Ok(*db_schema.config.precision())
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

    fn database_table_exist(
        &self,
        database: &str,
        table: Option<&ResolvedTable>,
    ) -> Result<(), MetaError> {
        let data_info =
            self.meta_client
                .get_db_info(database)?
                .ok_or_else(|| MetaError::DatabaseNotFound {
                    database: database.to_string(),
                })?;

        if data_info.is_hidden() {
            return Err(MetaError::DatabaseNotFound {
                database: database.to_string(),
            });
        }

        if let Some(table) = table {
            data_info
                .tables
                .get(table.table())
                .ok_or_else(|| MetaError::TableNotFound {
                    table: table.to_string(),
                })?;
        }

        Ok(())
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
        self.func_manager.udf(name).ok().or(self
            .session
            .inner()
            .scalar_functions()
            .get(name)
            .cloned())
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.func_manager.udaf(name).ok()
    }

    fn get_variable_type(&self, variable_names: &[String]) -> Option<DataType> {
        if variable_names.is_empty() {
            return None;
        }

        let var_type = if is_system_variables(variable_names) {
            VarType::System
        } else {
            VarType::UserDefined
        };

        self.session
            .inner()
            .execution_props()
            .get_var_provider(var_type)
            .and_then(|p| p.get_type(variable_names))
    }

    fn options(&self) -> &ConfigOptions {
        // TODO refactor
        &self.config_options
    }

    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.func_manager.udwf(name).ok()
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
        self.dbs.entry(db.into()).or_default().push_table(tbl);
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

// "cnosdb" tenant additional check "public" and "CLUSTER_SCHEMA"
// other tenant only check "INFORMATION_SCHEMA" and "usage_schema"
pub fn is_system_database(tenant: &str, database: &str) -> bool {
    if tenant.eq_ignore_ascii_case(DEFAULT_CATALOG)
        && (database.eq_ignore_ascii_case(DEFAULT_DATABASE)
            || database.eq_ignore_ascii_case(CLUSTER_SCHEMA))
    {
        return true;
    }
    database.eq_ignore_ascii_case(INFORMATION_SCHEMA) || database.eq_ignore_ascii_case(USAGE_SCHEMA)
}
