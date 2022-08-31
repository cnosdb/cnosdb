use std::sync::Arc;

use datafusion::{
    arrow::datatypes::DataType,
    catalog::schema::SchemaProvider,
    datasource::{DefaultTableSource, TableProvider},
    error::DataFusionError,
    logical_expr::{AggregateUDF, ScalarUDF, TableSource},
    sql::{planner::ContextProvider, ResolvedTableReference, TableReference},
};
use spi::{
    catalog::factory::CatalogManager,
    query::{function::FunctionMetadataManager, QueryError},
};

use spi::query::Result;

pub struct MetadataProvider {
    catalog_manager: Arc<dyn CatalogManager>,
    function_manager: Arc<dyn FunctionMetadataManager + Send + Sync>,
    default_catalog: String,
    default_schema: String,
}

impl MetadataProvider {
    #[inline(always)]
    pub fn new(
        catalog_manager: Arc<dyn CatalogManager>,
        function_manager: Arc<dyn FunctionMetadataManager + Send + Sync>,
        default_catalog: String,
        default_schema: String,
    ) -> Self {
        Self {
            catalog_manager,
            function_manager,
            default_catalog,
            default_schema,
        }
    }
}

impl MetadataProvider {
    fn resolve_table_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> ResolvedTableReference<'a> {
        table_ref
            .into()
            .resolve(&self.default_catalog, &self.default_schema)
    }

    fn schema_for_ref<'a>(
        &'a self,
        table_ref: impl Into<TableReference<'a>>,
    ) -> Result<Arc<dyn SchemaProvider>> {
        let resolved_ref = self.resolve_table_ref(table_ref);
        self.catalog_manager
            .catalog(resolved_ref.catalog)
            .ok_or_else(|| QueryError::Analyzer {
                err: format!("failed to resolve catalog: {}", resolved_ref.catalog),
            })?
            .schema(resolved_ref.schema)
            .ok_or_else(|| QueryError::Analyzer {
                err: format!("failed to resolve schema: {}", resolved_ref.schema),
            })
    }
}

impl ContextProvider for MetadataProvider {
    fn get_table_provider(
        &self,
        name: TableReference,
    ) -> std::result::Result<Arc<dyn TableSource>, DataFusionError> {
        let resolved_ref = self.resolve_table_ref(name);
        match self.schema_for_ref(resolved_ref) {
            Ok(schema) => {
                let provider = schema.table(resolved_ref.table).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "'{}.{}.{}' not found",
                        resolved_ref.catalog, resolved_ref.schema, resolved_ref.table
                    ))
                })?;
                Ok(provider_as_source(provider))
            }
            Err(_) => Err(DataFusionError::Plan(format!(
                "'{}.{}.{}' not found",
                resolved_ref.catalog, resolved_ref.schema, resolved_ref.table
            ))),
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.function_manager.udf(name).ok()
    }

    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.function_manager.udaf(name).ok()
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        // TODO
        None
    }
}

/// Wrap TableProvider in TableSource
pub fn provider_as_source(table_provider: Arc<dyn TableProvider>) -> Arc<dyn TableSource> {
    Arc::new(DefaultTableSource::new(table_provider))
}
