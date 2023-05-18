use std::any::Any;
use std::fmt::Display;
use std::sync::Arc;
use std::write;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{OwnedTableReference, Result as DFResult};
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::{provider_as_source, TableProvider};
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{
    LogicalPlan, LogicalPlanBuilder, TableProviderAggregationPushDown, TableProviderFilterPushDown,
    TableSource,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use spi::query::datasource::stream::StreamProviderRef;
use trace::{debug, warn};

use super::batch::tskv::ClusterTable;
use super::WriteExecExt;
use crate::extension::logical::logical_plan_builder::LogicalPlanBuilderExt;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;

pub const TEMP_LOCATION_TABLE_NAME: &str = "external_location_table";

pub struct TableSourceAdapter {
    database_name: String,
    table_name: String,
    table_handle: TableHandle,

    plan: LogicalPlan,
}

impl TableSourceAdapter {
    pub fn try_new(
        table_ref: impl Into<OwnedTableReference>,
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        table_handle: impl Into<TableHandle>,
    ) -> Result<Self, DataFusionError> {
        let database_name = database_name.into();
        let table_name: String = table_name.into();

        let table_handle = table_handle.into();
        let plan = match &table_handle {
            // TableScan
            TableHandle::External(t) => {
                let table_source = provider_as_source(t.clone());
                LogicalPlanBuilder::scan(table_ref, table_source, None)?.build()?
            }
            // TableScan
            TableHandle::Tskv(t) => {
                let table_source = provider_as_source(t.clone());
                LogicalPlanBuilder::scan(table_ref, table_source, None)?.build()?
            }
            // TableScan
            TableHandle::TableProvider(t) => {
                let table_source = provider_as_source(t.clone());
                if let Some(plan) = table_source.get_logical_plan() {
                    LogicalPlanBuilder::from(plan.clone()).build()?
                } else {
                    LogicalPlanBuilder::scan(table_ref, table_source, None)?.build()?
                }
            }
            // StreamScan
            TableHandle::StreamProvider(s) => {
                LogicalPlanBuilder::stream_scan(table_ref, s.clone())?
                    .watermark(s.watermark().clone())?
                    .build()?
            }
        };

        debug!(
            "Table source logical plan node of {}:\n{}",
            table_name,
            plan.display_indent_schema()
        );

        Ok(Self {
            database_name,
            table_name,
            table_handle,
            plan,
        })
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn table_handle(&self) -> &TableHandle {
        &self.table_handle
    }
}

#[async_trait]
impl TableSource for TableSourceAdapter {
    fn name(&self) -> &str {
        "TableSourceAdapter"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_handle.schema()
    }

    fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.table_handle.supports_filters_pushdown(filter)
    }

    fn supports_aggregate_pushdown(
        &self,
        group_expr: &[Expr],
        aggr_expr: &[Expr],
    ) -> DFResult<TableProviderAggregationPushDown> {
        self.table_handle
            .supports_aggregate_pushdown(group_expr, aggr_expr)
    }

    /// Called by [`InlineTableScan`]
    fn get_logical_plan(&self) -> Option<&LogicalPlan> {
        Some(&self.plan)
    }
}

#[async_trait]
impl WriteExecExt for TableSourceAdapter {
    async fn write(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Arc<TableWriterExec>> {
        let table_handle = self.table_handle();

        let table_write: &dyn WriteExecExt = match table_handle {
            TableHandle::Tskv(e) => e.as_ref() as _,
            TableHandle::External(e) => e.as_ref() as _,
            _ => {
                warn!("Table not support write.");
                return Err(DataFusionError::Plan(
                    "Table not support write.".to_string(),
                ));
            }
        };

        let result = table_write.write(state, input).await?;

        Ok(result)
    }
}

pub enum TableHandle {
    TableProvider(Arc<dyn TableProvider>),
    External(Arc<ListingTable>),
    Tskv(Arc<ClusterTable>),
    StreamProvider(StreamProviderRef),
}

impl TableHandle {
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::External(t) => t.schema(),
            Self::Tskv(t) => t.schema(),
            Self::StreamProvider(t) => t.schema(),
            Self::TableProvider(t) => t.schema(),
        }
    }

    pub fn supports_filters_pushdown(
        &self,
        filter: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        match self {
            Self::External(t) => t.supports_filters_pushdown(filter),
            Self::Tskv(t) => t.supports_filters_pushdown(filter),
            Self::StreamProvider(t) => t.supports_filters_pushdown(filter),
            Self::TableProvider(t) => t.supports_filters_pushdown(filter),
        }
    }

    pub fn supports_aggregate_pushdown(
        &self,
        group_expr: &[Expr],
        aggr_expr: &[Expr],
    ) -> DFResult<TableProviderAggregationPushDown> {
        match self {
            Self::External(t) => t.supports_aggregate_pushdown(group_expr, aggr_expr),
            Self::Tskv(t) => t.supports_aggregate_pushdown(group_expr, aggr_expr),
            Self::StreamProvider(t) => t.supports_aggregate_pushdown(group_expr, aggr_expr),
            Self::TableProvider(t) => t.supports_aggregate_pushdown(group_expr, aggr_expr),
        }
    }
}

impl From<Arc<dyn TableProvider>> for TableHandle {
    fn from(value: Arc<dyn TableProvider>) -> Self {
        TableHandle::TableProvider(value)
    }
}

impl From<Arc<ListingTable>> for TableHandle {
    fn from(value: Arc<ListingTable>) -> Self {
        TableHandle::External(value)
    }
}

impl From<Arc<ClusterTable>> for TableHandle {
    fn from(value: Arc<ClusterTable>) -> Self {
        TableHandle::Tskv(value)
    }
}

impl From<StreamProviderRef<i64>> for TableHandle {
    fn from(value: StreamProviderRef<i64>) -> Self {
        TableHandle::StreamProvider(value)
    }
}

impl Display for TableHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::External(e) => write!(f, "External({:?})", e.table_paths()),
            Self::StreamProvider(_) => write!(f, "StreamProvider"),
            Self::TableProvider(_) => write!(f, "TableProvider"),
            Self::Tskv(e) => write!(f, "Tskv({})", e.table_schema().name),
        }
    }
}
