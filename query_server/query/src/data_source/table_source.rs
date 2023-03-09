use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DFResult;
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
use models::oid::Oid;
use spi::query::datasource::stream::StreamProviderRef;
use trace::{debug, warn};

use super::batch::tskv::ClusterTable;
use super::WriteExecExt;
use crate::extension::logical::logical_plan_builder::LogicalPlanBuilderExt;
use crate::extension::physical::plan_node::table_writer::TableWriterExec;

pub const TEMP_LOCATION_TABLE_NAME: &str = "external_location_table";

pub struct TableSourceAdapter {
    tenant_id: Oid,
    tenant_name: String,
    database_name: String,
    table_name: String,
    table_handle: TableHandle,

    plan: LogicalPlan,
}

impl TableSourceAdapter {
    pub fn try_new(
        tenant_id: Oid,
        tenant_name: impl Into<String>,
        database_name: impl Into<String>,
        table_name: impl Into<String>,
        table_handle: impl Into<TableHandle>,
    ) -> Result<Self, DataFusionError> {
        let table_name = table_name.into();
        let table_handle = table_handle.into();
        let plan = match &table_handle {
            // TableScan
            TableHandle::External(t) => {
                let table_source = provider_as_source(t.clone());
                LogicalPlanBuilder::scan(&table_name, table_source, None)?.build()?
            }
            // TableScan
            TableHandle::Tskv(t) => {
                let table_source = provider_as_source(t.clone());
                LogicalPlanBuilder::scan(&table_name, table_source, None)?.build()?
            }
            // TableScan
            TableHandle::TableProvider(t) => {
                let table_source = provider_as_source(t.clone());
                LogicalPlanBuilder::scan(&table_name, table_source, None)?.build()?
            }
            // StreamScan
            TableHandle::StreamProvider(s) => {
                LogicalPlanBuilder::stream_scan(&table_name, s.clone())?
                    .watermark(s.watermark().clone())?
                    .build()?
            }
        };

        debug!(
            "Table logical plan node of {}:\n{}",
            table_name,
            plan.display_indent_schema()
        );

        Ok(Self {
            tenant_id,
            tenant_name: tenant_name.into(),
            database_name: database_name.into(),
            table_name,
            table_handle,
            plan,
        })
    }

    pub fn tenant_id(&self) -> Oid {
        self.tenant_id
    }

    pub fn tenant_name(&self) -> &str {
        &self.tenant_name
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
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.table_handle.schema()
    }

    fn supports_filter_pushdown(&self, filter: &Expr) -> DFResult<TableProviderFilterPushDown> {
        self.table_handle.supports_filter_pushdown(filter)
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
    StreamProvider(StreamProviderRef<i64>),
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

    pub fn supports_filter_pushdown(&self, filter: &Expr) -> DFResult<TableProviderFilterPushDown> {
        match self {
            Self::External(t) => t.supports_filter_pushdown(filter),
            Self::Tskv(t) => t.supports_filter_pushdown(filter),
            Self::StreamProvider(t) => t.supports_filter_pushdown(filter),
            Self::TableProvider(t) => t.supports_filter_pushdown(filter),
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
