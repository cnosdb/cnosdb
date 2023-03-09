use std::sync::Arc;

use datafusion::common::DFSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::prelude::Expr;
use models::schema::Watermark;
use spi::query::datasource::stream::StreamProviderRef;

use super::plan_node::expand::ExpandNode;
use super::plan_node::stream_scan::StreamScanPlanNode;
use super::plan_node::watermark::WatermarkNode;

/// Used to extend the function of datafusion's [`LogicalPlanBuilder`]
pub trait LogicalPlanBuilderExt: Sized {
    /// Apply a expand with specific projections
    fn expand(self, projections: Vec<Vec<Expr>>) -> Result<Self>;
    fn watermark(self, watermark: Watermark) -> Result<Self>;
    fn stream_scan(table_name: impl Into<String>, table_source: StreamProviderRef) -> Result<Self>;
}

impl LogicalPlanBuilderExt for LogicalPlanBuilder {
    fn expand(self, projections: Vec<Vec<Expr>>) -> Result<Self> {
        let input = Arc::new(self.build()?);

        let expand = Arc::new(ExpandNode::try_new(projections, input)?);

        let plan = LogicalPlan::Extension(Extension { node: expand });

        Ok(Self::from(plan))
    }

    fn watermark(self, watermark: Watermark) -> Result<Self> {
        let input = Arc::new(self.build()?);

        let Watermark { column, delay } = watermark;

        let watermark_node = Arc::new(WatermarkNode::try_new(column, delay, input)?);

        let plan = LogicalPlan::Extension(Extension {
            node: watermark_node,
        });

        Ok(Self::from(plan))
    }

    /// Convert a stream provider into a builder with a [`StreamScanPlanNode`]
    fn stream_scan(table_name: impl Into<String>, table_source: StreamProviderRef) -> Result<Self> {
        let table_name = table_name.into();

        if table_name.is_empty() {
            return Err(DataFusionError::Plan(
                "table_name cannot be empty".to_string(),
            ));
        }

        let schema = table_source.schema();

        let projected_schema = DFSchema::try_from_qualified_schema(&table_name, &schema)?;

        let node = Arc::new(StreamScanPlanNode {
            table_name,
            source: table_source,
            projected_schema: Arc::new(projected_schema),
            projection: None,
            filters: vec![],
            agg_with_grouping: None,
            fetch: None,
        });

        Ok(Self::from(LogicalPlan::Extension(Extension { node })))
    }
}
