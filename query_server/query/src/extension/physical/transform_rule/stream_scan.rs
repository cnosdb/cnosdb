use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::expr_rewriter::unnormalize_cols;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use spi::query::datasource::stream::Offset;

use crate::extension::logical::plan_node::stream_scan::StreamScanPlanNode;
use crate::extension::utils::downcast_plan_node;

/// Physical planner for StreamScan nodes
#[derive(Default)]
pub struct StreamScanPlanner {
    source_to_range: HashMap<String, (Option<Offset>, Offset)>,
}

impl StreamScanPlanner {
    pub fn new(source_to_range: HashMap<String, (Option<Offset>, Offset)>) -> Self {
        Self { source_to_range }
    }
}

#[async_trait]
impl ExtensionPlanner for StreamScanPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(StreamScanPlanNode {
            source,
            projection,
            filters,
            agg_with_grouping,
            ..
        }) = downcast_plan_node(node)
        {
            let unaliased = unnormalize_cols(filters.iter().cloned());

            let range = self.source_to_range.get(&source.id());

            let plan = source
                .scan(
                    session_state,
                    projection.as_ref(),
                    &unaliased,
                    agg_with_grouping.as_ref(),
                    range,
                )
                .await?;

            Ok(Some(plan))
        } else {
            Ok(None)
        }
    }
}
