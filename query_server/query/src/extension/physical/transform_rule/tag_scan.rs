use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    execution::context::SessionState,
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{planner::ExtensionPlanner, ExecutionPlan, PhysicalPlanner},
};

use crate::extension::logical::plan_node::tag_scan::TagScanPlanNode;

use datafusion::error::Result;

/// Physical planner for TopK nodes
pub struct TagScanPlanner {}

#[async_trait]
impl ExtensionPlanner for TagScanPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(TagScanPlanNode {
                table_name: _,
                source,
                projection: _,
                projected_schema,
                filters,
                fetch,
            }) = as_tag_scan_plan_node(node)
            {
                let tag_scan = source
                    .tag_scan(session_state, projected_schema, filters, *fetch)
                    .await?;

                Some(tag_scan)
            } else {
                None
            },
        )
    }
}

fn as_tag_scan_plan_node(node: &dyn UserDefinedLogicalNode) -> Option<&TagScanPlanNode> {
    node.as_any().downcast_ref::<TagScanPlanNode>()
}
