use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use spi::query::stream::watermark_tracker::WatermarkTrackerRef;

use crate::extension::logical::plan_node::watermark::WatermarkNode;
use crate::extension::physical::plan_node::watermark::WatermarkExec;
use crate::extension::utils::downcast_plan_node;

/// Physical planner for TopK nodes
pub struct WatermarkPlanner {
    watermark_tracker: WatermarkTrackerRef,
}

impl WatermarkPlanner {
    pub fn new(watermark_tracker: WatermarkTrackerRef) -> Self {
        Self { watermark_tracker }
    }
}

#[async_trait]
impl ExtensionPlanner for WatermarkPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(WatermarkNode { watermark, .. }) = downcast_plan_node(node) {
            assert!(physical_inputs.len() == 1);
            let child = physical_inputs[0].clone();

            let plan =
                WatermarkExec::try_new(watermark.clone(), self.watermark_tracker.clone(), child)?;

            Ok(Some(Arc::new(plan)))
        } else {
            Ok(None)
        }
    }
}
