use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{displayable, ExecutionPlan, PhysicalPlanner};
use trace::{debug, trace};

use crate::data_source::WriteExecExt;
use crate::extension::logical::plan_node::table_writer::{
    as_table_writer_plan_node, TableWriterPlanNode,
};

/// Physical planner for TableWriter nodes
pub struct TableWriterPlanner {}

#[async_trait]
impl ExtensionPlanner for TableWriterPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(
            if let Some(TableWriterPlanNode {
                target_table_name: _,
                target_table,
                ..
            }) = as_table_writer_plan_node(node)
            {
                debug!("Input user defined logical node: TableWriterPlanNode");
                trace!("Full input user defined logical plan:\n{:?}", node);

                debug_assert_eq!(
                    1,
                    physical_inputs.len(),
                    "TableWriterPlanNode has multiple inputs."
                );
                let physical_input = physical_inputs[0].clone();

                let table_provider = source_as_provider(target_table)?;

                let result = table_provider.write(session_state, physical_input).await?;

                debug!(
                    "After Apply TableWriterPlanner. Transformed physical plan: {}",
                    displayable(result.as_ref()).indent()
                );
                trace!("Full transformed physical plan:\n {:?}", result);

                Some(result)
            } else {
                None
            },
        )
    }
}
