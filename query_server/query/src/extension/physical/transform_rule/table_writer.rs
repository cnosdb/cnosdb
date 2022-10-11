use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    error::DataFusionError,
    execution::context::SessionState,
    logical_plan::{source_as_provider, LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{displayable, planner::ExtensionPlanner, ExecutionPlan, PhysicalPlanner},
};
use trace::debug;
use trace::trace;

use crate::{
    extension::logical::plan_node::table_writer::TableWriterPlanNode, table::ClusterTable,
};

use datafusion::error::Result;

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
                target_table_name,
                target_table,
                ..
            }) = as_table_writer_plan_node(node)
            {
                debug!("Input user defined logical node: TableWriterPlanNode");
                trace!("Full input user defined logical plan:\n{:?}", node);

                let physical_input = physical_inputs[0].clone();

                let table_provider = source_as_provider(target_table)?;

                let table_writer = table_provider
                    .as_any()
                    .downcast_ref::<ClusterTable>()
                    .ok_or_else(|| {
                        debug!("Table {} not support write.", target_table_name);

                        DataFusionError::Plan(format!(
                            "Table {} not support write.",
                            target_table_name
                        ))
                    })?;

                debug!("Success to resolve table_writer.");

                let result = table_writer.write(session_state, physical_input).await?;

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

fn as_table_writer_plan_node(node: &dyn UserDefinedLogicalNode) -> Option<&TableWriterPlanNode> {
    node.as_any().downcast_ref::<TableWriterPlanNode>()
}
