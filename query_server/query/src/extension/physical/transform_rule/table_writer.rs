use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::planner::{create_aggregate_expr, ExtensionPlanner};
use datafusion::physical_plan::{displayable, ExecutionPlan, PhysicalPlanner};
use trace::{debug, trace};

use crate::data_source::{source_downcast_adapter, WriteExecExt};
use crate::extension::logical::plan_node::table_writer::TableWriterPlanNode;
use crate::extension::logical::plan_node::table_writer_merge::TableWriterMergePlanNode;
use crate::extension::physical::plan_node::table_writer_merge::TableWriterMergeExec;
use crate::extension::utils::downcast_plan_node;

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
            }) = downcast_plan_node::<TableWriterPlanNode>(node)
            {
                debug!("Input user defined logical node: TableWriterPlanNode");
                trace!("Full input user defined logical plan:\n{:?}", node);

                debug_assert_eq!(
                    1,
                    physical_inputs.len(),
                    "TableWriterPlanNode has multiple inputs."
                );

                let batch_size = session_state.config().batch_size();
                // CoalesceBatchesExec combines small batches into larger batches for more efficient writing
                let physical_input = Arc::new(CoalesceBatchesExec::new(
                    physical_inputs[0].clone(),
                    batch_size,
                ));

                let table_provider = source_downcast_adapter(target_table)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let result = table_provider.write(session_state, physical_input).await?;

                debug!(
                    "After Apply TableWriterPlanner. Transformed physical plan: {}",
                    displayable(result.as_ref()).indent()
                );
                trace!("Full transformed physical plan:\n {:?}", result);

                Some(result)
            } else if let Some(TableWriterMergePlanNode {
                input, agg_expr, ..
            }) = downcast_plan_node::<TableWriterMergePlanNode>(node)
            {
                debug!("Input user defined logical node: TableWriterMergePlanNode");
                trace!("Full input user defined logical plan:\n{:?}", node);

                debug_assert_eq!(
                    1,
                    physical_inputs.len(),
                    "TableWriterMergePlanNode has multiple inputs."
                );
                let physical_input = physical_inputs[0].clone();

                let logical_input_schema = input.schema();
                let physical_input_schema = physical_input.schema();
                let physical_aggr_expr = agg_expr
                    .iter()
                    .map(|e| {
                        create_aggregate_expr(
                            e,
                            logical_input_schema.as_ref(),
                            physical_input_schema.as_ref(),
                            session_state.execution_props(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let result = Arc::new(TableWriterMergeExec::try_new(
                    physical_input,
                    physical_aggr_expr,
                )?);

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
