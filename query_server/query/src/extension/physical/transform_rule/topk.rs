use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::planner::{create_physical_sort_expr, ExtensionPlanner};
use datafusion::physical_plan::{ExecutionPlan, PhysicalPlanner};
use datafusion::prelude::Expr;

use super::super::super::logical::plan_node::topk::TopKPlanNode;
use super::super::plan_node::topk::TopKExec;

/// Physical planner for TopK nodes
pub struct TopKPlanner {}

#[async_trait]
impl ExtensionPlanner for TopKPlanner {
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
            if let Some(topk_node) = node.as_any().downcast_ref::<TopKPlanNode>() {
                let physical_input = physical_inputs[0].clone();

                let input_schema = physical_input.as_ref().schema();
                let input_dfschema = topk_node.input().schema();

                let sort_exprs = topk_node
                    .expr()
                    .iter()
                    .map(|e| match e {
                        Expr::Sort {
                            expr,
                            asc,
                            nulls_first,
                        } => create_physical_sort_expr(
                            expr,
                            input_dfschema,
                            &input_schema,
                            SortOptions {
                                descending: !*asc,
                                nulls_first: *nulls_first,
                            },
                            &session_state.execution_props,
                        ),
                        _ => Err(DataFusionError::Plan(
                            "TopK only accepts sort expressions".to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>>>()?;

                // figure out input name
                Some(Arc::new(TopKExec::new(
                    physical_inputs[0].clone(),
                    sort_exprs,
                    topk_node.options().clone(),
                )))
            } else {
                None
            },
        )
    }
}
