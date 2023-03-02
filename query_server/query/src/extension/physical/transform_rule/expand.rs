use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DFSchema;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::planner::{physical_name, ExtensionPlanner};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, PhysicalPlanner};
use datafusion::prelude::Expr;
use models::errors::tuple_err;

use crate::extension::logical::plan_node::expand::ExpandNode;
use crate::extension::physical::plan_node::expand::ExpandExec;

/// Physical planner for Expand nodes
#[derive(Default)]
pub struct ExpandPlanner {}

impl ExpandPlanner {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for ExpandPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(match as_expand_plan_node(node) {
            Some(ExpandNode {
                projections, input, ..
            }) => {
                assert_eq!(1, physical_inputs.len());

                let input_exec = physical_inputs[0].clone();
                let input_schema = input.as_ref().schema();

                let physical_exprs = projections
                    .iter()
                    .map(|expr| {
                        create_physical_expr_with_name(
                            session_state,
                            planner,
                            expr,
                            input_exec.clone(),
                            input_schema,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                Some(Arc::new(ExpandExec::try_new(physical_exprs, input_exec)?))
            }
            _ => None,
        })
    }
}

fn create_physical_expr_with_name(
    session_state: &SessionState,
    planner: &dyn PhysicalPlanner,
    expr: &[Expr],
    input_exec: Arc<dyn ExecutionPlan>,
    input_schema: &DFSchema,
) -> Result<Vec<(Arc<dyn PhysicalExpr>, String)>> {
    expr.iter()
        .map(|e| {
            let physical_name = if let Expr::Column(col) = e {
                match input_schema.index_of_column(col) {
                    Ok(idx) => {
                        // index physical field using logical field index
                        Ok(input_exec.schema().field(idx).name().to_string())
                    }
                    // logical column is not a derived column, safe to pass along to
                    // physical_name
                    Err(_) => physical_name(e),
                }
            } else {
                physical_name(e)
            };

            tuple_err((
                planner.create_physical_expr(e, input_schema, &input_exec.schema(), session_state),
                physical_name,
            ))
        })
        .collect::<Result<Vec<_>>>()
}

fn as_expand_plan_node(node: &dyn UserDefinedLogicalNode) -> Option<&ExpandNode> {
    node.as_any().downcast_ref::<ExpandNode>()
}
