use std::sync::Arc;

use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::planner::ExtensionPlanner;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, PhysicalPlanner};

use crate::extension::logical::plan_node::gapfill::{FillStrategy, GapFill};
use crate::extension::physical::plan_node::gapfill::{GapFillExec, GapFillExecParams};
use crate::extension::utils::{downcast_plan_node, try_map_bound, try_map_range};

/// Physical planner for GapFill nodes
#[derive(Default)]
pub struct GapFillPlanner {}

impl GapFillPlanner {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl ExtensionPlanner for GapFillPlanner {
    /// Create a physical plan for an extension node
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(match downcast_plan_node::<GapFill>(node) {
            Some(gap_fill) => {
                assert_eq!(1, physical_inputs.len());

                let gap_fill_exec = plan_gap_fill(
                    session_state.execution_props(),
                    gap_fill,
                    logical_inputs,
                    physical_inputs,
                )?;

                Some(Arc::new(gap_fill_exec))
            }
            _ => None,
        })
    }
}

/// Called by the extension planner to plan a [GapFill] node.
fn plan_gap_fill(
    execution_props: &ExecutionProps,
    gap_fill: &GapFill,
    logical_inputs: &[&LogicalPlan],
    physical_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<GapFillExec> {
    if logical_inputs.len() != 1 {
        return Err(DataFusionError::Internal(
            "GapFillExec: wrong number of logical inputs".to_string(),
        ));
    }
    if physical_inputs.len() != 1 {
        return Err(DataFusionError::Internal(
            "GapFillExec: wrong number of physical inputs".to_string(),
        ));
    }

    let input_dfschema = logical_inputs[0].schema().as_ref();
    let input_schema = physical_inputs[0].schema();
    let input_schema = input_schema.as_ref();

    let group_expr: Result<Vec<_>> = gap_fill
        .group_expr
        .iter()
        .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
        .collect();
    let group_expr = group_expr?;

    let aggr_expr: Result<Vec<_>> = gap_fill
        .aggr_expr
        .iter()
        .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
        .collect();
    let aggr_expr = aggr_expr?;

    let logical_time_column = gap_fill.params.time_column.try_into_col()?;
    let time_column = Column::new_with_schema(&logical_time_column.name, input_schema)?;

    let stride = create_physical_expr(
        &gap_fill.params.stride,
        input_dfschema,
        input_schema,
        execution_props,
    )?;

    let sliding = create_physical_expr(
        &gap_fill.params.sliding,
        input_dfschema,
        input_schema,
        execution_props,
    )?;

    let time_range = &gap_fill.params.time_range;
    let time_range = try_map_range(time_range, |b| {
        try_map_bound(b.as_ref(), |e| {
            create_physical_expr(e, input_dfschema, input_schema, execution_props)
        })
    })?;

    let origin = gap_fill
        .params
        .origin
        .as_ref()
        .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
        .transpose()?;

    let fill_strategy = gap_fill
        .params
        .fill_strategy
        .iter()
        .map(|(e, fs)| {
            Ok((
                create_physical_expr(e, input_dfschema, input_schema, execution_props)?,
                fs.clone(),
            ))
        })
        .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, FillStrategy)>>>()?;

    let params = GapFillExecParams::new(
        stride,
        sliding,
        time_column,
        origin,
        time_range,
        fill_strategy,
    );
    GapFillExec::try_new(
        Arc::clone(&physical_inputs[0]),
        group_expr,
        aggr_expr,
        params,
    )
}
