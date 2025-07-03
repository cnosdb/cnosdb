use datafusion::common::tree_node::{Transformed, TransformedResult as _, TreeNode};
use datafusion::common::Column;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::{Extension, Filter, LogicalPlan, Projection};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::Expr;

use crate::extension::expr::TimeSeriesGenFunc;
use crate::extension::logical::plan_node::ts_gen_func::TimeSeriesGenFuncNode;
use crate::extension::utils::downcast_plan_node;

#[derive(Debug)]
pub struct AddTimeForTimeSeriesGenFunc {}

impl AnalyzerRule for AddTimeForTimeSeriesGenFunc {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_internal).data()
    }

    fn name(&self) -> &str {
        "AddTimeForTimeSeriesGenFunc"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Projection(Projection { expr, input, .. }) = &plan {
        let mut temp_input = input;

        if let LogicalPlan::Filter(Filter { input, .. }) = temp_input.as_ref() {
            temp_input = input;
        }

        if let LogicalPlan::Extension(Extension { node }) = temp_input.as_ref() {
            if let Some(ts_gen_func) = downcast_plan_node::<TimeSeriesGenFuncNode>(node.as_ref()) {
                if ts_gen_func.symbol == TimeSeriesGenFunc::TimestampRepair {
                    let mut new_expr = expr.clone();
                    new_expr.insert(0, Expr::Column(Column::new_unqualified("time")));
                    return Ok(Transformed::yes(LogicalPlan::Projection(
                        Projection::try_new(new_expr, input.clone())?,
                    )));
                }
            }
        }
    }

    Ok(Transformed::no(plan))
}
