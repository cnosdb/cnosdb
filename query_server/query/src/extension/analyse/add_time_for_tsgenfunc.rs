use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::{Extension, Filter, LogicalPlan, Projection};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::Expr;

use crate::extension::expr::TsGenFunc;
use crate::extension::logical::plan_node::ts_gen_func::TSGenFuncNode;
use crate::extension::utils::downcast_plan_node;

pub struct AddTimeForTSGenFunc {}

impl AnalyzerRule for AddTimeForTSGenFunc {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_internal)
    }

    fn name(&self) -> &str {
        "AddTimeForTSGenFunc"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Projection(Projection { expr, input, .. }) = &plan {
        let mut temp_input = input;

        if let LogicalPlan::Filter(Filter { input, .. }) = temp_input.as_ref() {
            temp_input = input;
        }

        if let LogicalPlan::Extension(Extension { node }) = temp_input.as_ref() {
            if let Some(tsgenfunc) = downcast_plan_node::<TSGenFuncNode>(node.as_ref()) {
                if tsgenfunc.symbol == TsGenFunc::TimestampRepair {
                    let mut new_expr = expr.clone();
                    new_expr.insert(0, Expr::Column(Column::new(None, "time")));
                    return Ok(Transformed::Yes(LogicalPlan::Projection(
                        Projection::try_new(new_expr, input.clone())?,
                    )));
                }
            }
        }
    }

    Ok(Transformed::No(plan))
}
