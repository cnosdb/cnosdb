use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Column;
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::expr::{AggregateFunction, AggregateUDF};
use datafusion::logical_expr::{aggregate_function, Aggregate, LogicalPlan, Projection};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;

/// convert exact_count to count, but unsupported pushdown
pub struct TransformExactCountToCountRule {}

impl AnalyzerRule for TransformExactCountToCountRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_internal)
    }

    fn name(&self) -> &str {
        "transform_exact_count_to_count"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Projection(Projection { expr, input, .. }) = &plan {
        if let LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            ..
        }) = input.as_ref()
        {
            if aggr_expr.len() == 1 {
                if let Expr::AggregateUDF(AggregateUDF {
                    fun,
                    args,
                    filter,
                    order_by,
                }) = &aggr_expr[0]
                {
                    if fun.name == "exact_count" {
                        let new_aggr_expr = vec![Expr::AggregateFunction(AggregateFunction {
                            fun: aggregate_function::AggregateFunction::Count,
                            args: args.clone(),
                            distinct: false,
                            filter: filter.clone(),
                            order_by: order_by.clone(),
                        })];
                        let new_group_expr = if group_expr.is_empty() {
                            // add a dummy group by for forbid pushdown
                            vec![Expr::Literal(ScalarValue::Boolean(None))]
                        } else {
                            group_expr.clone()
                        };
                        let new_aggr_plan = Arc::new(LogicalPlan::Aggregate(Aggregate::try_new(
                            input.clone(),
                            new_group_expr,
                            new_aggr_expr,
                        )?));
                        let mut new_proj_expr = expr.clone();
                        for e in &mut new_proj_expr {
                            if let Expr::Column(Column { name, .. }) = e {
                                if let Some(new_name) =
                                    name.replacen("exact_count", "COUNT", 1).into()
                                {
                                    *name = new_name;
                                }
                            }
                        }
                        let new_proj_plan = LogicalPlan::Projection(Projection::try_new(
                            new_proj_expr,
                            new_aggr_plan,
                        )?);
                        return Ok(Transformed::Yes(new_proj_plan));
                    }
                }
            }
        }
    }

    Ok(Transformed::No(plan.clone()))
}
