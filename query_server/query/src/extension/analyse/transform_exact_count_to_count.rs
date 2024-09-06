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

/// Convert query statement to query tag operation
///
/// Triggering conditions:
/// 1. convert exact_count to count
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
    if let LogicalPlan::Projection(Projection { input, .. }) = &plan {
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
                    filter,
                    order_by,
                    ..
                }) = &aggr_expr[0]
                {
                    if fun.name == "exact_count_star" {
                        let new_aggr_expr = vec![Expr::AggregateFunction(AggregateFunction {
                            fun: aggregate_function::AggregateFunction::Count,
                            args: vec![Expr::Literal(ScalarValue::UInt8(Some(0)))],
                            distinct: false,
                            filter: filter.clone(),
                            order_by: order_by.clone(),
                        })];
                        let new_aggr_plan = Arc::new(LogicalPlan::Aggregate(Aggregate::try_new(
                            input.clone(),
                            group_expr.clone(),
                            new_aggr_expr,
                        )?));
                        let new_proj_expr = vec![Expr::Column(Column {
                            relation: None,
                            name: "COUNT(UInt8(0))".to_string(),
                        })];
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
