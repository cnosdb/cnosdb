use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::{aggregate_function, Aggregate, LogicalPlan};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::Expr;

pub struct TransformCountGenTimeColRule {}

impl AnalyzerRule for TransformCountGenTimeColRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_internal)
    }

    fn name(&self) -> &str {
        "transform_count_gen_time_col"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Aggregate(Aggregate { aggr_expr, .. }) = &plan {
        if aggr_expr.len() == 1 {
            if let Expr::AggregateFunction(AggregateFunction { fun, args, .. }) = &aggr_expr[0] {
                if fun == &aggregate_function::AggregateFunction::Count && args.len() == 1 {
                    if let Expr::Literal(_) = &args[0] {
                        let mut plan_vec = vec![plan.clone()];
                        loop {
                            let last = plan_vec.last().unwrap().clone();
                            match last {
                                LogicalPlan::TableScan(scan) => {
                                    // add time column to projection
                                    let mut new_projection =
                                        scan.projection.clone().unwrap_or_default();
                                    new_projection.insert(0, 0_usize);
                                    let mut new_scan = scan.clone();
                                    new_scan.projection = Some(new_projection);

                                    // change plan
                                    plan_vec.pop();
                                    let mut new_plan = LogicalPlan::TableScan(new_scan);
                                    while let Some(last) = plan_vec.pop() {
                                        new_plan = last.with_new_inputs(&[new_plan])?;
                                    }

                                    return Ok(Transformed::Yes(new_plan));
                                }
                                LogicalPlan::Join(_) | LogicalPlan::CrossJoin(_) => {
                                    return Ok(Transformed::No(plan.clone()));
                                }
                                _ => {}
                            }
                            plan_vec.push(last.inputs()[0].clone());
                        }
                    }
                }
            }
        }
    }

    Ok(Transformed::No(plan.clone()))
}
