use datafusion::common::tree_node::{Transformed, TransformedResult as _, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source_as_provider;
use datafusion::error::Result;
use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion::logical_expr::{Aggregate, LogicalPlan};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::Expr;

use crate::data_source::batch::tskv::ClusterTable;

#[derive(Debug)]
pub struct TransformCountGenTimeColRule {}

impl AnalyzerRule for TransformCountGenTimeColRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_internal).data()
    }

    fn name(&self) -> &str {
        "transform_count_gen_time_col"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if let LogicalPlan::Aggregate(Aggregate { aggr_expr, .. }) = &plan {
        for expr in aggr_expr {
            if let Expr::AggregateFunction(AggregateFunction {
                func,
                params: AggregateFunctionParams { args, .. },
            }) = &expr
            {
                if func.name() == "count" {
                    let mut only_literal = true;
                    for arg in args {
                        match arg {
                            Expr::Literal(_) => {}
                            _ => {
                                only_literal = false;
                                break;
                            }
                        }
                    }
                    if only_literal {
                        let mut plan_vec = vec![plan.clone()];
                        loop {
                            let last = plan_vec.last().unwrap().clone();
                            match last {
                                LogicalPlan::TableScan(scan) => {
                                    if source_as_provider(&scan.source)?
                                        .as_any()
                                        .downcast_ref::<ClusterTable>()
                                        .is_some()
                                    {
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
                                            new_plan = last.with_new_exprs(
                                                last.expressions(),
                                                vec![new_plan],
                                            )?;
                                        }

                                        return Ok(Transformed::yes(new_plan));
                                    } else {
                                        return Ok(Transformed::no(plan.clone()));
                                    }
                                }
                                LogicalPlan::Join(_) => {
                                    return Ok(Transformed::no(plan.clone()));
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

    Ok(Transformed::no(plan.clone()))
}
