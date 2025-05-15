// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Push Down Aggregation optimizer rule ensures that aggregations are applied as early as possible in the plan

use std::ops::Deref;
use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::common::Column;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::functions_aggregate::sum::Sum;
use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{
    Aggregate, AggregateUDF, LogicalPlan, LogicalPlanBuilder, Projection,
    TableProviderAggregationPushDown, TableScan, TableScanAggregate,
};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;

/// Push Down Aggregation optimizer rule pushes aggregation clauses down the plan
/// # Introduction
/// TODO
#[derive(Default, Debug)]
pub struct PushDownAggregation {}

impl PushDownAggregation {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for PushDownAggregation {
    fn name(&self) -> &str {
        "push_down_aggregation"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        if let LogicalPlan::Aggregate(Aggregate {
            input,
            group_expr,
            aggr_expr,
            schema,
            ..
        }) = plan
        {
            if !determine_whether_support_push_down(&aggr_expr) {
                return Ok(None);
            }

            let mut temp_input = input;

            if let LogicalPlan::Projection(Projection { input, .. }) = temp_input.deref() {
                temp_input = input.clone();
            }

            if let LogicalPlan::TableScan(TableScan {
                table_name,
                source,
                projection: _,
                projected_schema: _,
                filters,
                aggregate,
                fetch,
            }) = temp_input.deref()
            {
                if aggregate.is_none() && filters.is_empty() {
                    let new_plan = match source
                        .supports_aggregate_pushdown(&group_expr, &aggr_expr)?
                    {
                        TableProviderAggregationPushDown::Unsupported => None,
                        TableProviderAggregationPushDown::Ungrouped => {
                            // Save final agg node, can remove partial agg node
                            // Change the optimized logical plan to reflect the pushed down aggregate
                            //
                            // e.g.
                            //
                            // Aggregate: groupBy=[[]], aggr=[[min(c1), max(c1)]]
                            //   TableScan: t1 projection=[c1]
                            // ->
                            // == Optimized Logical Plan ==
                            // Aggregate: groupBy=[[]], aggr=[[min(min(c1)) as min(c1), max(max(c1)) as max(c1)]]
                            //   TableScan: t1 projection=[c1] groupBy=[[]], aggr=[[min(c1), max(c1)]]
                            let new_agg_expr_with_alias = aggr_expr
                            .iter()
                            .map(|e| {
                                let col_name = e.display_name()?;
                                let column = Column::from_name(col_name.clone());

                                let new_expr = match e {
                                    Expr::AggregateFunction(AggregateFunction {
                                        func,
                                        params: AggregateFunctionParams {
                                            args,
                                            distinct,
                                            filter,
                                            order_by,
                                            null_treatment,
                                            can_be_pushed_down,
                                        },
                                    }) => {
                                        let new_agg_func = match func.name() {
                                            /* AggregateFunctionName::Max => {
                                                AggregateFunction {
                                                    fun: AggregateFunctionName::Max,
                                                    args: vec![Expr::Column(column)],
                                                    distinct: *distinct,
                                                    filter: filter.clone(),
                                                    order_by: order_by.clone(),
                                                }
                                            },
                                            AggregateFunctionName::Min => {
                                                AggregateFunction {
                                                    fun: AggregateFunctionName::Min,
                                                    args: vec![Expr::Column(column)],
                                                    distinct: *distinct,
                                                    filter: filter.clone(),
                                                    order_by: order_by.clone(),
                                                }
                                            },
                                            AggregateFunctionName::Sum => {
                                                AggregateFunction {
                                                    fun: AggregateFunctionName::Sum,
                                                    args: vec![Expr::Column(column)],
                                                    distinct: *distinct,
                                                    filter: filter.clone(),
                                                    order_by: order_by.clone(),
                                                }
                                            }, */
                                            "count" => {
                                                AggregateFunction::new_udf(
                                                    Arc::new(AggregateUDF::new_from_impl(Sum::new())),
                                                    vec![Expr::Column(column)],
                                                    *distinct,
                                                    filter.clone(),
                                                    order_by.clone(),
                                                    null_treatment.clone(),
                                                    *can_be_pushed_down
                                                )
                                            },
                                            // not support other agg func
                                            _ => return Err(DataFusionError::Internal(format!("Unreachable, not support {func:?} push down."))),
                                        };

                                        Expr::AggregateFunction(new_agg_func)
                                    },
                                    _ => return Err(DataFusionError::Internal("Invalid logical plan, Aggregate's aggr_expr contains non-aggregate expr.".to_string())),
                                };

                                let alias = Expr::Column(Column::from_name(new_expr.schema_name())).alias(col_name);

                                Ok((new_expr, alias))
                            })
                            .collect::<Result<Vec<_>>>()?;

                            let (new_agg_expr, projection_agg_expr): (Vec<_>, Vec<_>) =
                                new_agg_expr_with_alias.into_iter().unzip();

                            // Find distinct group by exprs in the case where we have a grouping set
                            let mut new_required_columns = Default::default();
                            let all_group_expr: Vec<Expr> = grouping_set_to_exprlist(&group_expr)?;
                            exprlist_to_columns(&all_group_expr, &mut new_required_columns)?;

                            let projection_expr = new_required_columns
                                .into_iter()
                                .map(Expr::Column)
                                .chain(projection_agg_expr)
                                .collect::<Vec<_>>();

                            let new_table_scan = LogicalPlan::TableScan(TableScan {
                                table_name: table_name.clone(),
                                source: source.clone(),
                                projection: None,
                                projected_schema: schema.clone(),
                                filters: filters.clone(),
                                fetch: *fetch,
                                aggregate: Some(TableScanAggregate {
                                    group_expr: group_expr.clone(),
                                    aggr_expr: aggr_expr.clone(),
                                    schema: schema.clone(),
                                }),
                            });

                            let new_plan = LogicalPlanBuilder::from(new_table_scan)
                                .aggregate(group_expr.clone(), new_agg_expr)?
                                .project(projection_expr)?
                                .build()?;

                            Some(new_plan)
                        }
                        TableProviderAggregationPushDown::Grouped => {
                            // Remove `Aggregate` node
                            // Change the optimized logical plan to reflect the pushed down aggregate
                            //
                            // e.g.
                            //
                            // Aggregate: groupBy=[[]], aggr=[[min(c1), max(c1)]]
                            //   TableScan: t1 projection=[c1]
                            // ->
                            // == Optimized Logical Plan ==
                            // TableScan: t1 projection=[c1] groupBy=[[]], aggr=[[min(c1), max(c1)]]
                            Some(LogicalPlan::TableScan(TableScan {
                                table_name: table_name.clone(),
                                source: source.clone(),
                                projection: None,
                                projected_schema: schema.clone(),
                                filters: filters.clone(),
                                fetch: *fetch,
                                aggregate: Some(TableScanAggregate {
                                    group_expr: group_expr.clone(),
                                    aggr_expr: aggr_expr.clone(),
                                    schema: schema.clone(),
                                }),
                            }))
                        }
                    };

                    return Ok(new_plan);
                }
            };
        };

        optimize_children(self, plan, config)
    }
}

fn determine_whether_support_push_down(aggr_expr: &[Expr]) -> bool {
    aggr_expr.iter().all(|e| match e {
        Expr::AggregateFunction(AggregateFunction {
            func,
            params:
                AggregateFunctionParams {
                    args,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                    can_be_pushed_down,
                },
            ..
        }) => {
            let support_agg_func = matches!(
                func.name(),
                "count" // | "sum"
                        // | "min"
                        // | "max"
            );

            support_agg_func && !distinct
        }
        _ => false,
    })
}
