use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{DFField, DFSchema};
use datafusion::error::Result;
use datafusion::logical_expr::{Aggregate, AggregateFunction, Expr, LogicalPlan, TableScan};
use datafusion::optimizer::optimizer::{ApplyOrder, Optimizer};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use models::arrow::{DataType, TimeUnit};

pub struct RecognitionAgg {}

impl OptimizerRule for RecognitionAgg {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::Aggregate(Aggregate { aggr_expr, .. }) = plan {
            let mut is_need_sort = false;
            aggr_expr.iter().for_each(|expr| {
                if let Expr::AggregateFunction(agg_func) = expr {
                    if agg_func.fun == AggregateFunction::Correlation
                        || agg_func.fun == AggregateFunction::Covariance
                        || agg_func.fun == AggregateFunction::CovariancePop
                        || agg_func.fun == AggregateFunction::Stddev
                        || agg_func.fun == AggregateFunction::StddevPop
                        || agg_func.fun == AggregateFunction::Variance
                        || agg_func.fun == AggregateFunction::VariancePop
                    {
                        is_need_sort = true;
                    }
                }
            });

            if is_need_sort {
                let optimizer = Optimizer::with_rules(vec![Arc::new(AddTimeForScan {})]);
                return optimizer.optimize_recursively(
                    optimizer.rules.first().unwrap(),
                    plan,
                    _optimizer_config,
                );
            }
        }

        Ok(None)
    }

    fn name(&self) -> &str {
        "recognition_agg"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

struct AddTimeForScan {}

impl OptimizerRule for AddTimeForScan {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if let LogicalPlan::TableScan(TableScan {
            table_name,
            source,
            projection,
            projected_schema,
            filters,
            agg_with_grouping,
            fetch,
        }) = plan
        {
            if let Some(projection) = projection.clone() {
                let mut new_projection = vec![0];
                for i in projection {
                    new_projection.push(i);
                }
                let mut new_projected_schema = DFSchema::new_with_metadata(
                    vec![DFField::new(
                        Some(table_name.table().to_string()),
                        "time",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    )],
                    HashMap::new(),
                )?;
                new_projected_schema.merge(projected_schema);
                return Ok(Some(LogicalPlan::TableScan(TableScan {
                    table_name: table_name.clone(),
                    source: source.clone(),
                    projection: Some(new_projection),
                    projected_schema: Arc::new(new_projected_schema),
                    filters: filters.clone(),
                    agg_with_grouping: agg_with_grouping.clone(),
                    fetch: *fetch,
                })));
            }
        }

        Ok(None)
    }

    fn name(&self) -> &str {
        "add_time_for_scan"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}
