use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, Result as DFResult, ToDFSchema};
use datafusion::config::ConfigOptions;
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{col, Expr};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::expressions::{
    Correlation, Covariance, CovariancePop, Stddev, StddevPop, Variance, VariancePop,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::udaf::AggregateFunctionExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::create_physical_sort_expr;
use models::arrow::{DataType, Field, Schema, TimeUnit};
use models::schema::TIME_FIELD_NAME;

use crate::extension::physical::plan_node::tskv_exec::TskvExec;
use crate::extension::utils::downcast_execution_plan;

#[non_exhaustive]
pub struct AddSortExec {}

impl AddSortExec {
    pub fn new() -> Self {
        Self {}
    }

    fn optimize_inner(&self, plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            if let Some(tskv_exec) = downcast_execution_plan::<TskvExec>(plan.as_ref()) {
                let mut new_tskv_exec = tskv_exec.clone();
                let schema = new_tskv_exec.schema();
                let mut fields = schema
                    .all_fields()
                    .iter()
                    .map(|f| (**f).clone())
                    .collect::<Vec<Field>>();
                let mut contain_time = false;

                for field in &fields {
                    if field.name() == TIME_FIELD_NAME {
                        contain_time = true;
                    }
                }

                if !contain_time {
                    let mut time_field = Field::new(
                        "time",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        false,
                    );
                    let mut metadata = HashMap::new();
                    metadata.insert("column_encoding".to_string(), "DEFAULT".to_string());
                    metadata.insert("column_id".to_string(), "0".to_string());
                    time_field = time_field.with_metadata(metadata);
                    fields.push(time_field);
                    fields.reverse();
                    let new_schema = Arc::new(Schema::new_with_metadata(
                        fields.clone(),
                        schema.metadata().clone(),
                    ));
                    new_tskv_exec.set_schema(new_schema);
                }

                let physical_sort_expr = create_physical_sort_expr(
                    &Expr::Sort(Sort::new(Box::new(col("time")), true, false)),
                    &new_tskv_exec.schema().to_dfschema()?,
                    &new_tskv_exec.schema(),
                    &ExecutionProps::new(),
                )?;
                let sort_plan = Arc::new(
                    datafusion::physical_plan::sorts::sort::SortExec::new(
                        vec![physical_sort_expr.clone()],
                        Arc::new(new_tskv_exec) as Arc<dyn ExecutionPlan>,
                    )
                    .with_preserve_partitioning(true),
                );
                let sort_merge_plan = Arc::new(SortPreservingMergeExec::new(
                    vec![physical_sort_expr],
                    sort_plan,
                ));

                if !contain_time {
                    let mut physical_projection_exprs = Vec::new();
                    for field in fields.iter().skip(1) {
                        let col = Column::from_name(field.name());
                        let physical_projection_expr = create_physical_expr(
                            &Expr::Column(col),
                            &sort_merge_plan.schema().to_dfschema()?,
                            &sort_merge_plan.schema(),
                            &ExecutionProps::new(),
                        )?;
                        physical_projection_exprs
                            .push((physical_projection_expr, field.name().clone()));
                    }
                    let projection = Arc::new(ProjectionExec::try_new(
                        physical_projection_exprs,
                        sort_merge_plan,
                    )?);
                    return Ok(Transformed::Yes(projection));
                }

                return Ok(Transformed::Yes(sort_merge_plan));
            }

            Ok(Transformed::No(plan))
        })
    }
}

impl Default for AddSortExec {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalOptimizerRule for AddSortExec {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            if let Some(agg_exec) = downcast_execution_plan::<AggregateExec>(plan.as_ref()) {
                let mut is_need_sort = false;
                agg_exec.aggr_expr().iter().for_each(|expr| {
                    if let Some(agg_func_expr) =
                        expr.as_any().downcast_ref::<AggregateFunctionExpr>()
                    {
                        if agg_func_expr.fun().name == "stats_agg"
                            || agg_func_expr.fun().name == "gauge_agg"
                        {
                            is_need_sort = true;
                        }
                    } else if expr.as_any().is::<Correlation>()
                        || expr.as_any().is::<Covariance>()
                        || expr.as_any().is::<CovariancePop>()
                        || expr.as_any().is::<Stddev>()
                        || expr.as_any().is::<StddevPop>()
                        || expr.as_any().is::<Variance>()
                        || expr.as_any().is::<VariancePop>()
                    {
                        is_need_sort = true;
                    }
                });

                if is_need_sort {
                    return Ok(Transformed::Yes(self.optimize_inner(plan.clone())?));
                }
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "add_sort_exec"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
