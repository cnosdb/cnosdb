use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::Result;
use datafusion::logical_expr::{Cast, LogicalPlan};
use datafusion::optimizer::optimizer::{ApplyOrder, OptimizerRule};
use datafusion::optimizer::OptimizerConfig;
use datafusion::prelude::Expr;
use models::schema::{OptimizerRuleConfig, Precision};

use crate::extension::logical::plan_node::LogicalPlanExt;

pub struct CastTimeUseConfigPrecision;

impl OptimizerRule for CastTimeUseConfigPrecision {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        let to_unit: TimeUnit = Precision::new(
            config
                .options()
                .extensions
                .get::<OptimizerRuleConfig>()
                .map(|item| item.input_precision.as_str())
                .unwrap_or("NS"),
        )
        .unwrap_or_default()
        .into();
        let plan = plan.transform_expressions_down(&|expr: &Expr| match expr {
            Expr::Cast(Cast { expr, data_type }) => {
                if let DataType::Timestamp(_, _) = data_type {
                    let new_cast = Cast {
                        expr: Box::new(Expr::Cast(Cast {
                            expr: expr.clone(),
                            data_type: DataType::Timestamp(to_unit.clone(), None),
                        })),
                        data_type: data_type.clone(),
                    };
                    Some(Expr::Cast(new_cast))
                } else {
                    None
                }
            }
            _ => None,
        })?;
        Ok(Some(plan))
    }

    fn name(&self) -> &str {
        "cast_time_use_config_precision"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}
