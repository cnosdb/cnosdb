use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::DFSchema;
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{Alias, ScalarFunction, Sort as SortExpr};
use datafusion::logical_expr::{
    Expr, ExprSchemable, Extension, LogicalPlan, Projection, ScalarUDF, Sort,
};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::scalar::ScalarValue;
use models::arrow::{DataType, Field};
use spi::DFResult;

use crate::extension::expr::TimeSeriesGenFunc;
use crate::extension::logical::plan_node::ts_gen_func::TimeSeriesGenFuncNode;

/// Transform scalar UDF `TimeSeriesGenFunc` in a logical plan'a top-level expression
/// into a plan node instead of calling the registered UDF.
///
/// - If there are another expressions in the plan, it returns an error.
/// - If the UDF is not `TimeSeriesGenFunc`, it returns the original plan.
/// - If the UDF is `TimeSeriesGenFunc`, it returns a new plan:
pub struct TransformTimeSeriesGenFunc;

impl AnalyzerRule for TransformTimeSeriesGenFunc {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up(|plan| Self::analyze_internal(plan))
    }

    fn name(&self) -> &str {
        "TransformTimeSeriesGenFunc"
    }
}

impl TransformTimeSeriesGenFunc {
    fn analyze_internal(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        for expr in plan.expressions() {
            let (expr, alias) = if let Expr::Alias(Alias { expr, name, .. }) = &expr {
                (expr.as_ref(), Cow::Borrowed(name))
            } else {
                (expr, Cow::Owned(expr.to_string()))
            };
            if let Expr::ScalarFunction(ScalarFunction { func, args }) = expr {
                if let Some(ts_gen_func) = TimeSeriesGenFunc::try_from_str(&func.name()) {
                    if let LogicalPlan::Projection(projection) = &plan {
                        if projection.expr.len() != 1 {
                            return Err(datafusion::error::DataFusionError::Plan(
                                format!("Only one expression supported when using time-series-generation functions, but got {} expressions",
                                    projection.expr.len()),
                            ));
                        }
                        let new_plan =
                            Self::wrap_plan(func, args, &projection.input, &alias, ts_gen_func)?;
                        return Ok(Transformed::Yes(new_plan));
                    } else {
                        return Err(datafusion::error::DataFusionError::Plan(
                            "Time-series-generation function must be used in SELECT clause"
                                .to_string(),
                        ));
                    }
                }
            }
        }
        Ok(Transformed::No(plan))
    }

    /// For input plan **input**, return
    ///
    /// - Extension: TimeSeriesGenFuncNode
    ///   - Sort: sort by time asc
    ///     - Projection: time, field
    ///       - **input**
    fn wrap_plan(
        udf: &ScalarUDF,
        args: &[Expr],
        input: &LogicalPlan,
        alias: &str,
        symbol: TimeSeriesGenFunc,
    ) -> DFResult<LogicalPlan> {
        let arg_types = args
            .iter()
            .map(|arg| arg.get_type(input.schema()))
            .collect::<DFResult<Vec<_>>>()?;

        let time_type = &arg_types[0];
        let time_expr = &args[0];
        let field_expr = &args[1];
        let field_type = udf.return_type(&arg_types)?;
        let arg_expr = if args.len() >= 3 {
            let ext_arg = &args[args.len() - 1];
            // TODO(zipper): ext_arg is a string expression but not a literal value.
            if !matches!(ext_arg, Some(Expr::Literal(ScalarValue::Utf8(Some(_))))) {
                return Err(datafusion::error::DataFusionError::Plan(format!(
                    "{} expects the last argument to be a string, but got {ext_arg:?}",
                    udf.name(),
                )));
            }
            Some(ext_arg.clone())
        } else {
            None
        };

        let new_projection = Arc::new(LogicalPlan::Projection(Projection::try_new(
            vec![time_expr.clone(), field_expr.clone()],
            Arc::new(input),
        )?));

        let new_sort = Arc::new(LogicalPlan::Sort(Sort {
            input: new_projection,
            expr: vec![Expr::Sort(SortExpr {
                expr: Box::new(time_expr.clone()),
                asc: true,
                nulls_first: false,
            })],
            fetch: None,
        }));

        let time_field = Arc::new(Field::new("time", time_type.clone(), false));
        let field_field = Arc::new(Field::new(alias, field_type, false));
        let schema = Arc::new(DFSchema::new_with_metadata(
            vec![(None, time_field), (None, field_field)],
            HashMap::new(),
        )?);

        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(TimeSeriesGenFuncNode {
                time_expr,
                field_expr,
                arg_expr,
                input: new_sort,
                symbol,
                schema,
            }),
        }))
    }
}
