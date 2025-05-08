use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFField, DFSchema};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::{ScalarUDF as ScalarUDFExpr, Sort as SortExpr};
use datafusion::logical_expr::{
    Expr, ExprSchemable, Extension, LogicalPlan, Projection, ScalarUDF, Sort,
};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::scalar::ScalarValue;
use models::arrow::DataType;
use spi::DFResult;

use crate::extension::expr::TsGenFunc;
use crate::extension::logical::plan_node::ts_gen_func::TSGenFuncNode;

pub struct TransformTSGenFunc;

impl AnalyzerRule for TransformTSGenFunc {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        plan.transform_up(&analyze_internal)
    }

    fn name(&self) -> &str {
        "TransformTSGenFunc"
    }
}

fn analyze_internal(plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
    let exprs = plan.expressions();
    for expr in exprs.iter() {
        let (expr, alias) = if let Expr::Alias(expr, alias) = expr {
            (expr.as_ref(), Cow::Borrowed(alias))
        } else {
            (expr, Cow::Owned(expr.to_string()))
        };
        if let Expr::ScalarUDF(ScalarUDFExpr { fun, args }) = expr {
            if let Some(symbol) = TsGenFunc::try_from_str(&fun.name) {
                if let LogicalPlan::Projection(projection) = &plan {
                    if projection.expr.len() != 1 {
                        return Err(datafusion::error::DataFusionError::Plan(
                            "Projection must have only one expression when using time series generation function"
                                .to_string(),
                        ));
                    }
                    let new_plan = new_plan(fun, args, &projection.input, &alias, symbol)?;
                    return Ok(Transformed::Yes(new_plan));
                } else {
                    return Err(datafusion::error::DataFusionError::Plan(
                        "Time series generation function must be used in projection".to_string(),
                    ));
                }
            }
        }
    }
    Ok(Transformed::No(plan))
}

fn new_plan(
    udf: &ScalarUDF,
    args: &[Expr],
    input: &LogicalPlan,
    alias: &str,
    symbol: TsGenFunc,
) -> DFResult<LogicalPlan> {
    let arg_types = args
        .iter()
        .map(|arg| arg.get_type(input.schema()))
        .collect::<DFResult<Vec<_>>>()?;
    let return_type = (udf.return_type)(&arg_types)?;
    let time_expr = args[0].clone();
    let mut field_exprs = args[1..].to_vec();
    let arg_expr = if arg_types[arg_types.len() - 1] == DataType::Utf8 {
        if !matches!(
            field_exprs.last(),
            Some(Expr::Literal(ScalarValue::Utf8(Some(_))))
        ) {
            return Err(datafusion::error::DataFusionError::Plan(
                "Expected argument to be a string literal".to_string(),
            ));
        }
        field_exprs.pop()
    } else {
        None
    };

    let new_projection = Arc::new(LogicalPlan::Projection(Projection::try_new(
        [time_expr.clone()]
            .iter()
            .chain(field_exprs.iter())
            .cloned()
            .collect(),
        Arc::new(input.clone()),
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

    let res_time_field = DFField::new_unqualified("time", arg_types[0].clone(), false);
    let res_field_field = DFField::new_unqualified(alias, (*return_type).clone(), true);
    let schema = Arc::new(DFSchema::new_with_metadata(
        vec![res_time_field, res_field_field],
        HashMap::new(),
    )?);

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(TSGenFuncNode {
            time_expr,
            field_exprs,
            arg_expr,
            input: new_sort,
            symbol,
            schema,
        }),
    }))
}
