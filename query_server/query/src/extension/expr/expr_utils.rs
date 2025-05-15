use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::utils::find_exprs_in_exprs;
use datafusion::logical_expr::{expr, BinaryExpr, Operator};
use datafusion::prelude::Expr;
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use spi::AnalyzerSnafu;

use super::selector_function::{BOTTOM, TOPK};

pub fn check_args(func_name: &str, expects: usize, input: &[DataType]) -> DFResult<()> {
    if input.len() != expects {
        return Err(DataFusionError::External(Box::new(
            AnalyzerSnafu {
                err: format!(
                    "The function {:?} expects {} arguments, but {} were provided",
                    func_name,
                    expects,
                    input.len()
                ),
            }
            .build(),
        )));
    }

    Ok(())
}

pub fn check_args_eq_any(func_name: &str, expects: &[usize], input: &[DataType]) -> DFResult<()> {
    let len = input.len();
    if !expects.iter().any(|e| e.eq(&len)) {
        return Err(DataFusionError::External(Box::new(
            AnalyzerSnafu {
                err: format!(
                    "The function {:?} expects {:?} arguments, but {} were provided",
                    func_name,
                    expects,
                    input.len()
                ),
            }
            .build(),
        )));
    }
    Ok(())
}

pub fn can_exact_filter(expr: &Expr, schema: TskvTableSchemaRef) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            ((is_column(left, schema.clone()) && is_literal(right))
                || (is_column(right, schema.clone()) && is_literal(left)))
                && matches!(
                    op,
                    Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
                )
        }
        // Expr::IsNull(col) | Expr::IsNotNull(col) => is_column(col, schema),
        _ => false,
    }
}

pub fn is_column(expr: &Expr, schema: TskvTableSchemaRef) -> bool {
    let col = if let Expr::Column(col) = expr {
        Some(col)
    } else if let Expr::Cast(cast) = expr {
        if let Expr::Column(col) = cast.expr.as_ref() {
            Some(col)
        } else {
            None
        }
    } else {
        None
    };

    if let Some(col) = col {
        return schema.get_column_by_name(&col.name).is_some();
    }

    false
}

pub fn is_literal(expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_))
}

/// Replace 'replace' in 'exprs' with 'with'
pub fn replace_expr_with(exprs: &[Expr], replace: &Expr, with: &Expr) -> Vec<Expr> {
    exprs
        .iter()
        .map(|e| {
            if e.eq(replace) {
                return with.clone();
            }

            e.clone()
        })
        .collect()
}

/// Collect all deeply nested selector function. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_selector_function_exprs(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::ScalarUDF(expr::ScalarFunction {
                func,
                ..
            }) if func.name.eq_ignore_ascii_case(BOTTOM)
            || func.name.eq_ignore_ascii_case(TOPK)
        )
    })
}

/// Collect all deeply nested selector function. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_selector_function_exprs_deeply_nested(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs_deeply_nested(exprs, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::ScalarUDF(expr::ScalarFunction {
                func,
                ..
            }) if func.name.eq_ignore_ascii_case(BOTTOM)
            || func.name.eq_ignore_ascii_case(TOPK)
        )
    })
}

/// Search the provided `Expr`'s, and all of their nested `Expr`, for any that
/// pass the provided test. The returned `Expr`'s are deduplicated and returned
/// in order of appearance (depth first).
pub fn find_exprs_in_exprs_deeply_nested<F>(exprs: &[Expr], test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    find_exprs_in_exprs(exprs, test_fn)
        .into_iter()
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}
