use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::utils::find_exprs_in_expr;
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
        Expr::BinaryExpr(BinaryExpr { op, .. }) => {
            matches!(
                op,
                Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
            )
        }
        Expr::IsNull(col) | Expr::IsNotNull(col) => is_field_column(col, schema),
        _ => false,
    }
}

pub fn is_field_column(expr: &Expr, schema: TskvTableSchemaRef) -> bool {
    if let Expr::Column(c) = expr {
        if let Some(col) = schema.column(&c.name) {
            return col.column_type.is_field();
        }
    }
    false
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
            Expr::ScalarUDF(expr::ScalarUDF {
                fun,
                ..
            }) if fun.name.eq_ignore_ascii_case(BOTTOM)
            || fun.name.eq_ignore_ascii_case(TOPK)
        )
    })
}

/// Search the provided `Expr`'s, not has their nested `Expr`
pub fn find_exprs_in_exprs<F>(exprs: &[Expr], test_fn: &F) -> Vec<Expr>
where
    F: Fn(&Expr) -> bool,
{
    exprs
        .iter()
        .filter(|e| test_fn(e))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(expr) {
                acc.push(expr.clone())
            }
            acc
        })
}

/// Collect all deeply nested selector function. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_selector_function_exprs_deeply_nested(exprs: &[Expr]) -> Vec<Expr> {
    find_exprs_in_exprs_deeply_nested(exprs, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::ScalarUDF(expr::ScalarUDF {
                fun,
                ..
            }) if fun.name.eq_ignore_ascii_case(BOTTOM)
            || fun.name.eq_ignore_ascii_case(TOPK)
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
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}
