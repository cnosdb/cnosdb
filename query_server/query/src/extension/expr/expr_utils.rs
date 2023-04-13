use datafusion::logical_expr::utils::find_exprs_in_expr;
use datafusion::logical_expr::{BinaryExpr, Operator};
use datafusion::prelude::Expr;
use models::schema::TIME_FIELD_NAME;

use super::selector_function::{BOTTOM, TOPK};

pub fn is_time_filter(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            (is_time_column(left) || is_time_column(right))
                && matches!(
                    op,
                    Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
                )
        }
        _ => false,
    }
}

pub fn is_time_column(expr: &Expr) -> bool {
    if let Expr::Column(c) = expr {
        c.name == TIME_FIELD_NAME
    } else {
        false
    }
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
            Expr::ScalarUDF {
                fun,
                ..
            } if fun.name.eq_ignore_ascii_case(BOTTOM)
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
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(
            nested_expr,
            Expr::ScalarUDF {
                fun,
                ..
            } if fun.name.eq_ignore_ascii_case(BOTTOM)
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
