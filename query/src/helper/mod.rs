mod domain;

pub use domain::RowExpressionToDomainsVisitor;

use std::result;

use datafusion::{
    error::DataFusionError,
    logical_expr::Volatility,
    logical_plan,
    logical_plan::{ExprVisitable, ExpressionVisitor, Recursion},
    prelude::Expr,
};

pub type Result<T> = result::Result<T, DataFusionError>;
/// The `ExpressionVisitor` for `expr_applicable_for_cols`. Walks the tree to
/// validate that the given expression is applicable with only the `col_names`
/// set of columns.
struct ApplicabilityVisitor<'a> {
    col_names: &'a [String],
    is_applicable: &'a mut bool,
}

impl ApplicabilityVisitor<'_> {
    fn visit_volatility(self, volatility: Volatility) -> Recursion<Self> {
        match volatility {
            Volatility::Immutable => Recursion::Continue(self),
            // TODO: Stable functions could be `applicable`, but that would require access to the
            // context
            Volatility::Stable | Volatility::Volatile => {
                *self.is_applicable = false;
                Recursion::Stop(self)
            }
        }
    }
}

impl ExpressionVisitor for ApplicabilityVisitor<'_> {
    fn pre_visit(self, expr: &Expr) -> Result<Recursion<Self>> {
        let rec = match expr {
            Expr::Column(logical_plan::Column { ref name, .. }) => {
                *self.is_applicable &= self.col_names.contains(name);
                Recursion::Stop(self) // leaf node anyway
            }
            Expr::Literal(_)
            | Expr::Alias(..)
            | Expr::ScalarVariable(..)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::Cast { .. }
            | Expr::TryCast { .. }
            | Expr::BinaryExpr { .. }
            | Expr::Between { .. }
            | Expr::InList { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::ScalarSubquery(_)
            | Expr::GetIndexedField { .. }
            | Expr::GroupingSet(_)
            | Expr::Case { .. } => Recursion::Continue(self),

            Expr::ScalarFunction { fun, .. } => self.visit_volatility(fun.volatility()),
            Expr::ScalarUDF { fun, .. } => self.visit_volatility(fun.signature.volatility),

            // TODO other expressions are not handled yet:
            // - AGGREGATE, WINDOW and SORT should not end up in filter conditions, except maybe in
            //   some edge cases
            // - Can `Wildcard` be considered as a `Literal`?
            // - ScalarVariable could be `applicable`, but that would require access to the context
            Expr::AggregateUDF { .. }
            | Expr::AggregateFunction { .. }
            | Expr::Sort { .. }
            | Expr::WindowFunction { .. }
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. } => {
                *self.is_applicable = false;
                Recursion::Stop(self)
            }
        };
        Ok(rec)
    }
}

/// Check whether the given expression can be resolved using only the columns `col_names`.
/// This means that if this function returns true:
/// - the table provider can filter the table partition values with this expression
/// - the expression can be marked as `TableProviderFilterPushDown::Exact` once this filtering
/// was performed

#[allow(dead_code)]
pub fn expr_applicable_for_cols(col_names: &[String], expr: &Expr) -> bool {
    let mut is_applicable = true;
    expr.accept(ApplicabilityVisitor {
        col_names,
        is_applicable: &mut is_applicable,
    })
    .unwrap();
    is_applicable
}
