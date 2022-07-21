use std::{collections::HashSet, sync::Arc};

use datafusion::{
    arrow::{array::BooleanArray, compute::filter_record_batch, record_batch::RecordBatch},
    error::DataFusionError,
    logical_expr::{utils::expr_to_columns, Expr, Operator},
    physical_expr::PhysicalExpr,
};
use datafusion::arrow::error::ArrowError;
use logger::info;

type ArrowResult<T> = std::result::Result<T, ArrowError>;
pub type PredicateRef = Arc<Predicate>;

#[derive(Debug,Default)]
pub struct Predicate {
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl Predicate {
    pub fn new(filters: Vec<Expr>, limit: Option<usize>) -> Self {
        Self { filters, limit }
    }
    pub fn set_limit(mut self, limit: Option<usize>) -> Predicate {
        self.limit = limit;
        self
    }
    pub fn combine_expr(&self) -> Option<Expr> {
        let mut res: Option<Expr> = None;
        for i in &self.filters {
            if let Some(e) = res {
                res = Some(e.and(i.clone()))
            } else {
                res = Some(i.clone())
            }
        }
        res
    }

    pub fn split_expr(predicate: &Expr, predicates: &mut Vec<Expr>) {
        match predicate {
            Expr::BinaryExpr { right, op: Operator::And, left } => {
                Self::split_expr(left, predicates);
                Self::split_expr(right, predicates);
            },
            other => predicates.push(other.clone()),
        }
    }
    pub fn primitive_binary_expr(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr { left, op, right } => {
                matches!((&**left, &**right),
                         (Expr::Column(_), Expr::Literal(_)) | (Expr::Literal(_), Expr::Column(_)))
                && matches!(op,
                            Operator::Eq
                            | Operator::NotEq
                            | Operator::Lt
                            | Operator::LtEq
                            | Operator::Gt
                            | Operator::GtEq)
            },
            _ => false,
        }
    }
    pub fn pushdown_exprs(mut self, filters: &[Expr]) -> Predicate {
        let mut exprs = vec![];
        filters.iter().for_each(|expr| Self::split_expr(expr, &mut exprs));

        let mut pushdown: Vec<Expr> = vec![];
        let exprs_result =
            exprs.into_iter().try_for_each::<_, Result<_, DataFusionError>>(|expr| {
                                 let mut columns = HashSet::new();
                                 expr_to_columns(&expr, &mut columns)?;

                                 if columns.len() == 1 && Self::primitive_binary_expr(&expr) {
                                     pushdown.push(expr);
                                 }
                                 Ok(())
                             });

        match exprs_result {
            Ok(()) => {
                self.filters.append(&mut pushdown);
            },
            Err(e) => {
                info!(
                      "Error, {}, building push-down predicates for filters: {:#?}. No
                predicates are pushed down",
                      e, filters
                );
            },
        }
        self
    }
}

pub fn batch_filter(batch: &RecordBatch,
                    predicate: &Arc<dyn PhysicalExpr>)
                    -> ArrowResult<RecordBatch> {
    predicate.evaluate(batch)
             .map(|v| v.into_array(batch.num_rows()))
             .map_err(DataFusionError::into)
             .and_then(|array| {
                 array.as_any()
                      .downcast_ref::<BooleanArray>()
                      .ok_or_else(|| {
                          DataFusionError::Internal(
                        "Filter predicate evaluated to non-boolean value".to_string(),
                    )
                        .into()
                      })
                      // apply filter array to record batch
                      .and_then(|filter_array| filter_record_batch(batch, filter_array))
             })
}
