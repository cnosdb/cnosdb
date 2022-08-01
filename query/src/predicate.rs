use std::{collections::HashSet, sync::Arc};

use datafusion::{
    arrow::{
        array::BooleanArray, compute::filter_record_batch, error::ArrowError,
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    logical_expr::{utils::expr_to_columns, Expr, Operator},
    physical_expr::PhysicalExpr,
};
use trace::info;

type ArrowResult<T> = Result<T, ArrowError>;
pub type PredicateRef = Arc<Predicate>;

#[derive(Default, Debug)]
pub struct TimeRange {
    pub max_ts: i64,
    pub min_ts: i64,
}

impl TimeRange {
    pub fn new(max_ts: i64, min_ts: i64) -> Self {
        Self { max_ts, min_ts }
    }

    pub fn overlaps(&self, range: &TimeRange) -> bool {
        !(self.min_ts > range.max_ts || self.max_ts < range.min_ts)
    }
}

#[derive(Debug, Default)]
pub struct Predicate {
    filters: Vec<Expr>,
    limit: Option<usize>,
    timeframe: TimeRange,
}

impl Predicate {
    pub fn new(filters: Vec<Expr>, limit: Option<usize>, timeframe: TimeRange) -> Self {
        Self { filters, limit, timeframe }
    }
    pub fn set_limit(mut self, limit: Option<usize>) -> Predicate {
        self.limit = limit;
        self
    }
    pub fn combine_expr(&self) -> Option<Expr> {
        let mut res: Option<Expr> = None;
        for i in &self.filters {
            if let Some(e) = res { res = Some(e.and(i.clone())) } else { res = Some(i.clone()) }
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
                      .and_then(|filter_array| filter_record_batch(batch, filter_array))
             })
}
