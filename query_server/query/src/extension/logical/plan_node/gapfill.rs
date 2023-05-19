//! This module contains code that implements
//! a gap-filling extension to DataFusion
use std::fmt::Debug;
use std::ops::{Bound, Range};
use std::sync::Arc;

use datafusion::common::DFSchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::prelude::Expr;

use crate::extension::utils::{bound_extract, try_map_bound, try_map_range};

/// A logical node that represents the gap filling operation.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFill {
    /// The incoming logical plan
    pub input: Arc<LogicalPlan>,
    /// Grouping expressions
    pub group_expr: Vec<Expr>,
    /// Aggregate expressions
    pub aggr_expr: Vec<Expr>,
    /// Parameters to configure the behavior of the
    /// gap-filling operation
    pub params: GapFillParams,
}

/// Parameters to the GapFill operation
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct GapFillParams {
    /// The stride argument from the call to TIME_WINDOW_GAPFILL
    pub stride: Expr,
    /// The sliding argument from the call to TIME_WINDOW_GAPFILL
    pub sliding: Expr,
    /// The source time column
    pub time_column: Expr,
    /// The origin argument from the call to TIME_WINDOW_GAPFILL
    pub origin: Option<Expr>,
    /// The time range of the time column inferred from predicates
    /// in the overall query. The lower bound may be [`Bound::Unbounded`]
    /// which implies that gap-filling should just start from the
    /// first point in each series.
    pub time_range: Range<Bound<Expr>>,
    /// What to do when filling aggregate columns.
    /// The first item in the tuple will be the column
    /// reference for the aggregate column.
    pub fill_strategy: Vec<(Expr, FillStrategy)>,
}

/// Describes how to fill gaps in an aggregate column.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum FillStrategy {
    /// Fill with null values.
    /// This is the InfluxQL behavior for `FILL(NULL)` or `FILL(NONE)`.
    Null,
    /// Fill with the most recent value in the input column.
    /// Null values in the input are preserved.
    #[allow(dead_code)]
    PrevNullAsIntentional,
    /// Fill with the most recent non-null value in the input column.
    /// This is the InfluxQL behavior for `FILL(PREVIOUS)`.
    PrevNullAsMissing,
    /// Fill the gaps between points linearly.
    /// Null values will not be considered as missing, so two non-null values
    /// with a null in between will not be filled.
    LinearInterpolate,
}

impl GapFillParams {
    // Extract the expressions so they can be optimized.
    fn expressions(&self) -> Vec<Expr> {
        let mut exprs = vec![
            self.stride.clone(),
            self.sliding.clone(),
            self.time_column.clone(),
        ];
        if let Some(e) = self.origin.as_ref() {
            exprs.push(e.clone())
        }
        if let Some(start) = bound_extract(&self.time_range.start) {
            exprs.push(start.clone());
        }
        exprs.push(
            bound_extract(&self.time_range.end)
                .unwrap_or_else(|| panic!("upper time bound is required"))
                .clone(),
        );
        exprs
    }

    #[allow(clippy::wrong_self_convention)] // follows convention of UserDefinedLogicalNode
    fn from_template(&self, exprs: &[Expr], aggr_expr: &[Expr]) -> Self {
        assert!(
            exprs.len() >= 3,
            "should be a at least stride, source and origin in params"
        );
        let mut iter = exprs.iter().cloned();
        let stride = iter.next().unwrap();
        let sliding = iter.next().unwrap();
        let time_column = iter.next().unwrap();
        let origin = self.origin.as_ref().map(|_| iter.next().unwrap());
        let time_range = try_map_range(&self.time_range, |b| {
            try_map_bound(b.as_ref(), |_| {
                Ok(iter.next().expect("expr count should match template"))
            })
        })
        .unwrap();

        let fill_strategy = aggr_expr
            .iter()
            .cloned()
            .zip(
                self.fill_strategy
                    .iter()
                    .map(|(_expr, fill_strategy)| fill_strategy)
                    .cloned(),
            )
            .collect();

        Self {
            stride,
            sliding,
            time_column,
            origin,
            time_range,
            fill_strategy,
        }
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    fn replace_fill_strategy(&mut self, e: &Expr, mut fs: FillStrategy) -> Option<FillStrategy> {
        for expr_fs in &mut self.fill_strategy {
            if &expr_fs.0 == e {
                std::mem::swap(&mut fs, &mut expr_fs.1);
                return Some(fs);
            }
        }
        None
    }
}

impl GapFill {
    /// Create a new gap-filling operator.
    pub fn try_new(
        input: Arc<LogicalPlan>,
        group_expr: Vec<Expr>,
        aggr_expr: Vec<Expr>,
        params: GapFillParams,
    ) -> Result<Self> {
        if params.time_range.end == Bound::Unbounded {
            return Err(DataFusionError::Internal(
                "missing upper bound in GapFill time range".to_string(),
            ));
        }
        Ok(Self {
            input,
            group_expr,
            aggr_expr,
            params,
        })
    }

    // Find the expression that matches `e` and replace its fill strategy.
    // If such an expression is found, return the old strategy, and `None` otherwise.
    pub(crate) fn replace_fill_strategy(
        &mut self,
        e: &Expr,
        fs: FillStrategy,
    ) -> Option<FillStrategy> {
        self.params.replace_fill_strategy(e, fs)
    }
}

impl UserDefinedLogicalNodeCore for GapFill {
    fn name(&self) -> &str {
        "GapFill"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.group_expr
            .iter()
            .chain(&self.aggr_expr)
            .chain(&self.params.expressions())
            .cloned()
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let aggr_expr: String = self
            .params
            .fill_strategy
            .iter()
            .map(|(e, fs)| match fs {
                FillStrategy::PrevNullAsIntentional => format!("LOCF(null-as-intentional, {})", e),
                FillStrategy::PrevNullAsMissing => format!("LOCF({})", e),
                FillStrategy::LinearInterpolate => format!("INTERPOLATE({})", e),
                FillStrategy::Null => e.to_string(),
            })
            .collect::<Vec<String>>()
            .join(", ");
        write!(
            f,
            "{}: groupBy=[{:?}], aggr=[[{}]], time_column={}, stride={}, range={:?}",
            self.name(),
            self.group_expr,
            aggr_expr,
            self.params.time_column,
            self.params.stride,
            self.params.time_range,
        )
    }

    fn from_template(&self, exprs: &[Expr], inputs: &[LogicalPlan]) -> Self {
        let mut group_expr: Vec<_> = exprs.to_vec();
        let mut aggr_expr = group_expr.split_off(self.group_expr.len());
        let param_expr = aggr_expr.split_off(self.aggr_expr.len());
        let params = self.params.from_template(&param_expr, &aggr_expr);
        Self::try_new(Arc::new(inputs[0].clone()), group_expr, aggr_expr, params)
            .expect("should not fail")
    }
}
