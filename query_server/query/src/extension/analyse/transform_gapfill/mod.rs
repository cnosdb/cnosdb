//! An optimizer rule that transforms a plan
//! to fill gaps in time series data.

pub mod range_predicate;

use std::collections::HashSet;
use std::ops::{Bound, Range};
use std::sync::Arc;

use datafusion::common::tree_node::{
    RewriteRecursion, Transformed, TreeNode, TreeNodeRewriter, VisitRecursion,
};
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::utils::expr_to_columns;
use datafusion::logical_expr::{Aggregate, Extension, GetIndexedField, LogicalPlan, Projection};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::prelude::{col, Expr};

use crate::extension::expr::{
    INTERPOLATE, LOCF, TIME_WINDOW_GAPFILL, TIME_WINDOW_UDF, WINDOW_START,
};
use crate::extension::logical::plan_node::gapfill::{FillStrategy, GapFill, GapFillParams};

/// This optimizer rule enables gap-filling semantics for SQL queries
/// that contain calls to `TIME_WINDOW_GAPFILL()` and related functions
/// like `LOCF()`.
///
/// In SQL a typical gap-filling query might look like this:
/// ```sql
/// SELECT
///   location,
///   TIME_WINDOW_GAPFILL(INTERVAL '1 minute', time, '1970-01-01T00:00:00Z') AS minute,
///   LOCF(AVG(temp))
/// FROM temps
/// WHERE time > NOW() - INTERVAL '6 hours' AND time < NOW()
/// GROUP BY LOCATION, MINUTE
/// ```
///
/// The initial logical plan will look like this:
///
/// ```text
///   Projection: location, time_window_gapfill(...) as minute, LOCF(AVG(temps.temp))
///     Aggregate: groupBy=[[location, time_window_gapfill(...)]], aggr=[[AVG(temps.temp)]]
///       ...
/// ```
///
/// This optimizer rule transforms it to this:
///
/// ```text
///   Projection: location, time_window_gapfill(...) as minute, AVG(temps.temp)
///     GapFill: groupBy=[[location, time_window_gapfill(...))]], aggr=[[LOCF(AVG(temps.temp))]], start=..., stop=...
///       Aggregate: groupBy=[[location, date_bin(...))]], aggr=[[AVG(temps.temp)]]
///         ...
/// ```
///
/// For `Aggregate` nodes that contain calls to `TIME_WINDOW_GAPFILL`, this rule will:
/// - Convert `TIME_WINDOW_GAPFILL()` to `DATE_BIN()`
/// - Create a `GapFill` node that fills in gaps in the query
/// - The range for gap filling is found by analyzing any preceding `Filter` nodes
///
/// If there is a `Projection` above the `GapFill` node that gets created:
/// - Look for calls to gap-filling functions like `LOCF`
/// - Push down these functions into the `GapFill` node, updating the fill strategy for the column.
///
/// Note: both `TIME_WINDOW_GAPFILL` and `LOCF` are functions that don't have implementations.
/// This rule must rewrite the plan to get rid of them.
pub struct TransformGapFill;

impl TransformGapFill {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for TransformGapFill {
    fn default() -> Self {
        Self::new()
    }
}

impl AnalyzerRule for TransformGapFill {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&handle_gap_fill)
    }

    fn name(&self) -> &str {
        "transform_gap_fill"
    }
}

fn handle_gap_fill(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let transformed_plan = match &plan {
        LogicalPlan::Aggregate(aggr) => handle_aggregate(aggr)?,
        LogicalPlan::Projection(proj) => handle_projection(proj)?,
        _ => None,
    };

    if transformed_plan.is_none() {
        // no transformation was applied,
        // so make sure the plan is not using gap filling
        // functions in an unsupported way.
        check_node(&plan)?;
    }

    let res = transformed_plan
        .map(Transformed::Yes)
        .unwrap_or(Transformed::No(plan));

    Ok(res)
}

fn handle_aggregate(aggr: &Aggregate) -> Result<Option<LogicalPlan>> {
    let Aggregate {
        input,
        group_expr,
        aggr_expr,
        schema,
        ..
    } = aggr;

    // new_group_expr has TIME_WINDOW_GAPFILL replaced with DATE_BIN.
    let RewriteInfo {
        new_group_expr,
        time_window_gapfill_index,
        time_window_gapfill_args,
    } = if let Some(v) = replace_time_window_gapfill(group_expr)? {
        v
    } else {
        return Ok(None);
    };

    let new_aggr_plan = {
        // Create the aggregate node with the same output schema as the orignal
        // one. This means that there will be an output column called `time_window_gapfill(...)`
        // even though the actual expression populating that column will be `date_bin(...)`.
        // This seems acceptable since it avoids having to deal with renaming downstream.
        let new_aggr_plan = Aggregate::try_new_with_schema(
            Arc::clone(input),
            new_group_expr,
            aggr_expr.clone(),
            Arc::clone(schema),
        )?;
        let new_aggr_plan = LogicalPlan::Aggregate(new_aggr_plan);
        check_node(&new_aggr_plan)?;
        new_aggr_plan
    };

    let new_gap_fill_plan = build_gapfill_node(
        new_aggr_plan,
        time_window_gapfill_index,
        time_window_gapfill_args,
    )?;
    Ok(Some(new_gap_fill_plan))
}

fn build_gapfill_node(
    new_aggr_plan: LogicalPlan,
    time_window_gapfill_index: usize,
    time_window_gapfill_args: Vec<Expr>,
) -> Result<LogicalPlan> {
    if time_window_gapfill_args.len() < 2 {
        return Err(DataFusionError::Plan(format!(
            "{TIME_WINDOW_GAPFILL} requires at least 2 arguments",
        )));
    }

    let mut args_iter = time_window_gapfill_args.into_iter();

    // Ensure that the source argument is a column
    let time_col = args_iter.next().unwrap().try_into_col().map_err(|_| {
        DataFusionError::Plan(
            "TIME_WINDOW_GAPFILL requires a column as the source argument".to_string(),
        )
    })?;

    // Ensure that window_duration argument is a scalar
    let window_duration = args_iter.next().unwrap();
    validate_scalar_expr(
        "window_duration argument to TIME_WINDOW_GAPFILL",
        &window_duration,
    )?;

    // Ensure that a time range was specified and is valid for gap filling
    let time_range = range_predicate::find_time_range(new_aggr_plan.inputs()[0], &time_col)?;
    validate_time_range(&time_range)?;

    // Ensure that slide_duration argument is a scalar
    let slide_duration = args_iter.next();
    let mut origin = None;
    if let Some(ref slide_duration) = slide_duration {
        validate_scalar_expr(
            "slide_duration argument to TIME_WINDOW_GAPFILL",
            slide_duration,
        )?;

        // Ensure that origin argument is a scalar
        origin = args_iter.next();
        if let Some(ref origin) = origin {
            validate_scalar_expr("start_time argument to TIME_WINDOW_GAPFILL", origin)?;
        }
    }

    // Make sure the time output to the gapfill node matches what the
    // aggregate output was.
    let time_column =
        col(new_aggr_plan.schema().fields()[time_window_gapfill_index].qualified_column());

    let aggr = Aggregate::try_from_plan(&new_aggr_plan)?;
    let mut new_group_expr: Vec<_> = aggr
        .schema
        .fields()
        .iter()
        .map(|f| Expr::Column(f.qualified_column()))
        .collect();
    let aggr_expr = new_group_expr.split_off(aggr.group_expr.len());

    // For now, we can only fill with null values.
    // In the future, this rule will allow a projection to be pushed into the
    // GapFill node, e.g., if it contains an item like `LOCF(<col>)`.
    let fill_behavior = aggr_expr
        .iter()
        .cloned()
        .map(|e| (e, FillStrategy::Null))
        .collect();

    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(GapFill::try_new(
            Arc::new(new_aggr_plan),
            new_group_expr,
            aggr_expr,
            GapFillParams {
                stride: window_duration.clone(),
                sliding: slide_duration.unwrap_or(window_duration),
                time_column,
                origin,
                time_range,
                fill_strategy: fill_behavior,
            },
        )?),
    }))
}

fn validate_time_range(range: &Range<Bound<Expr>>) -> Result<()> {
    let Range { ref start, ref end } = range;
    let (start, end) = match (start, end) {
        (Bound::Unbounded, Bound::Unbounded) => {
            return Err(DataFusionError::Plan(
                "no time bounds found for gap fill query".to_string(),
            ))
        }
        (Bound::Unbounded, _) => Err(DataFusionError::Plan(
            "no lower time bound found for gap fill query".to_string(),
        )),
        (_, Bound::Unbounded) => Err(DataFusionError::Plan(
            "no upper time bound found for gap fill query".to_string(),
        )),
        (
            Bound::Included(start) | Bound::Excluded(start),
            Bound::Included(end) | Bound::Excluded(end),
        ) => Ok((start, end)),
    }?;
    validate_scalar_expr("lower time bound", start)?;
    validate_scalar_expr("upper time bound", end)
}

fn validate_scalar_expr(what: &str, e: &Expr) -> Result<()> {
    let mut cols = HashSet::new();
    expr_to_columns(e, &mut cols)?;
    if !cols.is_empty() {
        Err(DataFusionError::Plan(format!(
            "{what} for gap fill query must evaluate to a scalar"
        )))
    } else {
        Ok(())
    }
}

struct RewriteInfo {
    // Group expressions with TIME_WINDOW_GAPFILL rewritten to DATE_BIN.
    new_group_expr: Vec<Expr>,
    // The index of the group expression that contained the call to TIME_WINDOW_GAPFILL.
    time_window_gapfill_index: usize,
    // The arguments to the call to TIME_WINDOW_GAPFILL.
    time_window_gapfill_args: Vec<Expr>,
}

// Iterate over the group expression list.
// If it finds no occurrences of time_window_gapfill, it will return None.
// If it finds more than one occurrence it will return an error.
// Otherwise it will return a RewriteInfo for the optimizer rule to use.
fn replace_time_window_gapfill(group_expr: &[Expr]) -> Result<Option<RewriteInfo>> {
    let mut time_window_gapfill_count = 0;
    let mut dbg_idx = None;
    group_expr
        .iter()
        .enumerate()
        .try_for_each(|(i, e)| -> Result<()> {
            let fn_cnt = count_udf(e, TIME_WINDOW_GAPFILL)?;
            time_window_gapfill_count += fn_cnt;
            if fn_cnt > 0 {
                dbg_idx = Some(i);
            }
            Ok(())
        })?;
    match time_window_gapfill_count {
        0 => return Ok(None),
        2.. => {
            return Err(DataFusionError::Plan(
                "TIME_WINDOW_GAPFILL specified more than once".to_string(),
            ))
        }
        _ => (),
    }
    let time_window_gapfill_index = dbg_idx.expect("should be found exactly one call");

    let mut rewriter = TimeWindowGapfillRewriter { args: None };
    let group_expr = group_expr
        .iter()
        .enumerate()
        .map(|(i, e)| {
            if i == time_window_gapfill_index {
                e.clone().rewrite(&mut rewriter)
            } else {
                Ok(e.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let time_window_gapfill_args = rewriter.args.expect("should have found args");

    Ok(Some(RewriteInfo {
        new_group_expr: group_expr,
        time_window_gapfill_index,
        time_window_gapfill_args,
    }))
}

struct TimeWindowGapfillRewriter {
    args: Option<Vec<Expr>>,
}

impl TreeNodeRewriter for TimeWindowGapfillRewriter {
    type N = Expr;
    fn pre_visit(&mut self, expr: &Expr) -> Result<RewriteRecursion> {
        match expr {
            Expr::ScalarUDF { fun, .. } if fun.name == TIME_WINDOW_GAPFILL => {
                Ok(RewriteRecursion::Mutate)
            }
            _ => Ok(RewriteRecursion::Continue),
        }
    }

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        // We need to preserve the name of the original expression
        // so that everything stays wired up.
        let orig_name = expr.display_name()?;
        match expr {
            Expr::ScalarUDF { fun, args } if fun.name == TIME_WINDOW_GAPFILL => {
                self.args = Some(args.clone());
                // Ok(Expr::ScalarUDF {
                //     fun: TIME_WINDOW_UDF.clone(),
                //     args,
                // }
                // .alias(orig_name));

                let expr = Expr::ScalarUDF {
                    fun: TIME_WINDOW_UDF.clone(),
                    args,
                };

                Ok(
                    Expr::GetIndexedField(GetIndexedField::new(
                        Box::new(expr),
                        WINDOW_START.into(),
                    ))
                    .alias(orig_name),
                )
            }
            _ => Ok(expr),
        }
    }
}

fn udf_to_fill_strategy(name: &str) -> Option<FillStrategy> {
    match name {
        LOCF => Some(FillStrategy::PrevNullAsMissing),
        INTERPOLATE => Some(FillStrategy::LinearInterpolate),
        _ => None,
    }
}

fn handle_projection(proj: &Projection) -> Result<Option<LogicalPlan>> {
    let Projection {
        input,
        expr: proj_exprs,
        schema: proj_schema,
        ..
    } = proj;
    let Some(child_gapfill) = (match input.as_ref() {
        LogicalPlan::Extension(Extension { node }) => node.as_any().downcast_ref::<GapFill>(),
        _ => None,
    }) else {
        // If this is not a projection that is a parent to a GapFill node,
        // then there is nothing to do.
        return Ok(None)
    };

    let fill_cols: Vec<(&Expr, FillStrategy, &str)> = proj_exprs
        .iter()
        .filter_map(|e| match e {
            Expr::ScalarUDF { fun, args } => {
                if let Some(strategy) = udf_to_fill_strategy(&fun.name) {
                    let col = &args[0];
                    Some((col, strategy, fun.name.as_str()))
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();
    if fill_cols.is_empty() {
        // No special gap-filling functions, nothing to do.
        return Ok(None);
    }

    // Clone the existing GapFill node, then modify it in place
    // to reflect the new fill strategy.
    let mut new_gapfill = child_gapfill.clone();
    for (e, fs, fn_name) in fill_cols {
        if new_gapfill.replace_fill_strategy(e, fs).is_none() {
            // There was a gap filling function called on a non-aggregate column.
            return Err(DataFusionError::Plan(format!(
                "{fn_name} must be called on an aggregate column in a gap-filling query"
            )));
        }
    }

    // Remove the gap filling functions from the projection.
    let new_proj_exprs: Vec<Expr> = proj_exprs
        .iter()
        .cloned()
        .map(|e| match e {
            Expr::ScalarUDF { fun, mut args } if udf_to_fill_strategy(&fun.name).is_some() => {
                args.remove(0)
            }
            _ => e,
        })
        .collect();

    let new_proj = {
        let mut proj = proj.clone();
        proj.expr = new_proj_exprs;
        proj.input = Arc::new(LogicalPlan::Extension(Extension {
            node: Arc::new(new_gapfill),
        }));
        proj.schema = Arc::clone(proj_schema);
        LogicalPlan::Projection(proj)
    };

    Ok(Some(new_proj))
}

fn count_udf(e: &Expr, name: &str) -> Result<usize> {
    let mut count = 0;
    e.apply(&mut |expr| {
        match expr {
            Expr::ScalarUDF { fun, .. } if fun.name == name => {
                count += 1;
            }
            _ => (),
        };
        Ok(VisitRecursion::Continue)
    })?;
    Ok(count)
}

fn check_node(node: &LogicalPlan) -> Result<()> {
    node.expressions().iter().try_for_each(|expr| {
        let dbg_count = count_udf(expr, TIME_WINDOW_GAPFILL)?;
        if dbg_count > 0 {
            return Err(DataFusionError::Plan(format!(
                "{TIME_WINDOW_GAPFILL} may only be used as a GROUP BY expression"
            )));
        }

        for fn_name in [LOCF, INTERPOLATE] {
            if count_udf(expr, fn_name)? > 0 {
                return Err(DataFusionError::Plan(format!(
                    "{fn_name} may only be used in the SELECT list of a gap-filling query"
                )));
            }
        }
        Ok(())
    })
}
