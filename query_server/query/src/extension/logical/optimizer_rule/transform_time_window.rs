use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::utils::expand_wildcard;
use datafusion::logical_expr::{GetIndexedField, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::{and, cast, col, lit, Expr};
use datafusion::scalar::ScalarValue;
use lazy_static::lazy_static;
use models::duration::DAY;
use spi::QueryError;
use trace::debug;

use crate::extension::expr::expr_fn::{ge, is_not_null, lt, minus, modulo, multiply, plus};
use crate::extension::expr::expr_utils::find_exprs_in_exprs_deeply_nested;
use crate::extension::expr::{TIME_WINDOW, WINDOW_COL_NAME, WINDOW_END, WINDOW_START};
use crate::extension::logical::logical_plan_builder::LogicalPlanBuilderExt;
use crate::extension::logical::plan_node::LogicalPlanExt;
use crate::utils::duration::parse_duration;

lazy_static! {
    static ref INIT_TIME: Expr = lit(ScalarValue::TimestampNanosecond(Some(0), None));
}

/// Convert the [`TIME_WINDOW`] function to Expand or project
pub struct TransformTimeWindowRule;

impl OptimizerRule for TransformTimeWindowRule {
    fn try_optimize(
        &self,
        plan: &LogicalPlan,
        _optimizer_config: &dyn OptimizerConfig,
    ) -> Result<Option<LogicalPlan>> {
        if plan.inputs().len() == 1 {
            let child = plan.inputs()[0];
            let child_project_exprs = expand_wildcard(child.schema().as_ref(), child)?;
            let window_expressions = find_window_exprs(plan);

            // Only support a single window expression for now
            if window_expressions.len() > 1 {
                return Err(DataFusionError::Plan(format!("Only support a single window expression for now, but found: {window_expressions:?}")));
            }

            if window_expressions.len() == 1 {
                let window_expr = unsafe { window_expressions.get_unchecked(0) };
                let window = make_time_window(window_expr)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                debug!("Construct time window: {:?}", window);

                let window_plan = if window.is_tumbling_window() {
                    // tumbling_window
                    build_tumbling_window_plan(&window, child.clone(), child_project_exprs)?
                } else {
                    // sliding_window
                    build_sliding_window_plan(&window, child.clone(), child_project_exprs)?
                };

                // replace current plan's exprs and child
                let final_plan =
                    replace_window_expr(col(WINDOW_COL_NAME).alias(&window.window_alias), plan)?
                        .with_new_inputs(&[window_plan])?;
                return Ok(Some(final_plan));
            }
        }

        Ok(None)
    }

    fn name(&self) -> &str {
        "transform_time_window"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

fn find_window_exprs(plan: &LogicalPlan) -> Vec<Expr> {
    let exprs = plan.expressions();
    find_exprs_in_exprs_deeply_nested(&exprs, &|nested_expr| {
        matches!(nested_expr, Expr::ScalarUDF {
            fun,
            ..
        } if fun.name == TIME_WINDOW)
    })
}

fn make_time_window(expr: &Expr) -> Result<TimeWindow, QueryError> {
    let window_alias = expr.display_name()?;
    match expr {
        Expr::ScalarUDF { fun, args } if fun.name == TIME_WINDOW => {
            if args.len() < 2 {
                return Err(QueryError::Internal {
                    reason: format!("Invalid signature of {TIME_WINDOW}"),
                });
            }
            // first arg: time_column
            let time_column = unsafe { args.get_unchecked(0) };
            // second arg: window_duration
            let window_duration = unsafe { args.get_unchecked(1) };
            let window_duration = valid_duration(parse_duration_arg(window_duration)?)?;

            let time_window_builder =
                TimeWindowBuilder::new(window_alias, time_column.clone(), window_duration);

            // time_window(time, interval '10 seconds', interval '5 milliseconds')
            // third arg: slide_duration
            if let Some(slide_duration) = args.get(2) {
                let slide_duration = valid_duration(parse_duration_arg(slide_duration)?)?;
                let time_window = time_window_builder
                    .with_slide_duration(slide_duration)
                    .build();
                return Ok(time_window);
            }

            Ok(time_window_builder.build())
        }
        _ => Err(QueryError::Internal {
            reason: format!("Expected TimeWindow, but found {expr}"),
        }),
    }
}

fn valid_duration(dur: Duration) -> Result<Duration, QueryError> {
    if dur.as_millis() > (365 * DAY).into() || dur.as_millis() == 0 {
        return Err(QueryError::InvalidTimeWindowParam {
            reason: format!("Max duration is (0s, 365d], but found {}s", dur.as_secs()),
        });
    }

    Ok(dur)
}

/// Convert string time duration to [`Duration`] \
/// Support duration unit: d | h | m | s | ms
fn parse_duration_arg(expr: &Expr) -> Result<Duration, QueryError> {
    let duration = to_string(expr).ok_or_else(|| QueryError::InvalidTimeWindowParam {
        reason: format!("{expr}"),
    })?;
    debug!("duration str: {}", duration);
    parse_duration(&duration).map_err(|reason| QueryError::InvalidTimeWindowParam { reason })
}

fn to_string(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(lit) => Some(lit.to_string()),
        _ => None,
    }
}

#[derive(Debug)]
struct TimeWindow {
    window_alias: String,
    time_column: Expr,
    // interval, such as: '5s'
    window_duration: Duration,
    // interval
    slide_duration: Duration,
    start_time: i64,
}

impl TimeWindow {
    fn is_tumbling_window(&self) -> bool {
        self.window_duration == self.slide_duration
    }
}

struct TimeWindowBuilder {
    window_alias: String,
    time_column: Expr,
    window_duration: Duration,
    slide_duration: Option<Duration>,
    start_time: Option<i64>,
}

impl TimeWindowBuilder {
    pub fn new(window_alias: String, time_column: Expr, window_duration: Duration) -> Self {
        Self {
            window_alias,
            time_column,
            window_duration,
            slide_duration: Default::default(),
            start_time: Default::default(),
        }
    }

    pub fn with_slide_duration(mut self, slide_duration: Duration) -> Self {
        self.slide_duration = Some(slide_duration);
        self
    }

    pub fn _with_start_time(mut self, start_time: i64) -> Self {
        self.start_time = Some(start_time);
        self
    }

    pub fn build(self) -> TimeWindow {
        TimeWindow {
            window_alias: self.window_alias,
            time_column: self.time_column,
            window_duration: self.window_duration,
            slide_duration: self.slide_duration.unwrap_or(self.window_duration),
            start_time: self.start_time.unwrap_or(0),
        }
    }
}

/// Generate a window start expression(alias name [`WINDOW_START`])
/// and a window end expression(alias name [`WINDOW_END`])
/// based on the given [`TimeWindow`] parameter
fn make_window_expr(i: i64, window: &TimeWindow) -> Expr {
    let TimeWindow {
        time_column,
        window_duration,
        slide_duration,
        start_time,
        ..
    } = window;

    let ns_type = DataType::Timestamp(TimeUnit::Nanosecond, None);

    // Convert interval to bigint
    let window_duration = lit(window_duration.as_nanos() as i64);
    let slide_duration = lit(slide_duration.as_nanos() as i64);

    let ns_time = cast(time_column.clone(), ns_type.clone());
    // TODO may overflow
    // i64::MAX (9223372036854775807) => 2262-04-11 23:47:16.854775807
    let i64_time = cast(ns_time, DataType::Int64);

    let last_start = minus(
        i64_time.clone(),
        modulo(
            // maybe overflow 2262-04-11 23:47:16.854775807 + <slide_duration>
            plus(minus(i64_time, lit(*start_time)), slide_duration.clone()),
            slide_duration.clone(),
        ),
    );
    let window_start = minus(last_start, multiply(lit(i), slide_duration));
    let window_end = plus(window_start.clone(), window_duration);

    // Convert bigint to timestamp
    let window_start = cast(window_start, ns_type.clone());
    let window_end = cast(window_end, ns_type);

    let args = vec![
        (WINDOW_START.to_string(), window_start),
        (WINDOW_END.to_string(), window_end),
    ];

    Expr::NamedStruct(Box::new(args)).alias(WINDOW_COL_NAME)
}

/// Convert tumbling window to new plan
///
/// Original Schema[c1, c2, c3]
///
/// New Schema[_start, _end, c1, c2, c3]
fn build_tumbling_window_plan(
    window: &TimeWindow,
    child: LogicalPlan,
    child_project_exprs: Vec<Expr>,
) -> Result<LogicalPlan> {
    let window_expr = make_window_expr(0, window);
    let mut window_projection: Vec<Expr> = Vec::with_capacity(child_project_exprs.len() + 1);
    window_projection.push(window_expr);
    window_projection.extend(child_project_exprs);

    let filter = is_not_null(window.time_column.clone());
    // Project: [$start, $end, <child exprs>]
    let project_node = LogicalPlanBuilder::from(child)
        .filter(filter)?
        .project(window_projection)?
        .build()?;

    Ok(project_node)
}

/// Convert sliding window to new plan
///
/// Original Schema[c1, c2, c3]
///
/// New Schema[_start, _end, c1, c2, c3]
fn build_sliding_window_plan(
    window: &TimeWindow,
    child: LogicalPlan,
    child_project_exprs: Vec<Expr>,
) -> Result<LogicalPlan> {
    let TimeWindow {
        time_column,
        window_duration,
        slide_duration,
        ..
    } = window;

    let window_ns = window_duration.as_nanos();
    let slide_ns = slide_duration.as_nanos();
    // prevent window_duration + slide_duration from overflowing
    let overlapping_windows = (window_ns + slide_ns - 1) / slide_ns;

    let windows = (0..overlapping_windows)
        .map(|i| make_window_expr(i as i64, window))
        .collect::<Vec<_>>();

    let filter = if window_ns % slide_ns == 0 {
        // When the condition windowDuration % slideDuration = 0 is fulfilled,
        // the estimation of the number of windows becomes exact one,
        // which means all produced windows are valid.
        is_not_null(time_column.clone())
    } else {
        let window_expr = Box::new(windows[0].clone());
        let start = Expr::GetIndexedField(GetIndexedField::new(
            window_expr.clone(),
            ScalarValue::Utf8(Some(WINDOW_START.to_string())),
        ));
        let end = Expr::GetIndexedField(GetIndexedField::new(
            window_expr,
            ScalarValue::Utf8(Some(WINDOW_END.to_string())),
        ));
        and(ge(time_column.clone(), start), lt(time_column.clone(), end))
    };

    // Generate project exprs for each window
    let projections = windows
        .into_iter()
        .map(|e| {
            let mut result: Vec<Expr> = Vec::with_capacity(child_project_exprs.len() + 1);
            result.push(e);
            result.extend(child_project_exprs.clone());
            result
        })
        .collect::<Vec<Vec<_>>>();

    // Expand: [$start, $end, <child exprs>]
    let expand_node = LogicalPlanBuilder::from(child)
        .expand(projections)?
        .filter(filter)?
        .build()?;

    Ok(expand_node)
}

/// Replace udf [`TIME_WINDOW`] with the specified expression
fn replace_window_expr(new_expr: Expr, plan: &LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_expressions_down(&|expr: &Expr| {
        if matches!(expr, Expr::ScalarUDF {
            fun,
            ..
        } if fun.name == TIME_WINDOW)
        {
            Some(new_expr.clone())
        } else {
            None
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::utils::duration::parse_duration;

    #[test]
    fn test_parse_duration() {
        assert!(parse_duration("0.001ms").is_err());
        assert!(parse_duration("0.000001s").is_err());
        assert!(parse_duration("2.1ms").is_err());
        assert!(parse_duration("0.000001m").is_err());

        assert_eq!(parse_duration("0.1s").unwrap(), Duration::from_millis(100));
        assert_eq!(
            parse_duration("0.1m").unwrap(),
            Duration::from_millis(6_000)
        );
    }
}
