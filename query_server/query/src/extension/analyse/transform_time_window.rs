use std::time::Duration;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{Transformed, TransformedResult as _, TreeNode};
use datafusion::common::DFSchemaRef;
use datafusion::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::utils::expand_wildcard;
use datafusion::logical_expr::{expr, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::optimizer::{OptimizerConfig, OptimizerContext};
use datafusion::prelude::{and, cast, col, get_field, lit, named_struct, Expr};
use datafusion::scalar::ScalarValue;
use models::duration::DAY;
use spi::QueryError;
use trace::debug;

use crate::extension::expr::expr_fn::{ge, is_not_null, lt, minus, modulo, multiply, plus};
use crate::extension::expr::expr_utils::find_exprs_in_exprs_deeply_nested;
use crate::extension::expr::{
    DEFAULT_TIME_WINDOW_START, TIME_WINDOW, WINDOW_COL_NAME, WINDOW_END, WINDOW_START,
};
use crate::extension::logical::logical_plan_builder::LogicalPlanBuilderExt;
use crate::extension::logical::plan_node::LogicalPlanExt;

/// Convert the [`TIME_WINDOW`] function to Expand or project
#[derive(Debug)]
pub struct TransformTimeWindowRule;

impl AnalyzerRule for TransformTimeWindowRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up(&analyze_internal).data()
    }

    fn name(&self) -> &str {
        "transform_time_window"
    }
}

fn analyze_internal(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if plan.inputs().len() == 1 {
        let child = plan.inputs()[0];
        let child_project_exprs = expand_wildcard(child.schema().as_ref(), child, None)?;
        let mut window_expressions = find_window_exprs(&plan);

        // Only support a single window expression for now
        if window_expressions.len() > 1 {
            return Err(DataFusionError::Plan(format!("Only support a single window expression for now, but found: {window_expressions:?}")));
        }

        if window_expressions.len() == 1 {
            let window_expr = window_expressions.remove(0);
            let window = make_time_window(window_expr, plan.schema().clone())
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            debug!("Construct time window: {:?}", window);

            let window_plan = if window.is_tumbling_window() {
                // tumbling_window
                build_tumbling_window_plan(&window, child.clone(), child_project_exprs)?
            } else {
                // sliding_window
                build_sliding_window_plan(&window, child.clone(), child_project_exprs)?
            };

            debug!("Origin plan: {}", plan.display_indent_schema());
            debug!("Window_plan: {}", window_plan.display_indent_schema());

            // replace current plan's child
            let wait_replaced_plan = plan.with_new_exprs(plan.expressions(), vec![window_plan])?;

            debug!(
                "Wait_replaced_plan: {}",
                wait_replaced_plan.display_indent_schema()
            );

            // replace new plan's exprs
            let final_plan = replace_window_expr(
                col(WINDOW_COL_NAME).alias(&window.window_alias),
                &wait_replaced_plan,
            )?;

            debug!("Final plan: {}", final_plan.display_indent_schema());

            return Ok(Transformed::yes(final_plan));
        }
    }

    Ok(Transformed::no(plan))
}

fn find_window_exprs(plan: &LogicalPlan) -> Vec<Expr> {
    let exprs = plan.expressions();
    find_exprs_in_exprs_deeply_nested(&exprs, &|nested_expr| {
        matches!(nested_expr, Expr::ScalarFunction(expr::ScalarFunction {
            func,
            ..
        }) if func.name() == TIME_WINDOW)
    })
}

fn make_time_window(expr: Expr, schema: DFSchemaRef) -> Result<TimeWindow, QueryError> {
    let window_alias = expr.schema_name().to_string();
    match expr {
        Expr::ScalarFunction(expr::ScalarFunction { func, args }) if func.name() == TIME_WINDOW => {
            let mut args = args.into_iter();

            // first arg: time_column
            let time_column = args.next().ok_or_else(|| QueryError::Internal {
                reason: format!("Invalid signature of {TIME_WINDOW}"),
            })?;
            // second arg: window_duration
            let window_duration = args.next().ok_or_else(|| QueryError::Internal {
                reason: format!("Invalid signature of {TIME_WINDOW}"),
            })?;
            let window_duration = simplify_expr(window_duration, schema.clone())?;
            let window_duration = valid_duration(parse_duration_arg(&window_duration)?)?;

            let mut time_window_builder =
                TimeWindowBuilder::new(window_alias, time_column, window_duration);

            // time_window(time, interval '10 seconds', interval '5 milliseconds')
            // third arg: slide_duration
            if let Some(slide_duration) = args.next() {
                let slide_duration = simplify_expr(slide_duration, schema)?;
                let slide_duration = valid_duration(parse_duration_arg(&slide_duration)?)?;
                time_window_builder.with_slide_duration(slide_duration);

                args.next()
                    .map(|start_time| time_window_builder.with_start_time(start_time));
            }

            Ok(time_window_builder.build())
        }
        _ => Err(QueryError::Internal {
            reason: format!("Expected TimeWindow, but found {expr}"),
        }),
    }
}

fn valid_duration(dur: Duration) -> Result<Duration, QueryError> {
    if dur.as_millis() > (365 * DAY) as u128 || dur.as_millis() == 0 {
        return Err(QueryError::InvalidTimeWindowParam {
            reason: format!("Max duration is (0s, 365d], but found {}s", dur.as_secs()),
        });
    }

    Ok(dur)
}

/// Convert string time duration to [`Duration`] \
/// Only support [`ScalarValue::IntervalYearMonth`] | [`ScalarValue::IntervalMonthDayNano`] | [`ScalarValue::IntervalDayTime`]
fn parse_duration_arg(expr: &Expr) -> Result<Duration, QueryError> {
    const NS_IN_1_MS: u64 = 1_000_000; // 1 ms = 1_000_000 ns
    const NS_IN_1_DAY: u64 = 24 * 60 * 60 * 1_000_000_000; // 1 day = 24 hours.
    const NS_IN_30_DAYS: u64 = NS_IN_1_DAY * 30; // assuming 1 month = 30 days.

    let nano = match expr {
        Expr::Literal(ScalarValue::IntervalYearMonth(months)) => {
            months.map(|v| (v as u64) * NS_IN_30_DAYS)
        }
        Expr::Literal(ScalarValue::IntervalMonthDayNano(month_day_ns)) => month_day_ns.map(|v| {
            (v.months as u64) * NS_IN_30_DAYS + (v.days as u64) * NS_IN_1_DAY + v.nanoseconds as u64
        }),
        Expr::Literal(ScalarValue::IntervalDayTime(day_ms)) => {
            day_ms.map(|v| (v.days as u64) * NS_IN_1_DAY + (v.milliseconds as u64) * NS_IN_1_MS)
        }
        _ => {
            return Err(QueryError::InvalidTimeWindowParam {
                reason: format!("Expected interval, but found {expr}"),
            })
        }
    };

    let duration = nano.ok_or_else(|| QueryError::InvalidTimeWindowParam {
        reason: format!("{expr}"),
    })?;
    debug!("duration str: {}", duration);
    Ok(Duration::from_nanos(duration as u64))
}

#[derive(Debug)]
pub struct TimeWindow {
    window_alias: String,
    time_column: Expr,
    // interval, such as: '5s'
    window_duration: Duration,
    // interval
    slide_duration: Duration,
    start_time: Expr,
}

impl TimeWindow {
    pub fn new(
        window_alias: impl Into<String>,
        time_column: Expr,
        window_duration: Duration,
        slide_duration: Duration,
        start_time: Expr,
    ) -> Self {
        Self {
            window_alias: window_alias.into(),
            time_column,
            window_duration,
            slide_duration,
            start_time,
        }
    }

    fn is_tumbling_window(&self) -> bool {
        self.window_duration == self.slide_duration
    }
}

struct TimeWindowBuilder {
    window_alias: String,
    time_column: Expr,
    window_duration: Duration,
    slide_duration: Option<Duration>,
    start_time: Expr,
}

impl TimeWindowBuilder {
    pub fn new(window_alias: String, time_column: Expr, window_duration: Duration) -> Self {
        Self {
            window_alias,
            time_column,
            window_duration,
            slide_duration: Default::default(),
            // Default to unix EPOCH
            start_time: Expr::Literal(DEFAULT_TIME_WINDOW_START.clone()),
        }
    }

    pub fn with_slide_duration(&mut self, slide_duration: Duration) -> &mut Self {
        self.slide_duration = Some(slide_duration);
        self
    }

    pub fn with_start_time(&mut self, start_time: Expr) -> &mut Self {
        self.start_time = start_time;
        self
    }

    pub fn build(self) -> TimeWindow {
        TimeWindow {
            window_alias: self.window_alias,
            time_column: self.time_column,
            window_duration: self.window_duration,
            slide_duration: self.slide_duration.unwrap_or(self.window_duration),
            start_time: self.start_time,
        }
    }
}

/// Generate a window start expression(alias name [`WINDOW_START`])
/// and a window end expression(alias name [`WINDOW_END`])
/// based on the given [`TimeWindow`] parameter
pub fn make_window_expr(i: i64, window: &TimeWindow) -> Expr {
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
    let i64_start_time = modulo(
        cast(start_time.clone(), DataType::Int64),
        window_duration.clone(),
    );

    let last_start = minus(
        i64_time.clone(),
        modulo(
            // maybe overflow 2262-04-11 23:47:16.854775807 + <slide_duration>
            plus(minus(i64_time, i64_start_time), slide_duration.clone()),
            slide_duration.clone(),
        ),
    );
    let window_start = minus(last_start, multiply(lit(i), slide_duration));
    let window_end = plus(window_start.clone(), window_duration);

    // Convert bigint to timestamp
    let window_start = cast(window_start, ns_type.clone());
    let window_end = cast(window_end, ns_type);

    named_struct(vec![
        lit(WINDOW_START),
        window_start,
        lit(WINDOW_END),
        window_end,
    ])
    .alias(WINDOW_COL_NAME)
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

    // Do not allow windows to overlap too much
    if overlapping_windows > 100 {
        return Err(DataFusionError::Plan(format!(
            "Too many overlapping windows: {}",
            overlapping_windows
        )));
    }

    let windows = (0..overlapping_windows)
        .map(|i| make_window_expr(i as i64, window))
        .collect::<Vec<_>>();

    let filter = if window_ns % slide_ns == 0 {
        // When the condition windowDuration % slideDuration = 0 is fulfilled,
        // the estimation of the number of windows becomes exact one,
        // which means all produced windows are valid.
        is_not_null(time_column.clone())
    } else {
        let window_expr = windows[0].clone();
        let start = get_field(
            window_expr.clone(),
            ScalarValue::Utf8(Some(WINDOW_START.to_string())),
        );
        let end = get_field(window_expr, ScalarValue::Utf8(Some(WINDOW_END.to_string())));
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
        if matches!(expr, Expr::ScalarFunction(expr::ScalarFunction {
            func,
            ..
        }) if func.name() == TIME_WINDOW)
        {
            Some(new_expr.clone())
        } else {
            None
        }
    })
}

fn simplify_expr(expr: Expr, schema: DFSchemaRef) -> Result<Expr> {
    let mut execution_props = ExecutionProps::new();
    let ctx = OptimizerContext::new();
    execution_props.query_execution_start_time = ctx.query_execution_start_time();
    let info = SimplifyContext::new(&execution_props).with_schema(schema);
    let simplifier = ExprSimplifier::new(info);
    simplifier.simplify(expr)
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
