use std::sync::Arc;

use chrono::Duration;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{
    DataType, Field, IntervalDayTimeType, IntervalMonthDayNanoType, TimeUnit,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::type_coercion::aggregates::TIMESTAMPS;
use datafusion::logical_expr::{
    ReturnTypeFunction, ScalarUDF, Signature, TypeSignature, Volatility,
};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;
use once_cell::sync::Lazy;
use spi::query::function::FunctionMetadataManager;
use spi::Result;

use super::{TIME_WINDOW, WINDOW_END, WINDOW_START};
use crate::extension::expr::INTERVALS;

pub static TIME_WINDOW_UDF: Lazy<Arc<ScalarUDF>> = Lazy::new(|| Arc::new(new()));
pub static DEFAULT_TIME_WINDOW_START: Lazy<ScalarValue> =
    Lazy::new(|| ScalarValue::TimestampNanosecond(Some(0), Some("+00:00".into())));

pub fn register_udf(func_manager: &mut dyn FunctionMetadataManager) -> Result<ScalarUDF> {
    let udf = new();
    func_manager.register_udf(udf.clone())?;
    Ok(udf)
}

/// export this function for gapfill
pub fn signature() -> Signature {
    // time_window
    // - timeColumn
    // - windowDuration
    // - slideDuration
    // - startTime
    //
    // group by time_window(time, interval '10 second') => group by time_window(time, interval '10 second', interval '5 second', '1970-01-01T00:00:00.000Z')
    // group by time_window(time, interval '10 second', interval '5 second') => group by time_window(time, interval '10 second', interval '5 second', '1970-01-01T00:00:00.000Z')
    // group by time_window(time, interval '10 second', interval '5 second', '1999-12-31T00:00:00.000Z')
    let type_signatures = TIMESTAMPS
        .iter()
        .flat_map(|first| {
            INTERVALS.iter().flat_map(|second| {
                INTERVALS
                    .iter()
                    .flat_map(|third| {
                        [
                            TypeSignature::Exact(vec![
                                first.clone(),
                                second.clone(),
                                third.clone(),
                            ]),
                            TypeSignature::Exact(vec![
                                first.clone(),
                                second.clone(),
                                third.clone(),
                                DataType::Timestamp(TimeUnit::Nanosecond, None),
                            ]),
                        ]
                    })
                    .chain([TypeSignature::Exact(vec![first.clone(), second.clone()])])
            })
        })
        .collect();

    Signature::one_of(type_signatures, Volatility::Immutable)
}

fn new() -> ScalarUDF {
    let func = |_: &[ArrayRef]| {
        Err(DataFusionError::Execution(format!(
            "{} has no specific implementation, should be converted to Expand operator.",
            TIME_WINDOW
        )))
    };
    let func = make_scalar_function(func);

    // Struct(_start, _end)
    let return_type: ReturnTypeFunction = Arc::new(move |input_expr_types| {
        let window = DataType::Struct(vec![
            Field::new(WINDOW_START, input_expr_types[0].clone(), false),
            Field::new(WINDOW_END, input_expr_types[0].clone(), false),
        ]);

        Ok(Arc::new(window))
    });

    ScalarUDF::new(TIME_WINDOW, &signature(), &return_type, &func)
}

/// Return the earliest window where timestamp_ns is located,
/// if timestamp_ns is not in any window, return the first window after timestamp_ns
pub fn ceil_sliding_window(
    timestamp_ns: i64,
    window_duration: &ColumnarValue,
    slide_duration: &ColumnarValue,
    start_time: &ColumnarValue,
) -> DFResult<(i64, i64)> {
    let window_ns = extract_interval_ns(window_duration)?;
    let slide_ns = extract_interval_ns(slide_duration)?;
    let start_time = columnar_value_to_timestamp_ns(start_time)?;

    // all windows
    let overlapping_windows = (window_ns + slide_ns - 1) / slide_ns;

    let mut ceil_window: Option<(i64, i64)> = None;

    for i in (0..overlapping_windows).rev() {
        match sliding_window(
            timestamp_ns,
            window_ns,
            slide_ns,
            start_time,
            // last window
            i,
        )? {
            window @ (start, end) if timestamp_ns >= start && timestamp_ns < end => {
                return Ok(window)
            }
            other => {
                // 不符合条件的窗口，直接跳过
                trace::trace!("Window {other:?} Not match timestamp: {timestamp_ns}, window_duration : {window_ns}, slide_duration: {slide_ns}, start_time: {start_time}");
                ceil_window = Some(other);
            }
        }
    }

    // timestamp_ns 不在任何窗口中，返回 timestamp_ns 后面的第一个窗口
    if let Some(ceil_window) = ceil_window {
        let mut next_window = ceil_window;
        while timestamp_ns >= next_window.1 {
            next_window = (next_window.0 + slide_ns, next_window.1 + slide_ns);
        }
        return Ok(next_window);
    }

    // not reach here
    Err(DataFusionError::Execution(
        "Not match any time window, this maybe a bug".to_string(),
    ))
}

/// Returns the latest window that timestamp_ns is in,
/// or the first window before timestamp_ns if timestamp_ns is not in any window
pub fn floor_sliding_window(
    timestamp_ns: i64,
    window_duration: &ColumnarValue,
    slide_duration: &ColumnarValue,
    start_time: &ColumnarValue,
) -> DFResult<(i64, i64)> {
    let window_ns = extract_interval_ns(window_duration)?;
    let slide_ns = extract_interval_ns(slide_duration)?;
    let start_time = columnar_value_to_timestamp_ns(start_time)?;

    let floor_window = match sliding_window(
        timestamp_ns,
        window_ns,
        slide_ns,
        start_time,
        // last window
        0,
    )? {
        window @ (start, end) if timestamp_ns >= start && timestamp_ns < end => return Ok(window),
        other => {
            // 不符合条件的窗口，直接跳过
            trace::trace!("Window {other:?} Not match timestamp: {timestamp_ns}, window_duration : {window_ns}, slide_duration: {slide_ns}, start_time: {start_time}");
            other
        }
    };

    // timestamp_ns 不在任何窗口中，返回 timestamp_ns 前面的第一个窗口
    let mut next_window = floor_window;
    while timestamp_ns < next_window.0 {
        next_window = (next_window.0 - slide_ns, next_window.1 - slide_ns);
    }

    Ok(next_window)
}

fn sliding_window(
    i64_time: i64,
    window_duration: i64,
    slide_duration: i64,
    start_time: i64,
    i: i64,
) -> DFResult<(i64, i64)> {
    let i64_start_time = start_time % window_duration;
    let last_start = i64_time - (i64_time - i64_start_time + slide_duration) % slide_duration;

    let window_start = last_start - i * slide_duration;
    let window_end = window_start + window_duration;

    Ok((window_start, window_end))
}

fn extract_interval_ns(interval: &ColumnarValue) -> DFResult<i64> {
    let ns = match interval {
        ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(v))) => {
            let (days, ms) = IntervalDayTimeType::to_parts(*v);
            let nanos =
                (Duration::days(days as i64) + Duration::milliseconds(ms as i64)).num_nanoseconds();
            match nanos {
                Some(v) => v,
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Interval is too large, {days}days & {ms}ms"
                    )))
                }
            }
        }
        ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(v))) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);
            if months != 0 {
                return Err(DataFusionError::NotImplemented(
                    "Not support month intervals".to_string(),
                ));
            }
            let nanos =
                (Duration::days(days as i64) + Duration::nanoseconds(nanos)).num_nanoseconds();
            match nanos {
                Some(v) => v,
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "Interval is too large, {days}days & {nanos:?}ns"
                    )))
                }
            }
        }
        ColumnarValue::Scalar(v) => {
            return Err(DataFusionError::Execution(format!(
                "Expect INTERVAL but got {}",
                v.get_datatype()
            )))
        }
        ColumnarValue::Array(_) => {
            return Err(DataFusionError::NotImplemented(
                "Only supports literal values, not arrays".to_string(),
            ))
        }
    };

    Ok(ns)
}

fn columnar_value_to_timestamp_ns(value: &ColumnarValue) -> DFResult<i64> {
    match value {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => Ok(*v),
        ColumnarValue::Scalar(v) => Err(DataFusionError::Execution(format!(
            "TIME_WINDOW expects start_time argument to be a TIMESTAMP but got {}",
            v.get_datatype()
        ))),
        ColumnarValue::Array(_) => Err(DataFusionError::NotImplemented(
            "TIME_WINDOW only supports literal values for the start_time argument, not arrays"
                .to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::physical_plan::ColumnarValue;
    use datafusion::scalar::ScalarValue;

    use super::{ceil_sliding_window, floor_sliding_window};

    fn ceil_sliding_window_once(
        timestamp_ns: i64,
        window_duration: i64,
        slide_duration: i64,
        start_time: i64,
    ) -> (i64, i64) {
        let window_duration: &ColumnarValue = &ColumnarValue::Scalar(
            ScalarValue::IntervalMonthDayNano(Some(window_duration as i128)),
        );
        let slide_duration: &ColumnarValue = &ColumnarValue::Scalar(
            ScalarValue::IntervalMonthDayNano(Some(slide_duration as i128)),
        );
        let start_time: &ColumnarValue =
            &ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(start_time), None));

        ceil_sliding_window(timestamp_ns, window_duration, slide_duration, start_time).unwrap()
    }

    fn floor_sliding_window_once(
        timestamp_ns: i64,
        window_duration: i64,
        slide_duration: i64,
        start_time: i64,
    ) -> (i64, i64) {
        let window_duration: &ColumnarValue = &ColumnarValue::Scalar(
            ScalarValue::IntervalMonthDayNano(Some(window_duration as i128)),
        );
        let slide_duration: &ColumnarValue = &ColumnarValue::Scalar(
            ScalarValue::IntervalMonthDayNano(Some(slide_duration as i128)),
        );
        let start_time: &ColumnarValue =
            &ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(start_time), None));

        floor_sliding_window(timestamp_ns, window_duration, slide_duration, start_time).unwrap()
    }

    fn test_sliding_window(first: bool, args: Vec<(i64, i64, i64, i64)>, expects: Vec<(i64, i64)>) {
        args.into_iter().zip(expects).for_each(|(arg, expect)| {
            let result = if first {
                ceil_sliding_window_once(arg.0, arg.1, arg.2, arg.3)
            } else {
                floor_sliding_window_once(arg.0, arg.1, arg.2, arg.3)
            };

            assert_eq!(result, expect)
        });
    }

    #[test]
    fn test_first_sliding_window_start_bound() {
        #[rustfmt::skip]
        test_sliding_window(
            true,
            vec![
                (9, 5, 2, 0),
                (9, 5, 2, 1),
                (9, 4, 2, 0),
                (0, 5, 2, 0),
                (10, 5, 2, 0),
                (10, 2, 5, 0),
                (946598400000, 5, 10, 2),
                (946598400000, 5000000000000000, 10, 2),
            ],
            vec![
                (6, 11),
                (5, 10),
                (6, 10),
                (-4, 1),
                (6, 11),
                (10, 12),
                (946598400002, 946598400007),
                (-4999053401599998, 946598400002),
            ],
        );
    }

    #[test]
    fn test_last_sliding_window_start_bound() {
        #[rustfmt::skip]
        test_sliding_window(
            false,
            vec![
                (9, 5, 2, 0),
                (9, 5, 2, 1),
                (9, 4, 2, 0),
                (0, 5, 2, 0),
                (10, 5, 2, 0),
                (13, 2, 5, 0),
            ],
            vec![
                (8, 13),
                (9, 14),
                (8, 12),
                (0, 5),
                (10, 15),
                (10, 12),
            ],
        );
    }
}
