//! Evaluate the parameters to be used for gap filling.
use std::collections::HashMap;
use std::ops::Bound;

use chrono::Duration;
use datafusion::arrow::datatypes::{IntervalMonthDayNanoType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::datetime_expressions::date_bin;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::ColumnarValue;
use datafusion::scalar::ScalarValue;

use super::{try_map_bound, try_map_range, FillStrategy, GapFillExecParams};

/// The parameters to gap filling. Included here are the parameters
/// that remain constant during gap filling, i.e., not the streaming table
/// data, or anything else.
/// When we support `locf` for aggregate columns, that will be tracked here.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GapFillParams {
    /// The stride in nanoseconds of the timestamps to be output.
    pub stride: i64,
    /// The first timestamp (inclusive) to be output for each series,
    /// in nanoseconds since the epoch. `None` means gap filling should
    /// start from the first timestamp in each series.
    pub first_ts: Option<i64>,
    /// The last timestamp (inclusive!) to be output for each series,
    /// in nanoseconds since the epoch.
    pub last_ts: i64,
    /// What to do when filling gaps in aggregate columns.
    /// The map is keyed on the columns offset in the schema.
    pub fill_strategy: HashMap<usize, FillStrategy>,
}

impl GapFillParams {
    /// Create a new [GapFillParams] by figuring out the actual values (as native i64) for the stride,
    /// first and last timestamp for gap filling.
    pub(super) fn try_new(schema: SchemaRef, params: &GapFillExecParams) -> Result<Self> {
        let batch = RecordBatch::new_empty(schema);
        let stride = params.stride.evaluate(&batch)?;
        let origin = params
            .origin
            .as_ref()
            .map(|e| e.evaluate(&batch))
            .transpose()?;

        // Evaluate the upper and lower bounds of the time range
        let range = try_map_range(&params.time_range, |b| {
            try_map_bound(b.as_ref(), |pe| {
                extract_timestamp_nanos(&pe.evaluate(&batch)?)
            })
        })?;

        // Find the smallest timestamp that might appear in the
        // range. There might not be one, which is okay.
        let first_ts = match range.start {
            Bound::Included(v) => Some(v),
            Bound::Excluded(v) => Some(v + 1),
            Bound::Unbounded => None,
        };

        // Find the largest timestamp that might appear in the
        // range
        let last_ts = match range.end {
            Bound::Included(v) => v,
            Bound::Excluded(v) => v - 1,
            Bound::Unbounded => {
                return Err(DataFusionError::Execution(
                    "missing upper time bound for gap filling".to_string(),
                ))
            }
        };

        // Call date_bin on the timestamps to find the first and last time bins
        // for each series
        let mut args = vec![stride, i64_to_columnar_ts(first_ts)];
        if let Some(v) = origin {
            args.push(v)
        }
        let first_ts = first_ts
            .map(|_| extract_timestamp_nanos(&date_bin(&args)?))
            .transpose()?;
        args[1] = i64_to_columnar_ts(Some(last_ts));
        let last_ts = extract_timestamp_nanos(&date_bin(&args)?)?;

        let fill_strategy = params
            .fill_strategy
            .iter()
            .map(|(e, fs)| {
                let idx = e
                    .as_any()
                    .downcast_ref::<Column>()
                    .ok_or(DataFusionError::Internal(format!(
                        "fill strategy aggr expr was not a column: {e:?}",
                    )))?
                    .index();
                Ok((idx, fs.clone()))
            })
            .collect::<Result<HashMap<usize, FillStrategy>>>()?;

        Ok(Self {
            stride: extract_interval_nanos(&args[0])?,
            first_ts,
            last_ts,
            fill_strategy,
        })
    }

    /// Returns the number of rows remaining for a series that starts with first_ts.
    pub fn valid_row_count(&self, first_ts: i64) -> usize {
        if self.last_ts >= first_ts {
            ((self.last_ts - first_ts) / self.stride + 1) as usize
        } else {
            0
        }
    }
}

fn i64_to_columnar_ts(i: Option<i64>) -> ColumnarValue {
    match i {
        Some(i) => ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(i), None)),
        None => ColumnarValue::Scalar(ScalarValue::Null),
    }
}

fn extract_timestamp_nanos(cv: &ColumnarValue) -> Result<i64> {
    Ok(match cv {
        ColumnarValue::Scalar(ScalarValue::TimestampNanosecond(Some(v), _)) => *v,
        _ => {
            return Err(DataFusionError::Execution(
                "gap filling argument must be a scalar timestamp".to_string(),
            ))
        }
    })
}

fn extract_interval_nanos(cv: &ColumnarValue) -> Result<i64> {
    match cv {
        ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(v))) => {
            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(*v);

            if months != 0 {
                return Err(DataFusionError::Execution(
                    "gap filling does not support month intervals".to_string(),
                ));
            }

            let nanos =
                (Duration::days(days as i64) + Duration::nanoseconds(nanos)).num_nanoseconds();
            nanos.ok_or_else(|| {
                DataFusionError::Execution("gap filling argument is too large".to_string())
            })
        }
        _ => Err(DataFusionError::Execution(
            "gap filling expects a stride parameter to be a scalar interval".to_string(),
        )),
    }
}
