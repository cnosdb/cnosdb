use std::ops::{Bound, Range};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::{ColumnarValue, TableSource, UserDefinedLogicalNode};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::scalar::ScalarValue;
use models::arrow::DataType;

pub fn downcast_plan_node<T: 'static>(node: &dyn UserDefinedLogicalNode) -> Option<&T> {
    node.as_any().downcast_ref::<T>()
}

pub fn downcast_execution_plan<T: 'static>(plan: &dyn ExecutionPlan) -> Option<&T> {
    plan.as_any().downcast_ref::<T>()
}

pub fn downcast_table_source<T: 'static>(plan: &dyn TableSource) -> Option<&T> {
    plan.as_any().downcast_ref::<T>()
}

pub fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> DFResult<RecordBatch> {
    predicate
        .evaluate(batch)
        .map(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            Ok(as_boolean_array(array?.as_ref())?)
                // apply filter array to record batch
                .and_then(|filter_array| Ok(filter_record_batch(batch, filter_array)?))
        })
}

pub fn try_map_range<T, U, F>(tr: &Range<T>, mut f: F) -> DFResult<Range<U>>
where
    F: FnMut(&T) -> DFResult<U>,
{
    Ok(Range {
        start: f(&tr.start)?,
        end: f(&tr.end)?,
    })
}

pub fn try_map_bound<T, U, F>(bt: Bound<T>, mut f: F) -> DFResult<Bound<U>>
where
    F: FnMut(T) -> DFResult<U>,
{
    Ok(match bt {
        Bound::Excluded(t) => Bound::Excluded(f(t)?),
        Bound::Included(t) => Bound::Included(f(t)?),
        Bound::Unbounded => Bound::Unbounded,
    })
}

pub fn bound_extract<T>(b: &Bound<T>) -> Option<&T> {
    match b {
        Bound::Included(t) | Bound::Excluded(t) => Some(t),
        Bound::Unbounded => None,
    }
}

pub fn numeric_arr_as_f64_iter(values: &dyn Array) -> DFResult<Box<dyn Iterator<Item = f64> + '_>> {
    Ok(match values.data_type() {
        DataType::Float64 => Box::new(
            values
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "type mismatch: expected Float64Array, got {:?}",
                        values.data_type()
                    ))
                })?
                .iter()
                .map(|x| x.unwrap_or(f64::NAN)),
        ),
        DataType::Int64 => Box::new(
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "type mismatch: expected Int64Array, got {:?}",
                        values.data_type()
                    ))
                })?
                .iter()
                .map(|x| x.map_or(f64::NAN, |x| x as f64)),
        ),
        DataType::UInt64 => Box::new(
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "type mismatch: expected UInt64Array, got {:?}",
                        values.data_type()
                    ))
                })?
                .iter()
                .map(|x| x.map_or(f64::NAN, |x| x as f64)),
        ),
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "type cannot be converted to f64: {:?}",
            values.data_type()
        )))?,
    })
}

pub fn timestamp_arr_as_i64ns_iter(
    values: &dyn Array,
) -> DFResult<Box<dyn Iterator<Item = DFResult<i64>> + '_>> {
    Ok(match values.data_type() {
        DataType::Timestamp(models::arrow::TimeUnit::Millisecond, None) => Box::new(
            values
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "type mismatch: expected TimestampMillisecondArray, got {:?}",
                        values.data_type()
                    ))
                })?
                .iter()
                .map(|x| {
                    x.map(|i| i * 1_000_000).ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "null value in timestamp array".to_string(),
                        )
                    })
                }),
        ),
        DataType::Timestamp(models::arrow::TimeUnit::Microsecond, None) => Box::new(
            values
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "type mismatch: expected TimestampMicrosecondArray, got {:?}",
                        values.data_type()
                    ))
                })?
                .iter()
                .map(|x| {
                    x.map(|i| i * 1_000).ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "null value in timestamp array".to_string(),
                        )
                    })
                }),
        ),
        DataType::Timestamp(models::arrow::TimeUnit::Nanosecond, None) => Box::new(
            values
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "type mismatch: expected TimestampNanosecondArray, got {:?}",
                        values.data_type()
                    ))
                })?
                .iter()
                .map(|x| {
                    x.ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "null value in timestamp array".to_string(),
                        )
                    })
                }),
        ),
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "type cannot be converted to i64: {:?}",
            values.data_type()
        )))?,
    })
}

pub fn f64_vec_to_numeric_arr(values: Vec<f64>, data_type: &DataType) -> DFResult<ArrayRef> {
    match data_type {
        DataType::Float64 => Ok(Arc::new(Float64Array::from(values))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            values.iter().map(|&x| x as i64).collect::<Vec<_>>(),
        ))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(
            values.iter().map(|&x| x as u64).collect::<Vec<_>>(),
        ))),
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "values cannot be converted to data_type: {:?}",
            data_type
        ))),
    }
}

pub fn i64ns_vec_to_timestamp_arr(values: Vec<i64>, data_type: &DataType) -> DFResult<ArrayRef> {
    match data_type {
        DataType::Timestamp(models::arrow::TimeUnit::Millisecond, None) => Ok(Arc::new(
            TimestampMillisecondArray::from_iter_values(values.iter().map(|&x| x / 1_000_000)),
        )),
        DataType::Timestamp(models::arrow::TimeUnit::Microsecond, None) => Ok(Arc::new(
            TimestampMicrosecondArray::from_iter_values(values.iter().map(|&x| x / 1_000)),
        )),
        DataType::Timestamp(models::arrow::TimeUnit::Nanosecond, None) => Ok(Arc::new(
            TimestampNanosecondArray::from_iter_values(values.iter().copied()),
        )),
        _ => Err(datafusion::error::DataFusionError::Execution(format!(
            "values cannot be converted to data_type: {:?}",
            data_type
        ))),
    }
}

pub fn columnar_value_to_array(columnar_value: ColumnarValue) -> DFResult<ArrayRef> {
    match columnar_value {
        ColumnarValue::Array(array) => Ok(array),
        _ => Err(datafusion::error::DataFusionError::Internal(
            "Expected array".to_string(),
        )),
    }
}

pub fn columnar_value_to_scalar(columnar_value: ColumnarValue) -> DFResult<ScalarValue> {
    match columnar_value {
        ColumnarValue::Scalar(scalar) => Ok(scalar),
        _ => Err(datafusion::error::DataFusionError::Internal(
            "Expected scalar".to_string(),
        )),
    }
}
