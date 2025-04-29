use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{PhysicalExpr, RecordBatchStream, SendableRecordBatchStream};
use datafusion::scalar::ScalarValue;
use futures::{ready, Stream, StreamExt};
use models::arrow::SchemaRef;
use spi::DFResult;

use crate::extension::expr::TimeSeriesGenFunc;
use crate::extension::utils::{
    columnar_value_to_array, columnar_value_to_scalar, f64_vec_to_numeric_arr,
    i64ns_vec_to_timestamp_arr, numeric_arr_as_f64_iter, timestamp_arr_as_i64ns_iter,
};

pub struct TimeSeriesGenFuncStream {
    input: SendableRecordBatchStream,
    time_expr: Arc<dyn PhysicalExpr>,
    field_expr: Arc<dyn PhysicalExpr>,
    arg_expr: Option<Arc<dyn PhysicalExpr>>,
    symbol: TimeSeriesGenFunc,
    schema: SchemaRef,
    time_vec: Vec<i64>,
    field_vec: Vec<f64>,
    /// Arguments for the function, url-encoded string.
    arg: Option<String>,
    finished: bool,
}

impl TimeSeriesGenFuncStream {
    pub fn new(
        input: SendableRecordBatchStream,
        time_expr: Arc<dyn PhysicalExpr>,
        field_expr: Arc<dyn PhysicalExpr>,
        arg_expr: Option<Arc<dyn PhysicalExpr>>,
        symbol: TimeSeriesGenFunc,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            time_expr,
            field_expr,
            arg_expr,
            symbol,
            schema,
            time_vec: Vec::new(),
            field_vec: Vec::new(),
            arg: None,
            finished: false,
        }
    }

    /// Append a record batch to the stream.
    fn append_record_batch(&mut self, batch: RecordBatch) -> DFResult<()> {
        let time_column = columnar_value_to_array(self.time_expr.evaluate(&batch)?)?;
        let field_column = columnar_value_to_array(self.field_expr.evaluate(&batch)?)?;
        if self.time_vec.is_empty() {
            self.arg = if let Some(arg_expr) = &self.arg_expr {
                let scalar = columnar_value_to_scalar(arg_expr.evaluate(&batch)?)?;
                if let ScalarValue::Utf8(Some(s)) = scalar {
                    Some(s)
                } else {
                    return Err(datafusion::error::DataFusionError::Internal(
                        "TimeSeriesGenFuncExec expected arg_expr to be Utf8 scalar".to_string(),
                    ));
                }
            } else {
                None
            };
        }

        append_time(&mut self.time_vec, time_column)?;
        append_field(&mut self.field_vec, field_column)?;
        Ok(())
    }

    /// Generate time series after appending all record batches,
    /// get the result RecordBatch with generated time-series data.
    fn generate_time_series(&mut self) -> DFResult<RecordBatch> {
        let (time_res_vec, values_res_vec) =
            self.symbol
                .compute(&mut self.time_vec, &mut self.field_vec, self.arg.as_deref())?;

        Ok(RecordBatch::try_new(
            self.schema.clone(),
            vec![
                i64ns_vec_to_timestamp_arr(time_res_vec, self.schema.field(0).data_type())?,
                f64_vec_to_numeric_arr(values_res_vec, self.schema.field(1).data_type())?,
            ],
        )?)
    }
}

impl RecordBatchStream for TimeSeriesGenFuncStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for TimeSeriesGenFuncStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let mut more_input = true;
        while more_input {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if let Err(e) = self.append_record_batch(batch) {
                        self.finished = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Some(Err(e)) => {
                    self.finished = true;
                    return Poll::Ready(Some(Err(e)));
                }
                None => more_input = false,
            }
        }

        self.finished = true;
        Poll::Ready(Some(self.generate_time_series()))
    }
}

fn append_time(time_vec: &mut Vec<i64>, time_column: ArrayRef) -> DFResult<()> {
    let iter = timestamp_arr_as_i64ns_iter(&time_column)?;
    for time in iter {
        time_vec.push(time?);
    }
    Ok(())
}

fn append_field(field_vec: &mut Vec<f64>, field_column: ArrayRef) -> DFResult<()> {
    let iter = numeric_arr_as_f64_iter(&field_column)?;
    field_vec.extend(iter);
    Ok(())
}
