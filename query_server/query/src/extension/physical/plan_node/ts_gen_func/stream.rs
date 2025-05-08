use std::sync::Arc;
use std::task::Poll;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{PhysicalExpr, RecordBatchStream, SendableRecordBatchStream};
use datafusion::scalar::ScalarValue;
use futures::{ready, Stream, StreamExt};
use models::arrow::SchemaRef;
use spi::DFResult;

use crate::extension::expr::TsGenFunc;
use crate::extension::utils::{
    columnar_value_to_array, columnar_value_to_scalar, f64_vec_to_numeric_arr,
    i64ns_vec_to_timestamp_arr, numeric_arr_as_f64_iter, timestamp_arr_as_i64ns_iter,
};

pub struct TSGenFuncStream {
    input: SendableRecordBatchStream,
    time_expr: Arc<dyn PhysicalExpr>,
    field_exprs: Vec<Arc<dyn PhysicalExpr>>,
    arg_expr: Option<Arc<dyn PhysicalExpr>>,
    symbol: TsGenFunc,
    schema: SchemaRef,
    time_vec: Vec<i64>,
    field_vecs: Vec<Vec<f64>>,
    /// Arguments for the function, url-encoded string.
    arg: Option<String>,
    finished: bool,
}

impl TSGenFuncStream {
    pub fn new(
        input: SendableRecordBatchStream,
        time_expr: Arc<dyn PhysicalExpr>,
        field_exprs: Vec<Arc<dyn PhysicalExpr>>,
        arg_expr: Option<Arc<dyn PhysicalExpr>>,
        symbol: TsGenFunc,
        schema: SchemaRef,
    ) -> Self {
        let field_num = field_exprs.len();
        Self {
            input,
            time_expr,
            field_exprs,
            arg_expr,
            symbol,
            schema,
            time_vec: Vec::new(),
            field_vecs: vec![Vec::new(); field_num],
            arg: None,
            finished: false,
        }
    }

    fn get_result_record_batch(&mut self) -> DFResult<RecordBatch> {
        let (time_res_vec, values_res_vec) = self.symbol.compute(
            &mut self.time_vec,
            &mut self.field_vecs,
            self.arg.as_deref(),
        )?;

        Ok(RecordBatch::try_new(
            self.schema.clone(),
            vec![
                i64ns_vec_to_timestamp_arr(time_res_vec, self.schema.field(0).data_type())?,
                f64_vec_to_numeric_arr(values_res_vec, self.schema.field(1).data_type())?,
            ],
        )?)
    }

    fn append_batch_to_vecs(&mut self, batch: RecordBatch) -> DFResult<()> {
        let time_column = columnar_value_to_array(self.time_expr.evaluate(&batch)?)?;
        let field_columns = self
            .field_exprs
            .iter()
            .map(|expr| columnar_value_to_array(expr.evaluate(&batch)?))
            .collect::<DFResult<Vec<_>>>()?;
        if self.time_vec.is_empty() {
            self.arg = if let Some(arg_expr) = &self.arg_expr {
                let scalar = columnar_value_to_scalar(arg_expr.evaluate(&batch)?)?;
                if let ScalarValue::Utf8(Some(s)) = scalar {
                    Some(s)
                } else {
                    return Err(datafusion::error::DataFusionError::Internal(
                        "TSGenFuncExec expected arg_expr to be Utf8 scalar".to_string(),
                    ));
                }
            } else {
                None
            };
        }

        append_time(&mut self.time_vec, time_column)?;
        append_field(&mut self.field_vecs, field_columns)?;
        Ok(())
    }
}

impl RecordBatchStream for TSGenFuncStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for TSGenFuncStream {
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
                    if let Err(e) = self.append_batch_to_vecs(batch) {
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
        Poll::Ready(Some(self.get_result_record_batch()))
    }
}

fn append_time(time_vec: &mut Vec<i64>, time_column: ArrayRef) -> DFResult<()> {
    let iter = timestamp_arr_as_i64ns_iter(&time_column)?;
    for time in iter {
        time_vec.push(time?);
    }
    Ok(())
}

fn append_field(field_vecs: &mut [Vec<f64>], field_columns: Vec<ArrayRef>) -> DFResult<()> {
    for (field_vec, field_column) in field_vecs.iter_mut().zip(field_columns) {
        field_vec.extend(numeric_arr_as_f64_iter(&field_column)?);
    }

    Ok(())
}
