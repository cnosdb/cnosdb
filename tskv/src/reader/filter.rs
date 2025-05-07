use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::PhysicalExpr;
use futures::{Stream, StreamExt};
use trace::debug;

use super::{
    BatchReader, BatchReaderRef, Predicate, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::reader::metrics::BaselineMetrics;
use crate::reader::utils::reassign_predicate_columns;
use crate::TskvResult;

pub struct DataFilter {
    predicate: Arc<Predicate>,
    input: BatchReaderRef,
    metrics: Arc<ExecutionPlanMetricsSet>,
}
impl DataFilter {
    pub fn new(
        predicate: Arc<Predicate>,
        input: BatchReaderRef,
        metrics: Arc<ExecutionPlanMetricsSet>,
    ) -> Self {
        Self {
            predicate,
            input,
            metrics,
        }
    }
}
impl BatchReader for DataFilter {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        let input = self.input.process()?;
        let schema = input.schema();
        debug!(
            "DataFilter schema: {:?}, predicate: {:?}",
            schema, self.predicate
        );
        let new_predicate = reassign_predicate_columns(self.predicate.clone(), schema.clone())?;
        debug!("Reassigned columns predicate : {:?}", new_predicate);

        Ok(Box::pin(DataFilterStream {
            schema,
            predicate: new_predicate,
            input,
            metrics: BaselineMetrics::new(self.metrics.as_ref()),
        }))
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self.predicate.expr() {
            None => {
                write!(f, "DataFilter: expr=None")
            }
            Some(e) => {
                write!(f, "DataFilter: expr=[{}]", e)
            }
        }
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        vec![self.input.clone()]
    }
}

struct DataFilterStream {
    schema: SchemaRef,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    input: SendableSchemableTskvRecordBatchStream,
    // runtime metrics recording
    metrics: BaselineMetrics,
}

impl SchemableTskvRecordBatchStream for DataFilterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl DataFilterStream {
    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<TskvResult<RecordBatch>>> {
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(value) => match value {
                    Some(Ok(batch)) => {
                        // 记录过滤数据所用时间
                        let _timer = self.metrics.elapsed_compute().timer();
                        let batch = if let Some(filter) = self.predicate.as_ref() {
                            batch_filter(&batch, filter)?
                        } else {
                            batch
                        };
                        // skip entirely filtered batches
                        if batch.num_rows() == 0 {
                            continue;
                        }
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    _ => {
                        return Poll::Ready(value);
                    }
                },
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

impl Stream for DataFilterStream {
    type Item = TskvResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        self.metrics.record_poll(poll)
    }
}

fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> TskvResult<RecordBatch, DataFusionError> {
    predicate
        .evaluate(batch)
        .map(|v| v.into_array(batch.num_rows()))
        .and_then(|into_array_ret| {
            let array = into_array_ret?;
            Ok(as_boolean_array(&array)?)
                // apply filter array to record batch
                .and_then(|filter_array| Ok(filter_record_batch(batch, filter_array)?))
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
    use datafusion::assert_batches_eq;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_plan::expressions::{binary, Column, Literal};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::PhysicalExpr;
    use datafusion::scalar::ScalarValue;
    use futures::TryStreamExt;

    use crate::reader::filter::DataFilter;
    use crate::reader::{BatchReader, MemoryBatchReader, Predicate};

    fn file_record_batchs() -> Vec<RecordBatch> {
        let batch = RecordBatch::try_new(
            file_schema(),
            vec![
                Arc::new(Int64Array::from(vec![-1, 2, 4, 18, 8])),
                Arc::new(StringArray::from(vec![
                    Some("z"),
                    Some("y"),
                    Some("x"),
                    Some("w"),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 4.0, 18.0, 8.0])),
                Arc::new(UInt64Array::from(vec![1, 2, 4, 18, 8])),
            ],
        )
        .expect("create record batch");
        vec![batch]
    }

    fn file_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c1", DataType::UInt64, true),
        ]))
    }

    fn output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("c1", DataType::UInt64, true),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::Utf8, true),
            Field::new("c4", DataType::Boolean, true),
        ]))
    }

    /// time > 2
    fn time_filter() -> Arc<dyn PhysicalExpr> {
        let lhs = Arc::new(Column::new("time", 0));
        let op = Operator::Gt;
        let rhs = Arc::new(Literal::new(ScalarValue::Int64(Some(2))));
        binary(lhs, op, rhs, output_schema().as_ref()).expect("binary predicate")
    }
    /// c1 < 5
    fn c1_filter() -> Arc<dyn PhysicalExpr> {
        let lhs = Arc::new(Column::new("c1", 1));
        let op = Operator::Lt;
        let rhs = Arc::new(Literal::new(ScalarValue::UInt64(Some(5))));
        binary(lhs, op, rhs, output_schema().as_ref()).expect("binary predicate")
    }
    /// c4 = true
    fn c4_filter() -> Arc<dyn PhysicalExpr> {
        let lhs = Arc::new(Column::new("c4", 4));
        let op = Operator::Eq;
        let rhs = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        binary(lhs, op, rhs, output_schema().as_ref()).expect("binary predicate")
    }

    fn and_filter(lhs: Arc<dyn PhysicalExpr>, rhs: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        let op = Operator::And;
        binary(lhs, op, rhs, output_schema().as_ref()).expect("binary predicate")
    }

    #[tokio::test]
    async fn test() {
        let reader = MemoryBatchReader::new(file_schema(), file_record_batchs());
        // time > 2 and c1 < 5 and c4 = true
        let predicate = and_filter(and_filter(time_filter(), c1_filter()), c4_filter());
        let predicate = Arc::new(Predicate::new(Some(predicate), output_schema(), None));

        let filter = DataFilter::new(
            predicate,
            Arc::new(reader),
            Arc::new(ExecutionPlanMetricsSet::new()),
        );

        let stream = filter.process().expect("filter");

        let result = stream.try_collect::<Vec<_>>().await.unwrap();

        let expected = [
            "+------+----+-----+----+",
            "| time | c3 | c2  | c1 |",
            "+------+----+-----+----+",
            "| 4    | x  | 4.0 | 4  |",
            "+------+----+-----+----+",
        ];

        assert_batches_eq!(expected, &result);
    }
}
