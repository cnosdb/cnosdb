use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use datafusion::common::cast::as_boolean_array;
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use trace::debug;

use super::{
    BatchReader, BatchReaderRef, Predicate, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::Result;

pub struct DataFilter {
    predicate: Arc<Predicate>,
    input: BatchReaderRef,
}
impl DataFilter {
    pub fn new(predicate: Arc<Predicate>, input: BatchReaderRef) -> Self {
        Self { predicate, input }
    }
}
impl BatchReader for DataFilter {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
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
        }))
    }
}

/// Re-assign column indices referenced in predicate according to given schema.
/// If a column is not found in the schema, it will be replaced.
///
/// if we have a file, has [time, c1, c2],
/// If the filter is 'c3 = 1', then it will be replaced by a expr 'true'.
fn reassign_predicate_columns(
    pred: Arc<Predicate>,
    file_schema: SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
    let full_schema = pred.schema();
    let expr = pred.expr();

    let mut rewriter = PredicateColumnsReassigner {
        file_schema,
        full_schema,
        has_col_not_in_file: false,
    };

    expr.rewrite(&mut rewriter)
}

struct PredicateColumnsReassigner {
    file_schema: SchemaRef,
    full_schema: SchemaRef,
    has_col_not_in_file: bool,
}

impl TreeNodeRewriter for PredicateColumnsReassigner {
    type N = Arc<dyn PhysicalExpr>;

    fn mutate(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            // find the column index in file schema
            let index = match self.file_schema.index_of(column.name()) {
                Ok(idx) => idx,
                Err(_) => {
                    // the column expr must be in the full schema
                    return match self.full_schema.field_with_name(column.name()) {
                        Ok(_) => {
                            // 标记 predicate 中含有文件中不存在的列
                            self.has_col_not_in_file = true;
                            Ok(expr)
                        }
                        Err(e) => {
                            // If the column is not in the full schema, should throw the error
                            Err(DataFusionError::ArrowError(e))
                        }
                    };
                }
            };

            return Ok(Arc::new(Column::new(column.name(), index)));
        } else if expr.as_any().downcast_ref::<BinaryExpr>().is_some() && self.has_col_not_in_file {
            let true_value = ScalarValue::Boolean(Some(true));
            // 重置标记
            self.has_col_not_in_file = false;
            return Ok(Arc::new(Literal::new(true_value)));
        }

        Ok(expr)
    }
}

struct DataFilterStream {
    schema: SchemaRef,
    predicate: Arc<dyn PhysicalExpr>,
    input: SendableSchemableTskvRecordBatchStream,
    // TODO runtime metrics recording
}

impl SchemableTskvRecordBatchStream for DataFilterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for DataFilterStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(value) => match value {
                    Some(Ok(batch)) => {
                        let filtered_batch = batch_filter(&batch, &self.predicate)?;
                        // skip entirely filtered batches
                        if filtered_batch.num_rows() == 0 {
                            continue;
                        }
                        return Poll::Ready(Some(Ok(filtered_batch)));
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

fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Result<RecordBatch, DataFusionError> {
    predicate
        .evaluate(batch)
        .map(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
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
        let predicate = Arc::new(Predicate::new(predicate, output_schema()));

        let filter = DataFilter::new(predicate, Arc::new(reader));

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
