use std::sync::Arc;

use arrow::compute::{interleave, sort_to_indices};
use arrow::util::data_gen::create_random_batch;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::executor::block_on;
use futures::StreamExt;

use crate::reader::{SchemableMemoryBatchReaderStream, SendableSchemableTskvRecordBatchStream};

// create random record_batch [RecordBatch; batch_num]
pub fn random_record_batches(
    schema: SchemaRef,
    column_name: &str,
    batch_size: usize,
    batch_num: usize,
    stream_num: usize,
    range: Option<i64>,
) -> Vec<Vec<RecordBatch>> {
    let batch = create_random_batch(schema.clone(), batch_size * batch_num, 0.35, 0.7).unwrap();
    let chunk_num = batch.num_rows() / stream_num;

    let batches = (0..stream_num)
        .map(|i| batch.slice(i * chunk_num, chunk_num))
        .collect::<Vec<_>>();

    batches
        .into_iter()
        .map(|b| {
            let (idx, _) = schema.column_with_name(column_name).unwrap();
            let array = b.column(idx);
            let array = match range {
                None => array.clone(),
                Some(range) => {
                    let bin_expr = Arc::new(BinaryExpr::new(
                        Arc::new(CastExpr::new(
                            Arc::new(Column::new(column_name, idx)),
                            DataType::Int64,
                            None,
                        )),
                        Operator::Modulo,
                        Arc::new(Literal::new(ScalarValue::Int64(Some(range)))),
                    ));

                    let expr = Arc::new(CastExpr::new(bin_expr, array.data_type().clone(), None));
                    let column_value = expr.evaluate(&b).unwrap();

                    column_value.into_array(array.len()).unwrap()
                }
            };

            let mut new_columns = b.columns().to_vec();
            new_columns[idx] = array.clone();

            let b = RecordBatch::try_new(schema.clone(), new_columns).unwrap();

            let indices = sort_to_indices(&array, Some(SortOptions::default()), None).unwrap();
            let indices = indices
                .iter()
                .filter_map(|i| i.map(|i| (0usize, i as usize)))
                .collect::<Vec<_>>();
            let columns = b
                .columns()
                .iter()
                .map(|a| interleave(&[a.as_ref()], &indices).unwrap())
                .collect::<Vec<_>>();
            let record_batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
            (0..(batch_num / stream_num))
                .map(|i| record_batch.slice(i * batch_size, batch_size))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

pub fn create_sort_merge_stream(
    schema: SchemaRef,
    batches: Vec<Vec<RecordBatch>>,
    column_name: &str,
    batch_size: usize,
) -> SendableSchemableTskvRecordBatchStream {
    let streams = SchemableMemoryBatchReaderStream::new_partitions(schema.clone(), batches);
    crate::reader::sort_merge::sort_merge(
        streams,
        schema,
        batch_size,
        column_name,
        &ExecutionPlanMetricsSet::new(),
    )
    .unwrap()
}

pub fn collect_stream(stream: SendableSchemableTskvRecordBatchStream) -> Vec<RecordBatch> {
    block_on(stream.map(|r| r.unwrap()).collect())
}

pub fn collect_df_stream(stream: SendableRecordBatchStream) -> Vec<RecordBatch> {
    block_on(stream.map(|r| r.unwrap()).collect())
}

pub fn test_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("f0", DataType::Int64, true),
        Field::new("f1", DataType::Float64, true),
        Field::new("f2", DataType::UInt64, true),
        Field::new("f3", DataType::Utf8, true),
        Field::new("f4", DataType::Boolean, true),
    ]))
}

// pub fn create_dedup_stream(
//     schema: SchemaRef,
//     batches: Vec<Vec<RecordBatch>>,
//     batch_size: usize,
//     column_name: &str,
// ) -> SendableRecordBatchStream {
//     let streams = SchemableMemoryBatchReaderStream::new_partitions(schema.clone(), batches)
//         .into_iter()
//         .map(|s| Box::pin(TskvToDFStreamAdapter::new(s)) as SendableRecordBatchStream)
//         .collect::<Vec<_>>();
//
//     let (idx, _) = schema.column_with_name(column_name).unwrap();
//
//     let expr = vec![PhysicalSortExpr {
//         expr: Arc::new(Column::new(column_name, idx)),
//         options: Default::default(),
//     }];
//
//     let stream = streaming_merge(streams, schema.clone(), expr.as_slice(), batch_size).unwrap();
//     Box::pin(DeduplicateStream::new(stream, column_name, schema).unwrap())
// }

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use arrow::compute::concat_batches;
    use arrow_array::{Array, TimestampMicrosecondArray};

    use crate::reader::test_util::{
        collect_stream, create_sort_merge_stream, random_record_batches, test_schema,
    };

    #[test]
    fn test_sort_merge() {
        let schema = test_schema();
        let batch_size = 4096;
        let batch_num = 1000;
        let column_name = "time";
        let batches = random_record_batches(
            schema.clone(),
            column_name,
            batch_size,
            batch_num,
            4,
            Some(1000),
        );
        let stream = create_sort_merge_stream(schema.clone(), batches, column_name, batch_size);
        let res = collect_stream(stream);
        let res_batches = concat_batches(&schema, res.iter()).unwrap();
        let _ = res_batches.num_rows();

        let mut set = BTreeSet::new();
        let array = res_batches.column_by_name("time").unwrap();
        let array = array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let mut last = i64::MIN;
        array.iter().for_each(|v| {
            let v = v.unwrap();
            assert!(v > last);
            last = v;
            assert!(set.insert(v))
        });

        // let stream = create_dedup_stream(schema.clone(), batches.clone(), batch_size, "time");
        // let res = collect_df_stream(stream);
        // let res_batches = concat_batches(&schema, res.iter()).unwrap();
        // let res3 = res_batches.num_rows();
        //
        // let mut set = BTreeSet::new();
        // let array = res_batches.column_by_name("time").unwrap();
        // let array = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
        // let mut last = i64::MIN;
        // array.iter().for_each(|v| {
        //     let v = v.unwrap();
        //     assert!(v > last);
        //     last = v;
        //     assert!(set.insert(v))
        // });
        //
        // assert_eq!(res3, res1);
    }
}
