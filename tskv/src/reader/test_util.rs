use arrow::compute::{interleave, sort_to_indices};
use arrow::util::data_gen::create_random_batch;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions, TimeUnit};
use futures::executor::block_on;
use futures::StreamExt;

use crate::reader::cut_merge::CutMergeStream;
use crate::reader::{SchemableMemoryBatchReaderStream, SendableSchemableTskvRecordBatchStream};

// create random record_batch [RecordBatch; batch_num]
pub fn random_record_batches(
    schema: SchemaRef,
    batch_size: usize,
    batch_num: usize,
    column_name: &str,
) -> Vec<RecordBatch> {
    let batch = create_random_batch(schema.clone(), batch_size * batch_num, 0.35, 0.7).unwrap();
    let array = batch.column_by_name(column_name).unwrap();
    let indices = sort_to_indices(array, Some(SortOptions::default()), None).unwrap();
    let indices = indices
        .iter()
        .filter_map(|i| i.map(|i| (0usize, i as usize)))
        .collect::<Vec<_>>();
    let columns = batch
        .columns()
        .iter()
        .map(|a| interleave(&[a.as_ref()], &indices).unwrap())
        .collect::<Vec<_>>();
    let record_batch = RecordBatch::try_new(schema, columns).unwrap();
    (0..batch_num)
        .map(|i| record_batch.slice(i * batch_size, batch_size))
        .collect::<Vec<_>>()
}

/// create SortStream {
///          input: Vec<Stream { input:  Vec<RecordBatch> }>,
///        };
pub fn create_sort_merge_stream(
    schema: SchemaRef,
    batch_size: usize,
    column_name: &str,
    batches: Vec<RecordBatch>,
    chunk_num: usize,
) -> SendableSchemableTskvRecordBatchStream {
    let batches = batches
        .chunks(chunk_num)
        .map(|batches| batches.to_vec())
        .collect::<Vec<_>>();
    let streams = SchemableMemoryBatchReaderStream::new_partitions(schema.clone(), batches);
    crate::reader::sort_merge::sort_merge(streams, schema, batch_size, column_name).unwrap()
}

pub fn merge_same_column(
    schema: SchemaRef,
    batch_size: usize,
    column_name: &str,
    batches: Vec<RecordBatch>,
) -> Vec<RecordBatch> {
    assert_eq!(batches[0].num_rows(), 4096);
    let stream = create_sort_merge_stream(schema, batch_size, column_name, batches, 125);
    let res = collect_stream(stream);
    assert_eq!(res[0].num_rows(), 4096);
    res
}

pub fn collect_stream(stream: SendableSchemableTskvRecordBatchStream) -> Vec<RecordBatch> {
    block_on(stream.map(|r| r.unwrap()).collect())
}

pub fn test_schema() -> SchemaRef {
    SchemaRef::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("f0", DataType::Int64, true),
        Field::new("f1", DataType::Float64, true),
        Field::new("f2", DataType::UInt64, true),
        Field::new("f3", DataType::Utf8, true),
        Field::new("f4", DataType::Boolean, true),
    ]))
}

pub fn create_cut_merge_stream(
    schema: SchemaRef,
    batches: Vec<Vec<RecordBatch>>,
    batch_size: usize,
    column_name: &str,
) -> SendableSchemableTskvRecordBatchStream {
    let streams = SchemableMemoryBatchReaderStream::new_partitions(schema.clone(), batches);
    Box::pin(CutMergeStream::new(schema, streams, batch_size, column_name).unwrap())
}

pub fn create_sort_batches(
    schema: SchemaRef,
    batch_size: usize,
    stream_num: usize,
    batch_num: usize,
    column_name: &str,
) -> Vec<Vec<RecordBatch>> {
    let batches = random_record_batches(schema, batch_size, batch_num, column_name);
    batches
        .chunks(batch_num / stream_num)
        .map(|rs| rs.to_vec())
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod test {
    use crate::reader::test_util::{
        collect_stream, create_cut_merge_stream, create_sort_batches, test_schema,
    };

    #[test]
    fn test_cut_merge() {
        let schema = test_schema();
        let batch_size = 4096;
        let batch_num = 100;
        let column_name = "time";
        let batches = create_sort_batches(schema.clone(), batch_size, 4, batch_num, column_name);
        let stream = create_cut_merge_stream(schema, batches, batch_size, column_name);
        collect_stream(stream);
    }
}
