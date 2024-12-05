use arrow::compute::filter_record_batch;
use arrow_array::{
    BooleanArray, Int64Array, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray,
};
use arrow_buffer::builder::BooleanBufferBuilder;
use arrow_schema::{ArrowError, DataType, TimeUnit};
use models::predicate::domain::TimeRange;

pub fn filter_record_batch_by_time_range(
    record_batch: RecordBatch,
    time_range: TimeRange,
) -> Result<RecordBatch, ArrowError> {
    let num_rows = record_batch.num_rows();
    let mut bit_set = BooleanBufferBuilder::new(num_rows);
    bit_set.append_n(num_rows, false);
    let time_column_type = record_batch.schema().field(0).data_type().clone();
    let time_column = match time_column_type {
        DataType::Timestamp(TimeUnit::Nanosecond, _) => record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .values(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .values(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => record_batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values(),
        DataType::Int64 => record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        _ => {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Unsupported time column type: {:?}",
                time_column_type
            )));
        }
    };
    // todo: handle time column
    for (idx, value) in time_column.iter().enumerate() {
        let ts = *value;
        if time_range.contains(ts) {
            bit_set.set_bit(idx, true);
        }
    }
    let boolean_array = BooleanArray::new(bit_set.finish(), None);
    filter_record_batch(&record_batch, &boolean_array)
}
