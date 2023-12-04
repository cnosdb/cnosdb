use datafusion::arrow::record_batch::RecordBatch;

pub fn limit_record_batch(remain: Option<&mut usize>, batch: RecordBatch) -> Option<RecordBatch> {
    match remain {
        Some(remain) => {
            if *remain == 0 {
                None
            } else if *remain > batch.num_rows() {
                *remain -= batch.num_rows();
                Some(batch)
            } else {
                let batch = batch.slice(0, *remain);
                *remain = 0;
                Some(batch)
            }
        }
        None => Some(batch),
    }
}
