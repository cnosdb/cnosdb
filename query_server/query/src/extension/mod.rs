use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{ready, Stream, StreamExt};

pub mod analyse;
pub mod expr;
pub mod logical;
pub mod physical;
pub mod utils;

const EVENT_TIME_COLUMN: &str = "event_time_column";
const WATERMARK_DELAY_MS: &str = "watermark_delay";

/// Filter out empty [`RecordBatch`] in the input stream
pub struct DropEmptyRecordBatchStream {
    input: SendableRecordBatchStream,
}

impl DropEmptyRecordBatchStream {
    pub fn new(input: SendableRecordBatchStream) -> Self {
        Self { input }
    }
}

impl RecordBatchStream for DropEmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

impl Stream for DropEmptyRecordBatchStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                }
                other => return Poll::Ready(other),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::arrow::error::ArrowError;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::Result;
    use datafusion::from_slice::FromSlice;
    use datafusion::physical_plan::RecordBatchStream;
    use futures::{Stream, TryStreamExt};

    use super::DropEmptyRecordBatchStream;

    fn test_custom_schema_ref() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("c1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]))
    }

    fn test_custom_record_batch() -> Result<RecordBatch, ArrowError> {
        RecordBatch::try_new(
            test_custom_schema_ref(),
            vec![
                Arc::new(Int32Array::from_slice([1, 10, 10, 100])),
                Arc::new(Int32Array::from_slice([2, 12, 12, 120])),
            ],
        )
    }

    fn test_empty_record_batch() -> RecordBatch {
        RecordBatch::new_empty(test_custom_schema_ref())
    }

    struct TestRecordBatchStream {
        nb_batch: i32,
    }

    impl RecordBatchStream for TestRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            test_custom_schema_ref()
        }
    }

    impl Stream for TestRecordBatchStream {
        type Item = Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.nb_batch <= 0 {
                Poll::Ready(None)
            } else if self.nb_batch % 2 == 0 {
                self.get_mut().nb_batch -= 1;
                Poll::Ready(Some(test_custom_record_batch().map_err(Into::into)))
            } else {
                self.get_mut().nb_batch -= 1;
                Poll::Ready(Some(Ok(test_empty_record_batch())))
            }
        }
    }

    #[tokio::test]
    async fn test_drop_empty_record_batch_stream() {
        let input = TestRecordBatchStream { nb_batch: 3 };
        let stream = DropEmptyRecordBatchStream::new(Box::pin(input));
        let res: Vec<RecordBatch> = stream.try_collect::<Vec<RecordBatch>>().await.unwrap();
        assert_eq!(res.len(), 1)
    }
}
