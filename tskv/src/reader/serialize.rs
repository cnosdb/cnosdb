use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, Stream, StreamExt};
use models::record_batch_encode;
use protos::kv_service::BatchBytesResponse;
use trace::SpanRecorder;

use crate::error::{Error as TskvError, Result};
use crate::reader::SendableTskvRecordBatchStream;

pub struct TonicRecordBatchEncoder {
    input: SendableTskvRecordBatchStream,
    #[allow(unused)]
    span_recorder: SpanRecorder,
}

impl TonicRecordBatchEncoder {
    pub fn new(input: SendableTskvRecordBatchStream, span_recorder: SpanRecorder) -> Self {
        Self {
            input,
            span_recorder,
        }
    }
}

impl Stream for TonicRecordBatchEncoder {
    type Item = Result<BatchBytesResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => match record_batch_encode(&batch) {
                Ok(body) => {
                    let resp = BatchBytesResponse {
                        data: body,
                        ..Default::default()
                    };
                    Poll::Ready(Some(Ok(resp)))
                }
                Err(err) => {
                    let err = TskvError::Arrow { source: err };
                    Poll::Ready(Some(Err(err)))
                }
            },
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}
