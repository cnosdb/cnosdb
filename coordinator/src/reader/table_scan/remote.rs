use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::{ready, Stream, StreamExt};
use models::{record_batch_decode, record_batch_encode};
use protos::kv_service::BatchBytesResponse;
use tonic::Streaming;
use trace::SpanRecorder;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::{
    CoordinatorRecordBatchStream, SendableCoordinatorRecordBatchStream, SUCCESS_RESPONSE_CODE,
};

pub struct TonicRecordBatchDecoder {
    stream: Streaming<BatchBytesResponse>,
}

impl TonicRecordBatchDecoder {
    pub fn new(stream: Streaming<BatchBytesResponse>) -> Self {
        Self { stream }
    }
}

impl Stream for TonicRecordBatchDecoder {
    type Item = Result<RecordBatch, CoordinatorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.stream.poll_next_unpin(cx)) {
            Some(Ok(received)) => {
                // TODO https://github.com/cnosdb/cnosdb/issues/1196
                if received.code != SUCCESS_RESPONSE_CODE {
                    return Poll::Ready(Some(Err(CoordinatorError::GRPCRequest {
                        msg: format!(
                            "server status: {}, {:?}",
                            received.code,
                            String::from_utf8(received.data)
                        ),
                    })));
                }
                match record_batch_decode(&received.data) {
                    Ok(batch) => Poll::Ready(Some(Ok(batch))),
                    Err(err) => Poll::Ready(Some(Err(err.into()))),
                }
            }
            Some(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            None => Poll::Ready(None),
        }
    }
}

impl CoordinatorRecordBatchStream for TonicRecordBatchDecoder {}

pub struct TonicRecordBatchEncoder {
    input: SendableCoordinatorRecordBatchStream,
    #[allow(unused)]
    span_recorder: SpanRecorder,
}

impl TonicRecordBatchEncoder {
    pub fn new(input: SendableCoordinatorRecordBatchStream, span_recorder: SpanRecorder) -> Self {
        Self {
            input,
            span_recorder,
        }
    }
}

impl Stream for TonicRecordBatchEncoder {
    type Item = CoordinatorResult<BatchBytesResponse>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => match record_batch_encode(&batch) {
                Ok(body) => {
                    let resp = BatchBytesResponse {
                        code: SUCCESS_RESPONSE_CODE,
                        data: body,
                    };
                    Poll::Ready(Some(Ok(resp)))
                }
                Err(err) => {
                    let err = CoordinatorError::ArrowError { source: err };
                    Poll::Ready(Some(Err(err)))
                }
            },
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}
