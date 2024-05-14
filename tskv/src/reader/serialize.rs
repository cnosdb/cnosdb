use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{ready, Stream, StreamExt};
use models::record_batch_encode;
use protos::kv_service::BatchBytesResponse;
use snafu::IntoError;
use trace::Span;

use crate::error::{ArrowSnafu, TskvResult};
use crate::reader::SendableTskvRecordBatchStream;

pub struct TonicRecordBatchEncoder {
    input: SendableTskvRecordBatchStream,
    #[allow(unused)]
    span: Span,
}

impl TonicRecordBatchEncoder {
    pub fn new(input: SendableTskvRecordBatchStream, span: Span) -> Self {
        Self { input, span }
    }
}

impl Stream for TonicRecordBatchEncoder {
    type Item = TskvResult<BatchBytesResponse>;

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
                Err(err) => Poll::Ready(Some(Err(ArrowSnafu.into_error(err)))),
            },
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}
