use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use config::QueryConfig;
use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, StreamExt};
use meta::model::AdminMetaRef;
use models::{record_batch_decode, record_batch_encode};
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{BatchBytesResponse, QueryRecordBatchRequest};
use tonic::transport::Channel;
use tonic::{Request, Streaming};
use tower::timeout::Timeout;
use trace::SpanRecorder;

use crate::errors::{CoordinatorError, CoordinatorResult};
use crate::{
    CoordinatorRecordBatchStream, SendableCoordinatorRecordBatchStream, SUCCESS_RESPONSE_CODE,
};

pub struct TonicTskvTableScanStream {
    state: StreamState,
}

impl TonicTskvTableScanStream {
    pub fn new(
        config: QueryConfig,
        node_id: u64,
        request: Request<QueryRecordBatchRequest>,
        admin_meta: AdminMetaRef,
    ) -> Self {
        let fetch_result_stream = async move {
            let channel = admin_meta.get_node_conn(node_id).await?;
            let timeout_channel =
                Timeout::new(channel, Duration::from_millis(config.read_timeout_ms));
            let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
            let resp_stream = client
                .query_record_batch(request)
                .await
                .map_err(|_| CoordinatorError::FailoverNode { id: node_id })?
                .into_inner();

            Ok(resp_stream)
        };

        let state = StreamState::Open {
            fetch_result_stream: Box::pin(fetch_result_stream),
        };

        Self { state }
    }
}

impl Stream for TonicTskvTableScanStream {
    type Item = Result<RecordBatch, CoordinatorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                StreamState::Open {
                    fetch_result_stream,
                } => {
                    match ready!(fetch_result_stream.poll_unpin(cx)) {
                        Ok(stream) => {
                            self.state = StreamState::Scan { stream };
                        }
                        Err(err) => return Poll::Ready(Some(Err(err))),
                    };
                }
                StreamState::Scan { stream } => {
                    match ready!(stream.poll_next_unpin(cx)) {
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
                                Ok(batch) => return Poll::Ready(Some(Ok(batch))),
                                Err(err) => return Poll::Ready(Some(Err(err.into()))),
                            }
                        }
                        Some(Err(err)) => return Poll::Ready(Some(Err(err.into()))),
                        None => return Poll::Ready(None),
                    }
                }
            }
        }
    }
}

impl CoordinatorRecordBatchStream for TonicTskvTableScanStream {}

pub type RespFuture = BoxFuture<'static, Result<Streaming<BatchBytesResponse>, CoordinatorError>>;

enum StreamState {
    Open {
        fetch_result_stream: RespFuture,
    },
    Scan {
        stream: Streaming<BatchBytesResponse>,
    },
}

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
