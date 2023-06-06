use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use config::QueryConfig;
use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, StreamExt};
use meta::model::AdminMetaRef;
use models::record_batch_decode;
use protos::kv_service::tskv_service_client::TskvServiceClient;
use protos::kv_service::{BatchBytesResponse, QueryRecordBatchRequest};
use tonic::transport::Channel;
use tonic::{Request, Streaming};
use tower::timeout::Timeout;
use tskv::query_iterator::TskvSourceMetrics;

use crate::errors::CoordinatorError;
use crate::{CoordinatorRecordBatchStream, SUCCESS_RESPONSE_CODE};

pub struct TonicTskvTagScanStream {
    state: StreamState,
}

impl TonicTskvTagScanStream {
    pub fn new(
        config: QueryConfig,
        node_id: u64,
        request: Request<QueryRecordBatchRequest>,
        admin_meta: AdminMetaRef,
        metrics: TskvSourceMetrics,
    ) -> Self {
        let fetch_result_stream = async move {
            let _timer = metrics.elapsed_build_resp_stream().timer();
            // TODO cache channel
            let channel = admin_meta.get_node_conn(node_id).await?;
            let timeout_channel =
                Timeout::new(channel, Duration::from_millis(config.read_timeout_ms));
            let mut client = TskvServiceClient::<Timeout<Channel>>::new(timeout_channel);
            let resp_stream = client
                .tag_scan(request)
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

impl Stream for TonicTskvTagScanStream {
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

impl CoordinatorRecordBatchStream for TonicTskvTagScanStream {}

pub type RespFuture = BoxFuture<'static, Result<Streaming<BatchBytesResponse>, CoordinatorError>>;

enum StreamState {
    Open {
        fetch_result_stream: RespFuture,
    },
    Scan {
        stream: Streaming<BatchBytesResponse>,
    },
}
