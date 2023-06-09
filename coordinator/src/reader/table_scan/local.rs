use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, TryFutureExt};
use metrics::count::U64Counter;
use models::meta_data::VnodeId;
use tokio::runtime::Runtime;
use trace::SpanRecorder;
use tskv::query_iterator::{QueryOption, RowIterator};
use tskv::{EngineRef, Error};

use crate::errors::CoordinatorError;
use crate::CoordinatorRecordBatchStream;

type Result<T, E = CoordinatorError> = std::result::Result<T, E>;

pub struct LocalTskvTableScanStream {
    state: StreamState,
    data_out: U64Counter,
    #[allow(unused)]
    span_recorder: SpanRecorder,
}

impl LocalTskvTableScanStream {
    pub fn new(
        vnode_id: VnodeId,
        option: QueryOption,
        kv_inst: EngineRef,
        runtime: Arc<Runtime>,
        data_out: U64Counter,
        span_recorder: SpanRecorder,
    ) -> Self {
        let iter_future = Box::pin(RowIterator::new(
            runtime,
            kv_inst,
            option,
            vnode_id,
            span_recorder.child("RowIterator"),
        ));
        let state = StreamState::Open { iter_future };

        Self {
            state,
            data_out,
            span_recorder,
        }
    }

    fn poll_inner(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                StreamState::Open { iter_future } => match ready!(iter_future.try_poll_unpin(cx)) {
                    Ok(iterator) => {
                        self.state = StreamState::Scan { iterator };
                    }
                    Err(err) => {
                        return Poll::Ready(Some(Err(CoordinatorError::TskvError { source: err })))
                    }
                },
                StreamState::Scan { iterator } => {
                    return iterator
                        .next()
                        .boxed()
                        .poll_unpin(cx)
                        .map_err(|err| CoordinatorError::TskvError { source: err });
                }
            }
        }
    }
}

impl CoordinatorRecordBatchStream for LocalTskvTableScanStream {}

impl Stream for LocalTskvTableScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.poll_inner(cx);
        if let Poll::Ready(Some(Ok(batch))) = &poll {
            self.data_out.inc(batch.get_array_memory_size() as u64);
        }
        poll
    }
}

pub type RowIteratorFuture = BoxFuture<'static, Result<RowIterator, Error>>;

enum StreamState {
    Open { iter_future: RowIteratorFuture },
    Scan { iterator: RowIterator },
}
