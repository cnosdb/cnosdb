use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, FutureExt, Stream, TryFutureExt};
use models::meta_data::VnodeId;
use tokio::runtime::Runtime;
use trace::SpanRecorder;

use crate::reader::{QueryOption, RowIterator};
use crate::{EngineRef, Error};

type Result<T, E = Error> = std::result::Result<T, E>;

pub struct LocalTskvTableScanStream {
    state: StreamState,
    #[allow(unused)]
    span_recorder: SpanRecorder,
}

impl LocalTskvTableScanStream {
    pub fn new(
        vnode_id: VnodeId,
        option: QueryOption,
        kv_inst: EngineRef,
        runtime: Arc<Runtime>,
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
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                StreamState::Scan { iterator } => {
                    return iterator.next().boxed().poll_unpin(cx);
                }
            }
        }
    }
}

impl Stream for LocalTskvTableScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.poll_inner(cx)
    }
}

pub type RowIteratorFuture = BoxFuture<'static, Result<RowIterator, Error>>;

enum StreamState {
    Open { iter_future: RowIteratorFuture },
    Scan { iterator: RowIterator },
}
