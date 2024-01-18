use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use futures::{ready, Stream, StreamExt, TryFutureExt};
use models::meta_data::VnodeId;
use tokio::runtime::Runtime;
use trace::Span;

use super::{iterator, SendableTskvRecordBatchStream};
use crate::reader::QueryOption;
use crate::{EngineRef, TskvError};

type Result<T, E = TskvError> = std::result::Result<T, E>;

pub struct LocalTskvTableScanStream {
    state: StreamState,
    #[allow(unused)]
    span: Span,
}

impl LocalTskvTableScanStream {
    pub fn new(
        vnode_id: VnodeId,
        option: QueryOption,
        kv_inst: EngineRef,
        runtime: Arc<Runtime>,
        span: Span,
    ) -> Self {
        let iter_future = Box::pin(iterator::execute(
            runtime,
            kv_inst,
            option,
            vnode_id,
            Span::enter_with_parent("build vnode stream", &span),
        ));
        let state = StreamState::Open { iter_future };

        Self { state, span }
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
                    return iterator.poll_next_unpin(cx);
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

pub type RowIteratorFuture = BoxFuture<'static, Result<SendableTskvRecordBatchStream, TskvError>>;

enum StreamState {
    Open {
        iter_future: RowIteratorFuture,
    },
    Scan {
        iterator: SendableTskvRecordBatchStream,
    },
}
