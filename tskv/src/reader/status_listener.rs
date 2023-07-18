use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::record_batch::RecordBatch;
use futures::{ready, Stream, StreamExt};
use meta::model::MetaRef;
use models::meta_data::VnodeId;

use crate::error::Result;
use crate::reader::SendableTskvRecordBatchStream;

pub struct VnodeStatusListener {
    tenant: String,
    meta: MetaRef,
    vnode_id: VnodeId,
    input: SendableTskvRecordBatchStream,
}

impl VnodeStatusListener {
    pub fn new(
        tenant: impl Into<String>,
        meta: MetaRef,
        vnode_id: VnodeId,
        input: SendableTskvRecordBatchStream,
    ) -> Self {
        Self {
            tenant: tenant.into(),
            meta,
            vnode_id,
            input,
        }
    }
}

impl Stream for VnodeStatusListener {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => Poll::Ready(Some(Ok(batch))),
            Some(Err(err)) => Poll::Ready(Some(Err(err))),
            None => Poll::Ready(None),
        }
    }
}
