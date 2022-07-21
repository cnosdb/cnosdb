use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    physical_plan::RecordBatchStream,
};
use futures::Stream;

use crate::predicate::PredicateRef;

pub struct TableScanStream {
    // data: Vec<RecordBatch>,
    // index: usize,
    proj_schema: SchemaRef,
    filter: PredicateRef,
}

impl TableScanStream {
    pub fn try_new(proj_schema: SchemaRef, filter: PredicateRef) -> Self {
        Self { proj_schema, filter }
    }
}

type ArrowResult<T> = std::result::Result<T, ArrowError>;

impl Stream for TableScanStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>,
                 context: &mut std::task::Context<'_>)
                 -> std::task::Poll<Option<Self::Item>> {
        // let batch_size = context.session_config().batch_size;

        std::task::Poll::Ready(None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // todo   (self.data.len(), Some(self.data.len()))
        (0, Some(0))
    }
}

impl RecordBatchStream for TableScanStream {
    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }
}
