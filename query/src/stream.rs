use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    physical_plan::RecordBatchStream,
};
use futures::Stream;
use crate::predicate::PredicateRef;
use tskv::engine::EngineRef;

pub struct TableScanStream {
    // data: Vec<RecordBatch>,
    // index: usize,
    proj_schema: SchemaRef,
    filter: PredicateRef,
    batch_size: usize,
    store_engine: EngineRef,
}

impl TableScanStream {
    pub fn new(proj_schema: SchemaRef, filter: PredicateRef, batch_size: usize, store_engine: EngineRef) -> Self {
        Self {
            proj_schema,
            filter,
            batch_size,
            store_engine
        }
    }
}

type ArrowResult<T> = Result<T, ArrowError>;

impl Stream for TableScanStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        // todo: 1. filter series by filter;
        //      2. get fieldid by proj_schema;
        //
        // read()
        
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
