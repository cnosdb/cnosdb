use std::pin::Pin;
use std::task::{ready, Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt};

use crate::reader::{SchemableTskvRecordBatchStream, SendableSchemableTskvRecordBatchStream};

pub struct TskvToDFStreamAdapter {
    input: SendableSchemableTskvRecordBatchStream,
}

impl TskvToDFStreamAdapter {
    pub fn new(input: SendableSchemableTskvRecordBatchStream) -> Self {
        Self { input }
    }
}

impl Stream for TskvToDFStreamAdapter {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = ready!(self.input.poll_next_unpin(cx))
            .map(|r| r.map_err(|e| DataFusionError::External(Box::new(e))));
        Poll::Ready(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl RecordBatchStream for TskvToDFStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}

pub struct DFToTskvStreamAdapter {
    input: SendableRecordBatchStream,
}

impl Stream for DFToTskvStreamAdapter {
    type Item = Result<RecordBatch, crate::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = ready!(self.input.poll_next_unpin(cx))
            .map(|res| res.map_err(|e| crate::Error::DatafusionError { source: e }));
        Poll::Ready(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}

impl SchemableTskvRecordBatchStream for DFToTskvStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
