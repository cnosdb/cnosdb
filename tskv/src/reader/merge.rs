use std::pin::Pin;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow_array::RecordBatch;
use futures::{Stream, StreamExt};
use models::arrow::stream::{BoxStream, ParallelMergeStream};

use super::utils::CombinedRecordBatchStream;
use super::{
    BatchReader, BatchReaderRef, SchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::{Error, Result};

/// 对相同series的数据进行合并
pub struct DataMerger {
    inputs: Vec<BatchReaderRef>,
}
impl DataMerger {
    pub fn new(inputs: Vec<BatchReaderRef>) -> Self {
        Self { inputs }
    }
}
impl BatchReader for DataMerger {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        // TODO: Perform data merging
        // Check if there are any inputs
        if self.inputs.is_empty() {
            return Err(Error::CommonError {
                reason: "No inputs provided for DataMerger".to_string(),
            });
        }

        let streams = self
            .inputs
            .iter()
            .map(|e| e.process())
            .collect::<Result<Vec<_>>>()?;

        let schema = streams[0].schema();

        Ok(Box::pin(CombinedRecordBatchStream::try_new(
            schema, streams,
        )?))
    }
}

pub struct ParallelMergeAdapter {
    schema: SchemaRef,
    inputs: Vec<BatchReaderRef>,
}

impl ParallelMergeAdapter {
    pub fn try_new(schema: SchemaRef, inputs: Vec<BatchReaderRef>) -> Result<Self> {
        if inputs.is_empty() {
            return Err(Error::CommonError {
                reason: "No inputs provided for ParallelMergeAdapter".to_string(),
            });
        }

        Ok(Self { schema, inputs })
    }
}

impl BatchReader for ParallelMergeAdapter {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        let streams = self
            .inputs
            .iter()
            .map(|e| -> Result<BoxStream<_>> { Ok(e.process()?) })
            .collect::<Result<Vec<_>>>()?;

        let stream = ParallelMergeStream::new(None, streams);

        Ok(Box::pin(SchemableParallelMergeStream {
            schema: self.schema.clone(),
            stream,
        }))
    }
}

pub struct SchemableParallelMergeStream {
    schema: SchemaRef,
    stream: ParallelMergeStream<Error>,
}

impl SchemableTskvRecordBatchStream for SchemableParallelMergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SchemableParallelMergeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.poll_next_unpin(cx)
    }
}
