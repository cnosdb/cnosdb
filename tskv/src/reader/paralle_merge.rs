use datafusion::arrow::datatypes::SchemaRef;
use models::schema::TIME_FIELD;

use crate::reader::cut_merge::CutMergeStream;
use crate::reader::{BatchReader, BatchReaderRef, SendableSchemableTskvRecordBatchStream};
use crate::{Error, Result};

pub struct ParallelMergeAdapter {
    schema: SchemaRef,
    inputs: Vec<BatchReaderRef>,
    batch_size: usize,
}

impl ParallelMergeAdapter {
    pub fn try_new(
        schema: SchemaRef,
        inputs: Vec<BatchReaderRef>,
        batch_size: usize,
    ) -> Result<Self> {
        if inputs.is_empty() {
            return Err(Error::CommonError {
                reason: "No inputs provided for ParallelMergeAdapter".to_string(),
            });
        }

        Ok(Self {
            schema,
            inputs,
            batch_size,
        })
    }
}

impl BatchReader for ParallelMergeAdapter {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        let streams = self
            .inputs
            .iter()
            .map(|e| e.process())
            .collect::<Result<Vec<SendableSchemableTskvRecordBatchStream>>>()?;

        Ok(Box::pin(CutMergeStream::new(
            self.schema.clone(),
            streams,
            self.batch_size,
            TIME_FIELD,
        )?))
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ParallelMergeAdapter:")
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        self.inputs.clone()
    }
}
