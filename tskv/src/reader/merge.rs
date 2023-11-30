use arrow::datatypes::SchemaRef;
use models::schema::TIME_FIELD;

use super::{
    BatchReader, BatchReaderRef, EmptySchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::reader::cut_merge::CutMergeStream;
use crate::reader::sort_merge::sort_merge;
use crate::Result;

/// 对相同series的数据进行合并
pub struct DataMerger {
    schema: SchemaRef,
    inputs: Vec<BatchReaderRef>,
    batch_size: usize,
    single_stream_has_duplication: bool,
}

impl DataMerger {
    pub fn new(schema: SchemaRef, inputs: Vec<BatchReaderRef>, batch_size: usize) -> Self {
        Self {
            schema,
            inputs,
            batch_size,
            single_stream_has_duplication: false,
        }
    }

    pub fn new_with_single_stream_has_duplication(
        schema: SchemaRef,
        inputs: Vec<BatchReaderRef>,
        batch_size: usize,
    ) -> Self {
        Self {
            schema,
            inputs,
            batch_size,
            single_stream_has_duplication: true,
        }
    }

    pub fn process_cut_merge(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        if self.inputs.is_empty() {
            return Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
                self.schema.clone(),
            )));
        }
        let streams = self
            .inputs
            .iter()
            .map(|e| e.process())
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::pin(CutMergeStream::new(
            self.schema.clone(),
            streams,
            self.batch_size,
            TIME_FIELD,
        )?))
    }

    pub fn process_sort_merge(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        if self.inputs.is_empty() {
            return Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
                self.schema.clone(),
            )));
        }
        let streams = self
            .inputs
            .iter()
            .map(|e| e.process())
            .collect::<Result<Vec<_>>>()?;
        sort_merge(streams, self.schema.clone(), self.batch_size, TIME_FIELD)
    }
}

impl BatchReader for DataMerger {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        if self.single_stream_has_duplication {
            self.process_sort_merge()
        } else {
            self.process_cut_merge()
        }
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataMerger: ")
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        self.inputs.clone()
    }
}
