use arrow::datatypes::SchemaRef;

use super::{BatchReader, BatchReaderRef, SendableSchemableTskvRecordBatchStream};
use crate::Result;

/// 对数据模式进行调整和对齐，使其与预期完整和一致
pub struct SchemaAlignmenter {
    input: BatchReaderRef,
    schema: SchemaRef,
}

impl SchemaAlignmenter {
    pub fn new(input: BatchReaderRef, schema: SchemaRef) -> Self {
        Self { input, schema }
    }
}

impl BatchReader for SchemaAlignmenter {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        // TODO
        self.input.process()
    }
}
