use std::sync::Arc;

use datafusion::physical_plan::PhysicalExpr;

use super::{BatchReader, BatchReaderRef, SendableSchemableTskvRecordBatchStream};
use crate::Result;

pub struct DataFilter {
    predicate: Arc<dyn PhysicalExpr>,
    input: BatchReaderRef,
}
impl DataFilter {
    pub fn new(predicate: Arc<dyn PhysicalExpr>, input: BatchReaderRef) -> Self {
        // TODO predicate 中的column索引可能会与 reader 返回的 RecordBatch 中的索引不一致，需要重新构建索引
        Self { predicate, input }
    }
}
impl BatchReader for DataFilter {
    fn process(&self) -> Result<SendableSchemableTskvRecordBatchStream> {
        // TODO
        self.input.process()
    }
}
