use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use models::schema::TIME_FIELD_NAME;

use super::{
    BatchReader, BatchReaderRef, EmptySchemableTskvRecordBatchStream,
    SendableSchemableTskvRecordBatchStream,
};
use crate::reader::sort_merge::sort_merge;
use crate::TskvResult;

/// 对相同series的数据进行合并
pub struct DataMerger {
    schema: SchemaRef,
    inputs: Vec<BatchReaderRef>,
    batch_size: usize,

    metrics: Arc<ExecutionPlanMetricsSet>,
}

impl DataMerger {
    pub fn new(
        schema: SchemaRef,
        inputs: Vec<BatchReaderRef>,
        batch_size: usize,
        metrics: Arc<ExecutionPlanMetricsSet>,
    ) -> Self {
        Self {
            schema,
            inputs,
            batch_size,
            metrics,
        }
    }

    fn process_sort_merge(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        if self.inputs.is_empty() {
            return Ok(Box::pin(EmptySchemableTskvRecordBatchStream::new(
                self.schema.clone(),
            )));
        }
        let streams = self
            .inputs
            .iter()
            .map(|e| e.process())
            .collect::<TskvResult<Vec<_>>>()?;
        sort_merge(
            streams,
            self.schema.clone(),
            self.batch_size,
            TIME_FIELD_NAME,
            &self.metrics,
        )
    }
}

impl BatchReader for DataMerger {
    fn process(&self) -> TskvResult<SendableSchemableTskvRecordBatchStream> {
        self.process_sort_merge()
    }

    fn fmt_as(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DataMerger: ")
    }

    fn children(&self) -> Vec<BatchReaderRef> {
        self.inputs.clone()
    }
}
