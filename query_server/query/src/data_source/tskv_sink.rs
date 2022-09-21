use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;

use protos::kv_service::WritePointsRpcRequest;
use snafu::ResultExt;
use trace::debug;
use tskv::engine::EngineRef;

use crate::{schema::TableSchema, utils::point_util::record_batch_to_points_flat_buffer};

use super::sink::{RecordBatchSink, RecordBatchSinkPrivider};

use super::PointUtilSnafu;
use super::Result;
use super::TskvSnafu;

pub struct TskvRecordBatchSink {
    engine: EngineRef,
    partition: usize,
    schema: TableSchema,
}

#[async_trait]
impl RecordBatchSink for TskvRecordBatchSink {
    async fn append(&self, record_batch: RecordBatch) -> Result<()> {
        debug!(
            "Partition: {}, \nTableSchema: {:?}, \nTskvRecordBatchSink::append: {:?}",
            self.partition, self.schema, record_batch,
        );

        let points = record_batch_to_points_flat_buffer(&record_batch, self.schema.clone())
            .context(PointUtilSnafu)?;

        let req = WritePointsRpcRequest { version: 0, points };

        let _ = self.engine.write(req).await.context(TskvSnafu)?;

        Ok(())
    }
}

pub struct TskvRecordBatchSinkPrivider {
    engine: EngineRef,
    schema: TableSchema,
}

impl TskvRecordBatchSinkPrivider {
    pub fn new(engine: EngineRef, schema: TableSchema) -> Self {
        Self { engine, schema }
    }
}

impl RecordBatchSinkPrivider for TskvRecordBatchSinkPrivider {
    fn create_batch_sink(&self, partition: usize) -> Box<dyn RecordBatchSink> {
        Box::new(TskvRecordBatchSink {
            engine: self.engine.clone(),
            partition,
            schema: self.schema.clone(),
        })
    }
}
