use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;

use datafusion::physical_plan::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use models::schema::TableSchema;
use protos::kv_service::WritePointsRpcRequest;
use snafu::ResultExt;
use trace::debug;
use tskv::engine::EngineRef;

use crate::utils::point_util::record_batch_to_points_flat_buffer;

use super::sink::{RecordBatchSink, RecordBatchSinkProvider};

use super::PointUtilSnafu;
use super::Result;
use super::TskvSnafu;

pub struct TskvRecordBatchSink {
    engine: EngineRef,
    partition: usize,
    schema: TableSchema,

    metrics: TskvSinkMetrics,
}

#[async_trait]
impl RecordBatchSink for TskvRecordBatchSink {
    async fn append(&self, record_batch: RecordBatch) -> Result<()> {
        debug!(
            "Partition: {}, \nTableSchema: {:?}, \nTskvRecordBatchSink::append: {:?}",
            self.partition, self.schema, record_batch,
        );

        // record batchs to points
        let timer = self.metrics.elapsed_record_batch_to_point().timer();
        let points = record_batch_to_points_flat_buffer(&record_batch, self.schema.clone())
            .context(PointUtilSnafu)?;
        timer.done();

        // points write request
        let timer = self.metrics.elapsed_point_write().timer();
        let req = WritePointsRpcRequest { version: 0, points };
        let _ = self.engine.write(req).await.context(TskvSnafu)?;
        timer.done();

        Ok(())
    }
}

pub struct TskvRecordBatchSinkProvider {
    engine: EngineRef,
    schema: TableSchema,
}

impl TskvRecordBatchSinkProvider {
    pub fn new(engine: EngineRef, schema: TableSchema) -> Self {
        Self { engine, schema }
    }
}

impl RecordBatchSinkProvider for TskvRecordBatchSinkProvider {
    fn create_batch_sink(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink> {
        Box::new(TskvRecordBatchSink {
            engine: self.engine.clone(),
            partition,
            schema: self.schema.clone(),
            metrics: TskvSinkMetrics::new(metrics, partition),
        })
    }
}

/// Stores metrics about the tskv sink execution.
#[derive(Debug)]
pub struct TskvSinkMetrics {
    elapsed_record_batch_to_point: metrics::Time,
    elapsed_point_write: metrics::Time,
}

impl TskvSinkMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let elapsed_record_batch_to_point =
            MetricBuilder::new(metrics).subset_time("elapsed_record_batch_to_point", partition);

        let elapsed_point_write =
            MetricBuilder::new(metrics).subset_time("elapsed_point_write", partition);

        Self {
            elapsed_record_batch_to_point,
            elapsed_point_write,
        }
    }

    pub fn elapsed_record_batch_to_point(&self) -> &metrics::Time {
        &self.elapsed_record_batch_to_point
    }

    pub fn elapsed_point_write(&self) -> &metrics::Time {
        &self.elapsed_point_write
    }
}
