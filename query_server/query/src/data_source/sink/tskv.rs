use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use models::consistency_level::ConsistencyLevel;
use models::schema::TskvTableSchemaRef;
use protos::kv_service::WritePointsRequest;
use spi::Result;

use crate::data_source::{RecordBatchSink, RecordBatchSinkProvider, SinkMetadata};
use crate::utils::point_util::record_batch_to_points_flat_buffer;

pub struct TskvRecordBatchSink {
    coord: CoordinatorRef,
    partition: usize,
    schema: TskvTableSchemaRef,

    metrics: TskvSinkMetrics,
}

#[async_trait]
impl RecordBatchSink for TskvRecordBatchSink {
    async fn append(&self, record_batch: RecordBatch) -> Result<SinkMetadata> {
        trace::trace!(
            "Partition: {}, \nTskvTableSchema: {:?}, \nTskvRecordBatchSink::append: {:?}",
            self.partition,
            self.schema,
            record_batch,
        );

        let rows_writed = record_batch.num_rows();

        // record batchs to points
        let timer = self.metrics.elapsed_record_batch_to_point().timer();
        let (points, time_unit) =
            record_batch_to_points_flat_buffer(&record_batch, self.schema.clone())?;
        // .context(PointUtilSnafu)?;
        timer.done();
        let bytes_writed = points.len();

        // points write request
        let timer = self.metrics.elapsed_point_write().timer();
        let req = WritePointsRequest {
            version: 0,
            meta: None,
            points,
        };

        self.coord
            .write_points(
                self.schema.tenant.clone(),
                ConsistencyLevel::Any,
                time_unit.into(),
                req,
            )
            .await?;

        timer.done();

        Ok(SinkMetadata::new(rows_writed, bytes_writed))
    }
}

pub struct TskvRecordBatchSinkProvider {
    coord: CoordinatorRef,
    schema: TskvTableSchemaRef,
}

impl TskvRecordBatchSinkProvider {
    pub fn new(coord: CoordinatorRef, schema: TskvTableSchemaRef) -> Self {
        Self { coord, schema }
    }
}

impl RecordBatchSinkProvider for TskvRecordBatchSinkProvider {
    fn create_batch_sink(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Box<dyn RecordBatchSink> {
        Box::new(TskvRecordBatchSink {
            coord: self.coord.clone(),
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
