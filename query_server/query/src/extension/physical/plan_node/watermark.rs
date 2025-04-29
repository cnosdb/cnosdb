use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{as_primitive_array, ArrayRef};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::{
    DataType, Schema, SchemaRef, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use models::schema::stream_table_schema::Watermark;
use spi::{QueryError, QueryResult};
use trace::debug;

use crate::extension::WATERMARK_DELAY_MS;
use crate::stream::watermark_tracker::WatermarkTrackerRef;

/// Execution plan for a WatermarkExec
#[derive(Debug)]
pub struct WatermarkExec {
    watermark: Watermark,
    watermark_tracker: WatermarkTrackerRef,
    /// The schema once the projection has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl WatermarkExec {
    pub fn try_new(
        watermark: Watermark,
        watermark_tracker: WatermarkTrackerRef,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Self> {
        let schema = input.schema();
        let fields = schema
            .fields()
            .iter()
            .map(|e| {
                let mut field = e.as_ref().clone();
                if e.name() == &watermark.column {
                    let mut metadata = e.metadata().clone();
                    let _ = metadata.insert(
                        WATERMARK_DELAY_MS.into(),
                        watermark.delay.as_millis().to_string(),
                    );
                    field.set_metadata(metadata);
                }
                Arc::new(field)
            })
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()));

        Ok(Self {
            watermark,
            watermark_tracker,
            schema,
            input: input.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl ExecutionPlan for WatermarkExec {
    fn name(&self) -> &str {
        "WatermarkExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        assert!(children.len() == 1);

        Ok(Arc::new(WatermarkExec::try_new(
            self.watermark.clone(),
            self.watermark_tracker.clone(),
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        debug!(
            "Start ExpandExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let input = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let stream = WatermarkStream {
            watermark_tracker: self.watermark_tracker.clone(),
            watermark: self.watermark.clone(),
            schema: self.schema(),
            input,
            baseline_metrics,
            max_event_time: None,
        };

        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> DFResult<Statistics> {
        self.input.statistics()
    }
}

impl DisplayAs for WatermarkExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "WatermarkExec: event_time={}, delay={}ms",
                    self.watermark.column,
                    self.watermark.delay.as_millis()
                )
            }
            DisplayFormatType::TreeRender => {
                // TODO(zipper): implement this.
                write!(f, "")
            }
        }
    }
}

struct WatermarkStream {
    watermark_tracker: WatermarkTrackerRef,
    watermark: Watermark,
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    max_event_time: Option<i64>,
}

impl WatermarkStream {
    fn compute_watermark(&mut self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let batch = RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec())?;

        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        let xx = {
            match batch.column_by_name(&self.watermark.column) {
                Some(array) => match max_timestamp(&self.watermark.column, array) {
                    Ok(value) => {
                        if let Some(value) = value {
                            let _ = self.max_event_time.replace(value);
                        }
                        Ok(())
                    }
                    Err(err) => Err(err),
                },
                None => Err(QueryError::ColumnNotFound {
                    col: self.watermark.column.clone(),
                }),
            }
        };
        match xx {
            Ok(_) => Ok(batch),
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }
}

impl Stream for WatermarkStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => Some(self.compute_watermark(batch)),
            None => {
                if let Some(max_event_time) = self.max_event_time {
                    self.watermark_tracker
                        .update_watermark(max_event_time, self.watermark.delay.as_nanos() as i64);
                }
                x
            }
            other => other,
        });

        self.baseline_metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for WatermarkStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub fn max_timestamp(col: &str, array: &ArrayRef) -> QueryResult<Option<i64>> {
    let value = match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            let primitive_array = as_primitive_array::<TimestampSecondType>(array.as_ref());
            compute::max(primitive_array)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let primitive_array = as_primitive_array::<TimestampMillisecondType>(array.as_ref());
            compute::max(primitive_array)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let primitive_array = as_primitive_array::<TimestampMicrosecondType>(array.as_ref());
            compute::max(primitive_array)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let primitive_array = as_primitive_array::<TimestampNanosecondType>(array.as_ref());
            compute::max(primitive_array)
        }
        _ => {
            return Err(QueryError::InvalidDataType {
                column_name: col.into(),
                data_type: "Timestamp".into(),
            });
        }
    };

    Ok(value)
}
