use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    Statistics,
};
use futures::{StreamExt, TryStreamExt};
use models::schema::TskvTableSchemaRef;
use protos::kv_service::UpdateSetValue;
use spi::query::AFFECTED_ROWS;

use crate::extension::DropEmptyRecordBatchStream;

pub struct UpdateTagExec {
    scan: Arc<dyn ExecutionPlan>,
    table_schema: TskvTableSchemaRef,
    metrics: ExecutionPlanMetricsSet,
    schema: SchemaRef,

    assigns: Vec<(String, Arc<dyn PhysicalExpr>)>,
    coord: CoordinatorRef,
}

impl UpdateTagExec {
    pub fn new(
        scan: Arc<dyn ExecutionPlan>,
        table_schema: TskvTableSchemaRef,
        assigns: Vec<(String, Arc<dyn PhysicalExpr>)>,
        coord: CoordinatorRef,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new(
            AFFECTED_ROWS.0,
            AFFECTED_ROWS.1,
            false,
        )]));
        Self {
            scan,
            table_schema,
            metrics: ExecutionPlanMetricsSet::new(),
            schema,
            assigns,
            coord,
        }
    }
}

impl Debug for UpdateTagExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fmt_as(DisplayFormatType::Default, f)
    }
}

#[async_trait]
impl ExecutionPlan for UpdateTagExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.scan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.scan.output_ordering()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.scan.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(UpdateTagExec {
            scan: children[0].clone(),
            table_schema: self.table_schema.clone(),
            metrics: self.metrics.clone(),
            schema: self.schema.clone(),
            assigns: self.assigns.clone(),
            coord: self.coord.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let scan = self.scan.execute(partition, context)?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(do_update(
                self.schema.clone(),
                self.table_schema.clone(),
                scan,
                self.assigns.clone(),
                self.coord.clone(),
            ))
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "UpdateValueTagExec: [{}]", self.table_schema.name,)
            }
            DisplayFormatType::Verbose => {
                let schemas = self
                    .schema
                    .fields()
                    .iter()
                    .map(|e| e.name().to_string())
                    .collect::<Vec<_>>();
                write!(
                    f,
                    "UpdateValueTagExec: [{}], output=[{}]",
                    self.table_schema.name,
                    schemas.join(",")
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.scan.statistics()
    }
}

async fn do_update(
    schema: SchemaRef,
    table_schema: TskvTableSchemaRef,
    scan: SendableRecordBatchStream,
    assigns: Vec<(String, Arc<dyn PhysicalExpr>)>,
    coord: CoordinatorRef,
) -> Result<SendableRecordBatchStream> {
    let mut new_tags = vec![];
    for (name, expr) in assigns {
        let value = expr
            .evaluate(&RecordBatch::new_empty(Arc::new(Schema::empty())))?
            .into_array(1);
        let value = value
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(DataFusionError::Execution(String::from(
                "tag value must be string",
            )))?
            .value(0);
        let update_set_value = UpdateSetValue {
            key: name.as_bytes().to_vec(),
            value: Some(value.as_bytes().to_vec()),
        };
        new_tags.push(update_set_value);
    }

    let mut rows_wrote = 0;
    let mut batches = vec![];
    let mut stream: DropEmptyRecordBatchStream = DropEmptyRecordBatchStream::new(scan);
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let num_rows = batch.num_rows();
        rows_wrote += num_rows;
        batches.push(batch);
    }
    coord
        .update_tags_value(table_schema.clone(), new_tags.clone(), batches)
        .await
        .map_err(|err| DataFusionError::Execution(err.to_string()))?;

    aggregate_statistics(schema, rows_wrote)
}

fn aggregate_statistics(schema: SchemaRef, rows_wrote: usize) -> Result<SendableRecordBatchStream> {
    let output_rows_col = Arc::new(UInt64Array::from(vec![rows_wrote as u64]));

    let batch = RecordBatch::try_new(schema.clone(), vec![output_rows_col])?;

    Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
}
