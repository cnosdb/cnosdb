use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use coordinator::service::CoordinatorRef;
use datafusion::arrow::array::{new_null_array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ColumnarValue, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use futures::{StreamExt, TryStreamExt};
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use protos::kv_service::UpdateSetValue;
use snafu::IntoError;
use spi::query::AFFECTED_ROWS;
use spi::CoordinatorSnafu;

use crate::extension::DropEmptyRecordBatchStream;

pub struct UpdateTagExec {
    scan: Arc<dyn ExecutionPlan>,
    table_schema: TskvTableSchemaRef,
    metrics: ExecutionPlanMetricsSet,
    schema: SchemaRef,
    // (tag name, tag value expr)
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
    fn name(&self) -> &str {
        "UpdateTagExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.scan.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.scan]
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

    fn statistics(&self) -> Result<Statistics> {
        self.scan.statistics()
    }
}

impl DisplayAs for UpdateTagExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let assigns = self
            .assigns
            .iter()
            .map(|(col, val_expr)| format!("{}={}", col, val_expr))
            .collect::<Vec<_>>()
            .join(", ");

        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "UpdateTagExec: table={}, set=[{}], ",
                    self.table_schema.name, assigns
                )
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
                    "UpdateTagExec: table={}, set=[{}], output=[{}]",
                    self.table_schema.name,
                    assigns,
                    schemas.join(",")
                )
            }
            DisplayFormatType::TreeRender => {
                // TODO(zipper): implement this.
                write!(f, "")
            }
        }
    }
}

// see https://github.com/apache/arrow-datafusion/blob/main/datafusion/optimizer/src/simplify_expressions/expr_simplifier.rs
fn evaluate_to_scalar(phys_expr: Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    let schema = Schema::new(vec![Field::new(".", DataType::Null, true)]);
    // Need a single "input" row to produce a single output row
    let col = new_null_array(&DataType::Null, 1);
    let input_batch = RecordBatch::try_new(Arc::new(schema), vec![col])?;

    let col_val = phys_expr.evaluate(&input_batch)?;
    match col_val {
        ColumnarValue::Array(a) => {
            if a.len() != 1 {
                Err(DataFusionError::Execution(format!(
                    "Could not evaluate the expression, found a result of length {}",
                    a.len()
                )))
            } else {
                Ok(ScalarValue::try_from_array(&a, 0)?)
            }
        }
        ColumnarValue::Scalar(s) => Ok(s),
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
        let scalar = evaluate_to_scalar(expr)?;
        let value = if scalar.is_null() {
            None
        } else {
            Some(scalar.to_string().as_bytes().to_vec())
        };

        let update_set_value = UpdateSetValue {
            key: name.as_bytes().to_vec(),
            value,
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

    // TODO use df session config
    if rows_wrote > 10000 {
        return Err(DataFusionError::Execution(format!(
            "The number of update records {rows_wrote} exceeds the maximum limit 10000",
        )));
    }

    coord
        .update_tags_value(table_schema.clone(), new_tags.clone(), batches)
        .await
        .map_err(|err| DataFusionError::External(Box::new(CoordinatorSnafu.into_error(err))))?;

    aggregate_statistics(schema, rows_wrote)
}

fn aggregate_statistics(schema: SchemaRef, rows_wrote: usize) -> Result<SendableRecordBatchStream> {
    let output_rows_col = Arc::new(UInt64Array::from(vec![rows_wrote as u64]));

    let batch = RecordBatch::try_new(schema.clone(), vec![output_rows_col])?;

    Ok(Box::pin(MemoryStream::try_new(vec![batch], schema, None)?))
}
