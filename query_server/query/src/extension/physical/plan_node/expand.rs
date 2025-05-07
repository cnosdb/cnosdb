use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    ColumnStatistics, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PhysicalExpr, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};
use futures::{Stream, StreamExt};
use trace::debug;

/// Execution plan for a Expand
#[derive(Debug)]
pub struct ExpandExec {
    /// The projection expressions stored as tuples of (expression, output column name)
    exprs: Vec<Vec<(Arc<dyn PhysicalExpr>, String)>>,
    /// The schema once the projection has been applied to the input
    schema: SchemaRef,
    /// The input plan
    input: Arc<dyn ExecutionPlan>,
    /// The alias map used to normalize out expressions like Partitioning and PhysicalSortExpr
    /// The key is the column from the input schema and the values are the columns from the output schema
    alias_map: HashMap<Column, Vec<Column>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    ///
    properties: PlanProperties,
}

impl ExpandExec {
    /// Create a projection on an input
    pub fn try_new(
        exprs: Vec<Vec<(Arc<dyn PhysicalExpr>, String)>>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        assert!(!exprs.is_empty());

        let input_schema = input.schema();

        let expr = &exprs[0];

        let fields = expr
            .iter()
            .map(|(e, name)| {
                let mut field = Field::new(
                    name,
                    e.data_type(&input_schema)?,
                    e.nullable(&input_schema)?,
                );
                field.set_metadata(get_field_metadata(e, &input_schema).unwrap_or_default());

                Ok(field)
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));

        let mut alias_map: HashMap<Column, Vec<Column>> = HashMap::new();
        for (expression, name) in expr.iter() {
            if let Some(column) = expression.as_any().downcast_ref::<Column>() {
                let new_col_idx = schema.index_of(name)?;
                // When the column name is the same, but index does not equal, treat it as Alias
                if (column.name() != name) || (column.index() != new_col_idx) {
                    let entry = alias_map.entry(column.clone()).or_default();
                    entry.push(Column::new(name, new_col_idx));
                }
            };
        }

        // Output Ordering need to respect the alias
        let input_eq_properties = input.equivalence_properties();
        let output_eq_properties = input_eq_properties.project(&alias_map, schema.clone());
        let output_partitioning = match input.output_partitioning() {
            Partitioning::Hash(exprs, part) => {
                let normalized_exprs = exprs
                    .into_iter()
                    .map(|expr| input_eq_properties.project_expr(expr, &alias_map))
                    .collect::<Vec<_>>();
                Partitioning::Hash(normalized_exprs, part)
            }
            other => other,
        };

        Ok(Self {
            exprs,
            schema: schema.clone(),
            input: input.clone(),
            alias_map,
            metrics: ExecutionPlanMetricsSet::new(),
            properties: PlanProperties::new(
                output_eq_properties,
                output_partitioning,
                emission_type,
                boundedness,
            ),
        })
    }

    /// The projection expressions stored as tuples of (expression, output column name)
    pub fn expr(&self) -> &[(Arc<dyn PhysicalExpr>, String)] {
        &self.exprs[0]
    }

    /// The input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl ExecutionPlan for ExpandExec {
    fn name(&self) -> &str {
        "ExpandExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // tell optimizer this operator doesn't reorder its input
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ExpandExec::try_new(
            self.exprs.clone(),
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start ExpandExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let exprs = self
            .exprs
            .iter()
            .map(|e| e.iter().map(|x| x.0.clone()).collect::<Vec<_>>())
            .collect();

        Ok(Box::pin(ExpandStream {
            schema: self.schema.clone(),
            exprs,
            input: self.input.execute(partition, context)?,
            baseline_metrics: BaselineMetrics::new(&self.metrics, partition),
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        stats_projection(
            self.input.statistics(),
            self.expr().iter().map(|(e, _)| Arc::clone(e)),
        )
    }
}

impl DisplayAs for ExpandExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let proj_strs = self
                    .exprs
                    .iter()
                    .map(|e| {
                        e.iter()
                            .map(|(e, alias)| {
                                let e = e.to_string();
                                if &e != alias {
                                    format!("{e} as {alias}")
                                } else {
                                    e
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    })
                    .collect::<Vec<_>>()
                    .join("], [");

                write!(f, "ExpandExec: exprs=[[{}]]", proj_strs)
            }
            DisplayFormatType::TreeRender => {
                // TODO(zipper): implement this.
                write!(f, "")
            }
        }
    }
}

/// If e is a direct column reference, returns the field level
/// metadata for that field, if any. Otherwise returns None
fn get_field_metadata(
    e: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Option<HashMap<String, String>> {
    let name = if let Some(column) = e.as_any().downcast_ref::<Column>() {
        column.name()
    } else {
        return None;
    };

    input_schema
        .field_with_name(name)
        .ok()
        .map(|f| f.metadata().clone())
}

fn stats_projection(
    stats: Statistics,
    exprs: impl Iterator<Item = Arc<dyn PhysicalExpr>>,
) -> Statistics {
    let column_statistics = stats.column_statistics.map(|input_col_stats| {
        exprs
            .map(|e| {
                if let Some(col) = e.as_any().downcast_ref::<Column>() {
                    input_col_stats[col.index()].clone()
                } else {
                    // TODO stats: estimate more statistics from expressions
                    // (expressions should compute their statistics themselves)
                    ColumnStatistics::default()
                }
            })
            .collect()
    });

    Statistics {
        num_rows: stats.num_rows,
        column_statistics,
        // TODO stats: knowing the type of the new columns we can guess the output size
        total_byte_size: None,
    }
}

impl ExpandStream {
    fn batch_project(
        &self,
        batch: &RecordBatch,
        expr: &[Arc<dyn PhysicalExpr>],
    ) -> Result<RecordBatch> {
        // records time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();
        let arrays = expr
            .iter()
            .map(|expr| expr.evaluate(batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;

        if arrays.is_empty() {
            let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
            RecordBatch::try_new_with_options(self.schema.clone(), arrays, &options)
                .map_err(Into::into)
        } else {
            RecordBatch::try_new(self.schema.clone(), arrays).map_err(Into::into)
        }
    }
}

/// Expand iterator
struct ExpandStream {
    schema: SchemaRef,
    exprs: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
}

impl Stream for ExpandStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.input.poll_next_unpin(cx).map(|x| match x {
            Some(Ok(batch)) => {
                let result = self
                    .exprs
                    .iter()
                    .map(|e| self.batch_project(&batch, e))
                    .collect::<Result<Vec<_>>>()
                    // TODO Need a more efficient way to replace [`concat_batches`]
                    .and_then(|e| concat_batches(&self.schema, e.iter()).map_err(Into::into));

                Some(result)
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

impl RecordBatchStream for ExpandStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
