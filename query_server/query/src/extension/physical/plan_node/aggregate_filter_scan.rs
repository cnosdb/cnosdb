use core::fmt;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use models::predicate::domain::{PredicateRef, PushedAggregateFunction};
use models::predicate::PlacedSplit;
use models::schema::TskvTableSchemaRef;
use trace::{debug, SpanContext, SpanExt, SpanRecorder};
use tskv::reader::QueryOption;

use super::tskv_exec::TableScanStream;
use crate::extension::physical::plan_node::TableScanMetrics;

#[derive(Clone)]
pub struct AggregateFilterTskvExec {
    coord: CoordinatorRef,
    schema: SchemaRef,
    table_schema: TskvTableSchemaRef,
    pushed_aggs: Vec<PushedAggregateFunction>,
    filter: PredicateRef,
    splits: Vec<PlacedSplit>,
    metrics: ExecutionPlanMetricsSet,
}

impl AggregateFilterTskvExec {
    pub fn new(
        coord: CoordinatorRef,
        schema: SchemaRef,
        table_schema: TskvTableSchemaRef,
        pushed_aggs: Vec<PushedAggregateFunction>,
        filter: PredicateRef,
        splits: Vec<PlacedSplit>,
    ) -> Self {
        Self {
            coord,
            schema,
            table_schema,
            pushed_aggs,
            filter,
            splits,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ExecutionPlan for AggregateFilterTskvExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.splits.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(self.deref().clone()))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let mut agg_columns = Vec::with_capacity(self.pushed_aggs.len());
        for agg in self.pushed_aggs.iter() {
            match agg {
                PushedAggregateFunction::Count(column) => {
                    if let Some(col) = self.table_schema.column(column) {
                        agg_columns.push(col.clone());
                    }
                }
            }
        }

        let split = unsafe {
            debug_assert!(partition < self.splits.len(), "Partition not exists");
            self.splits.get_unchecked(partition).clone()
        };
        debug!("Split of partition: {:?}", split);

        let metrics = TableScanMetrics::new(&self.metrics, partition);
        let query_opt = QueryOption::new(
            100_usize,
            split,
            Some(agg_columns),
            self.schema.clone(),
            self.table_schema.clone(),
        );

        let span_ctx = context.session_config().get_extension::<SpanContext>();
        let span_recorder =
            SpanRecorder::new(span_ctx.child_span("TableScanStream of AggregateFilterTskvExec"));

        let iterator = self
            .coord
            .table_scan(query_opt, span_recorder.span_ctx())
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        let table_stream = TableScanStream::with_iterator(
            self.schema.clone(),
            100_usize,
            self.coord.clone(),
            iterator,
            None,
            metrics,
            span_recorder,
        );

        Ok(Box::pin(table_stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "AggregateFilterTskvExec: agg=[{:?}], filter=[{:?}]",
            self.pushed_aggs, self.filter
        )
    }
}

impl Debug for AggregateFilterTskvExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateFilterTskvExec")
            .field("schema", &self.schema)
            .field("table_schema", &self.table_schema)
            .field("pushed_aggs", &self.pushed_aggs)
            .field("filter", &self.filter)
            .field("splits", &self.splits)
            .finish()
    }
}
