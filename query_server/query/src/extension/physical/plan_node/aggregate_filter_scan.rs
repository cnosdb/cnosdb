use core::fmt;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use models::predicate::domain::{PredicateRef, PushedAggregateFunction};
use models::schema::TskvTableSchemaRef;
use tskv::iterator::{QueryOption, TableScanMetrics};

use super::tskv_exec::TableScanStream;

#[derive(Debug, Clone)]
pub struct AggregateFilterTskvExec {
    coord: CoordinatorRef,
    schema: SchemaRef,
    table_schema: TskvTableSchemaRef,
    pushed_aggs: Vec<PushedAggregateFunction>,
    filter: PredicateRef,
}

impl AggregateFilterTskvExec {
    pub fn new(
        coord: CoordinatorRef,
        schema: SchemaRef,
        table_schema: TskvTableSchemaRef,
        pushed_aggs: Vec<PushedAggregateFunction>,
        filter: PredicateRef,
    ) -> Self {
        Self {
            coord,
            schema,
            table_schema,
            pushed_aggs,
            filter,
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
        Partitioning::UnknownPartitioning(1)
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
        let metrics = ExecutionPlanMetricsSet::new();
        let metrics = TableScanMetrics::new(&metrics, partition, Some(context.memory_pool()));
        let query_opt = QueryOption::new(
            100_usize,
            self.table_schema.tenant.clone(),
            self.filter.clone(),
            Some(agg_columns),
            self.schema.clone(),
            (*self.table_schema).clone(),
            metrics.tskv_metrics(),
        );

        let iterator = self
            .coord
            .read_record(query_opt)
            .map_err(|e| DataFusionError::Internal(e.to_string()))?;
        let table_stream = TableScanStream::with_iterator(
            self.schema.clone(),
            100_usize,
            self.coord.clone(),
            iterator,
            metrics,
        );

        Ok(Box::pin(table_stream))
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
