use core::fmt;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

use coordinator::service::CoordinatorRef;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use models::predicate::domain::{PredicateRef, PushedAggregateFunction};

#[derive(Debug, Clone)]
pub struct AggregateFilterTskvExec {
    _coord: CoordinatorRef,
    schema: SchemaRef,
    pushed_aggs: Vec<PushedAggregateFunction>,
    filter: PredicateRef,
}

impl AggregateFilterTskvExec {
    pub fn new(
        _coord: CoordinatorRef,
        schema: SchemaRef,
        pushed_aggs: Vec<PushedAggregateFunction>,
        filter: PredicateRef,
    ) -> Self {
        Self {
            _coord,
            schema,
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
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Err(DataFusionError::NotImplemented(
            "AggregateFilterTskvExec::execute".to_string(),
        ))
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
