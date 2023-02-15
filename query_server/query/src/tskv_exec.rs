use std::any::Any;
use std::fmt::{Display, Formatter};
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
use models::predicate::domain::PredicateRef;
use models::schema::TskvTableSchemaRef;
use trace::debug;
use tskv::iterator::TableScanMetrics;

use crate::stream::TableScanStream;

#[derive(Debug, Clone)]
pub struct TskvExec {
    // connection
    // db: CustomDataSource,
    table_schema: TskvTableSchemaRef,
    proj_schema: SchemaRef,
    filter: PredicateRef,
    coord: CoordinatorRef,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl TskvExec {
    pub(crate) fn new(
        table_schema: TskvTableSchemaRef,
        proj_schema: SchemaRef,
        filter: PredicateRef,
        coord: CoordinatorRef,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();

        Self {
            table_schema,
            proj_schema,
            filter,
            coord,
            metrics,
        }
    }
    pub fn filter(&self) -> PredicateRef {
        self.filter.clone()
    }
}

impl ExecutionPlan for TskvExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
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
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TskvExec {
            table_schema: self.table_schema.clone(),
            proj_schema: self.proj_schema.clone(),
            filter: self.filter.clone(),
            coord: self.coord.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start TskvExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let batch_size = context.session_config().batch_size();

        let metrics = TableScanMetrics::new(&self.metrics, partition);

        let table_stream = TableScanStream::new(
            self.table_schema.clone(),
            self.schema(),
            self.coord.clone(),
            self.filter(),
            batch_size,
            metrics,
        )
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(Box::pin(table_stream))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let filter = self.filter();
                let fields: Vec<_> = self
                    .proj_schema
                    .fields()
                    .iter()
                    .map(|x| x.name().to_owned())
                    .collect::<Vec<String>>();
                write!(
                    f,
                    "TskvExec: {}, projection=[{}]",
                    PredicateDisplay(&filter),
                    fields.join(","),
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        // TODO
        Statistics::default()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// A wrapper to customize PredicateRef display
#[derive(Debug)]
struct PredicateDisplay<'a>(&'a PredicateRef);

impl<'a> Display for PredicateDisplay<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let filter = self.0;
        write!(
            f,
            "limit={:?}, predicate={:?}",
            filter.limit(),
            filter.filter(),
        )
    }
}
