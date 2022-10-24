use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::Arc,
};

use datafusion::{
    arrow::datatypes::SchemaRef,
    error::{DataFusionError, Result},
    execution::context::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        metrics::ExecutionPlanMetricsSet, DisplayFormatType, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
};
use models::schema::TableSchema;

use crate::{
    predicate::PredicateRef,
    stream::{TableScanMetrics, TableScanStream},
};
use tskv::engine::EngineRef;

#[derive(Debug, Clone)]
pub struct TskvExec {
    // connection
    // db: CustomDataSource,
    table_schema: TableSchema,
    proj_schema: SchemaRef,
    filter: PredicateRef,
    engine: EngineRef,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl TskvExec {
    pub(crate) fn new(
        table_schema: TableSchema,
        proj_schema: SchemaRef,
        filter: PredicateRef,
        engine: EngineRef,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();

        Self {
            table_schema,
            proj_schema,
            filter,
            engine,
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
            engine: self.engine.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();

        let metrics = TableScanMetrics::new(&self.metrics, partition);

        let table_stream = match TableScanStream::new(
            self.table_schema.clone(),
            self.schema(),
            self.filter(),
            batch_size,
            self.engine.clone(),
            metrics,
        ) {
            Ok(s) => s,
            Err(err) => return Err(DataFusionError::Internal(err.to_string())),
        };

        Ok(Box::pin(table_stream))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
            filter.domains(),
        )
    }
}
