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
        DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
    },
};

use crate::{predicate::PredicateRef, schema::TableSchema, stream::TableScanStream};
use tskv::engine::EngineRef;

#[derive(Debug, Clone)]
pub struct TskvExec {
    // connection
    // db: CustomDataSource,
    table_schema: TableSchema,
    proj_schema: SchemaRef,
    filter: PredicateRef,
    engine: EngineRef,
}

impl TskvExec {
    pub(crate) fn new(
        table_schema: TableSchema,
        proj_schema: SchemaRef,
        filter: PredicateRef,
        engine: EngineRef,
    ) -> Self {
        Self {
            table_schema,
            proj_schema,
            filter,
            engine,
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
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();

        let table_stream = match TableScanStream::new(
            self.table_schema.clone(),
            self.schema(),
            self.filter(),
            batch_size,
            self.engine.clone(),
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
