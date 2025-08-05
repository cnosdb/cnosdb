use std::any::Any;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::task::Poll;

use coordinator::service::CoordinatorRef;
use coordinator::SendableCoordinatorRecordBatchStream;
use datafusion::arrow::datatypes::{SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use models::codec::Encoding;
use models::datafusion::limit_record_batch::limit_record_batch;
use models::predicate::domain::PredicateRef;
use models::predicate::PlacedSplit;
use models::schema::tskv_table_schema::{
    ColumnType, TableColumn, TskvTableSchema, TskvTableSchemaRef,
};
use models::schema::TIME_FIELD_NAME;
use snafu::ResultExt;
use spi::{CommonSnafu, CoordinatorSnafu, QueryResult};
use trace::span_ext::SpanExt;
use trace::{debug, Span, SpanContext};
use tskv::reader::QueryOption;

use crate::extension::physical::plan_node::TableScanMetrics;

#[derive(Clone)]
pub struct TskvExec {
    // connection
    // db: CustomDataSource,
    table_schema: TskvTableSchemaRef,
    proj_schema: SchemaRef,
    filter: PredicateRef,
    coord: CoordinatorRef,
    splits: Vec<PlacedSplit>,
    properties: PlanProperties,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl TskvExec {
    pub(crate) fn new(
        table_schema: TskvTableSchemaRef,
        proj_schema: SchemaRef,
        filter: PredicateRef,
        coord: CoordinatorRef,
        splits: Vec<PlacedSplit>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let partitioning = Partitioning::UnknownPartitioning(splits.len());
        Self {
            table_schema,
            proj_schema: proj_schema.clone(),
            filter,
            coord,
            splits,
            properties: PlanProperties::new(
                EquivalenceProperties::new(proj_schema),
                partitioning,
                EmissionType::Both,
                Boundedness::Bounded,
            ),
            metrics,
        }
    }
    pub fn filter(&self) -> PredicateRef {
        self.filter.clone()
    }
}

impl ExecutionPlan for TskvExec {
    fn name(&self) -> &str {
        "TskvExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TskvExec {
            table_schema: self.table_schema.clone(),
            proj_schema: self.proj_schema.clone(),
            filter: self.filter.clone(),
            coord: self.coord.clone(),
            splits: self.splits.clone(),
            properties: self.properties.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        debug!(
            "Start TskvExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        let split = unsafe {
            debug_assert!(partition < self.splits.len(), "Partition not exists");
            self.splits.get_unchecked(partition).clone()
        };

        debug!("Split of partition: {:?}", split);

        let batch_size = context.session_config().batch_size();

        let metrics = TableScanMetrics::new(&self.metrics, partition);

        let span_ctx = context.session_config().get_extension::<SpanContext>();

        let table_stream = TableScanStream::new(
            self.table_schema.clone(),
            self.schema(),
            self.coord.clone(),
            split,
            batch_size,
            metrics,
            Span::from_context(
                format!("TableScanStream ({partition})"),
                span_ctx.as_deref(),
            ),
        )
        .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(Box::pin(table_stream))
    }

    fn statistics(&self) -> DFResult<Statistics> {
        // TODO
        Ok(Statistics::default())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for TskvExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let filter = self.filter();
                let fields: Vec<_> = self
                    .proj_schema
                    .fields()
                    .iter()
                    .map(|x| x.name().to_owned())
                    .collect::<Vec<String>>();
                write!(
                    f,
                    "TskvExec: {}, split_num={}, projection=[{}]",
                    PredicateDisplay(&filter),
                    self.splits.len(),
                    fields.join(","),
                )
            }
            DisplayFormatType::TreeRender => {
                // TODO(zipper): implement this.
                write!(f, "")
            }
        }
    }
}

impl std::fmt::Debug for TskvExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TskvExec")
            .field("table_schema", &self.table_schema)
            .field("proj_schema", &self.proj_schema)
            .field("filter", &self.filter)
            .field("splits", &self.splits)
            .finish()
    }
}

/// A wrapper to customize PredicateRef display
struct PredicateDisplay<'a>(&'a PredicateRef);

impl<'a> Display for PredicateDisplay<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let filter = self.0;
        write!(
            f,
            "limit={:?}, predicate={:?}, filter={:?}",
            filter.limit(),
            filter.filter(),
            filter.physical_expr().map(|e| e.to_string()),
        )
    }
}

#[allow(dead_code)]
pub struct TableScanStream {
    proj_schema: SchemaRef,
    batch_size: usize,
    coord: CoordinatorRef,

    iterator: SendableCoordinatorRecordBatchStream,

    remain: Option<usize>,
    metrics: TableScanMetrics,
    #[allow(unused)]
    span: Span,
}

impl TableScanStream {
    pub fn new(
        table_schema: TskvTableSchemaRef,
        proj_schema: SchemaRef,
        coord: CoordinatorRef,
        split: PlacedSplit,
        batch_size: usize,
        metrics: TableScanMetrics,
        span: Span,
    ) -> QueryResult<Self> {
        let mut proj_fileds = Vec::with_capacity(proj_schema.fields().len());
        for item in proj_schema.fields().iter() {
            let field_name = item.name();
            if field_name == TIME_FIELD_NAME {
                let (encoding, column_type) = match table_schema.get_column_by_name(TIME_FIELD_NAME)
                {
                    None => (Encoding::Default, ColumnType::Time(TimeUnit::Nanosecond)),
                    Some(v) => (v.encoding, v.column_type.clone()),
                };
                proj_fileds.push(TableColumn::new(
                    0,
                    TIME_FIELD_NAME.to_string(),
                    column_type,
                    encoding,
                ));
                continue;
            }

            if let Some(v) = table_schema.get_column_by_name(field_name) {
                proj_fileds.push(v.clone());
            } else {
                return Err(CommonSnafu {
                    msg: format!(
                        "table stream build fail, because can't found field: {}",
                        field_name
                    ),
                }
                .build());
            }
        }

        let proj_table_schema = TskvTableSchema::new(
            table_schema.tenant.clone(),
            table_schema.db.clone(),
            table_schema.name.clone(),
            proj_fileds,
        );

        let remain = split.limit();

        let option = QueryOption::new(
            batch_size,
            split,
            None,
            proj_schema.clone(),
            proj_table_schema.into(),
            table_schema.build_meta(),
        );

        let span_ctx = span.context();
        let iterator = coord
            .table_scan(option, span_ctx.as_ref())
            .context(CoordinatorSnafu)?;

        Ok(Self {
            proj_schema,
            batch_size,
            coord,
            remain,
            iterator,
            metrics,
            span,
        })
    }

    pub fn with_iterator(
        proj_schema: SchemaRef,
        batch_size: usize,
        coord: CoordinatorRef,
        iterator: SendableCoordinatorRecordBatchStream,
        remain: Option<usize>,
        metrics: TableScanMetrics,
        span: Span,
    ) -> Self {
        Self {
            proj_schema,
            batch_size,
            coord,
            iterator,
            remain,
            metrics,
            span,
        }
    }
}

impl Stream for TableScanStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let metrics = &this.metrics;
        let timer = metrics.elapsed_compute().timer();

        let result = match this.iterator.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                Poll::Ready(limit_record_batch(this.remain.as_mut(), batch).map(Ok))
            }
            Poll::Ready(Some(Err(e))) => {
                Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))))
            }
            Poll::Ready(None) => {
                metrics.done();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        };

        timer.done();
        metrics.record_poll(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // todo   (self.data.len(), Some(self.data.len()))
        (0, Some(0))
    }
}

impl RecordBatchStream for TableScanStream {
    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }
}
