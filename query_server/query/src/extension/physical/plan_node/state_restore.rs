use core::fmt;
use core::fmt::Debug;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};
use futures::{Stream, StreamExt};
use trace::debug;

use crate::stream::state_store::{StateStore, StateStoreFactory};

/// Execution plan for a StateRestoreExec
#[derive(Debug)]
pub struct StateRestoreExec<T> {
    input: Arc<dyn ExecutionPlan>,
    state_store_factory: Arc<T>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl<T> StateRestoreExec<T> {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, state_store_factory: Arc<T>) -> DFResult<Self> {
        Ok(Self {
            input,
            state_store_factory,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl<T> ExecutionPlan for StateRestoreExec<T>
where
    T: StateStoreFactory + Send + Sync + Debug + 'static,
    T::SS: Send + Sync + Debug,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);

        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.state_store_factory.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let session_id = context.session_id();
        debug!(
            "Start StateRestoreExec::execute for partition {} of context session_id {} and task_id {:?}, metadata: {:?}",
            partition,
            context.session_id(),
            context.task_id(),
            self.input.schema().metadata(),
        );

        let input = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        // TODO
        let state_store = self
            .state_store_factory
            .get_or_default(session_id, partition, 0)?;

        let states = state_store.state()?;

        Ok(Box::pin(AppendStream {
            schema: self.schema(),
            input,
            states,
            baseline_metrics,
        }))
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "StateRestoreExec",)
            }
        }
    }
}

struct AppendStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    states: Vec<RecordBatch>,
    baseline_metrics: BaselineMetrics,
}

impl Stream for AppendStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.states.pop() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => {
                let poll = self.input.poll_next_unpin(cx).map(|x| match x {
                    Some(Ok(batch)) => {
                        trace::trace!(
                            "AppendStream of StateRestore , num rows: {}, rows:\n{:?}",
                            batch.num_rows(),
                            batch
                        );
                        Some(Ok(batch))
                    }
                    other => other,
                });

                self.baseline_metrics.record_poll(poll)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for AppendStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
