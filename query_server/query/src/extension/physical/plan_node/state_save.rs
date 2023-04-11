use core::fmt;
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::expressions::{binary, Column, GetIndexedFieldExpr, Literal};
use datafusion::physical_plan::metrics::{
    self, BaselineMetrics, ExecutionPlanMetricsSet, MetricBuilder,
};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use trace::debug;

use crate::extension::expr::WINDOW_END;
use crate::extension::utils::batch_filter;
use crate::extension::WATERMARK_DELAY_MS;
use crate::stream::state_store::{StateStore, StateStoreFactory};

/// Execution plan for a StateSaveExec
#[derive(Debug)]
pub struct StateSaveExec<T> {
    watermark_ns: i64,
    state_store_factory: Arc<T>,
    input: Arc<dyn ExecutionPlan>,
    watermark_predicate_for_data: Option<Arc<dyn PhysicalExpr>>,
    watermark_predicate_for_expired_data: Option<Arc<dyn PhysicalExpr>>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl<T> StateSaveExec<T> {
    pub fn try_new(
        watermark_ns: i64,
        state_store_factory: Arc<T>,
        input: Arc<dyn ExecutionPlan>,
    ) -> DFResult<Self> {
        let input_schema = input.schema();

        let watermark_predicate_for_data = create_watermark_predicate(
            input_schema.as_ref(),
            Operator::Gt,
            watermark_ns,
            Operator::Or,
        )?;
        let watermark_predicate_for_expired_data = create_watermark_predicate(
            input_schema.as_ref(),
            Operator::LtEq,
            watermark_ns,
            Operator::And,
        )?;

        Ok(Self {
            watermark_ns,
            state_store_factory,
            input,
            watermark_predicate_for_data,
            watermark_predicate_for_expired_data,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl<T> ExecutionPlan for StateSaveExec<T>
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
            self.watermark_ns,
            self.state_store_factory.clone(),
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let session_id = context.session_id();
        debug!(
            "Start StateSaveExec::execute for partition {} of context session_id {} and task_id {:?}, metadata: {:?}",
            partition,
            session_id,
            context.task_id(),
            self.input.schema().metadata(),
        );

        let input = self.input.execute(partition, context)?;
        let metrics = StateSaveMetrics::new(&self.metrics, partition);

        // TODO operator_id
        let state_store = self
            .state_store_factory
            .get_or_default(session_id, partition, 0)?;

        Ok(Box::pin(UpdateStream {
            schema: self.schema(),
            input,
            watermark_predicate_for_data: self.watermark_predicate_for_data.clone(),
            state_store,
            watermark_predicate_for_expired_data: self.watermark_predicate_for_expired_data.clone(),
            metrics,
        }))
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "StateSaveExec: watermark={}ns", self.watermark_ns)
            }
        }
    }
}

struct UpdateStream<T> {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    watermark_predicate_for_data: Option<Arc<dyn PhysicalExpr>>,
    state_store: Arc<T>,
    watermark_predicate_for_expired_data: Option<Arc<dyn PhysicalExpr>>,
    metrics: StateSaveMetrics,
}

impl<T> Stream for UpdateStream<T>
where
    T: StateStore,
{
    type Item = DFResult<RecordBatch>;

    ///  Complete mode: Result Table全量输出。支持聚合查询
    ///  Append mode (default): 追加模式，不会输出重复数据。支持select, where, map, flatMap, filter,join等那些添加到Result Table的行永远不会改变的查询
    ///  Update mode: 只要更新的 Row 都会被输出，相当于 Append mode 的加强版。适用有延迟数据的情况
    ///
    ///  Complete输出模式：
    ///    ○ 将所有记录写入[StateStore]
    ///    ○ 输出[StateStore]中所有记录
    ///  Append输出模式：
    ///    ○ 此模式必须带有水印（否则永远不会输出数据）
    ///    ○ 保留输入流中晚于waterMark的记录写入[StateStore]
    ///    ○ 从[StateStore]中移除所有早于waterMark的记录并输出
    ///  Update输出模式：
    ///    ○ 保留输入流中晚于waterMark的记录写入[StateStore]并输出
    ///    ○ 从[StateStore]中移除所有早于waterMark的记录
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(value) => match value {
                    // 保留输入流中晚于waterMark的记录写入[StateStore]并输出
                    Some(Ok(batch)) => {
                        let filtered_batch =
                            if let Some(ref predicate) = self.watermark_predicate_for_data {
                                let _timer = self.metrics.filter_late_data().timer();
                                let filtered_batch = batch_filter(&batch, predicate)?;
                                // skip entirely filtered batches
                                if filtered_batch.num_rows() == 0 {
                                    continue;
                                }
                                filtered_batch
                            } else {
                                batch
                            };

                        let timer = self.metrics.save_states().timer();
                        self.state_store.put(filtered_batch.clone())?;
                        timer.done();

                        poll = Poll::Ready(Some(Ok(filtered_batch)));
                        break;
                    }
                    // 从[StateStore]中移除所有早于waterMark的记录
                    None => {
                        if let Some(ref predicate) = self.watermark_predicate_for_expired_data {
                            let timer = self.metrics.discard_late_data().timer();
                            let _ = self.state_store.expire(predicate.clone())?;
                            timer.done();
                        }

                        let _commit_time_ns = self.state_store.commit()?;

                        poll = Poll::Ready(None);
                        break;
                    }
                    _ => {
                        poll = Poll::Ready(value);
                        break;
                    }
                },
                Poll::Pending => {
                    poll = Poll::Pending;
                    break;
                }
            }
        }

        self.metrics.record_poll(poll)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl<T> RecordBatchStream for UpdateStream<T>
where
    T: StateStore,
{
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Stores metrics about the StateSaveExec execution.
#[derive(Debug)]
pub struct StateSaveMetrics {
    baseline_metrics: BaselineMetrics,

    filter_late_data: metrics::Time,
    save_states: metrics::Time,
    discard_late_data: metrics::Time,
}

impl StateSaveMetrics {
    /// Create new metrics
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let baseline_metrics = BaselineMetrics::new(metrics, partition);

        let filter_late_data =
            MetricBuilder::new(metrics).subset_time("filter_late_data", partition);
        let save_states = MetricBuilder::new(metrics).subset_time("save_states", partition);
        let discard_late_data =
            MetricBuilder::new(metrics).subset_time("discard_late_data", partition);

        Self {
            baseline_metrics,
            filter_late_data,
            save_states,
            discard_late_data,
        }
    }

    pub fn elapsed_compute(&self) -> &metrics::Time {
        self.baseline_metrics.elapsed_compute()
    }

    pub fn filter_late_data(&self) -> &metrics::Time {
        &self.filter_late_data
    }

    pub fn save_states(&self) -> &metrics::Time {
        &self.save_states
    }

    pub fn discard_late_data(&self) -> &metrics::Time {
        &self.discard_late_data
    }

    pub fn record_poll(
        &self,
        poll: Poll<Option<DFResult<RecordBatch>>>,
    ) -> Poll<Option<DFResult<RecordBatch>>> {
        self.baseline_metrics.record_poll(poll)
    }

    /// Records the fact that this operator's execution is complete
    /// (recording the `end_time` metric).
    pub fn done(&self) {
        self.baseline_metrics.done()
    }
}

fn create_watermark_predicate(
    schema: &Schema,
    op: Operator,
    watermark_ns: i64,
    x: Operator,
) -> DFResult<Option<Arc<dyn PhysicalExpr>>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.metadata().contains_key(WATERMARK_DELAY_MS))
        .map(|(idx, f)| {
            let lhs: Arc<dyn PhysicalExpr> = match f.data_type() {
                DataType::Struct(fields) => {
                    let field =
                        fields
                            .iter()
                            .find(|f| f.name() == WINDOW_END)
                            .ok_or_else(|| {
                                DataFusionError::Plan(format!(
                                "Struct field {:?} does not have a event time column: {WINDOW_END}",
                                f.name()
                            ))
                            })?;

                    let arg = Arc::new(Column::new(field.name(), idx));
                    Arc::new(GetIndexedFieldExpr::new(
                        arg,
                        ScalarValue::Utf8(Some(field.name().clone())),
                    ))
                }
                _ => Arc::new(Column::new(f.name(), idx)),
            };

            let rhs = Arc::new(Literal::new(ScalarValue::TimestampNanosecond(
                Some(watermark_ns),
                None,
            )));
            binary(lhs, op, rhs, schema)
        })
        .reduce(|l, r| binary(l?, x, r?, schema))
        .transpose()
}
