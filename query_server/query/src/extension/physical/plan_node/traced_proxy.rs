use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Metric, PlanProperties,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use trace::span_ext::SpanExt;
use trace::{Span, SpanContext};

use crate::extension::physical::utils::one_line;

/// 封装物理节点，用于采集物理节点的执行时间和相关metrics信息
#[derive(Debug, Clone)]
pub struct TracedProxyExec {
    inner: Arc<dyn ExecutionPlan>,
    desc: String,
    name: String,
}

impl TracedProxyExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
        let desc = one_line(inner.as_ref()).to_string();
        let name = desc.chars().take_while(|x| *x != ':').collect();

        Self { inner, desc, name }
    }

    pub fn desc(&self) -> &str {
        &self.desc
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ExecutionPlan for TracedProxyExec {
    fn name(&self) -> &str {
        "TracedProxyExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inner = self.inner.clone().with_new_children(children)?;
        Ok(Arc::new(Self {
            inner,
            desc: self.desc.clone(),
            name: self.name.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let span_ctx = context.session_config().get_extension::<SpanContext>();
        let span = Span::from_context(
            format!("{} ({})", self.name(), partition),
            span_ctx.as_deref(),
        );

        let new_context = span
            .context()
            .map(|span_ctx| {
                let session_config = context
                    .session_config()
                    .clone()
                    .with_extension(Arc::new(span_ctx));
                // 仅为了把 span_ctx 传到 TableScan 节点
                // 丢弃了原有的 context 中的udf 和 udaf
                Arc::new(TaskContext::new(
                    context.task_id(),
                    context.session_id(),
                    session_config,
                    Default::default(),
                    Default::default(),
                    Default::default(),
                    context.runtime_env(),
                ))
            })
            .unwrap_or(context);

        let stream = self.inner.execute(partition, new_context)?;
        Ok(Box::pin(TracedStream::new(
            partition,
            stream,
            span,
            self.inner.clone(),
        )))
    }

    fn statistics(&self) -> Result<Statistics> {
        self.inner.statistics()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<LexRequirement>> {
        self.inner.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }
}

impl DisplayAs for TracedProxyExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "* ")?;
        self.inner.fmt_as(t, f)
    }
}

struct TracedStream {
    partition: usize,
    inner: SendableRecordBatchStream,
    span: Span,
    physical_plan: Arc<dyn ExecutionPlan>,
}

impl TracedStream {
    pub fn new(
        partition: usize,
        inner: SendableRecordBatchStream,
        span: Span,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            partition,
            inner,
            span,
            physical_plan,
        }
    }
}

impl RecordBatchStream for TracedStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl Stream for TracedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl Drop for TracedStream {
    fn drop(&mut self) {
        if self.span.context().is_some() {
            if let Some(metrics) = self.physical_plan.metrics() {
                let partition_metrics = partition_metrics(self.partition, &metrics);
                for m in partition_metrics {
                    self.span
                        .add_property(|| (m.value().name().to_string(), m.value().to_string()));
                }
            }
        }
    }
}

fn partition_metrics(partition: usize, metrics: &MetricsSet) -> Vec<&Arc<Metric>> {
    metrics
        .iter()
        .filter(|e| e.partition() == Some(partition) || e.partition().is_none())
        .collect()
}
