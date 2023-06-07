use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::arrow;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use trace::{debug, Span, SpanRecorder};

/// Stream wrapper that records DataFusion `MetricSets` into IOx
/// [`Span`]s when it is dropped.
pub struct TracedStream {
    inner: SendableRecordBatchStream,
    span_recorder: SpanRecorder,
    physical_plan: Arc<dyn ExecutionPlan>,
}

impl TracedStream {
    /// Return a stream that records DataFusion `MetricSets` from
    /// `physical_plan` into `span` when dropped.
    pub fn new(
        inner: SendableRecordBatchStream,
        span_recorder: SpanRecorder,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            inner,
            span_recorder,
            physical_plan,
        }
    }
}

impl RecordBatchStream for TracedStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

impl Stream for TracedStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl Drop for TracedStream {
    fn drop(&mut self) {
        if let Some(span) = self.span_recorder.span() {
            let default_end_time = Utc::now();
            send_metrics_to_tracing(default_end_time, span, self.physical_plan.as_ref(), true);
        }
    }
}

/// This function translates data in DataFusion `MetricSets` into IOx
/// [`Span`]s. It records a snapshot of the current state of the
/// DataFusion metrics, so it should only be invoked *after* a plan is
/// fully `collect`ed.
///
/// Each `ExecutionPlan` in the plan gets its own new [`Span`] that covers
/// the time spent executing its partitions and its children
///
/// Each `ExecutionPlan` also has a new [`Span`] for each of its
/// partitions that collected metrics
///
/// The start and end time of the span are taken from the
/// ExecutionPlan's metrics, falling back to the parent span's
/// timestamps if there are no metrics
///
/// Span metadata is used to record:
/// 1. If the ExecutionPlan had no metrics
/// 2. The total number of rows produced by the ExecutionPlan (if available)
/// 3. The elapsed compute time taken by the ExecutionPlan
fn send_metrics_to_tracing(
    default_end_time: DateTime<Utc>,
    parent_span: &Span,
    physical_plan: &dyn ExecutionPlan,
    per_partition_tracing: bool,
) {
    // Something like this when one_line is contributed back upstream
    //let plan_name = physical_plan.displayable().one_line().to_string();
    let desc = one_line(physical_plan).to_string();
    let operator_name: String = desc.chars().take_while(|x| *x != ':').collect();

    // Get the timings of the parent operator
    let parent_start_time = parent_span.start.unwrap_or(default_end_time);
    let parent_end_time = parent_span.end.unwrap_or(default_end_time);

    // A span for the operation, this is the aggregate of all the partition spans
    let mut operator_span = parent_span.child(operator_name.clone());
    operator_span.metadata.insert("desc".into(), desc.into());

    let mut operator_metrics = SpanMetrics {
        output_rows: None,
        elapsed_compute_nanos: None,
    };

    // The total duration for this span and all its children and partitions
    let mut operator_start_time = DateTime::<Utc>::MAX_UTC;
    let mut operator_end_time = DateTime::<Utc>::MIN_UTC;

    match physical_plan.metrics() {
        None => {
            // this DataFusion node had no metrics, so record that in
            // metadata and use the start/stop time of the parent span
            operator_span
                .metadata
                .insert("missing_statistics".into(), "true".into());
        }
        Some(metrics) => {
            // Create a separate span for each partition in the operator
            for (partition, metrics) in partition_metrics(metrics) {
                let (start_ts, end_ts) = get_timestamps(&metrics);

                let partition_start_time = start_ts.unwrap_or(parent_start_time);
                let partition_end_time = end_ts.unwrap_or(parent_end_time);

                let partition_metrics = SpanMetrics {
                    output_rows: metrics.output_rows(),
                    elapsed_compute_nanos: metrics.elapsed_compute(),
                };

                operator_start_time = operator_start_time.min(partition_start_time);
                operator_end_time = operator_end_time.max(partition_end_time);

                // Update the aggregate totals in the operator span
                operator_metrics.aggregate_child(&partition_metrics);

                // Generate a span for the partition if
                // - these metrics correspond to a partition
                // - per partition tracing is enabled
                if per_partition_tracing {
                    if let Some(partition) = partition {
                        let mut partition_span =
                            operator_span.child(format!("{operator_name} ({partition})"));

                        partition_span.start = Some(partition_start_time);
                        partition_span.end = Some(partition_end_time);

                        partition_metrics.add_to_span(&mut partition_span);

                        partition_span.export();
                    }
                }
            }
        }
    }

    // If we've not encountered any metrics to determine the operator's start
    // and end time, use those of the parent
    if operator_start_time == DateTime::<Utc>::MAX_UTC {
        operator_start_time = parent_span.start.unwrap_or(default_end_time);
    }

    if operator_end_time == DateTime::<Utc>::MIN_UTC {
        operator_end_time = parent_span.end.unwrap_or(default_end_time);
    }

    operator_span.start = Some(operator_start_time);
    operator_span.end = Some(operator_end_time);

    // recurse
    for child in physical_plan.children() {
        send_metrics_to_tracing(
            operator_end_time,
            &operator_span,
            child.as_ref(),
            per_partition_tracing,
        );
    }

    operator_metrics.add_to_span(&mut operator_span);
    operator_span.export();
}

#[derive(Debug)]
struct SpanMetrics {
    output_rows: Option<usize>,
    elapsed_compute_nanos: Option<usize>,
}

impl SpanMetrics {
    fn aggregate_child(&mut self, child: &Self) {
        if let Some(rows) = child.output_rows {
            *self.output_rows.get_or_insert(0) += rows;
        }

        if let Some(nanos) = child.elapsed_compute_nanos {
            *self.elapsed_compute_nanos.get_or_insert(0) += nanos;
        }
    }

    fn add_to_span(&self, span: &mut Span) {
        if let Some(rows) = self.output_rows {
            span.metadata
                .insert("output_rows".into(), (rows as i64).into());
        }

        if let Some(nanos) = self.elapsed_compute_nanos {
            span.metadata
                .insert("elapsed_compute_nanos".into(), (nanos as i64).into());
        }
    }
}

fn partition_metrics(metrics: MetricsSet) -> HashMap<Option<usize>, MetricsSet> {
    let mut hashmap = HashMap::<_, MetricsSet>::new();
    for metric in metrics.iter() {
        hashmap
            .entry(metric.partition())
            .or_default()
            .push(Arc::clone(metric))
    }
    hashmap
}

// todo contribute this back upstream to datafusion (add to `DisplayableExecutionPlan`)

/// Return a `Display`able structure that produces a single line, for
/// this node only (does not recurse to children)
pub fn one_line(plan: &dyn ExecutionPlan) -> impl fmt::Display + '_ {
    struct Wrapper<'a> {
        plan: &'a dyn ExecutionPlan,
    }
    impl<'a> fmt::Display for Wrapper<'a> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let t = DisplayFormatType::Default;
            self.plan.fmt_as(t, f)
        }
    }

    Wrapper { plan }
}

// TODO maybe also contribute these back upstream to datafusion (make
// as a method on MetricsSet)

/// Return the start, and end timestamps of the metrics set, if any
fn get_timestamps(metrics: &MetricsSet) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    let mut start_ts = None;
    let mut end_ts = None;

    for metric in metrics.iter() {
        if metric.labels().is_empty() {
            match metric.value() {
                MetricValue::StartTimestamp(ts) => {
                    if ts.value().is_some() && start_ts.is_some() {
                        debug!(
                            ?metric,
                            ?start_ts,
                            "WARNING: more than one StartTimestamp metric found"
                        )
                    }
                    start_ts = ts.value()
                }
                MetricValue::EndTimestamp(ts) => {
                    if ts.value().is_some() && end_ts.is_some() {
                        debug!(
                            ?metric,
                            ?end_ts,
                            "WARNING: more than one EndTimestamp metric found"
                        )
                    }
                    end_ts = ts.value()
                }
                _ => {}
            }
        }
    }

    (start_ts, end_ts)
}
