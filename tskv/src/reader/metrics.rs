use std::ops::Deref;
use std::task::Poll;

use arrow_array::RecordBatch;
use datafusion::physical_plan::metrics::{
    BaselineMetrics as DFBaselineMetrics, ExecutionPlanMetricsSet, RecordOutput,
};

use crate::Result;

#[derive(Debug, Clone)]
pub struct BaselineMetrics {
    inner: DFBaselineMetrics,
}

impl Deref for BaselineMetrics {
    type Target = DFBaselineMetrics;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl BaselineMetrics {
    /// Create a new BaselineMetric structure, and set  `start_time` to now
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        let partition = 0;
        Self {
            inner: DFBaselineMetrics::new(metrics, partition),
        }
    }

    pub fn record_poll(
        &self,
        poll: Poll<Option<Result<RecordBatch>>>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if let Poll::Ready(maybe_batch) = &poll {
            match maybe_batch {
                Some(Ok(batch)) => {
                    batch.record_output(self);
                }
                Some(Err(_)) => self.done(),
                None => self.done(),
            }
        }
        poll
    }
}
