use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

pub type SchedulerRef = Arc<dyn Scheduler + Send + Sync>;

pub trait Scheduler {
    /// Schedule the provided [`ExecutionPlan`] on this [`Scheduler`].
    ///
    /// Returns a [`ExecutionResults`] that can be used to receive results as they are produced,
    /// as a [`futures::Stream`] of [`RecordBatch`]
    fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults>;
}

pub struct ExecutionResults {
    stream: SendableRecordBatchStream,
}

impl ExecutionResults {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }

    /// Returns a [`SendableRecordBatchStream`] of this execution
    pub fn stream(self) -> SendableRecordBatchStream {
        self.stream
    }
}
