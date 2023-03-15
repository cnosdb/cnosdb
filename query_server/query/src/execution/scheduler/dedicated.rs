use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use futures::Future;
use models::runtime::cross_rt_stream::CrossRtStream;
use models::runtime::executor::DedicatedExecutor;
use spi::query::scheduler::{ExecutionResults, Scheduler};
use trace::info;

pub struct DedicatedScheduler {
    runtime: DedicatedExecutor,
}

impl DedicatedScheduler {
    #[allow(dead_code)]
    pub fn new(num_threads: usize) -> Self {
        info!("Init dedicated executor of query engine.");
        let runtime = DedicatedExecutor::new("query-dedicated-scheduler", num_threads);
        Self { runtime }
    }

    async fn run<F, T>(&self, f: F) -> Result<T>
    where
        F: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        self.runtime.spawn(f).await.unwrap_or_else(|e| {
            Err(DataFusionError::Context(
                "Await Error".to_string(),
                Box::new(DataFusionError::External(e.into())),
            ))
        })
    }
}

#[async_trait]
impl Scheduler for DedicatedScheduler {
    async fn schedule(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
    ) -> Result<ExecutionResults> {
        let partition_count = plan.output_partitioning().partition_count();

        let merged_plan = if partition_count > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        };

        debug_assert_eq!(1, merged_plan.output_partitioning().partition_count());

        let stream = self
            .run(async move { merged_plan.execute(0, context) })
            .await?;

        let schema = stream.schema();
        let stream = CrossRtStream::new_with_df_error_stream(stream, self.runtime.clone());
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        Ok(ExecutionResults::new(stream))
    }
}
