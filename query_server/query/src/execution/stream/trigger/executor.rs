use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use futures::Future;
use models::runtime::executor::{DedicatedExecutor, Job};
use spi::query::config::StreamTriggerInterval;

pub type TriggerExecutorFactoryRef = Arc<TriggerExecutorFactory>;

pub struct TriggerExecutorFactory {
    runtime: Arc<DedicatedExecutor>,
}

impl TriggerExecutorFactory {
    pub fn new(runtime: Arc<DedicatedExecutor>) -> Self {
        Self { runtime }
    }

    pub fn create(&self, trigger: &StreamTriggerInterval) -> TriggerExecutorRef {
        Arc::new(TriggerExecutor {
            trigger: trigger.clone(),
            runtime: self.runtime.clone(),
        })
    }
}

pub type TriggerExecutorRef = Arc<TriggerExecutor>;

pub struct TriggerExecutor {
    trigger: StreamTriggerInterval,
    runtime: Arc<DedicatedExecutor>,
}

impl TriggerExecutor {
    pub fn schedule<F, T>(&self, task: F, runtime: Arc<DedicatedExecutor>) -> Job<()>
    where
        F: Fn(i64) -> T,
        F: Send + Sync + 'static,
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let current_batch_id = AtomicI64::default();

        let fetch_add_batch_id = move || current_batch_id.fetch_add(1, Ordering::Relaxed);

        match self.trigger {
            StreamTriggerInterval::Once => self.runtime.spawn(async move {
                task(fetch_add_batch_id());
            }),
            StreamTriggerInterval::Interval(d) => self.runtime.spawn(async move {
                let mut ticker = tokio::time::interval(d);
                loop {
                    let _ = runtime.spawn(task(fetch_add_batch_id())).await;

                    ticker.tick().await;
                }
            }),
        }
    }
}
