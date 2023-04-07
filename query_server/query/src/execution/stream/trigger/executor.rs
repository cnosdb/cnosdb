use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

use futures::Future;
use models::runtime::executor::{DedicatedExecutor, Job};
use spi::query::config::StreamTriggerInterval;
use spi::QueryError;

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
            processed_count: Default::default(),
            err_counter: Default::default(),
        })
    }
}

impl Drop for TriggerExecutorFactory {
    fn drop(&mut self) {
        self.runtime.shutdown();
    }
}

pub type TriggerExecutorRef = Arc<TriggerExecutor>;

pub struct TriggerExecutor {
    trigger: StreamTriggerInterval,
    runtime: Arc<DedicatedExecutor>,
    processed_count: Arc<AtomicU64>,
    err_counter: Arc<AtomicU64>,
}

impl TriggerExecutor {
    pub fn schedule<F, T>(&self, task: F, runtime: Arc<DedicatedExecutor>) -> Job<()>
    where
        F: Fn(i64) -> T,
        F: Send + Sync + 'static,
        T: Future<Output = Result<(), QueryError>> + Send + 'static,
        // T::Output: Send + 'static,
    {
        let current_batch_id = AtomicI64::default();
        let fetch_add_batch_id = move || current_batch_id.fetch_add(1, Ordering::Relaxed);
        let processed_count = self.processed_count.clone();
        let err_counter = self.err_counter.clone();

        match self.trigger {
            StreamTriggerInterval::Once => self.runtime.spawn(async move {
                match task(fetch_add_batch_id()).await {
                    Ok(_) => {}
                    Err(_) => {
                        let _ = err_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let _ = processed_count.fetch_add(1, Ordering::Relaxed);
            }),
            StreamTriggerInterval::Interval(d) => self.runtime.spawn(async move {
                let mut ticker = tokio::time::interval(d);
                loop {
                    match runtime.spawn(task(fetch_add_batch_id())).await {
                        Ok(Ok(_)) => {}
                        _ => {
                            // Record failed status
                            let _ = err_counter.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    let _ = processed_count.fetch_add(1, Ordering::Relaxed);
                    ticker.tick().await;
                }
            }),
        }
    }

    pub fn processed_count(&self) -> u64 {
        self.processed_count.load(Ordering::Relaxed)
    }

    pub fn error_count(&self) -> u64 {
        self.err_counter.load(Ordering::Relaxed)
    }
}
