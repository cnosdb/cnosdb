use std::sync::Arc;

use memory_pool::{MemoryPool, MemoryPoolRef};
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use tokio::runtime::Runtime;

use crate::kv_option::StorageOptions;
use crate::Options;

#[derive(Debug)]
pub struct TskvContext {
    options: Arc<Options>,
    meta_manager: MetaRef,
    runtime: Arc<Runtime>,
    memory_pool: Arc<dyn MemoryPool>,
    metrics: Arc<MetricsRegister>,
}

impl TskvContext {
    pub fn new(
        meta: MetaRef,
        options: Arc<Options>,
        rt: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        metrics: Arc<MetricsRegister>,
    ) -> Self {
        Self {
            meta_manager: meta,
            options,
            runtime: rt.clone(),
            memory_pool,
            metrics,
        }
    }
    pub fn options(&self) -> &Arc<Options> {
        &self.options
    }

    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    pub fn storage_opt(&self) -> &Arc<StorageOptions> {
        &self.options.storage
    }

    pub fn meta(&self) -> &MetaRef {
        &self.meta_manager
    }

    pub fn metrics_register(&self) -> &Arc<MetricsRegister> {
        &self.metrics
    }

    pub fn memory_pool(&self) -> &Arc<dyn MemoryPool> {
        &self.memory_pool
    }
}

// for test
impl TskvContext {
    pub fn mock() -> Arc<Self> {
        use meta::model::meta_admin::AdminMeta;
        let config = config::get_config_for_test();

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(4)
                .build()
                .unwrap(),
        );
        let meta = runtime.block_on(async {
            let meta = Arc::new(AdminMeta::mock());
            let _ = meta.add_data_node().await;
            meta
        });
        let memory_pool = Arc::new(memory_pool::GreedyMemoryPool::new(1024 * 1024 * 1024));
        let register = Arc::new(MetricsRegister::default());
        let options = Arc::new(Options::from(&config));
        Arc::new(TskvContext::new(
            meta.clone(),
            options,
            runtime,
            memory_pool,
            register,
        ))
    }

    // return [`TskvContext`] with new config
    pub fn change_config(&self, old_config: &config::Config) -> Arc<Self> {
        Self {
            options: Arc::new(Options::from(old_config)),
            meta_manager: self.meta_manager.clone(),
            runtime: self.runtime.clone(),
            memory_pool: self.memory_pool.clone(),
            metrics: self.metrics.clone(),
        }
        .into()
    }

    // return [`TskvContext`] with new meta
    pub fn change_meta(&self, meta: MetaRef) -> Arc<Self> {
        Self {
            options: self.options.clone(),
            meta_manager: meta.clone(),
            runtime: self.runtime.clone(),
            memory_pool: self.memory_pool.clone(),
            metrics: self.metrics.clone(),
        }
        .into()
    }
}
