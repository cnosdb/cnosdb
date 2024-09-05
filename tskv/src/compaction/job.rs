use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{atomic, Arc};
use std::time::{Duration, Instant};

use metrics::metric_register::MetricsRegister;
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{oneshot, RwLock, RwLockWriteGuard, Semaphore};
use trace::{error, info};

use crate::compaction::metrics::{CompactionType, VnodeCompactionMetrics};
use crate::compaction::{flush, CompactTask, FlushReq, LevelCompactionPicker, Picker};
use crate::error::IndexErrSnafu;
use crate::summary::SummaryTask;
use crate::{TsKvContext, TseriesFamilyId, TskvResult};

const COMPACT_BATCH_CHECKING_SECONDS: u64 = 1;

struct CompactProcessor {
    vnode_ids: Vec<TseriesFamilyId>,
}

impl CompactProcessor {
    fn insert(&mut self, vnode_id: TseriesFamilyId) {
        if !self.vnode_ids.contains(&vnode_id) {
            self.vnode_ids.push(vnode_id)
        }
    }

    fn take(&mut self) -> Vec<TseriesFamilyId> {
        std::mem::replace(&mut self.vnode_ids, Vec::with_capacity(32))
    }
}

impl Default for CompactProcessor {
    fn default() -> Self {
        Self {
            vnode_ids: Vec::with_capacity(32),
        }
    }
}

pub struct CompactJob {
    inner: Arc<RwLock<CompactJobInner>>,
}

impl CompactJob {
    pub fn new(
        runtime: Arc<Runtime>,
        ctx: Arc<TsKvContext>,
        metrics_register: Arc<MetricsRegister>,
    ) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CompactJobInner::new(
                runtime,
                ctx,
                metrics_register,
            ))),
        }
    }

    pub async fn start_merge_compact_task_job(&self, compact_task_receiver: Receiver<CompactTask>) {
        self.inner
            .read()
            .await
            .start_merge_compact_task_job(compact_task_receiver);
    }

    pub async fn start_vnode_compaction_job(&self) {
        self.inner.read().await.start_vnode_compaction_job();
    }
}

impl std::fmt::Debug for CompactJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactJob").finish()
    }
}

struct CompactJobInner {
    ctx: Arc<TsKvContext>,
    runtime: Arc<Runtime>,
    metrics_register: Arc<MetricsRegister>,
    compact_processor: Arc<RwLock<CompactProcessor>>,
    enable_compaction: Arc<AtomicBool>,
    running_compactions: Arc<AtomicUsize>,
}

impl CompactJobInner {
    fn new(
        runtime: Arc<Runtime>,
        ctx: Arc<TsKvContext>,
        metrics_register: Arc<MetricsRegister>,
    ) -> Self {
        let compact_processor = Arc::new(RwLock::new(CompactProcessor::default()));

        Self {
            ctx,
            runtime,
            metrics_register,
            compact_processor,
            enable_compaction: Arc::new(AtomicBool::new(false)),
            running_compactions: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn start_merge_compact_task_job(&self, mut compact_task_receiver: Receiver<CompactTask>) {
        info!("Compaction: start merge compact task job");
        let compact_processor = self.compact_processor.clone();
        self.runtime.spawn(async move {
            while let Some(compact_task) = compact_task_receiver.recv().await {
                compact_processor.write().await.insert(compact_task.tsf_id);
            }
        });
    }

    fn start_vnode_compaction_job(&self) {
        info!("Compaction: start vnode compaction job");
        if self
            .enable_compaction
            .compare_exchange(
                false,
                true,
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            info!("Compaction: failed to change enable_compaction from false to true, compaction is already started");
            return;
        }

        let runtime_inner = self.runtime.clone();
        let compact_processor = self.compact_processor.clone();
        let enable_compaction = self.enable_compaction.clone();
        let running_compaction = self.running_compactions.clone();
        let ctx = self.ctx.clone();
        let metrics_registry = self.metrics_register.clone();

        self.runtime.spawn(async move {
            // TODO: Concurrent compactions should not over argument $cpu.
            let compaction_limit = Arc::new(Semaphore::new(
                ctx.options.storage.max_concurrent_compaction as usize,
            ));
            let mut check_interval =
                tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));

            loop {
                check_interval.tick().await;
                if !enable_compaction.load(atomic::Ordering::SeqCst) {
                    break;
                }
                if compact_processor.read().await.vnode_ids.is_empty() {
                    continue;
                }
                let vnode_ids = compact_processor.write().await.take();
                let vnode_ids_for_debug = vnode_ids.clone();
                let now = Instant::now();
                let mut features = Vec::with_capacity(vnode_ids.len());
                info!("Compacting on vnode(job start): {:?}", &vnode_ids_for_debug);
                for vnode_id in vnode_ids {
                    let ts_family = ctx
                        .version_set
                        .read()
                        .await
                        .get_tsfamily_by_tf_id(vnode_id)
                        .await;
                    if let Some(tsf) = ts_family {
                        info!("Starting compaction on ts_family {}", vnode_id);
                        if !tsf.read().await.can_compaction() {
                            info!("forbidden compaction on moving vnode {}", vnode_id);
                            return;
                        }
                        let picker = LevelCompactionPicker {};
                        let version = tsf.read().await.version();
                        let compact_req = picker.pick_compaction(version);
                        if let Some(req) = compact_req {
                            // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                            let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                            let enable_compaction = enable_compaction.clone();
                            let running_compaction = running_compaction.clone();

                            let ctx = ctx.clone();
                            let metrics_registry = metrics_registry.clone();
                            features.push(runtime_inner.spawn(async move {
                                // Check enable compaction
                                if !enable_compaction.load(atomic::Ordering::SeqCst) {
                                    return;
                                }
                                // Edit running_compaction
                                running_compaction.fetch_add(1, atomic::Ordering::SeqCst);
                                let _sub_running_compaction_guard = DeferGuard(Some(|| {
                                    running_compaction.fetch_sub(1, atomic::Ordering::SeqCst);
                                }));

                                let vnode_compaction_metrics = VnodeCompactionMetrics::new(
                                    &metrics_registry,
                                    ctx.options.storage.node_id,
                                    req.ts_family_id,
                                    CompactionType::Normal,
                                    ctx.options.storage.collect_compaction_metrics,
                                );
                                match super::run_compaction_job(
                                    req,
                                    ctx.global_ctx.clone(),
                                    vnode_compaction_metrics,
                                )
                                .await
                                {
                                    Ok(Some((version_edit, file_metas))) => {
                                        let (summary_tx, _summary_rx) = oneshot::channel();
                                        let _ = ctx
                                            .summary_task_sender
                                            .send(SummaryTask::new(
                                                tsf.clone(),
                                                version_edit,
                                                Some(file_metas),
                                                None,
                                                summary_tx,
                                            ))
                                            .await;

                                        // TODO Handle summary result using summary_rx.
                                    }
                                    Ok(None) => {
                                        info!("There is nothing to compact.");
                                    }
                                    Err(e) => {
                                        error!("Compaction job failed: {:?}", e);
                                    }
                                }
                                drop(permit);
                            }));
                        } else {
                            info!("There is no need to compact.");
                        }
                    }
                }
                for feature in features {
                    if let Err(e) = feature.await {
                        error!("Compaction job feature failed: {:?}", e);
                    }
                }
                info!(
                    "Compacting on vnode(job start): {:?} costs {} sec",
                    vnode_ids_for_debug,
                    now.elapsed().as_secs()
                );
            }
        });
    }
}

pub struct StartVnodeCompactionGuard<'a> {
    inner: RwLockWriteGuard<'a, CompactJobInner>,
}

impl<'a> Drop for StartVnodeCompactionGuard<'a> {
    fn drop(&mut self) {
        info!("StopCompactionGuard(drop): start vnode compaction job");
        self.inner.start_vnode_compaction_job();
    }
}

pub struct DeferGuard<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for DeferGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f()
        }
    }
}

#[derive(Clone)]
pub struct FlushJob {
    ctx: Arc<TsKvContext>,
    lock: Arc<RwLock<()>>,
}

impl FlushJob {
    pub fn new(ctx: Arc<TsKvContext>) -> Self {
        Self {
            ctx,
            lock: Arc::new(RwLock::new(())),
        }
    }

    pub async fn run_block(&self, request: FlushReq) -> TskvResult<()> {
        let ctx = self.ctx.clone();
        let lock = self.lock.clone();

        let result = Self::run(&request, ctx, lock).await;
        info!("block flush memcaches {} result: {:?}", request, result);

        result
    }

    pub fn run_spawn(&self, request: FlushReq) -> TskvResult<()> {
        let ctx = self.ctx.clone();
        let lock = self.lock.clone();

        self.ctx.runtime.spawn(async move {
            let result = Self::run(&request, ctx, lock).await;
            info!("spawn flush memcaches {} result: {:?}", request, result);
        });

        Ok(())
    }

    async fn run(
        request: &FlushReq,
        ctx: Arc<TsKvContext>,
        lock: Arc<RwLock<()>>,
    ) -> TskvResult<()> {
        let _write_guard = lock.write().await;
        info!("begin flush data {}", request);

        // flush index
        request
            .ts_index
            .write()
            .await
            .flush()
            .await
            .context(IndexErrSnafu)?;

        // check memecaches is empty
        let mut is_empty = true;
        let mems = request.ts_family.read().await.im_cache().clone();
        for mem in mems.iter() {
            if !mem.read().is_empty() {
                is_empty = false;
            }
        }

        // flush memecache
        if !is_empty {
            flush::flush_memtable(request, ctx, mems).await?;
        } else {
            info!("memcaches is empty exit flush {}", request);
        }

        let _ = request
            .ts_index
            .write()
            .await
            .clear_tombstone_series()
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{self, AtomicI32};
    use std::sync::Arc;

    use super::DeferGuard;
    use crate::compaction::job::CompactProcessor;
    use crate::TseriesFamilyId;

    #[test]
    fn test_build_compact_batch() {
        let mut compact_batch_builder = CompactProcessor::default();
        compact_batch_builder.insert(1);
        compact_batch_builder.insert(2);
        compact_batch_builder.insert(1);
        compact_batch_builder.insert(3);
        assert_eq!(compact_batch_builder.vnode_ids.len(), 3);

        let mut keys: Vec<TseriesFamilyId> = compact_batch_builder.vnode_ids.clone();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);

        let vnode_ids = compact_batch_builder.take();
        assert_eq!(vnode_ids, vec![1, 2, 3]);
    }

    #[test]
    fn test_defer_guard() {
        let a = Arc::new(AtomicI32::new(0));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .unwrap();
        {
            let a = a.clone();
            let jh = runtime.spawn(async move {
                a.fetch_add(1, atomic::Ordering::SeqCst);
                let _guard = DeferGuard(Some(|| {
                    a.fetch_sub(1, atomic::Ordering::SeqCst);
                }));
                a.fetch_add(1, atomic::Ordering::SeqCst);
                a.fetch_add(1, atomic::Ordering::SeqCst);

                assert_eq!(a.load(atomic::Ordering::SeqCst), 3);
            });
            let _ = runtime.block_on(jh);
        }
        assert_eq!(a.load(atomic::Ordering::SeqCst), 2);
    }
}
