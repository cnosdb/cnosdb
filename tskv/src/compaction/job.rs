use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{atomic, Arc};
use std::time::{Duration, Instant};

use flush::run_flush_memtable_job;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{oneshot, RwLock, RwLockWriteGuard, Semaphore};
use trace::{error, info};

use crate::compaction::{flush, CompactTask, FlushReq, LevelCompactionPicker, Picker};
use crate::summary::SummaryTask;
use crate::{TsKvContext, TseriesFamilyId};

const COMPACT_BATCH_CHECKING_SECONDS: u64 = 1;

struct CompactProcessor {
    vnode_ids: HashMap<TseriesFamilyId, bool>,
}

impl CompactProcessor {
    fn insert(&mut self, vnode_id: TseriesFamilyId, should_flush: bool) {
        let old_should_flush = self.vnode_ids.entry(vnode_id).or_insert(should_flush);
        if should_flush && !*old_should_flush {
            *old_should_flush = should_flush
        }
    }

    fn take(&mut self) -> HashMap<TseriesFamilyId, bool> {
        std::mem::replace(&mut self.vnode_ids, HashMap::with_capacity(32))
    }
}

impl Default for CompactProcessor {
    fn default() -> Self {
        Self {
            vnode_ids: HashMap::with_capacity(32),
        }
    }
}

pub struct CompactJob {
    inner: Arc<RwLock<CompactJobInner>>,
}

impl CompactJob {
    pub fn new(runtime: Arc<Runtime>, ctx: Arc<TsKvContext>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(CompactJobInner::new(runtime, ctx))),
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

    pub async fn prepare_stop_vnode_compaction_job(&self) -> StartVnodeCompactionGuard {
        info!("StopCompactionGuard(create):prepare stop vnode compaction job");
        let inner = self.inner.write().await;
        inner
            .enable_compaction
            .store(false, atomic::Ordering::SeqCst);
        StartVnodeCompactionGuard { inner }
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
    compact_processor: Arc<RwLock<CompactProcessor>>,
    enable_compaction: Arc<AtomicBool>,
    running_compactions: Arc<AtomicUsize>,
}

impl CompactJobInner {
    fn new(runtime: Arc<Runtime>, ctx: Arc<TsKvContext>) -> Self {
        let compact_processor = Arc::new(RwLock::new(CompactProcessor::default()));

        Self {
            ctx,
            runtime,
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
                // Vnode id to compact & whether vnode be flushed before compact
                let (vnode_id, flush_vnode) = match compact_task {
                    CompactTask::Vnode(id) => (id, false),
                    CompactTask::ColdVnode(id) => (id, true),
                };
                compact_processor
                    .write()
                    .await
                    .insert(vnode_id, flush_vnode);
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
                info!("Compacting on vnode(job start): {:?}", &vnode_ids_for_debug);
                for (vnode_id, flush_vnode) in vnode_ids {
                    let ts_family = ctx
                        .version_set
                        .read()
                        .await
                        .get_tsfamily_by_tf_id(vnode_id)
                        .await;
                    if let Some(tsf) = ts_family {
                        info!("Starting compaction on ts_family {}", vnode_id);
                        let start = Instant::now();
                        if !tsf.read().await.can_compaction() {
                            info!("forbidden compaction on moving vnode {}", vnode_id);
                            return;
                        }
                        let picker = LevelCompactionPicker::new(ctx.options.storage.clone());
                        let version = tsf.read().await.version();
                        let compact_req = picker.pick_compaction(version);
                        if let Some(req) = compact_req {
                            let database = req.database.clone();
                            let compact_ts_family = req.ts_family_id;
                            let out_level = req.out_level;

                            // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                            let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                            let enable_compaction = enable_compaction.clone();
                            let running_compaction = running_compaction.clone();

                            let ctx = ctx.clone();
                            runtime_inner.spawn(async move {
                                // Check enable compaction
                                if !enable_compaction.load(atomic::Ordering::SeqCst) {
                                    return;
                                }
                                // Edit running_compation
                                running_compaction.fetch_add(1, atomic::Ordering::SeqCst);
                                let _sub_running_compaction_guard = DeferGuard(Some(|| {
                                    running_compaction.fetch_sub(1, atomic::Ordering::SeqCst);
                                }));

                                if flush_vnode {
                                    let mut tsf_wlock = tsf.write().await;
                                    tsf_wlock.switch_to_immutable();
                                    let flush_req = tsf_wlock.build_flush_req(true);
                                    drop(tsf_wlock);
                                    if let Some(req) = flush_req {
                                        if let Err(e) =
                                            flush::run_flush_memtable_job(req, ctx.clone(), false)
                                                .await
                                        {
                                            error!("Failed to flush vnode {}: {:?}", vnode_id, e);
                                        }
                                    }
                                }

                                match super::run_compaction_job(req, ctx.global_ctx.clone()).await {
                                    Ok(Some((version_edit, file_metas))) => {
                                        metrics::incr_compaction_success();
                                        let (summary_tx, _summary_rx) = oneshot::channel();
                                        let _ = ctx
                                            .summary_task_sender
                                            .send(SummaryTask::new(
                                                vec![version_edit],
                                                Some(file_metas),
                                                None,
                                                summary_tx,
                                            ))
                                            .await;

                                        metrics::sample_tskv_compaction_duration(
                                            database.as_str(),
                                            compact_ts_family.to_string().as_str(),
                                            out_level.to_string().as_str(),
                                            start.elapsed().as_secs_f64(),
                                        )
                                        // TODO Handle summary result using summary_rx.
                                    }
                                    Ok(None) => {
                                        info!("There is nothing to compact.");
                                    }
                                    Err(e) => {
                                        metrics::incr_compaction_failed();
                                        error!("Compaction job failed: {:?}", e);
                                    }
                                }
                                drop(permit);
                            });
                        }
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

    pub async fn prepare_stop_vnode_compaction_job(&self) {
        self.enable_compaction
            .store(false, atomic::Ordering::SeqCst);
    }

    async fn wait_stop_vnode_compaction_job(&self) {
        let mut check_interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            check_interval.tick().await;
            if self.running_compactions.load(atomic::Ordering::SeqCst) == 0 {
                return;
            }
        }
    }
}

pub struct StartVnodeCompactionGuard<'a> {
    inner: RwLockWriteGuard<'a, CompactJobInner>,
}

impl<'a> StartVnodeCompactionGuard<'a> {
    pub async fn wait(&self) {
        info!("StopCompactionGuard(wait): wait vnode compaction job to stop");
        self.inner.wait_stop_vnode_compaction_job().await
    }
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

pub struct FlushJob {
    ctx: Arc<TsKvContext>,
    runtime: Arc<Runtime>,
}

impl FlushJob {
    pub fn new(runtime: Arc<Runtime>, ctx: Arc<TsKvContext>) -> Self {
        Self { ctx, runtime }
    }

    pub fn start_vnode_flush_job(&self, mut flush_req_receiver: Receiver<FlushReq>) {
        let runtime_inner = self.runtime.clone();
        let ctx = self.ctx.clone();
        self.runtime.spawn(async move {
            while let Some(x) = flush_req_receiver.recv().await {
                // TODO(zipper): this make config `flush_req_channel_cap` wasted
                // Run flush job and trigger compaction.
                runtime_inner.spawn(run_flush_memtable_job(x, ctx.clone(), true));
            }
        });
        info!("Flush task handler started");
    }
}

impl std::fmt::Debug for FlushJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushJob").finish()
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
        compact_batch_builder.insert(1, false);
        compact_batch_builder.insert(2, false);
        compact_batch_builder.insert(1, true);
        compact_batch_builder.insert(3, true);
        assert_eq!(compact_batch_builder.vnode_ids.len(), 3);
        let mut keys: Vec<TseriesFamilyId> =
            compact_batch_builder.vnode_ids.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(compact_batch_builder.vnode_ids.get(&1), Some(&true));
        assert_eq!(compact_batch_builder.vnode_ids.get(&2), Some(&false));
        assert_eq!(compact_batch_builder.vnode_ids.get(&3), Some(&true));
        let vnode_ids = compact_batch_builder.take();
        assert_eq!(vnode_ids.len(), 3);
        assert_eq!(vnode_ids.get(&1), Some(&true));
        assert_eq!(vnode_ids.get(&2), Some(&false));
        assert_eq!(vnode_ids.get(&3), Some(&true));
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
