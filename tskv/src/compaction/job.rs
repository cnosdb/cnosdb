use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, RwLock, Semaphore};
use tokio_util::sync::CancellationToken;
use trace::{error, info};

use crate::compaction::{flush, CompactTask, DeltaCompactionPicker, LevelCompactionPicker, Picker};
use crate::context::{GlobalContext, GlobalSequenceContext};
use crate::kv_option::StorageOptions;
use crate::summary::SummaryTask;
use crate::version_set::VersionSet;
use crate::TseriesFamilyId;

const COMPACT_BATCH_CHECKING_SECONDS: u64 = 1;

struct CompactProcessor {
    vnode_ids: HashMap<TseriesFamilyId, CompactStrategy>,
}

impl CompactProcessor {
    fn insert(&mut self, vnode_id: TseriesFamilyId, strategy: CompactStrategy) {
        let old_strategy = self.vnode_ids.entry(vnode_id).or_default();
        if strategy.flush_vnode {
            old_strategy.flush_vnode = true;
        }
        if strategy.delta_compaction {
            old_strategy.delta_compaction = true;
        }
    }

    fn take(&mut self) -> HashMap<TseriesFamilyId, CompactStrategy> {
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct CompactStrategy {
    flush_vnode: bool,
    delta_compaction: bool,
}

impl CompactStrategy {
    fn new(flush_vnode: bool, delta_compaction: bool) -> Self {
        Self {
            flush_vnode,
            delta_compaction,
        }
    }
}

pub fn run(
    storage_opt: Arc<StorageOptions>,
    runtime: Arc<Runtime>,
    mut receiver: Receiver<CompactTask>,
    ctx: Arc<GlobalContext>,
    seq_ctx: Arc<GlobalSequenceContext>,
    version_set: Arc<RwLock<VersionSet>>,
    summary_task_sender: Sender<SummaryTask>,
) {
    let runtime_inner = runtime.clone();
    let compact_processor = Arc::new(RwLock::new(CompactProcessor::default()));
    let compact_batch_processor = compact_processor.clone();
    let close_sig_tx = CancellationToken::new();
    let close_sig_rx = close_sig_tx.clone();
    runtime.spawn(async move {
        // TODO: Concurrent compactions should not over argument $cpu.
        let compaction_limit = Arc::new(Semaphore::new(
            storage_opt.max_concurrent_compaction as usize,
        ));
        let mut check_interval =
            tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));

        loop {
            if close_sig_rx.is_cancelled() {
                break;
            }
            check_interval.tick().await;
            let processor = compact_batch_processor.read().await;
            if processor.vnode_ids.is_empty() {
                continue;
            }
            drop(processor);
            let vnode_ids = compact_batch_processor.write().await.take();
            let vnode_ids_for_debug = vnode_ids.clone();
            let now = Instant::now();
            info!("Compacting on vnode(job start): {:?}", &vnode_ids_for_debug);
            for (
                vnode_id,
                CompactStrategy {
                    flush_vnode,
                    delta_compaction,
                },
            ) in vnode_ids
            {
                if close_sig_rx.is_cancelled() {
                    break;
                }

                let ts_family = version_set
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
                    // TODO(zipper): logical here is something clutter.
                    let picker: Box<dyn Picker> = if delta_compaction {
                        Box::new(DeltaCompactionPicker::new())
                    } else {
                        Box::new(LevelCompactionPicker::new())
                    };
                    let version = tsf.read().await.version();
                    let compact_req = picker.pick_compaction(version);
                    if let Some(req) = compact_req {
                        let database = req.database.clone();
                        let compact_ts_family = req.ts_family_id;
                        let out_level = req.out_level;

                        let ctx_inner = ctx.clone();
                        let seq_ctx_inner = seq_ctx.clone();
                        let version_set_inner = version_set.clone();
                        let summary_task_sender_inner = summary_task_sender.clone();

                        // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                        let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                        runtime_inner.spawn(async move {
                            if flush_vnode {
                                let mut tsf_wlock = tsf.write().await;
                                tsf_wlock.switch_to_immutable();
                                let flush_req = tsf_wlock.build_flush_req(true);
                                drop(tsf_wlock);
                                if let Some(req) = flush_req {
                                    if let Err(e) = flush::run_flush_memtable_job(
                                        req,
                                        ctx_inner.clone(),
                                        seq_ctx_inner.clone(),
                                        version_set_inner,
                                        summary_task_sender_inner.clone(),
                                        None,
                                    )
                                    .await
                                    {
                                        error!("Failed to flush vnode {}: {:?}", vnode_id, e);
                                    }
                                }
                            }

                            match super::run_compaction_job(req, ctx_inner).await {
                                Ok(Some((version_edit, file_metas))) => {
                                    metrics::incr_compaction_success();
                                    let (summary_tx, _summary_rx) = oneshot::channel();
                                    let _ = summary_task_sender_inner
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

    runtime.spawn(async move {
        while let Some(compact_task) = receiver.recv().await {
            let mut compact_processor_wlock = compact_processor.write().await;
            match compact_task {
                CompactTask::Normal(id) => compact_processor_wlock.insert(
                    id,
                    CompactStrategy {
                        flush_vnode: false,
                        delta_compaction: false,
                    },
                ),
                CompactTask::Cold(id) => compact_processor_wlock.insert(
                    id,
                    CompactStrategy {
                        flush_vnode: true,
                        delta_compaction: false,
                    },
                ),
                CompactTask::Delta(id) => compact_processor_wlock.insert(
                    id,
                    CompactStrategy {
                        flush_vnode: false,
                        delta_compaction: true,
                    },
                ),
            }
        }
        close_sig_tx.cancel();
    });
}

#[cfg(test)]
mod test {
    use crate::compaction::job::{CompactProcessor, CompactStrategy};
    use crate::TseriesFamilyId;

    #[test]
    fn test_build_compact_batch() {
        let mut compact_batch_builder = CompactProcessor::default();
        compact_batch_builder.insert(1, CompactStrategy::new(false, false));
        compact_batch_builder.insert(2, CompactStrategy::new(false, false));
        compact_batch_builder.insert(1, CompactStrategy::new(true, true));
        compact_batch_builder.insert(3, CompactStrategy::new(true, false));
        compact_batch_builder.insert(1, CompactStrategy::new(false, false));
        assert_eq!(compact_batch_builder.vnode_ids.len(), 3);
        let mut keys: Vec<TseriesFamilyId> =
            compact_batch_builder.vnode_ids.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(
            compact_batch_builder.vnode_ids.get(&1),
            Some(&CompactStrategy::new(true, true))
        );
        assert_eq!(
            compact_batch_builder.vnode_ids.get(&2),
            Some(&CompactStrategy::new(false, false))
        );
        assert_eq!(
            compact_batch_builder.vnode_ids.get(&3),
            Some(&CompactStrategy::new(true, false))
        );
        let vnode_ids = compact_batch_builder.take();
        assert_eq!(vnode_ids.len(), 3);
        assert_eq!(vnode_ids.get(&1), Some(&CompactStrategy::new(true, true)));
        assert_eq!(vnode_ids.get(&2), Some(&CompactStrategy::new(false, false)));
        assert_eq!(vnode_ids.get(&3), Some(&CompactStrategy::new(true, false)));
    }
}
