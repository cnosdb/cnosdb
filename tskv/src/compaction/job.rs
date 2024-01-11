use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, RwLock, Semaphore};
use trace::{error, info};

use crate::compaction::{flush, picker, CompactTask};
use crate::context::{GlobalContext, GlobalSequenceContext};
use crate::kv_option::StorageOptions;
use crate::summary::SummaryTask;
use crate::version_set::VersionSet;
use crate::TseriesFamilyId;

const COMPACT_BATCH_CHECKING_SECONDS: u64 = 1;

struct CompactProcessor {
    /// Maps (vnode_id, is_delta_compaction) to should_flush_vnode.
    compact_task_keys: HashMap<CompactTaskKey, bool>,
}

impl CompactProcessor {
    fn insert(&mut self, key: CompactTaskKey, should_flush: bool) {
        let old_should_flush = self.compact_task_keys.entry(key).or_insert(should_flush);
        if should_flush && !*old_should_flush {
            *old_should_flush = should_flush
        }
    }

    fn take(&mut self) -> HashMap<CompactTaskKey, bool> {
        std::mem::replace(&mut self.compact_task_keys, HashMap::with_capacity(32))
    }
}

impl Default for CompactProcessor {
    fn default() -> Self {
        Self {
            compact_task_keys: HashMap::with_capacity(32),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
struct CompactTaskKey {
    vnode_id: TseriesFamilyId,
    delta_compaction: bool,
}

impl CompactTaskKey {
    pub fn new(vnode_id: TseriesFamilyId, delta_compaction: bool) -> Self {
        Self {
            vnode_id,
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
    runtime.spawn(async move {
        // TODO: Concurrent compactions should not over argument $cpu.
        let compaction_limit = Arc::new(Semaphore::new(
            storage_opt.max_concurrent_compaction as usize,
        ));
        let mut check_interval =
            tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));

        loop {
            check_interval.tick().await;
            let processor = compact_batch_processor.read().await;
            if processor.compact_task_keys.is_empty() {
                continue;
            }
            drop(processor);
            let compact_tasks = compact_batch_processor.write().await.take();
            for (compact_task_key, should_flush) in compact_tasks {
                let ts_family = version_set
                    .read()
                    .await
                    .get_tsfamily_by_tf_id(compact_task_key.vnode_id)
                    .await;
                if let Some(tsf) = ts_family {
                    info!(
                        "Starting compaction on ts_family {}",
                        compact_task_key.vnode_id
                    );
                    let start = Instant::now();
                    if !tsf.read().await.can_compaction() {
                        info!(
                            "forbidden compaction on moving vnode {}",
                            compact_task_key.vnode_id
                        );
                        return;
                    }
                    let version = tsf.read().await.version();
                    let compact_req = if compact_task_key.delta_compaction {
                        picker::pick_delta_compaction(version)
                    } else {
                        picker::pick_level_compaction(version)
                    };
                    if let Some(req) = compact_req {
                        let database = req.database.clone();
                        let compact_ts_family = req.ts_family_id;
                        let in_level = req.in_level;
                        let out_level = req.out_level;

                        let ctx_inner = ctx.clone();
                        let seq_ctx_inner = seq_ctx.clone();
                        let version_set_inner = version_set.clone();
                        let summary_task_sender_inner = summary_task_sender.clone();

                        // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                        let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                        runtime_inner.spawn(async move {
                            if should_flush {
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
                                        error!(
                                            "Failed to flush vnode {}: {:?}",
                                            compact_task_key.vnode_id, e
                                        );
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
                                        in_level.to_string().as_str(),
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
        }
    });

    runtime.spawn(async move {
        while let Some(compact_task) = receiver.recv().await {
            let mut compact_processor_wlock = compact_processor.write().await;
            match compact_task {
                CompactTask::Normal(id) => compact_processor_wlock.insert(
                    CompactTaskKey {
                        vnode_id: id,
                        delta_compaction: false,
                    },
                    false,
                ),
                CompactTask::Cold(id) => compact_processor_wlock.insert(
                    CompactTaskKey {
                        vnode_id: id,
                        delta_compaction: false,
                    },
                    true,
                ),
                CompactTask::Delta(id) => compact_processor_wlock.insert(
                    CompactTaskKey {
                        vnode_id: id,
                        delta_compaction: true,
                    },
                    false,
                ),
            }
        }
    });
}

#[cfg(test)]
mod test {
    use crate::compaction::job::{CompactProcessor, CompactTaskKey};

    #[test]
    fn test_build_compact_batch() {
        let mut compact_batch_builder = CompactProcessor::default();
        compact_batch_builder.insert(CompactTaskKey::new(1, false), false);
        compact_batch_builder.insert(CompactTaskKey::new(2, false), false);
        compact_batch_builder.insert(CompactTaskKey::new(1, true), true);
        compact_batch_builder.insert(CompactTaskKey::new(3, false), true);
        compact_batch_builder.insert(CompactTaskKey::new(1, false), true);
        assert_eq!(compact_batch_builder.compact_task_keys.len(), 4);
        let mut keys: Vec<CompactTaskKey> = compact_batch_builder
            .compact_task_keys
            .keys()
            .cloned()
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            vec![
                CompactTaskKey::new(1, false),
                CompactTaskKey::new(1, true),
                CompactTaskKey::new(2, false),
                CompactTaskKey::new(3, false),
            ],
        );
        assert_eq!(
            compact_batch_builder
                .compact_task_keys
                .get(&CompactTaskKey::new(1, false)),
            Some(&true)
        );
        assert_eq!(
            compact_batch_builder
                .compact_task_keys
                .get(&CompactTaskKey::new(1, true)),
            Some(&true)
        );
        assert_eq!(
            compact_batch_builder
                .compact_task_keys
                .get(&CompactTaskKey::new(2, false)),
            Some(&false)
        );
        assert_eq!(
            compact_batch_builder
                .compact_task_keys
                .get(&CompactTaskKey::new(3, false)),
            Some(&true)
        );
        let compact_tasks = compact_batch_builder.take();
        assert_eq!(compact_tasks.len(), 4);
        assert_eq!(
            compact_tasks.get(&CompactTaskKey::new(1, true)),
            Some(&true)
        );
        assert_eq!(
            compact_tasks.get(&CompactTaskKey::new(1, false)),
            Some(&true)
        );
        assert_eq!(
            compact_tasks.get(&CompactTaskKey::new(2, false)),
            Some(&false)
        );
        assert_eq!(
            compact_tasks.get(&CompactTaskKey::new(3, false)),
            Some(&true)
        );
        let compact_tasks = compact_batch_builder.take();
        assert!(compact_tasks.is_empty());
    }
}
