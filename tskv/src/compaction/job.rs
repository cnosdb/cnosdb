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

const COMPACT_BATCH_CHECKING_SECONDS: u64 = 1;

struct CompactTaskGroup {
    /// Maps CompactTask to the number of times it inserted.
    compact_tasks: HashMap<CompactTask, usize>,
}

impl CompactTaskGroup {
    fn insert(&mut self, task: CompactTask) {
        let num = self.compact_tasks.entry(task).or_default();
        *num += 1;
    }

    fn try_take(&mut self) -> Option<HashMap<CompactTask, usize>> {
        if self.compact_tasks.is_empty() {
            return None;
        }
        let compact_tasks = std::mem::replace(&mut self.compact_tasks, HashMap::with_capacity(32));
        Some(compact_tasks)
    }

    fn is_empty(&self) -> bool {
        self.compact_tasks.is_empty()
    }
}

impl Default for CompactTaskGroup {
    fn default() -> Self {
        Self {
            compact_tasks: HashMap::with_capacity(32),
        }
    }
}

pub fn run(
    storage_opt: Arc<StorageOptions>,
    runtime: Arc<Runtime>,
    compact_task_sender: Sender<CompactTask>,
    mut compact_task_receiver: Receiver<CompactTask>,
    ctx: Arc<GlobalContext>,
    seq_ctx: Arc<GlobalSequenceContext>,
    version_set: Arc<RwLock<VersionSet>>,
    summary_task_sender: Sender<SummaryTask>,
) {
    let runtime_inner = runtime.clone();
    let compact_task_group_producer = Arc::new(RwLock::new(CompactTaskGroup::default()));
    let compact_task_group_consumer = compact_task_group_producer.clone();
    runtime.spawn(async move {
        // TODO: Concurrent compactions should not over argument $cpu.
        let compaction_limit = Arc::new(Semaphore::new(
            storage_opt.max_concurrent_compaction as usize,
        ));
        let mut check_interval =
            tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));

        loop {
            check_interval.tick().await;
            if compact_task_group_consumer.read().await.is_empty() {
                continue;
            }
            let compact_tasks = match compact_task_group_consumer.write().await.try_take() {
                Some(t) => t,
                None => continue,
            };
            for (compact_task, _c) in compact_tasks {
                if !storage_opt.enable_compaction {
                    match &compact_task {
                        t @ CompactTask::Manual(_) => {
                            info!("Compaction is disabled but runs manually: {t:?}",);
                        }
                        _ => {
                            continue;
                        }
                    }
                }

                let vnode_id = compact_task.ts_family_id();
                let vnode_opt = version_set
                    .read()
                    .await
                    .get_tsfamily_by_tf_id(vnode_id)
                    .await;
                if let Some(vnode) = vnode_opt {
                    info!("Starting compaction on ts_family {vnode_id}");
                    let start = Instant::now();

                    let version = {
                        let vnode_rlock = vnode.read().await;
                        if !vnode_rlock.can_compaction() {
                            info!("forbidden compaction on moving vnode {vnode_id}",);
                            return;
                        }
                        vnode_rlock.version()
                    };

                    if let Some(req) = picker::pick_compaction(compact_task, version).await {
                        let database = req.version.database();
                        let in_level = req.in_level;
                        let out_level = req.out_level;

                        let ctx_inner = ctx.clone();
                        let seq_ctx_inner = seq_ctx.clone();
                        let version_set_inner = version_set.clone();
                        let compact_task_sender = compact_task_sender.clone();
                        let summary_task_sender_inner = summary_task_sender.clone();

                        // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                        let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                        runtime_inner.spawn(async move {
                            if let CompactTask::Cold(_) = &compact_task {
                                let mut tsf_wlock = vnode.write().await;
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
                                        error!("Failed to flush vnode {vnode_id}: {e:?}",);
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
                                        vnode_id.to_string().as_str(),
                                        in_level.to_string().as_str(),
                                        out_level.to_string().as_str(),
                                        start.elapsed().as_secs_f64(),
                                    );
                                    // TODO Handle summary result using summary_rx.

                                    // Send a normal compact request if it's a delta compaction.
                                    if let CompactTask::Delta(vnode_id) = &compact_task {
                                        let _ = compact_task_sender
                                            .send(CompactTask::Normal(*vnode_id))
                                            .await;
                                    }
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
        while let Some(compact_task) = compact_task_receiver.recv().await {
            compact_task_group_producer
                .write()
                .await
                .insert(compact_task);
        }
    });
}

#[cfg(test)]
mod test {
    use crate::compaction::job::CompactTaskGroup;
    use crate::compaction::CompactTask;

    #[test]
    fn test_build_compact_batch() {
        let mut ctg = CompactTaskGroup::default();
        ctg.insert(CompactTask::Normal(2));
        ctg.insert(CompactTask::Normal(1));
        ctg.insert(CompactTask::Delta(2));
        ctg.insert(CompactTask::Cold(1));
        ctg.insert(CompactTask::Normal(1));
        ctg.insert(CompactTask::Normal(3));
        assert_eq!(ctg.compact_tasks.len(), 5);
        assert_eq!(ctg.compact_tasks.get(&CompactTask::Normal(1)), Some(&2));
        assert_eq!(ctg.compact_tasks.get(&CompactTask::Normal(2)), Some(&1));
        assert_eq!(ctg.compact_tasks.get(&CompactTask::Normal(3)), Some(&1));
        assert_eq!(ctg.compact_tasks.get(&CompactTask::Delta(2)), Some(&1));
        assert_eq!(ctg.compact_tasks.get(&CompactTask::Cold(1)), Some(&1));
        assert_eq!(ctg.compact_tasks.get(&CompactTask::Cold(2)), None);
        assert_eq!(ctg.compact_tasks.get(&CompactTask::Cold(3)), None);

        let compact_tasks_ori = ctg.compact_tasks.clone();
        let compact_tasks = ctg.try_take();
        assert!(compact_tasks.is_some());
        let compact_tasks = compact_tasks.unwrap();
        assert_eq!(compact_tasks, compact_tasks_ori);

        let compact_tasks = ctg.try_take();
        assert!(compact_tasks.is_none());
    }
}
