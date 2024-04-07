use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, OwnedSemaphorePermit, RwLock, Semaphore};
use trace::{error, info, warn};

use crate::compaction::{picker, CompactTask};
use crate::context::{GlobalContext, GlobalSequenceContext};
use crate::kv_option::StorageOptions;
use crate::summary::SummaryTask;
use crate::version_set::VersionSet;

const COMPACT_BATCH_CHECKING_SECONDS: u64 = 1;

/// A group of compact tasks that keeps the insertion order.
struct CompactTaskGroup {
    set: HashSet<CompactTask>,
    deque: VecDeque<CompactTask>,
}

impl CompactTaskGroup {
    fn push_back(&mut self, task: CompactTask) {
        if self.set.contains(&task) {
            return;
        }
        self.set.insert(task);
        self.deque.push_back(task);
    }

    fn extend<T: IntoIterator<Item = CompactTask>>(&mut self, iter: T) {
        for task in iter {
            self.push_back(task);
        }
    }

    fn pop_front(&mut self) -> Option<CompactTask> {
        if let Some(t) = self.deque.pop_front() {
            self.set.remove(&t);
            Some(t)
        } else {
            None
        }
    }

    fn is_empty(&self) -> bool {
        self.deque.is_empty()
    }
}

impl Default for CompactTaskGroup {
    fn default() -> Self {
        Self {
            set: HashSet::with_capacity(32),
            deque: VecDeque::with_capacity(32),
        }
    }
}

pub fn run(
    storage_opt: Arc<StorageOptions>,
    runtime: Arc<Runtime>,
    compact_task_sender: Sender<CompactTask>,
    compact_task_receiver: Receiver<CompactTask>,
    ctx: Arc<GlobalContext>,
    seq_ctx: Arc<GlobalSequenceContext>,
    version_set: Arc<RwLock<VersionSet>>,
    summary_task_sender: Sender<SummaryTask>,
) {
    let compact_job = CompactionJob {
        runtime,
        compaction_context: Arc::new(CompactionContext {
            version_set,
            ctx,
            seq_ctx,
            compact_task_sender,
            summary_task_sender,
        }),
        semaphore: Arc::new(Semaphore::new(
            storage_opt.max_concurrent_compaction as usize,
        )),
        compact_task_receiver,
    };

    compact_job.run_background_job();
}

struct CompactionJob {
    runtime: Arc<Runtime>,
    compaction_context: Arc<CompactionContext>,
    semaphore: Arc<Semaphore>,
    compact_task_receiver: Receiver<CompactTask>,
}

struct CompactionContext {
    version_set: Arc<RwLock<VersionSet>>,
    ctx: Arc<GlobalContext>,
    seq_ctx: Arc<GlobalSequenceContext>,
    compact_task_sender: Sender<CompactTask>,
    summary_task_sender: Sender<SummaryTask>,
}

impl CompactionJob {
    fn run_background_job(self) {
        let compact_task_group_producer = Arc::new(RwLock::new(CompactTaskGroup::default()));
        let compact_task_group_consumer = compact_task_group_producer.clone();
        let CompactionJob {
            runtime,
            compaction_context,
            semaphore: limiter,
            mut compact_task_receiver,
        } = self;

        runtime.spawn(async move {
            while let Some(compact_task) = compact_task_receiver.recv().await {
                compact_task_group_producer
                    .write()
                    .await
                    .push_back(compact_task);
            }
        });
        let runtime_inner = runtime.clone();
        runtime.spawn(async move {
            let mut check_interval =
                tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));
            loop {
                check_interval.tick().await;
                let compact_task = match compact_task_group_consumer.write().await.pop_front() {
                    Some(t) => t,
                    None => continue,
                };
                let permit = match limiter.clone().acquire_owned().await {
                    Ok(l) => l,
                    Err(e) => {
                        // Semaphore closed.
                        warn!("Stopping compaction job, semaphore closed: {e}");
                        break;
                    }
                };
                runtime_inner.spawn(Self::run_compact_task(
                    compact_task,
                    compaction_context.clone(),
                    permit,
                ));
            }
        });
    }

    async fn run_compact_task(
        task: CompactTask,
        context: Arc<CompactionContext>,
        _permit: OwnedSemaphorePermit,
    ) {
        info!("Starting compaction: {task}");
        let start = Instant::now();

        let vnode_id = task.ts_family_id();
        let version = {
            let vnode = {
                let version_set = context.version_set.read().await;
                match version_set.get_tsfamily_by_tf_id(vnode_id).await {
                    Some(v) => v,
                    None => return,
                }
            };
            let vnode = vnode.read().await;
            if !vnode.can_compaction() {
                info!(
                    "Compaction skipped: vnode_id: {vnode_id}, status: {}",
                    vnode.status()
                );
                return;
            }
            vnode.version()
        };

        let compact_req = match picker::pick_compaction(task, version).await {
            Some(req) => req,
            None => {
                info!("Finished compaction, did nothing");
                return;
            }
        };
        let database = compact_req.version.database();
        let in_level = compact_req.in_level;
        let out_level = compact_req.out_level;

        info!("Running compaction job: {task}, sending to summary write.");
        match super::run_compaction_job(compact_req, context.ctx.clone()).await {
            Ok(Some((version_edit, file_metas))) => {
                info!("Finished compaction, sending to summary write: {task}.");
                metrics::incr_compaction_success();
                let (summary_tx, summary_rx) = oneshot::channel();
                let _ = context
                    .summary_task_sender
                    .send(SummaryTask::new(
                        vec![version_edit.clone()],
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
                info!("Finished compaction, waiting for summary write: {task}.");
                match summary_rx.await {
                    Ok(Ok(())) => {
                        info!("Finished compaction, summary write success: {version_edit:?}");
                    }
                    Ok(Err(e)) => {
                        error!("Finished compaction, but failed to write summary: {e}");
                    }
                    Err(e) => {
                        error!(
                            "Finished compaction, but failed to receive summary write task: {e}",
                        );
                    }
                }
            }
            Ok(None) => {
                info!("Finished compaction: There is nothing to compact.");
            }
            Err(e) => {
                metrics::incr_compaction_failed();
                error!("Compaction: job failed: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::compaction::job::CompactTaskGroup;
    use crate::compaction::CompactTask;

    #[test]
    fn test_build_compact_batch() {
        let mut ctg = CompactTaskGroup::default();
        ctg.push_back(CompactTask::Normal(2));
        ctg.push_back(CompactTask::Normal(1));
        ctg.push_back(CompactTask::Delta(2));
        ctg.push_back(CompactTask::Normal(1));
        ctg.push_back(CompactTask::Normal(3));
        assert_eq!(ctg.pop_front(), Some(CompactTask::Normal(2)));
        assert_eq!(ctg.pop_front(), Some(CompactTask::Normal(1)));
        assert_eq!(ctg.pop_front(), Some(CompactTask::Delta(2)));
        assert_eq!(ctg.pop_front(), Some(CompactTask::Normal(3)));
        assert_eq!(ctg.pop_front(), None);

        ctg.extend(vec![
            CompactTask::Normal(1),
            CompactTask::Delta(1),
            CompactTask::Manual(1),
            CompactTask::Normal(1),
        ]);
        assert_eq!(ctg.pop_front(), Some(CompactTask::Normal(1)));
        assert_eq!(ctg.pop_front(), Some(CompactTask::Delta(1)));
        assert_eq!(ctg.pop_front(), Some(CompactTask::Manual(1)));
        assert_eq!(ctg.pop_front(), None);
    }
}
