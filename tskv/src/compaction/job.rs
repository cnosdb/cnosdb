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

struct CompactTaskGroup {
    /// Maps CompactTask to the number of times it inserted.
    compact_tasks: HashMap<CompactTask, usize>,
}

impl CompactTaskGroup {
    fn insert(&mut self, task: CompactTask) {
        let num = self.compact_tasks.entry(task).or_default();
        *num += 1;
    }

    fn extend<T: IntoIterator<Item = CompactTask>>(&mut self, iter: T) {
        for task in iter {
            self.insert(task);
        }
    }

    fn try_take(&mut self) -> Option<HashMap<TseriesFamilyId, Vec<CompactTask>>> {
        if self.compact_tasks.is_empty() {
            return None;
        }
        let compact_tasks = std::mem::replace(&mut self.compact_tasks, HashMap::with_capacity(32));
        let mut grouped_compact_tasks: HashMap<u32, Vec<CompactTask>> = HashMap::new();
        for task in compact_tasks.into_keys() {
            let vnode_id = task.ts_family_id();
            let tasks = grouped_compact_tasks.entry(vnode_id).or_default();
            tasks.push(task);
        }
        for tasks in grouped_compact_tasks.values_mut() {
            tasks.sort();
        }
        Some(grouped_compact_tasks)
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
        // Maps vnode_id to whether it's compacting.
        let vnode_compacting_map: Arc<RwLock<HashMap<TseriesFamilyId, bool>>> = Arc::new(RwLock::new(HashMap::new()));
        let mut check_interval =
            tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));

        loop {
            check_interval.tick().await;
            if !storage_opt.enable_compaction {
                let _ = compact_task_group_consumer.write().await.try_take();
                continue;
            }
            if compact_task_group_consumer.read().await.is_empty() {
                continue;
            }
            // Get vnode_id maps to compact tasks, and mark vnodes compacting.
            let vnode_compact_tasks = {
                let mut task_group = compact_task_group_consumer.write().await;
                let mut vnode_compacting = vnode_compacting_map.write().await;
                // Consume the compact tasks group.
                match task_group.try_take() {
                    Some(mut vnode_tasks) => {
                        let vnode_ids = vnode_tasks.keys().cloned().collect::<Vec<_>>();
                        for vnode_id in vnode_ids.iter(){
                            match vnode_compacting.get(vnode_id) {
                                Some(true) => {
                                    // If vnode is compacting, put the tasks back to the compact task group.
                                    trace::trace!("vnode {vnode_id} is compacting, skip this time");
                                    if let Some(tasks) = vnode_tasks.remove(vnode_id) {
                                        // Put the tasks back to the compact task group.
                                        task_group.extend(tasks)
                                    }
                                }
                                _ => {
                                    // If vnode is not compacting, mark it as compacting.
                                    vnode_compacting.insert(*vnode_id, true);
                                }
                            }
                        }
                        vnode_tasks
                    },
                    None => continue,
                }
            };
            for (vnode_id, compact_tasks) in vnode_compact_tasks {
                let vnode = match version_set
                    .read()
                    .await
                    .get_tsfamily_by_tf_id(vnode_id)
                    .await {
                        Some(vnode) => vnode,
                        None => continue,
                    };
                // Method acquire_owned() will return AcquireError if the semaphore has been closed.
                let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                let ctx = ctx.clone();
                let seq_ctx = seq_ctx.clone();
                let version_set = version_set.clone();
                let compact_task_sender = compact_task_sender.clone();
                let summary_task_sender = summary_task_sender.clone();
                let vnode_compacting_map = vnode_compacting_map.clone();
                runtime_inner.spawn(async move {
                    for compact_task in compact_tasks {
                        info!("Starting compaction: {compact_task}");
                        let start = Instant::now();

                        let version = {
                            let vnode_rlock = vnode.read().await;
                            if !vnode_rlock.can_compaction() {
                                info!("forbidden compaction on moving vnode {vnode_id}",);
                                return;
                            }
                            vnode_rlock.version()
                        };
                        let compact_req = match picker::pick_compaction(compact_task, version).await {
                            Some(req) => {
                                req
                            },
                            None => {
                                info!("Finished compaction, did nothing");
                                continue;
                            }
                        };
                        let database = compact_req.version.database();
                        let in_level = compact_req.in_level;
                        let out_level = compact_req.out_level;

                        if let CompactTask::Cold(_) = &compact_task {
                            let mut tsf_wlock = vnode.write().await;
                            tsf_wlock.switch_to_immutable();
                            let flush_req = tsf_wlock.build_flush_req(true);
                            drop(tsf_wlock);
                            if let Some(req) = flush_req {
                                if let Err(e) = flush::run_flush_memtable_job(
                                    req,
                                    ctx.clone(),
                                    seq_ctx.clone(),
                                    version_set.clone(),
                                    summary_task_sender.clone(),
                                    None,
                                )
                                .await
                                {
                                    error!("Failed to flush vnode {vnode_id}: {e:?}",);
                                }
                            }
                        }

                        info!("Running compaction job: {compact_task}, sending to summary write.");
                        match super::run_compaction_job(compact_req, ctx.clone()).await {
                            Ok(Some((version_edit, file_metas))) => {
                                info!("Finished compaction1: {compact_task}, sending to summary write.");
                                metrics::incr_compaction_success();
                                let (summary_tx, summary_rx) = oneshot::channel();
                                let _ = summary_task_sender
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
                                info!("Finished compaction2: {compact_task}, waiting for summary write.");
                                match summary_rx.await {
                                    Ok(Ok(())) => {
                                        info!("Compaction: summary write success: {version_edit:?}");
                                    }
                                    Ok(Err(e)) => {
                                        error!("Compaction: Failed to write summary: {}", e);
                                    }
                                    Err(e) => {
                                        error!(
                                        "Compaction: Failed to receive summary write task: {}",
                                        e
                                    );
                                    }
                                }
                                // Send a normal compact request if it's a delta compaction.
                                if let CompactTask::Delta(vnode_id) = &compact_task {
                                    let _ = compact_task_sender
                                        .send(CompactTask::Normal(*vnode_id))
                                        .await;
                                }
                            }
                            Ok(None) => {
                                info!("Compaction There is nothing to compact.");
                            }
                            Err(e) => {
                                metrics::incr_compaction_failed();
                                error!("Compaction: job failed: {}", e);
                            }
                        }
                    }
                    // Mark vnode as not compacting.
                    vnode_compacting_map.write().await.remove(&vnode_id);
                    drop(permit);
                });
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

        let compact_tasks = ctg.try_take();
        assert!(compact_tasks.is_some());
        let compact_tasks = compact_tasks.unwrap();
        assert_eq!(compact_tasks.len(), 3);
        assert_eq!(
            compact_tasks.get(&1),
            Some(&vec![CompactTask::Normal(1), CompactTask::Cold(1)])
        );
        assert_eq!(
            compact_tasks.get(&2),
            Some(&vec![CompactTask::Delta(2), CompactTask::Normal(2)])
        );
        assert_eq!(compact_tasks.get(&3), Some(&vec![CompactTask::Normal(3)]));

        let compact_tasks = ctg.try_take();
        assert!(compact_tasks.is_none());

        ctg.extend(vec![
            CompactTask::Normal(1),
            CompactTask::Delta(1),
            CompactTask::Manual(1),
        ]);
        let compact_tasks = ctg.try_take().unwrap();
        assert_eq!(compact_tasks.len(), 1);
        assert_eq!(
            compact_tasks.get(&1),
            Some(&vec![
                CompactTask::Manual(1),
                CompactTask::Delta(1),
                CompactTask::Normal(1),
            ])
        );
    }
}
