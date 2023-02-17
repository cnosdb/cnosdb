use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::compute::kernels::limit;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{broadcast, oneshot, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use trace::{error, info, warn};

use crate::compaction::{flush, CompactTask, LevelCompactionPicker, Picker};
use crate::context::GlobalContext;
use crate::error::Result;
use crate::kv_option::StorageOptions;
use crate::summary::SummaryTask;
use crate::version_set::VersionSet;
use crate::TseriesFamilyId;

pub fn run(
    storage_opt: Arc<StorageOptions>,
    runtime: Arc<Runtime>,
    mut receiver: Receiver<CompactTask>,
    ctx: Arc<GlobalContext>,
    version_set: Arc<RwLock<VersionSet>>,
    summary_task_sender: Sender<SummaryTask>,
) -> JoinHandle<()> {
    let runtime_inner = runtime.clone();

    runtime.spawn(async move {
        // TODO: Concurrent compactions should not over argument $cpu.
        let compaction_limit = Arc::new(Semaphore::new(
            storage_opt.max_concurrent_compaction as usize,
        ));

        while let Some(compact_task) = receiver.recv().await {
            let (vnode_id, flus_vnode) = match compact_task {
                CompactTask::Vnode(id) => (id, false),
                CompactTask::ColdVnode(id) => (id, true),
            };
            let ts_family = version_set
                .read()
                .await
                .get_tsfamily_by_tf_id(vnode_id)
                .await;
            if let Some(tsf) = ts_family {
                info!("Starting compaction on ts_family {}", vnode_id);
                let start = Instant::now();

                let picker = LevelCompactionPicker::new(storage_opt.clone());
                let version = tsf.read().await.version();
                let compact_req = picker.pick_compaction(version);
                if let Some(req) = compact_req {
                    let database = req.database.clone();
                    let compact_ts_family = req.ts_family_id;
                    let out_level = req.out_level;

                    let ctx_inner = ctx.clone();
                    let version_set_inner = version_set.clone();
                    let summary_task_sender_inner = summary_task_sender.clone();

                    let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                    runtime_inner.spawn(async move {
                        if flus_vnode {
                            let mut tsf_wlock = tsf.write().await;
                            tsf_wlock.switch_to_immutable();
                            let flush_req = tsf_wlock.flush_req(true);
                            drop(tsf_wlock);
                            if let Some(req) = flush_req {
                                if let Err(e) = flush::run_flush_memtable_job(
                                    req,
                                    ctx_inner.clone(),
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
                                let (summary_tx, summary_rx) = oneshot::channel();
                                let ret = summary_task_sender_inner.send(
                                    SummaryTask::new_column_file_task(
                                        file_metas,
                                        vec![version_edit],
                                        summary_tx,
                                    ),
                                );

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
    })
}
