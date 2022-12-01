use std::{sync::Arc, time::Duration};

use datafusion::arrow::compute::kernels::limit;
use tokio::{
    runtime::Runtime,
    sync::{
        broadcast,
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot, RwLock, Semaphore,
    },
    task::JoinHandle,
    time::Instant,
};
use trace::{error, info, warn};

use crate::{
    context::GlobalContext, error::Result, kv_option::StorageOptions, summary::SummaryTask,
    version_set::VersionSet, TseriesFamilyId,
};

pub fn run(
    storage_opt: Arc<StorageOptions>,
    runtime: Arc<Runtime>,
    mut receiver: UnboundedReceiver<TseriesFamilyId>,
    ctx: Arc<GlobalContext>,
    version_set: Arc<RwLock<VersionSet>>,
    summary_task_sender: UnboundedSender<SummaryTask>,
) -> JoinHandle<()> {
    let runtime_inner = runtime.clone();

    runtime.spawn(async move {
        // TODO: Concurrent compactions should not over argument $cpu.
        let compaction_limit = Arc::new(Semaphore::new(
            storage_opt.max_concurrent_compaction as usize,
        ));

        while let Some(ts_family_id) = receiver.recv().await {
            let ts_family = version_set
                .read()
                .await
                .get_tsfamily_by_tf_id(ts_family_id)
                .await;
            if let Some(tsf) = ts_family {
                info!("Starting compaction on ts_family {}", ts_family_id);
                let start = Instant::now();

                let compact_req = tsf.read().pick_compaction();
                if let Some(req) = compact_req {
                    let database = req.database.clone();
                    let compact_ts_family = req.ts_family_id;
                    let out_level = req.out_level;

                    let ctx_inner = ctx.clone();
                    let summary_task_sender_inner = summary_task_sender.clone();

                    let permit = compaction_limit.clone().acquire_owned().await.unwrap();
                    runtime_inner.spawn(async move {
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
