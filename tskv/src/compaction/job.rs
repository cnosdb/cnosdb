use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{Array, StringBuilder, Time32SecondBuilder, UInt32Builder};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use futures::Future;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, OwnedSemaphorePermit, RwLock, Semaphore};
use trace::{debug, error, info, warn};

use crate::compaction::{picker, CompactTask};
use crate::context::{GlobalContext, GlobalSequenceContext};
use crate::kv_option::StorageOptions;
use crate::summary::SummaryTask;
use crate::version_set::VersionSet;
use crate::TseriesFamilyId;

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
) -> CompactionInfoManager {
    let compact_job = CompactionJob {
        runtime,
        compaction_context: Arc::new(CompactionContext {
            version_set,
            ctx,
            seq_ctx,
            compact_task_sender,
            summary_task_sender,
            compaction_info: CompactionInfoManager::new(),
        }),
        semaphore: Arc::new(Semaphore::new(
            storage_opt.max_concurrent_compaction as usize,
        )),
        compact_task_receiver,
    };

    compact_job.run_background_job(storage_opt.enable_compaction)
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

    /// Keep each vnodes can only have one compaction task running.
    /// and stores the compaction state of those vnodes.
    compaction_info: CompactionInfoManager,
}

impl CompactionJob {
    fn run_background_job(self, enable_compaction: bool) -> CompactionInfoManager {
        let compact_task_group_producer = Arc::new(RwLock::new(CompactTaskGroup::default()));
        let compact_task_group_consumer = compact_task_group_producer.clone();
        let CompactionJob {
            runtime,
            compaction_context,
            semaphore: limiter,
            mut compact_task_receiver,
        } = self;

        // Background job to collect unique compact tasks.
        runtime.spawn(async move {
            while let Some(compact_task) = compact_task_receiver.recv().await {
                compact_task_group_producer
                    .write()
                    .await
                    .push_back(compact_task);
            }
        });

        let compaction_info_ref = compaction_context.compaction_info.clone();
        let runtime_ref = runtime.clone();
        // Background job to run compaction tasks.
        runtime.spawn(async move {
            let compacting_vnodes = compaction_context.compaction_info.vnodes.clone();
            let mut check_interval =
                tokio::time::interval(Duration::from_secs(COMPACT_BATCH_CHECKING_SECONDS));
            loop {
                let permit = match limiter.clone().acquire_owned().await {
                    Ok(l) => l,
                    Err(e) => {
                        // Semaphore closed.
                        warn!("Stopping compaction job, semaphore closed: {e}");
                        break;
                    }
                };
                let compact_task_opt = compact_task_group_consumer.write().await.pop_front();
                let (compact_task, after_compact, vnode_compaction_info) = match compact_task_opt {
                    Some(t) => {
                        let vnode_id = t.ts_family_id();
                        if compacting_vnodes.read().await.contains_key(&vnode_id) {
                            // If vnode is compacting, put the tasks back to the compact task group.
                            debug!("vnode {vnode_id} is compacting, skip this time");
                            compact_task_group_consumer.write().await.push_back(t);
                            continue;
                        } else {
                            let compaction_info = VnodeCompactionInfo::new(vnode_id);
                            // If vnode is not compacting, mark it as compacting.
                            compacting_vnodes
                                .write()
                                .await
                                .insert(vnode_id, compaction_info.clone());
                            let compacting_vnodes_ref = compacting_vnodes.clone();
                            let d = DeferGuard::new(runtime_ref.clone(), async move {
                                compacting_vnodes_ref.write().await.remove(&vnode_id);
                            });
                            (t, d, compaction_info)
                        }
                    }
                    None => {
                        check_interval.tick().await;
                        continue;
                    }
                };
                if enable_compaction {
                    runtime_ref.spawn(Self::run_compact_task(
                        compact_task,
                        compaction_context.clone(),
                        vnode_compaction_info,
                        permit,
                        after_compact,
                    ));
                } else {
                    debug!("Compaction is disabled, skip compaction task: {compact_task}");
                }
            }
        });

        compaction_info_ref
    }

    async fn run_compact_task(
        task: CompactTask,
        context: Arc<CompactionContext>,
        compaction_info: VnodeCompactionInfo,
        _permit: OwnedSemaphorePermit,
        _after_compact: DeferGuard<impl Future<Output = ()> + Send>,
    ) {
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
                return;
            }
            vnode.version()
        };

        compaction_info
            .change_state(VnodeCompactionState::PickingFiles)
            .await;
        let compact_req = match picker::pick_compaction(task, version).await {
            Some(req) => req,
            None => {
                compaction_info
                    .change_state(VnodeCompactionState::Finishing)
                    .await;
                debug!("Finished compactionb: {task}, did nothing");
                return;
            }
        };
        let database = compact_req.version.database();
        let in_level = compact_req.in_level;
        let out_level = compact_req.out_level;

        info!("Running compaction job: {task}.");
        compaction_info
            .change_state(VnodeCompactionState::Running)
            .await;
        match super::run_compaction_job(compact_req, context.ctx.clone()).await {
            Ok(Some((version_edit, file_metas))) => {
                info!("Finished compaction, sending to summary write: {task}.");
                metrics::incr_compaction_success();
                let (summary_tx, summary_rx) = oneshot::channel();
                compaction_info
                    .change_state(VnodeCompactionState::WritingSummary)
                    .await;
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
                info!("Finished compaction, waiting for summary write: {task}");
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
                info!("Finished compaction, nothing compacted");
            }
            Err(e) => {
                metrics::incr_compaction_failed();
                error!("Compaction: job failed: {e}");
            }
        }
        compaction_info
            .change_state(VnodeCompactionState::Finishing)
            .await;
    }
}

pub struct DeferGuard<F: Future<Output = ()> + Send + 'static> {
    runtime: Arc<Runtime>,
    f: Option<F>,
}

impl<F: Future<Output = ()> + Send + 'static> DeferGuard<F> {
    pub fn new(runtime: Arc<Runtime>, f: F) -> Self {
        Self {
            runtime,
            f: Some(f),
        }
    }
}

impl<F: Future<Output = ()> + Send + 'static> Drop for DeferGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            self.runtime.spawn(f);
        }
    }
}

#[derive(Clone)]
pub struct CompactionInfoManager {
    vnodes: Arc<RwLock<HashMap<TseriesFamilyId, VnodeCompactionInfo>>>,
}

impl CompactionInfoManager {
    pub fn new() -> Self {
        Self {
            vnodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn vnodes_owned(&self) -> HashMap<TseriesFamilyId, VnodeCompactionInfoInner> {
        let vnodes = self.vnodes.read().await;
        let mut vnodes_owned = HashMap::with_capacity(vnodes.len());
        for (vnode_id, vnode_info) in vnodes.iter() {
            vnodes_owned.insert(*vnode_id, vnode_info.inner_owned().await);
        }
        vnodes_owned
    }

    pub async fn to_record_batch(&self) -> Result<RecordBatch, ArrowError> {
        let mut vnode_ids = UInt32Builder::new();
        let mut start_times = Time32SecondBuilder::new();
        let mut states = StringBuilder::new();
        let vnode_compaction_infos = self.vnodes_owned().await;
        for (vnode_id, compaction_info) in vnode_compaction_infos.iter() {
            vnode_ids.append_value(*vnode_id);
            start_times.append_value(compaction_info.start_time().elapsed().as_secs() as i32);
            states.append_value(compaction_info.state().as_str());
        }
        let columns: Vec<Arc<dyn Array>> = vec![
            Arc::new(vnode_ids.finish()),
            Arc::new(start_times.finish()),
            Arc::new(states.finish()),
        ];
        let schema = Schema::new(vec![
            Field::new("vnode_id", DataType::UInt32, false),
            Field::new("elapsed", DataType::Time32(TimeUnit::Second), false),
            Field::new("state", DataType::Utf8, false),
        ]);
        RecordBatch::try_new(Arc::new(schema), columns)
    }
}

impl std::fmt::Debug for CompactionInfoManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompactionInfoManager").finish()
    }
}

impl Default for CompactionInfoManager {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct VnodeCompactionInfo(pub Arc<RwLock<VnodeCompactionInfoInner>>);

impl VnodeCompactionInfo {
    fn new(vnode_id: TseriesFamilyId) -> Self {
        Self(Arc::new(RwLock::new(VnodeCompactionInfoInner {
            vnode_id,
            start_time: Instant::now(),
            state: VnodeCompactionState::Starting,
        })))
    }

    async fn change_state(&self, state: VnodeCompactionState) {
        self.inner_write().await.state = state;
    }

    fn inner(&self) -> Arc<RwLock<VnodeCompactionInfoInner>> {
        self.0.clone()
    }

    async fn inner_read(&self) -> tokio::sync::RwLockReadGuard<'_, VnodeCompactionInfoInner> {
        self.0.read().await
    }

    async fn inner_write(&self) -> tokio::sync::RwLockWriteGuard<'_, VnodeCompactionInfoInner> {
        self.0.write().await
    }

    async fn inner_owned(&self) -> VnodeCompactionInfoInner {
        self.0.read().await.clone()
    }
}

#[derive(Debug, Clone)]
pub struct VnodeCompactionInfoInner {
    vnode_id: TseriesFamilyId,
    start_time: Instant,
    state: VnodeCompactionState,
}

impl VnodeCompactionInfoInner {
    fn vnode_id(&self) -> TseriesFamilyId {
        self.vnode_id
    }

    fn start_time(&self) -> &Instant {
        &self.start_time
    }

    fn state(&self) -> VnodeCompactionState {
        self.state
    }
}

impl std::fmt::Display for VnodeCompactionInfoInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Started {} seconds, state: {}",
            self.start_time.elapsed().as_secs(),
            self.state
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum VnodeCompactionState {
    Starting,
    PickingFiles,
    Running,
    WritingSummary,
    Finishing,
}

impl VnodeCompactionState {
    pub fn as_str(&self) -> &str {
        match self {
            VnodeCompactionState::Starting => "Starting",
            VnodeCompactionState::PickingFiles => "PickingFiles",
            VnodeCompactionState::Running => "Running",
            VnodeCompactionState::WritingSummary => "WritingSummary",
            VnodeCompactionState::Finishing => "Finishing",
        }
    }
}

impl std::fmt::Display for VnodeCompactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod test {
    use crate::compaction::job::{CompactTaskGroup, CompactionInfoManager, VnodeCompactionInfo};
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

    #[tokio::test]
    async fn test_compaction_info() {
        let info = CompactionInfoManager::new();
        {
            let mut vnodes = info.vnodes.write().await;
            vnodes.insert(1, VnodeCompactionInfo::new(1));
            vnodes.insert(1, VnodeCompactionInfo::new(2));
            vnodes.insert(1, VnodeCompactionInfo::new(3));
        }
        let records = info.to_record_batch().await.unwrap();
        datafusion::arrow::util::pretty::print_batches(&[records]).unwrap();
    }
}
