use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use memory_pool::{MemoryPool, MemoryPoolRef};
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::meta_data::VnodeId;
use models::predicate::domain::ColumnDomains;
use models::schema::{make_owner, DatabaseSchema};
use models::{SeriesId, SeriesKey};
use tokio::runtime::Runtime;
use tokio::sync::broadcast::{self, Sender as BroadcastSender};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use trace::{debug, error, info, warn};

use crate::compaction::job::{CompactJob, FlushJob};
use crate::compaction::{
    self, check, run_flush_memtable_job, CompactTask, LevelCompactionPicker, Picker,
};
use crate::database::Database;
use crate::error::Result;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::file_system::FileSystem;
use crate::kv_option::{Options, StorageOptions};
use crate::summary::{Summary, SummaryTask};
use crate::tseries_family::{SuperVersion, TseriesFamily};
use crate::version_set::VersionSet;
use crate::vnode_store::VnodeStorage;
use crate::{file_utils, Engine, TsKvContext, TseriesFamilyId};

// TODO: A small summay channel capacity can cause a block
pub const COMPACT_REQ_CHANNEL_CAP: usize = 1024;
pub const SUMMARY_REQ_CHANNEL_CAP: usize = 1024;

#[derive(Debug)]
pub struct TsKv {
    ctx: Arc<TsKvContext>,
    meta_manager: MetaRef,
    flush_job: FlushJob,
    compact_job: CompactJob,
    runtime: Arc<Runtime>,
    metrics: Arc<MetricsRegister>,
    memory_pool: Arc<dyn MemoryPool>,
    close_sender: BroadcastSender<Sender<()>>,
}

impl TsKv {
    pub async fn open(
        meta_manager: MetaRef,
        options: Options,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        metrics: Arc<MetricsRegister>,
    ) -> Result<TsKv> {
        let flush_channel_cap = options.storage.flush_req_channel_cap;
        let (flush_task_sender, flush_task_receiver) = mpsc::channel(flush_channel_cap);
        let (compact_task_sender, compact_task_receiver) = mpsc::channel(COMPACT_REQ_CHANNEL_CAP);
        let (summary_task_sender, summary_task_receiver) = mpsc::channel(SUMMARY_REQ_CHANNEL_CAP);

        let shared_options = Arc::new(options);
        let (version_set, summary) = Self::recover_summary(
            runtime.clone(),
            memory_pool.clone(),
            meta_manager.clone(),
            shared_options.clone(),
            compact_task_sender.clone(),
            metrics.clone(),
        )
        .await;

        let ctx = Arc::new(TsKvContext {
            version_set,
            flush_task_sender,
            compact_task_sender,
            summary_task_sender,
            options: shared_options.clone(),
            global_ctx: summary.global_context(),
        });

        let (close_sender, _close_receiver) = broadcast::channel(1);
        let compact_job = CompactJob::new(runtime.clone(), ctx.clone());
        let flush_job = FlushJob::new(runtime.clone(), ctx.clone());
        let core = Self {
            ctx,
            meta_manager,
            memory_pool,
            compact_job,
            flush_job,
            close_sender,
            metrics,
            runtime,
        };

        core.run_summary_job(summary, summary_task_receiver);
        core.compact_job
            .start_merge_compact_task_job(compact_task_receiver)
            .await;
        core.compact_job.start_vnode_compaction_job().await;
        core.flush_job.start_vnode_flush_job(flush_task_receiver);
        Ok(core)
    }

    pub fn context(&self) -> Arc<TsKvContext> {
        self.ctx.clone()
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_summary(
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        meta: MetaRef,
        opt: Arc<Options>,
        compact_task_sender: Sender<CompactTask>,
        metrics: Arc<MetricsRegister>,
    ) -> (Arc<RwLock<VersionSet>>, Summary) {
        let summary_dir = opt.storage.summary_dir();
        LocalFileSystem::create_dir_if_not_exists(Some(&summary_dir)).unwrap();

        let summary_file = file_utils::make_summary_file(&summary_dir, 0);
        let summary = if LocalFileSystem::try_exists(&summary_file) {
            Summary::recover(
                meta,
                opt,
                runtime,
                memory_pool,
                compact_task_sender,
                true,
                metrics.clone(),
            )
            .await
            .unwrap()
        } else {
            Summary::new(opt, runtime, meta, memory_pool, metrics)
                .await
                .unwrap()
        };
        let version_set = summary.version_set();

        (version_set, summary)
    }

    fn run_summary_job(
        &self,
        mut summary: Summary,
        mut summary_task_receiver: Receiver<SummaryTask>,
    ) {
        self.runtime.spawn(async move {
            let mut roll_file_time_stamp = models::utils::now_timestamp_secs();

            while let Some(task) = summary_task_receiver.recv().await {
                info!("Apply Summary task: {:?}", task.request.version_edit);
                let result = summary.apply_version_edit(&task.request).await;
                if let Err(err) = task.call_back.send(result) {
                    trace::info!("Response apply version edit failed: {:?}", err);
                }

                // Try to rolling summary file every 10mins
                if models::utils::now_timestamp_secs() - roll_file_time_stamp > 10 * 60 {
                    if let Err(err) = summary.roll_summary_file().await {
                        error!("roll summary file failed: {:?}", err);
                    } else {
                        roll_file_time_stamp = models::utils::now_timestamp_secs();
                    }
                }
            }
        });

        info!("Summary task handler started");
    }

    pub async fn get_db(&self, tenant: &str, database: &str) -> Option<Arc<RwLock<Database>>> {
        self.ctx.version_set.read().await.get_db(tenant, database)
    }

    pub(crate) async fn get_db_or_else_create(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> Result<Arc<RwLock<Database>>> {
        if let Some(db) = self.ctx.version_set.read().await.get_db(tenant, db_name) {
            return Ok(db);
        }

        let db = self
            .ctx
            .version_set
            .write()
            .await
            .create_db(DatabaseSchema::new(tenant, db_name))
            .await?;
        Ok(db)
    }

    pub(crate) async fn get_tsfamily_or_else_create(
        &self,
        id: TseriesFamilyId,
        db: Arc<RwLock<Database>>,
    ) -> Result<Arc<RwLock<TseriesFamily>>> {
        let mut db = db.write().await;
        if let Some(tf) = db.get_tsfamily(id) {
            return Ok(tf);
        }

        db.create_tsfamily(id, self.ctx.clone()).await
    }
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn open_tsfamily(
        &self,
        tenant: &str,
        db_name: &str,
        vnode_id: VnodeId,
    ) -> Result<VnodeStorage> {
        let db = self.get_db_or_else_create(tenant, db_name).await?;

        let ts_index = db.write().await.get_ts_index_or_add(vnode_id).await?;
        let ts_family = self
            .get_tsfamily_or_else_create(vnode_id, db.clone())
            .await?;

        Ok(VnodeStorage {
            db,
            ts_index,
            ts_family,
            id: vnode_id,
            ctx: self.ctx.clone(),
        })
    }

    async fn remove_tsfamily(&self, tenant: &str, database: &str, vnode_id: VnodeId) -> Result<()> {
        if let Some(db) = self.ctx.version_set.read().await.get_db(tenant, database) {
            let mut db_wlock = db.write().await;
            db_wlock.del_ts_index(vnode_id);
            db_wlock
                .del_tsfamily(vnode_id, self.ctx.summary_task_sender.clone())
                .await;

            let ts_dir = self
                .ctx
                .options
                .storage
                .ts_family_dir(&make_owner(tenant, database), vnode_id);
            match std::fs::remove_dir_all(&ts_dir) {
                Ok(()) => {
                    info!("Removed TsFamily directory '{}'", ts_dir.display());
                }
                Err(e) => {
                    error!(
                        "Failed to remove TsFamily directory '{}': {}",
                        ts_dir.display(),
                        e
                    );
                }
            }
        }

        Ok(())
    }

    async fn flush_tsfamily(&self, tenant: &str, database: &str, vnode_id: VnodeId) -> Result<()> {
        if let Some(db) = self.ctx.version_set.read().await.get_db(tenant, database) {
            if let Some(tsfamily) = db.read().await.get_tsfamily(vnode_id) {
                let request = {
                    let mut tsfamily = tsfamily.write().await;
                    tsfamily.switch_to_immutable();
                    tsfamily.build_flush_req(true)
                };

                if let Some(req) = request {
                    // Run flush job and trigger compaction.
                    run_flush_memtable_job(req, self.ctx.clone(), true).await?;
                }
            }

            if let Some(ts_index) = db.read().await.get_ts_index(vnode_id) {
                let _ = ts_index.flush().await;
            }
        }

        Ok(())
    }

    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        database: &str,
        tab: &str,
        vnode_id: VnodeId,
        filter: &ColumnDomains<String>,
    ) -> Result<Vec<SeriesId>> {
        let (schema, ts_index) = match self.ctx.version_set.read().await.get_db(tenant, database) {
            Some(db) => {
                let db = db.read().await;
                let schema = match db.get_table_schema(tab).await? {
                    None => return Ok(vec![]),
                    Some(schema) => schema,
                };
                let ts_index = match db.get_ts_index(vnode_id) {
                    Some(ts_index) => ts_index,
                    None => return Ok(vec![]),
                };
                (schema, ts_index)
            }
            None => return Ok(vec![]),
        };

        let res = ts_index.get_series_ids_by_domains(&schema, filter).await?;

        Ok(res)
    }

    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        _table: &str,
        vnode_id: VnodeId,
        series_id: &[SeriesId],
    ) -> Result<Vec<SeriesKey>> {
        if let Some(db) = self.ctx.version_set.read().await.get_db(tenant, database) {
            Ok(db.read().await.get_series_key(vnode_id, series_id).await?)
        } else {
            Ok(vec![])
        }
    }

    async fn get_db_version(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> Result<Option<Arc<SuperVersion>>> {
        let version_set = self.ctx.version_set.read().await;
        // Comment it, It's not a error, Maybe the data not right!
        // if !version_set.db_exists(tenant, database) {
        //     return Err(SchemaError::DatabaseNotFound {
        //         database: database.to_string(),
        //     }
        //     .into());
        // }
        if let Some(tsf) = version_set
            .get_tsfamily_by_name_id(tenant, database, vnode_id)
            .await
        {
            Ok(Some(tsf.read().await.super_version()))
        } else {
            debug!(
                "ts_family {} with db name '{}' not found.",
                vnode_id, database
            );
            Ok(None)
        }
    }

    fn get_storage_options(&self) -> Arc<StorageOptions> {
        self.ctx.options.storage.clone()
    }

    async fn compact(&self, vnode_ids: Vec<TseriesFamilyId>) -> Result<()> {
        for vnode_id in vnode_ids {
            if let Some(ts_family) = self
                .ctx
                .version_set
                .read()
                .await
                .get_tsfamily_by_tf_id(vnode_id)
                .await
            {
                // TODO: stop current and prevent next flush and compaction.
                if !ts_family.read().await.can_compaction() {
                    warn!("forbidden compaction on moving vnode {}", vnode_id);
                    return Ok(());
                }
                let mut tsf_wlock = ts_family.write().await;
                tsf_wlock.switch_to_immutable();
                let flush_req = tsf_wlock.build_flush_req(true);
                drop(tsf_wlock);
                if let Some(req) = flush_req {
                    // Run flush job but do not trigger compaction.
                    if let Err(e) = run_flush_memtable_job(req, self.ctx.clone(), false).await {
                        error!("Failed to flush vnode {}: {:?}", vnode_id, e);
                    }
                }

                let picker = LevelCompactionPicker::new(self.ctx.options.storage.clone());
                let version = ts_family.read().await.version();
                if let Some(req) = picker.pick_compaction(version) {
                    match compaction::run_compaction_job(req, self.ctx.global_ctx.clone()).await {
                        Ok(Some((version_edit, file_metas))) => {
                            let (summary_tx, _summary_rx) = oneshot::channel();
                            let _ = self
                                .ctx
                                .summary_task_sender
                                .send(SummaryTask::new(
                                    ts_family.clone(),
                                    version_edit,
                                    Some(file_metas),
                                    None,
                                    summary_tx,
                                ))
                                .await;

                            // let _ = summary_rx.await;
                        }
                        Ok(None) => {
                            info!("There is nothing to compact.");
                        }
                        Err(e) => {
                            error!("Compaction job failed: {:?}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn get_vnode_hash_tree(&self, vnode_id: VnodeId) -> Result<RecordBatch> {
        for database in self.ctx.version_set.read().await.get_all_db().values() {
            let db = database.read().await;
            if let Some(vnode) = db.ts_families().get(&vnode_id).cloned() {
                drop(db);
                let request = {
                    let mut tsfamily = vnode.write().await;
                    tsfamily.switch_to_immutable();
                    tsfamily.build_flush_req(true)
                };

                if let Some(req) = request {
                    // Run flush job but do not trigger compaction.
                    run_flush_memtable_job(req, self.ctx.clone(), false).await?;
                }
                return check::vnode_checksum(vnode).await;
            }
        }

        Ok(RecordBatch::new_empty(check::vnode_table_checksum_schema()))
    }

    async fn close(&self) {
        let (tx, mut rx) = mpsc::channel(1);
        if let Err(e) = self.close_sender.send(tx) {
            error!("Failed to broadcast close signal: {:?}", e);
        }
        while let Some(_x) = rx.recv().await {
            continue;
        }
        info!("TsKv closed");
    }
}

#[cfg(test)]
impl TsKv {
    pub(crate) fn global_ctx(&self) -> Arc<crate::context::GlobalContext> {
        self.ctx.global_ctx.clone()
    }

    pub(crate) fn version_set(&self) -> Arc<RwLock<VersionSet>> {
        self.ctx.version_set.clone()
    }

    pub(crate) fn summary_task_sender(&self) -> Sender<SummaryTask> {
        self.ctx.summary_task_sender.clone()
    }

    pub(crate) fn flush_task_sender(&self) -> Sender<compaction::FlushReq> {
        self.ctx.flush_task_sender.clone()
    }

    pub(crate) fn compact_task_sender(&self) -> Sender<CompactTask> {
        self.ctx.compact_task_sender.clone()
    }
}
