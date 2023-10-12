use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::panic;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::record_batch::RecordBatch;
use memory_pool::{MemoryPool, MemoryPoolRef};
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::codec::Encoding;
use models::meta_data::{VnodeId, VnodeStatus};
use models::predicate::domain::{ColumnDomains, ResolvedPredicate, TimeRange};
use models::schema::{make_owner, split_owner, DatabaseSchema, Precision, TableColumn};
use models::utils::unite_id;
use models::{ColumnId, SeriesId, SeriesKey, TagKey, TagValue, Timestamp};
use protos::kv_service::{WritePointsRequest, WritePointsResponse};
use protos::models as fb_models;
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::broadcast::{self, Sender as BroadcastSender};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use trace::{debug, error, info, warn, SpanContext, SpanExt, SpanRecorder};

use crate::compaction::{
    self, check, run_flush_memtable_job, CompactTask, FlushReq, LevelCompactionPicker, Picker,
};
use crate::context::GlobalContext;
use crate::database::Database;
use crate::error::{self, Result};
use crate::file_system::file_manager;
use crate::index::ts_index;
use crate::kv_option::{Options, StorageOptions};
use crate::schema::error::SchemaError;
use crate::summary::{CompactMeta, Summary, SummaryProcessor, SummaryTask, VersionEdit};
use crate::tseries_family::{SuperVersion, TseriesFamily};
use crate::tsm::codec::get_str_codec;
use crate::version_set::VersionSet;
use crate::wal::{self, Block, UpdateSeriesKeysBlock, WalDecoder, WalManager, WalTask};
use crate::{
    file_utils, tenant_name_from_request, Engine, Error, SnapshotFileMeta, TseriesFamilyId,
    UpdateSetValue, VnodeSnapshot,
};

// TODO: A small summay channel capacity can cause a block
pub const COMPACT_REQ_CHANNEL_CAP: usize = 1024;
pub const SUMMARY_REQ_CHANNEL_CAP: usize = 1024;

#[derive(Debug)]
pub struct TsKv {
    options: Arc<Options>,
    global_ctx: Arc<GlobalContext>,
    version_set: Arc<RwLock<VersionSet>>,
    meta_manager: MetaRef,

    runtime: Arc<Runtime>,
    memory_pool: Arc<dyn MemoryPool>,
    wal_sender: Sender<WalTask>,
    flush_task_sender: Sender<FlushReq>,
    compact_task_sender: Sender<CompactTask>,
    summary_task_sender: Sender<SummaryTask>,
    close_sender: BroadcastSender<Sender<()>>,
    metrics: Arc<MetricsRegister>,
}

impl TsKv {
    pub async fn open(
        meta_manager: MetaRef,
        opt: Options,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        metrics: Arc<MetricsRegister>,
    ) -> Result<TsKv> {
        let shared_options = Arc::new(opt);
        let (flush_task_sender, flush_task_receiver) =
            mpsc::channel::<FlushReq>(shared_options.storage.flush_req_channel_cap);
        let (compact_task_sender, compact_task_receiver) =
            mpsc::channel::<CompactTask>(COMPACT_REQ_CHANNEL_CAP);
        let (wal_sender, wal_receiver) =
            mpsc::channel::<WalTask>(shared_options.wal.wal_req_channel_cap);
        let (summary_task_sender, summary_task_receiver) =
            mpsc::channel::<SummaryTask>(SUMMARY_REQ_CHANNEL_CAP);
        let (close_sender, _close_receiver) = broadcast::channel(1);
        let (version_set, summary) = Self::recover_summary(
            runtime.clone(),
            memory_pool.clone(),
            meta_manager.clone(),
            shared_options.clone(),
            flush_task_sender.clone(),
            compact_task_sender.clone(),
            metrics.clone(),
        )
        .await;

        let core = Self {
            options: shared_options.clone(),
            global_ctx: summary.global_context(),
            version_set,
            meta_manager,
            runtime: runtime.clone(),
            memory_pool,
            wal_sender,
            flush_task_sender: flush_task_sender.clone(),
            compact_task_sender: compact_task_sender.clone(),
            summary_task_sender: summary_task_sender.clone(),
            close_sender,
            metrics,
        };

        let wal_manager = core.recover_wal().await;
        core.run_wal_job(wal_manager, wal_receiver);
        core.run_flush_job(
            flush_task_receiver,
            summary.global_context(),
            summary.version_set(),
            summary_task_sender.clone(),
            compact_task_sender.clone(),
        );
        compaction::job::run(
            shared_options.storage.clone(),
            runtime,
            compact_task_receiver,
            summary.global_context(),
            summary.version_set(),
            summary_task_sender.clone(),
        );
        core.run_summary_job(summary, summary_task_receiver);
        Ok(core)
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_summary(
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        meta: MetaRef,
        opt: Arc<Options>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
        metrics: Arc<MetricsRegister>,
    ) -> (Arc<RwLock<VersionSet>>, Summary) {
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir)
                .context(error::IOSnafu)
                .unwrap();
        }
        let summary_file = file_utils::make_summary_file(&summary_dir, 0);
        let summary = if file_manager::try_exists(&summary_file) {
            Summary::recover(
                meta,
                opt,
                runtime,
                memory_pool,
                flush_task_sender,
                compact_task_sender,
                true,
                metrics.clone(),
            )
            .await
            .unwrap()
        } else {
            Summary::new(opt, runtime, memory_pool, metrics)
                .await
                .unwrap()
        };
        let version_set = summary.version_set();

        (version_set, summary)
    }

    async fn recover_wal(&self) -> WalManager {
        let wal_manager = WalManager::open(self.options.wal.clone(), self.version_set.clone())
            .await
            .unwrap();

        let vnode_last_seq_map = self
            .version_set
            .read()
            .await
            .get_tsfamily_seq_no_map()
            .await;
        let vnode_wal_readers = wal_manager.recover(&vnode_last_seq_map).await;
        let mut recover_task = vec![];
        for (vnode_id, readers) in vnode_wal_readers {
            let vnode_seq = vnode_last_seq_map.get(&vnode_id).copied().unwrap_or(0);
            let task = async move {
                let mut decoder = WalDecoder::new();
                for mut reader in readers {
                    info!(
                        "Recover: reading wal '{}' for seq {} to {}",
                        reader.path().display(),
                        reader.min_sequence(),
                        reader.max_sequence(),
                    );
                    if reader.is_empty() {
                        continue;
                    }

                    loop {
                        match reader.next_wal_entry().await {
                            Ok(Some(wal_entry_blk)) => {
                                let seq = wal_entry_blk.seq;
                                match wal_entry_blk.block {
                                    Block::Write(blk) => {
                                        let vnode_id = blk.vnode_id();
                                        if vnode_seq >= seq {
                                            // If `seq_no` of TsFamily is greater than or equal to `seq`,
                                            // it means that data was writen to tsm.
                                            continue;
                                        }

                                        let res = self
                                            .write_from_wal(vnode_id, seq, &blk, &mut decoder)
                                            .await;
                                        if matches!(res, Err(Error::InvalidPoint)) {
                                            info!("Recover: deleted point, seq: {}", seq);
                                        } else if let Err(e) = res {
                                            error!("Recover: failed to write: {}", e);
                                        }
                                    }

                                    Block::DeleteVnode(blk) => {
                                        if let Err(e) = self.remove_tsfamily_from_wal(&blk).await {
                                            // Ignore delete vnode error.
                                            trace::error!("Recover: failed to delete vnode: {e}");
                                        }
                                    }
                                    Block::DeleteTable(blk) => {
                                        if let Err(e) =
                                            self.drop_table_from_wal(&blk, vnode_id).await
                                        {
                                            // Ignore delete table error.
                                            trace::error!("Recover: failed to delete table: {e}");
                                        }
                                    }
                                    Block::UpdateSeriesKeys(UpdateSeriesKeysBlock {
                                        tenant,
                                        database,
                                        vnode_id,
                                        old_series_keys,
                                        new_series_keys,
                                        series_ids,
                                    }) => {
                                        if let Err(err) = self
                                            .update_vnode_series_keys(
                                                &tenant,
                                                &database,
                                                vnode_id,
                                                old_series_keys,
                                                new_series_keys,
                                                series_ids,
                                                true,
                                            )
                                            .await
                                        {
                                            trace::error!(
                                                "Recover: failed to update series keys: {err}"
                                            );
                                        }
                                    }
                                    Block::Unknown | Block::RaftLog(_) => {}
                                }
                            }
                            Ok(None) | Err(Error::WalTruncated) => {
                                break;
                            }
                            Err(e) => {
                                panic!(
                                    "Failed to recover from {}: {:?}",
                                    reader.path().display(),
                                    e
                                );
                            }
                        }
                    }
                }
            };
            recover_task.push(task);
        }
        futures::future::join_all(recover_task).await;
        wal_manager
    }

    pub(crate) fn run_wal_job(&self, mut wal_manager: WalManager, mut receiver: Receiver<WalTask>) {
        async fn on_write(wal_manager: &mut WalManager, wal_task: WalTask) -> Result<()> {
            wal_manager.write(wal_task).await
        }

        async fn on_tick_sync(wal_manager: &WalManager) {
            if let Err(e) = wal_manager.sync().await {
                error!("Failed flushing WAL file: {:?}", e);
            }
        }

        async fn on_cancel(wal_manager: &mut WalManager) {
            info!("Job 'WAL' closing.");
            if let Err(e) = wal_manager.close().await {
                error!("Failed to close job 'WAL': {:?}", e);
            }
            info!("Job 'WAL' closed.");
        }

        info!("Job 'WAL' starting.");
        let mut close_receiver = self.close_sender.subscribe();
        self.runtime.spawn(async move {
            info!("Job 'WAL' started.");

            let sync_interval = wal_manager.sync_interval();
            if sync_interval == Duration::ZERO {
                loop {
                    tokio::select! {
                        wal_task = receiver.recv() => {
                            match wal_task {
                                Some(t) => {
                                    if let Err(e) = on_write(&mut wal_manager, t).await {
                                        error!("Failed to write to WAL: {:?}", e);
                                    }
                                },
                                _ => break
                            }
                        }
                        _ = close_receiver.recv() => {
                            on_cancel(&mut wal_manager).await;
                            break;
                        }
                    }
                }
            } else {
                let mut sync_ticker = tokio::time::interval(sync_interval);
                loop {
                    tokio::select! {
                        wal_task = receiver.recv() => {
                            match wal_task {
                                Some(t) => {
                                    if let Err(e) = on_write(&mut wal_manager, t).await {
                                        error!("Failed to write to WAL: {:?}", e);
                                    }
                                },
                                _ => break
                            }
                        }
                        _ = sync_ticker.tick() => {
                            on_tick_sync(&wal_manager).await;
                        }
                        _ = close_receiver.recv() => {
                            on_cancel(&mut wal_manager).await;
                            break;
                        }
                    }
                }
            }
        });
    }

    fn run_flush_job(
        &self,
        mut receiver: Receiver<FlushReq>,
        ctx: Arc<GlobalContext>,
        version_set: Arc<RwLock<VersionSet>>,
        summary_task_sender: Sender<SummaryTask>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let runtime = self.runtime.clone();
        let f = async move {
            while let Some(x) = receiver.recv().await {
                // TODO(zipper): this make config `flush_req_channel_cap` wasted
                // Run flush job and trigger compaction.
                runtime.spawn(run_flush_memtable_job(
                    x,
                    ctx.clone(),
                    version_set.clone(),
                    summary_task_sender.clone(),
                    Some(compact_task_sender.clone()),
                ));
            }
        };
        self.runtime.spawn(f);
        info!("Flush task handler started");
    }

    fn run_summary_job(&self, summary: Summary, mut summary_task_receiver: Receiver<SummaryTask>) {
        let f = async move {
            let mut summary_processor = SummaryProcessor::new(Box::new(summary));
            while let Some(x) = summary_task_receiver.recv().await {
                debug!("Apply Summary task");
                summary_processor.batch(x);
                summary_processor.apply().await;
            }
        };
        self.runtime.spawn(f);
        info!("Summary task handler started");
    }

    pub async fn get_db(&self, tenant: &str, database: &str) -> Option<Arc<RwLock<Database>>> {
        self.version_set.read().await.get_db(tenant, database)
    }

    pub(crate) async fn get_db_or_else_create(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> Result<Arc<RwLock<Database>>> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, db_name) {
            return Ok(db);
        }

        let db = self
            .version_set
            .write()
            .await
            .create_db(
                DatabaseSchema::new(tenant, db_name),
                self.meta_manager.clone(),
                self.memory_pool.clone(),
            )
            .await?;
        Ok(db)
    }

    pub(crate) async fn get_ts_index_or_else_create(
        &self,
        db: Arc<RwLock<Database>>,
        id: TseriesFamilyId,
    ) -> Result<Arc<ts_index::TSIndex>> {
        let opt_index = db.read().await.get_ts_index(id);
        match opt_index {
            Some(v) => Ok(v),
            None => db.write().await.get_ts_index_or_add(id).await,
        }
    }

    pub(crate) async fn get_tsfamily_or_else_create(
        &self,
        id: TseriesFamilyId,
        ve: Option<VersionEdit>,
        db: Arc<RwLock<Database>>,
    ) -> Result<Arc<RwLock<TseriesFamily>>> {
        let opt_tsf = db.read().await.get_tsfamily(id);
        match opt_tsf {
            Some(v) => Ok(v),
            None => {
                db.write()
                    .await
                    .add_tsfamily(
                        id,
                        ve,
                        self.summary_task_sender.clone(),
                        self.flush_task_sender.clone(),
                        self.compact_task_sender.clone(),
                        self.global_ctx.clone(),
                    )
                    .await
            }
        }
    }

    async fn delete_table(
        &self,
        database: Arc<RwLock<Database>>,
        table: &str,
        vnode_id: Option<VnodeId>,
    ) -> Result<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.
        let db_rlock = database.read().await;
        let db_owner = db_rlock.owner();

        let schemas = db_rlock.get_schemas();
        if let Some(fields) = schemas.get_table_schema(table)? {
            let column_ids: Vec<ColumnId> = fields.columns().iter().map(|f| f.id).collect();
            info!(
                "Drop table: deleting {} columns in table: {db_owner}.{table}",
                column_ids.len()
            );

            let time_range = &TimeRange {
                min_ts: Timestamp::MIN,
                max_ts: Timestamp::MAX,
            };
            if let Some(vnode_id) = vnode_id {
                if let Some(ts_family) = db_rlock.ts_families().get(&vnode_id) {
                    self.tsf_delete_table(
                        &db_rlock,
                        vnode_id,
                        ts_family.clone(),
                        table,
                        time_range,
                        &column_ids,
                    )
                    .await?;
                }
            } else {
                for (ts_family_id, ts_family) in database.read().await.ts_families().iter() {
                    // TODO: Concurrent delete on ts_family.
                    // TODO: Limit parallel delete to 1.
                    self.tsf_delete_table(
                        &db_rlock,
                        *ts_family_id,
                        ts_family.clone(),
                        table,
                        time_range,
                        &column_ids,
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    async fn tsf_delete_table<Db>(
        &self,
        db: &Db,
        ts_family_id: TseriesFamilyId,
        ts_family: Arc<RwLock<TseriesFamily>>,
        table: &str,
        time_range: &TimeRange,
        column_ids: &[ColumnId],
    ) -> Result<()>
    where
        Db: Deref<Target = Database>,
    {
        let db_owner = db.owner();
        if let Some(ts_index) = db.get_ts_index(ts_family_id) {
            let series_ids = ts_index.get_series_id_list(table, &[]).await?;
            ts_family
                .write()
                .await
                .delete_series(&series_ids, time_range);

            let field_ids: Vec<u64> = series_ids
                .iter()
                .flat_map(|sid| column_ids.iter().map(|fid| unite_id(*fid, *sid)))
                .collect();
            info!(
                "Drop table: vnode {ts_family_id} deleting {} fields in table: {db_owner}.{table}",
                field_ids.len()
            );

            let version = ts_family.read().await.super_version();
            for column_file in version.version.column_files(&field_ids, time_range) {
                column_file.add_tombstone(&field_ids, time_range).await?;
            }

            info!(
                "Drop table: index {ts_family_id} deleting {} fields in table: {db_owner}.{table}",
                series_ids.len()
            );

            for sid in series_ids {
                ts_index.del_series_info(sid).await?;
            }
        }
        Ok(())
    }

    async fn drop_columns(
        &self,
        database: Arc<RwLock<Database>>,
        table: &str,
        column_ids: &[ColumnId],
    ) -> Result<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.
        let db_rlock = database.read().await;
        let db_owner = db_rlock.owner();

        let schemas = db_rlock.get_schemas();
        if let Some(fields) = schemas.get_table_schema(table)? {
            let table_column_ids: HashSet<ColumnId> =
                fields.columns().iter().map(|f| f.id).collect();
            let mut to_drop_column_ids = Vec::with_capacity(column_ids.len());
            for cid in column_ids {
                if table_column_ids.contains(cid) {
                    to_drop_column_ids.push(*cid);
                }
            }

            let time_range = &TimeRange {
                min_ts: Timestamp::MIN,
                max_ts: Timestamp::MAX,
            };
            for (ts_family_id, ts_family) in database.read().await.ts_families().iter() {
                // TODO: Concurrent delete on ts_family.
                // TODO: Limit parallel delete to 1.
                if let Some(ts_index) = db_rlock.get_ts_index(*ts_family_id) {
                    let series_ids = ts_index.get_series_id_list(table, &[]).await?;
                    let field_ids: Vec<u64> = series_ids
                        .iter()
                        .flat_map(|sid| to_drop_column_ids.iter().map(|fid| unite_id(*fid, *sid)))
                        .collect();
                    info!(
                        "Drop table: vnode {ts_family_id} deleting {} fields in table: {db_owner}.{table}", field_ids.len()
                    );

                    ts_family.write().await.drop_columns(&field_ids);

                    let version = ts_family.read().await.super_version();
                    for column_file in version.version.column_files(&field_ids, time_range) {
                        column_file.add_tombstone(&field_ids, time_range).await?;
                    }
                } else {
                    continue;
                }
            }
        }

        Ok(())
    }

    async fn write_wal(
        &self,
        vnode_id: VnodeId,
        tenant: String,
        db_name: String,
        precision: Precision,
        points: Vec<u8>,
    ) -> Result<u64> {
        if !self.options.wal.enabled {
            return Ok(0);
        }

        let mut enc_points = Vec::with_capacity(points.len() / 2);
        get_str_codec(Encoding::Zstd)
            .encode(&[&points], &mut enc_points)
            .with_context(|_| error::EncodeSnafu)?;
        drop(points);

        let (wal_task, rx) = WalTask::new_write(tenant, db_name, vnode_id, precision, enc_points);
        self.wal_sender
            .send(wal_task)
            .await
            .map_err(|_| Error::ChannelSend {
                source: error::ChannelSendError::WalTask,
            })?;
        let (seq, _size) = rx.await.map_err(|e| Error::ChannelReceive {
            source: error::ChannelReceiveError::WriteWalResult { source: e },
        })??;

        Ok(seq)
    }

    /// Tskv write the gRPC message `WritePointsRequest`(which contains
    /// the tenant, user, database, some tables, and each table has some rows)
    /// that from a WAL into a storage unit managed by engine.
    ///
    /// Data is from the WAL(write-ahead-log), so won't write back to WAL, and
    /// would not create any schema, if database of vnode does not exist, record
    /// will be ignored.
    async fn write_from_wal(
        &self,
        vnode_id: TseriesFamilyId,
        seq: u64,
        block: &wal::WriteBlock,
        block_decoder: &mut WalDecoder,
    ) -> Result<()> {
        let tenant = {
            let tenant = block.tenant_utf8()?;
            if tenant.is_empty() {
                models::schema::DEFAULT_CATALOG
            } else {
                tenant
            }
        };
        let precision = block.precision();
        let points = match block_decoder.decode(block.points())? {
            Some(p) => p,
            None => return Ok(()),
        };
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = fb_points.db_ext()?;
        // If database does not exist, skip this record.
        let db = match self.get_db(tenant, db_name).await {
            Some(db) => db,
            None => return Ok(()),
        };
        // If vnode does not exist, skip this record.
        let tsf = match db.read().await.get_tsfamily(vnode_id) {
            Some(tsf) => tsf,
            None => return Ok(()),
        };

        let ts_index = self
            .get_ts_index_or_else_create(db.clone(), vnode_id)
            .await?;

        // Write data assuming schemas were created (strict mode).
        let write_group = db
            .read()
            .await
            .build_write_group_strict_mode(
                precision,
                fb_points.tables().ok_or(Error::CommonError {
                    reason: "points missing table".to_string(),
                })?,
                ts_index,
            )
            .await?;
        tsf.read().await.put_points(seq, write_group)?;

        Ok(())
    }

    /// Delete all data of a table.
    ///
    /// Data is from the WAL(write-ahead-log), so won't write back to WAL.
    async fn drop_table_from_wal(
        &self,
        block: &wal::DeleteTableBlock,
        vnode_id: VnodeId,
    ) -> Result<()> {
        let tenant = block.tenant_utf8()?;
        let database = block.database_utf8()?;
        let table = block.table_utf8()?;
        info!(
            "Recover: delete table, tenant: {}, database: {}, table: {}",
            &tenant, &database, &table
        );
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            return self.delete_table(db, table, Some(vnode_id)).await;
        }
        Ok(())
    }

    async fn update_vnode_series_keys(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
        old_series_keys: Vec<SeriesKey>,
        new_series_keys: Vec<SeriesKey>,
        sids: Vec<SeriesId>,
        recovering: bool,
    ) -> Result<()> {
        let db = match self.get_db(tenant, database).await {
            Some(db) => db,
            None => return Ok(()),
        };
        if let Some(ts_index) = db.read().await.get_ts_index(vnode_id) {
            // 更新索引
            ts_index
                .update_series_key(old_series_keys, new_series_keys, sids, recovering)
                .await
                .map_err(|err| {
                    error!(
                        "Update tags value tag of TSIndex({}): {}",
                        ts_index.path().display(),
                        err
                    );
                    err
                })?;
        }

        Ok(())
    }

    /// Remove the storage unit(caches and files) managed by TsKv,
    /// then remove directory of the storage unit.
    ///
    /// Data is from the WAL(write-ahead-log), so won't write back to WAL.
    async fn remove_tsfamily_from_wal(&self, block: &wal::DeleteVnodeBlock) -> Result<()> {
        let vnode_id = block.vnode_id();
        let tenant = block.tenant_utf8()?;
        let database = block.database_utf8()?;
        info!(
            "Recover: delete vnode, tenant: {}, database: {}, vnode_id: {vnode_id}",
            &tenant, &database
        );

        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            let mut db_wlock = db.write().await;
            db_wlock.del_ts_index(vnode_id);
            db_wlock
                .del_tsfamily(vnode_id, self.summary_task_sender.clone())
                .await;

            let ts_dir = self
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
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn write(
        &self,
        span_ctx: Option<&SpanContext>,
        vnode_id: TseriesFamilyId,
        precision: Precision,
        write_batch: WritePointsRequest,
    ) -> Result<WritePointsResponse> {
        let span_recorder = SpanRecorder::new(span_ctx.child_span("tskv engine write"));

        let tenant = tenant_name_from_request(&write_batch);
        let points = write_batch.points;
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = fb_points.db_ext()?;
        let db = self.get_db_or_else_create(&tenant, db_name).await?;
        let ts_index = self
            .get_ts_index_or_else_create(db.clone(), vnode_id)
            .await?;

        let tables = fb_points.tables().ok_or(Error::CommonError {
            reason: "points missing table".to_string(),
        })?;

        let write_group = {
            let mut span_recorder = span_recorder.child("build write group");
            db.read()
                .await
                .build_write_group(db_name, precision, tables, ts_index)
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        let tsf = self
            .get_tsfamily_or_else_create(vnode_id, None, db.clone())
            .await?;

        let seq = {
            let mut span_recorder = span_recorder.child("write wal");
            self.write_wal(vnode_id, tenant, db_name.to_string(), precision, points)
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        let res = {
            let mut span_recorder = span_recorder.child("put points");
            match tsf.read().await.put_points(seq, write_group) {
                Ok(points_number) => Ok(WritePointsResponse { points_number }),
                Err(err) => {
                    span_recorder.error(err.to_string());
                    Err(err)
                }
            }
        };
        tsf.write().await.check_to_flush().await;
        res
    }

    async fn write_memcache(
        &self,
        index: u64,
        tenant: &str,
        points: Vec<u8>,
        vnode_id: VnodeId,
        precision: Precision,
        span_ctx: Option<&SpanContext>,
    ) -> Result<WritePointsResponse> {
        let span_recorder = SpanRecorder::new(span_ctx.child_span("tskv engine write cache"));

        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = fb_points.db_ext()?;
        let db = self.get_db_or_else_create(tenant, db_name).await?;
        let ts_index = self
            .get_ts_index_or_else_create(db.clone(), vnode_id)
            .await?;

        let tables = fb_points.tables().ok_or(Error::CommonError {
            reason: "points missing table".to_string(),
        })?;

        let write_group = {
            let mut span_recorder = span_recorder.child("build write group");
            db.read()
                .await
                .build_write_group(db_name, precision, tables, ts_index)
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        let tsf = self
            .get_tsfamily_or_else_create(vnode_id, None, db.clone())
            .await?;

        let res = {
            let mut span_recorder = span_recorder.child("put points");
            match tsf.read().await.put_points(index, write_group) {
                Ok(points_number) => Ok(WritePointsResponse { points_number }),
                Err(err) => {
                    span_recorder.error(err.to_string());
                    Err(err)
                }
            }
        };
        tsf.write().await.check_to_flush().await;
        res
    }

    async fn drop_database(&self, tenant: &str, database: &str) -> Result<()> {
        if let Some(db) = self.version_set.write().await.delete_db(tenant, database) {
            let mut db_wlock = db.write().await;
            let ts_family_ids: Vec<TseriesFamilyId> = db_wlock
                .ts_families()
                .iter()
                .map(|(tsf_id, _tsf)| *tsf_id)
                .collect();
            for ts_family_id in ts_family_ids {
                db_wlock.del_ts_index(ts_family_id);
                db_wlock
                    .del_tsfamily(ts_family_id, self.summary_task_sender.clone())
                    .await;
            }
        }

        let db_dir = self
            .options
            .storage
            .database_dir(&make_owner(tenant, database));
        if let Err(e) = std::fs::remove_dir_all(&db_dir) {
            error!("Failed to remove dir '{}', e: {}", db_dir.display(), e);
        }

        Ok(())
    }

    async fn drop_table(&self, tenant: &str, database: &str, table: &str) -> Result<()> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            let (wal_task, rx) = WalTask::new_delete_table(
                tenant.to_string(),
                database.to_string(),
                table.to_string(),
            );
            self.wal_sender
                .send(wal_task)
                .await
                .map_err(|_| Error::ChannelSend {
                    source: error::ChannelSendError::WalTask,
                })?;
            // Receive WAL write action result.
            let _ = rx.await.map_err(|e| Error::ChannelReceive {
                source: error::ChannelReceiveError::WriteWalResult { source: e },
            })??;

            return self.delete_table(db, table, None).await;
        }

        Ok(())
    }

    async fn remove_tsfamily(&self, tenant: &str, database: &str, vnode_id: VnodeId) -> Result<()> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            // Store this action in WAL.
            let (wal_task, rx) =
                WalTask::new_delete_vnode(tenant.to_string(), database.to_string(), vnode_id);
            self.wal_sender
                .send(wal_task)
                .await
                .map_err(|_| Error::ChannelSend {
                    source: error::ChannelSendError::WalTask,
                })?;
            // Receive WAL write action result.
            let _ = rx.await.map_err(|e| Error::ChannelReceive {
                source: error::ChannelReceiveError::WriteWalResult { source: e },
            })??;

            let mut db_wlock = db.write().await;
            db_wlock.del_ts_index(vnode_id);
            db_wlock
                .del_tsfamily(vnode_id, self.summary_task_sender.clone())
                .await;

            let ts_dir = self
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

    async fn prepare_copy_vnode(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> Result<()> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            if let Some(tsfamily) = db.read().await.get_tsfamily(vnode_id) {
                tsfamily.write().await.update_status(VnodeStatus::Copying);
            }
        }
        self.flush_tsfamily(tenant, database, vnode_id).await
    }

    async fn flush_tsfamily(&self, tenant: &str, database: &str, vnode_id: VnodeId) -> Result<()> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            if let Some(tsfamily) = db.read().await.get_tsfamily(vnode_id) {
                let request = {
                    let mut tsfamily = tsfamily.write().await;
                    tsfamily.switch_to_immutable();
                    tsfamily.build_flush_req(true)
                };

                if let Some(req) = request {
                    // Run flush job and trigger compaction.
                    run_flush_memtable_job(
                        req,
                        self.global_ctx.clone(),
                        self.version_set.clone(),
                        self.summary_task_sender.clone(),
                        Some(self.compact_task_sender.clone()),
                    )
                    .await?;
                }
            }

            if let Some(ts_index) = db.read().await.get_ts_index(vnode_id) {
                let _ = ts_index.flush().await;
            }
        }

        Ok(())
    }

    async fn add_table_column(
        &self,
        _tenant: &str,
        _database: &str,
        _table: &str,
        _new_column: TableColumn,
    ) -> Result<()> {
        // let db = self.get_db(tenant, database).await?;
        // let db = db.read().await;
        // let sids = db.get_table_sids(table).await?;
        // for (_ts_family_id, ts_family) in db.ts_families().iter() {
        //     ts_family.read().await.add_column(&sids, &new_column);
        // }
        Ok(())
    }

    async fn drop_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_name: &str,
    ) -> Result<()> {
        // TODO(zipper): Store this action in WAL.
        let db = match self.get_db(tenant, database).await {
            Some(db) => db,
            None => return Ok(()),
        };

        let schema =
            db.read()
                .await
                .get_table_schema(table)?
                .ok_or_else(|| SchemaError::TableNotFound {
                    database: database.to_string(),
                    table: table.to_string(),
                })?;
        let column_id = schema
            .column(column_name)
            .ok_or_else(|| SchemaError::FieldNotFound {
                database: database.to_string(),
                table: table.to_string(),
                field: column_name.to_string(),
            })?
            .id;
        self.drop_columns(db, table, &[column_id]).await?;
        Ok(())
    }

    async fn change_table_column(
        &self,
        _tenant: &str,
        _database: &str,
        _table: &str,
        _column_name: &str,
        _new_column: TableColumn,
    ) -> Result<()> {
        // let db = self.get_db(tenant, database).await?;
        // let db = db.read().await;
        // let sids = db.get_table_sids(table).await?;

        // for (_ts_family_id, ts_family) in db.ts_families().iter() {
        //     ts_family
        //         .read()
        //         .await
        //         .change_column(&sids, column_name, &new_column);
        // }
        Ok(())
    }

    async fn rename_tag(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        tag_name: &str,
        new_tag_name: &str,
        dry_run: bool,
    ) -> Result<()> {
        let tag_name = tag_name.as_bytes().to_vec();
        let new_tag_name = new_tag_name.as_bytes().to_vec();

        let db = match self.get_db(tenant, database).await {
            Some(db) => db,
            None => return Ok(()),
        };

        for ts_index in db.read().await.ts_indexes().values() {
            ts_index
                .rename_tag(table, &tag_name, &new_tag_name, dry_run)
                .await
                .map_err(|err| {
                    error!(
                        "Rename tag of TSIndex({}): {}",
                        ts_index.path().display(),
                        err
                    );
                    err
                })?;
        }

        Ok(())
    }

    async fn update_tags_value(
        &self,
        tenant: &str,
        database: &str,
        new_tags: &[UpdateSetValue<TagKey, TagValue>],
        matched_series: &[SeriesKey],
        dry_run: bool,
    ) -> Result<()> {
        let db = match self.get_db(tenant, database).await {
            Some(db) => db,
            None => return Ok(()),
        };

        for (vnode_id, ts_index) in db.read().await.ts_indexes() {
            // 准备数据
            // 获取待更新的 series key，更新后的 series key 及其对应的 series id
            let (old_series_keys, new_series_keys, sids) = ts_index
                .prepare_update_tags_value(new_tags, matched_series)
                .await?;

            if dry_run {
                continue;
            }
            // 记录 wal
            let (wal_task, rx) = WalTask::new_update_tags(
                tenant.to_string(),
                database.to_string(),
                vnode_id,
                old_series_keys.clone(),
                new_series_keys.clone(),
                sids.clone(),
            );
            self.wal_sender
                .send(wal_task)
                .await
                .map_err(|_| Error::ChannelSend {
                    source: error::ChannelSendError::WalTask,
                })?;
            // Receive WAL write action result.
            let _ = rx.await.map_err(|e| Error::ChannelReceive {
                source: error::ChannelReceiveError::WriteWalResult { source: e },
            })??;
            // 更新索引
            ts_index
                .update_series_key(old_series_keys, new_series_keys, sids, false)
                .await
                .map_err(|err| {
                    error!(
                        "Update tags value tag of TSIndex({}): {}",
                        ts_index.path().display(),
                        err
                    );
                    err
                })?;
        }

        Ok(())
    }

    async fn delete_from_table(
        &self,
        vnode_id: VnodeId,
        tenant: &str,
        database: &str,
        table: &str,
        predicate: &ResolvedPredicate,
    ) -> Result<()> {
        // TODO wal

        let tag_domains = predicate.tags_filter();
        let time_ranges = predicate.time_ranges();

        let database_ref = match self.version_set.read().await.get_db(tenant, database) {
            Some(db) => db,
            None => return Ok(()),
        };
        let database_rlock = database_ref.read().await;
        let db_schemas = database_rlock.get_schemas();

        let column_ids: Vec<ColumnId> = match db_schemas.get_table_schema_by_meta(table).await? {
            Some(v) => v.columns().iter().map(|c| c.id).collect(),
            None => return Ok(()),
        };

        let vnode = match database_rlock.get_tsfamily(vnode_id) {
            Some(vnode) => vnode,
            None => return Ok(()),
        };
        let vnode_index = match database_rlock.get_ts_index(vnode_id) {
            Some(vnode) => vnode,
            None => return Ok(()),
        };

        drop(database_rlock);
        drop(database_ref);

        let series_ids = vnode_index.get_series_ids_by_domains(table, tag_domains).await?;

        let vnode = vnode.read().await;
        vnode.delete_series_by_time_ranges(&series_ids, time_ranges.as_ref());

        let field_ids = series_ids
            .iter()
            .flat_map(|sid| column_ids.iter().map(|fid| unite_id(*fid, *sid)))
            .collect::<Vec<_>>();
        info!(
            "Drop table: vnode {vnode_id} deleting {} fields in table: {table}",
            field_ids.len()
        );

        let version = vnode.super_version();
        for time_range in time_ranges.time_ranges() {
            for column_file in version.version.column_files(&field_ids, time_range) {
                column_file.add_tombstone(&field_ids, time_range).await?;
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
        let ts_index = match self.version_set.read().await.get_db(tenant, database) {
            Some(db) => match db.read().await.get_ts_index(vnode_id) {
                Some(ts_index) => ts_index,
                None => return Ok(vec![]),
            },
            None => return Ok(vec![]),
        };

        let res = ts_index.get_series_ids_by_domains(tab, filter).await?;

        Ok(res)
    }

    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
        series_id: SeriesId,
    ) -> Result<Option<SeriesKey>> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            return Ok(db.read().await.get_series_key(vnode_id, series_id).await?);
        }

        Ok(None)
    }

    async fn get_db_version(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> Result<Option<Arc<SuperVersion>>> {
        let version_set = self.version_set.read().await;
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
        self.options.storage.clone()
    }

    async fn get_vnode_summary(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
    ) -> Result<Option<VersionEdit>> {
        let version_set = self.version_set.read().await;
        if let Some(db) = version_set.get_db(tenant, database) {
            // TODO: Send file_metas to the destination node.
            let mut file_metas = HashMap::new();
            let tsf_opt = db.read().await.get_tsfamily(vnode_id);
            if let Some(tsf) = tsf_opt {
                let ve = tsf.read().await.build_version_edit(&mut file_metas);
                // it used for move vnode, set vnode status running at last
                tsf.write().await.update_status(VnodeStatus::Running);
                Ok(Some(ve))
            } else {
                warn!(
                    "ts_family {} with db name '{}.{}' not found.",
                    vnode_id, tenant, database
                );
                Ok(None)
            }
        } else {
            return Err(SchemaError::DatabaseNotFound {
                database: format!("{}.{}", tenant, database),
            }
            .into());
        }
    }

    async fn apply_vnode_summary(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: VnodeId,
        summary: VersionEdit,
    ) -> Result<()> {
        info!("apply tsfamily summary: {:?}", summary);
        // It should be a version edit that add a vnode.
        if !summary.add_tsf {
            return Ok(());
        }

        let db = self.get_db_or_else_create(tenant, database).await?;
        let mut db_wlock = db.write().await;
        // If there is a ts_family here, delete and re-build it.
        if db_wlock.get_tsfamily(vnode_id).is_some() {
            return Err(Error::CommonError {
                reason: format!("vnode:{}, already exist", vnode_id),
            });
        }

        db_wlock
            .add_tsfamily(
                vnode_id,
                Some(summary),
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
                self.compact_task_sender.clone(),
                self.global_ctx.clone(),
            )
            .await?;
        db_wlock.get_ts_index_or_add(vnode_id).await?;
        Ok(())
    }

    async fn drop_vnode(&self, vnode_id: TseriesFamilyId) -> Result<()> {
        let r_version_set = self.version_set.read().await;
        let all_db = r_version_set.get_all_db();
        for (db_name, db) in all_db {
            if db.read().await.get_tsfamily(vnode_id).is_none() {
                continue;
            }
            {
                let mut db_wlock = db.write().await;
                db_wlock.del_ts_index(vnode_id);
                db_wlock
                    .del_tsfamily(vnode_id, self.summary_task_sender.clone())
                    .await;
            }
            let tsf_dir = self.options.storage.ts_family_dir(db_name, vnode_id);
            if let Err(e) = std::fs::remove_dir_all(&tsf_dir) {
                error!("Failed to remove dir '{}', e: {}", tsf_dir.display(), e);
            }
            let index_dir = self.options.storage.index_dir(db_name, vnode_id);
            if let Err(e) = std::fs::remove_dir_all(&index_dir) {
                error!("Failed to remove dir '{}', e: {}", index_dir.display(), e);
            }
            break;
        }
        Ok(())
    }

    async fn compact(&self, vnode_ids: Vec<TseriesFamilyId>) -> Result<()> {
        for vnode_id in vnode_ids {
            if let Some(ts_family) = self
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
                    if let Err(e) = run_flush_memtable_job(
                        req,
                        self.global_ctx.clone(),
                        self.version_set.clone(),
                        self.summary_task_sender.clone(),
                        None,
                    )
                    .await
                    {
                        error!("Failed to flush vnode {}: {:?}", vnode_id, e);
                    }
                }

                let picker = LevelCompactionPicker::new(self.options.storage.clone());
                let version = ts_family.read().await.version();
                if let Some(req) = picker.pick_compaction(version) {
                    match compaction::run_compaction_job(req, self.global_ctx.clone()).await {
                        Ok(Some((version_edit, file_metas))) => {
                            let (summary_tx, _summary_rx) = oneshot::channel();
                            let _ = self
                                .summary_task_sender
                                .send(SummaryTask::new(
                                    vec![version_edit],
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
        for database in self.version_set.read().await.get_all_db().values() {
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
                    run_flush_memtable_job(
                        req,
                        self.global_ctx.clone(),
                        self.version_set.clone(),
                        self.summary_task_sender.clone(),
                        None,
                    )
                    .await?;
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

    async fn create_snapshot(&self, vnode_id: VnodeId) -> Result<VnodeSnapshot> {
        debug!("Snapshot: create snapshot on vnode: {vnode_id}");

        let (vnode_optional, vnode_index_optional) = self
            .version_set
            .read()
            .await
            .get_tsfamily_tsindex_by_tf_id(vnode_id)
            .await;
        if let Some(vnode) = vnode_optional {
            // Get snapshot directory.
            let storage_opt = self.options.storage.clone();
            let tenant_database = vnode.read().await.tenant_database();

            let snapshot_id = chrono::Local::now().format("%d%m%Y_%H%M%S_%3f").to_string();
            let snapshot_dir =
                storage_opt.snapshot_sub_dir(tenant_database.as_str(), vnode_id, &snapshot_id);
            let index_dir = storage_opt.index_dir(tenant_database.as_str(), vnode_id);
            let delta_dir = storage_opt.delta_dir(tenant_database.as_str(), vnode_id);
            let tsm_dir = storage_opt.tsm_dir(tenant_database.as_str(), vnode_id);
            let snap_index_dir =
                storage_opt.snapshot_index_dir(tenant_database.as_str(), vnode_id, &snapshot_id);
            let snap_delta_dir =
                storage_opt.snapshot_delta_dir(tenant_database.as_str(), vnode_id, &snapshot_id);
            let snap_tsm_dir =
                storage_opt.snapshot_tsm_dir(tenant_database.as_str(), vnode_id, &snapshot_id);

            let (flush_req_optional, mut ve_summary_snapshot) = {
                let mut vnode_wlock = vnode.write().await;
                vnode_wlock.switch_to_immutable();
                let flush_req_optional = vnode_wlock.build_flush_req(true);
                let mut _file_metas = HashMap::new();
                let ve_summary_snapshot = vnode_wlock.build_version_edit(&mut _file_metas);
                (flush_req_optional, ve_summary_snapshot)
            };

            // Run force flush
            let last_seq_no = match flush_req_optional {
                Some(flush_req) => {
                    let last_seq_no = flush_req.high_seq_no;
                    if let Some(ve_flushed_files) = run_flush_memtable_job(
                        flush_req,
                        self.global_ctx.clone(),
                        self.version_set.clone(),
                        self.summary_task_sender.clone(),
                        None,
                    )
                    .await?
                    {
                        // Normally flushed, and generated some tsm/delta files.
                        debug!("Snapshot: flush vnode {vnode_id} succeed.");
                        ve_summary_snapshot
                            .add_files
                            .extend(ve_flushed_files.add_files);
                    } else {
                        // Flushed but not generate any file.
                        warn!("Snapshot: flush vnode {vnode_id} did not generated any file.");
                    }
                    last_seq_no
                }
                None => 0,
            };

            // Do snapshot, file system operations.
            let files = {
                let _vnode_rlock = vnode.read().await;

                debug!(
                    "Snapshot: removing snapshot directory {}.",
                    snapshot_dir.display()
                );
                let _ = std::fs::remove_dir_all(&snapshot_dir);

                fn create_snapshot_dir(dir: &PathBuf) -> Result<()> {
                    std::fs::create_dir_all(dir).with_context(|_| {
                        debug!(
                            "Snapshot: failed to create snapshot directory {}.",
                            dir.display()
                        );
                        error::CreateFileSnafu { path: dir.clone() }
                    })
                }
                debug!(
                    "Snapshot: creating snapshot directory {}.",
                    snapshot_dir.display()
                );
                create_snapshot_dir(&snap_delta_dir)?;
                create_snapshot_dir(&snap_tsm_dir)?;

                // Copy index directory.
                if let Some(vnode_index) = vnode_index_optional {
                    if let Err(e) = vnode_index.flush().await {
                        error!("Snapshot: failed to flush vnode index: {e}.");
                        return Err(Error::IndexErr { source: e });
                    }
                    if let Err(e) = dircpy::copy_dir(&index_dir, &snap_index_dir) {
                        error!(
                            "Snapshot: failed to copy vnode index directory {} to {}: {e}",
                            index_dir.display(),
                            snap_index_dir.display()
                        );
                        return Err(Error::IO { source: e });
                    }
                } else {
                    debug!("Snapshot: no vnode index, skipped coping.")
                }

                let mut files = Vec::with_capacity(ve_summary_snapshot.add_files.len());
                for f in ve_summary_snapshot.add_files {
                    // Get tsm/delta file path and snapshot file path
                    let (file_path, snapshot_path) = if f.is_delta {
                        (
                            file_utils::make_delta_file(&delta_dir, f.file_id),
                            file_utils::make_delta_file(&snap_delta_dir, f.file_id),
                        )
                    } else {
                        (
                            file_utils::make_tsm_file(&tsm_dir, f.file_id),
                            file_utils::make_tsm_file(&snap_tsm_dir, f.file_id),
                        )
                    };

                    files.push(SnapshotFileMeta::from(&f));

                    // Create hard link to tsm/delta file.
                    debug!(
                        "Snapshot: creating hard link {} to {}.",
                        file_path.display(),
                        snapshot_path.display()
                    );
                    if let Err(e) =
                        std::fs::hard_link(&file_path, &snapshot_path).context(error::IOSnafu)
                    {
                        error!(
                            "Snapshot: failed to create hard link {} to {}: {e}.",
                            file_path.display(),
                            snapshot_path.display()
                        );
                        return Err(e);
                    }
                }

                files
            };

            let (tenant, database) = split_owner(tenant_database.as_str());
            let snapshot = VnodeSnapshot {
                snapshot_id,
                node_id: 0,
                tenant: tenant.to_string(),
                database: database.to_string(),
                vnode_id,
                files,
                last_seq_no,
            };
            debug!("Snapshot: created snapshot: {snapshot:?}");
            Ok(snapshot)
        } else {
            // Vnode not found
            warn!("Snapshot: vnode {vnode_id} not found.");
            Err(Error::VnodeNotFound { vnode_id })
        }
    }

    async fn apply_snapshot(&self, snapshot: VnodeSnapshot) -> Result<()> {
        debug!("Snapshot: apply snapshot {snapshot:?} to create new vnode.");
        let VnodeSnapshot {
            snapshot_id: _,
            node_id: _,
            tenant,
            database,
            vnode_id,
            files,
            last_seq_no,
        } = snapshot;
        let tenant_database = make_owner(&tenant, &database);
        let storage_opt = self.options.storage.clone();
        let db = self.get_db_or_else_create(&tenant, &database).await?;
        let mut db_wlock = db.write().await;
        if db_wlock.get_tsfamily(vnode_id).is_some() {
            warn!("Snapshot: removing existing vnode {vnode_id}.");
            db_wlock
                .del_tsfamily(vnode_id, self.summary_task_sender.clone())
                .await;
            let vnode_dir = storage_opt.ts_family_dir(&tenant_database, vnode_id);
            debug!(
                "Snapshot: removing existing vnode directory {}.",
                vnode_dir.display()
            );
            if let Err(e) = std::fs::remove_dir_all(&vnode_dir) {
                error!(
                    "Snapshot: failed to remove existing vnode directory {}.",
                    vnode_dir.display()
                );
                return Err(Error::IO { source: e });
            }
        }

        let version_edit = VersionEdit {
            has_seq_no: true,
            seq_no: last_seq_no,
            add_files: files
                .iter()
                .map(|f| CompactMeta {
                    file_id: f.file_id,
                    file_size: f.file_id,
                    tsf_id: vnode_id,
                    level: f.level,
                    min_ts: f.min_ts,
                    max_ts: f.max_ts,
                    high_seq: last_seq_no,
                    low_seq: 0,
                    is_delta: f.level == 0,
                })
                .collect(),
            add_tsf: true,
            tsf_id: vnode_id,
            tsf_name: tenant_database,
            ..Default::default()
        };
        debug!("Snapshot: created version edit {version_edit:?}");

        // Create new vnode.
        if let Err(e) = db_wlock
            .add_tsfamily(
                vnode_id,
                Some(version_edit),
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
                self.compact_task_sender.clone(),
                self.global_ctx.clone(),
            )
            .await
        {
            error!("Snapshot: failed to create vnode {vnode_id}: {e}");
            return Err(e);
        }
        // Create series index for vnode.
        if let Err(e) = db_wlock.get_ts_index_or_add(vnode_id).await {
            error!("Snapshot: failed to create index for vnode {vnode_id}: {e}");
            return Err(e);
        }

        Ok(())
    }

    async fn delete_snapshot(&self, vnode_id: VnodeId) -> Result<()> {
        debug!("Snapshot: create snapshot on vnode: {vnode_id}");
        let vnode_optional = self
            .version_set
            .read()
            .await
            .get_tsfamily_by_tf_id(vnode_id)
            .await;
        if let Some(vnode) = vnode_optional {
            let tenant_database = vnode.read().await.tenant_database();
            let storage_opt = self.options.storage.clone();
            let snapshot_dir = storage_opt.snapshot_dir(tenant_database.as_str(), vnode_id);
            debug!(
                "Snapshot: removing snapshot directory {}.",
                snapshot_dir.display()
            );
            std::fs::remove_dir_all(&snapshot_dir).with_context(|_| {
                error!(
                    "Snapshot: failed to remove snapshot directory {}.",
                    snapshot_dir.display()
                );
                error::DeleteFileSnafu { path: snapshot_dir }
            })?;
            Ok(())
        } else {
            // Vnode not found
            warn!("Snapshot: vnode {vnode_id} not found.");
            Err(Error::VnodeNotFound { vnode_id })
        }
    }
}

#[cfg(test)]
impl TsKv {
    pub(crate) fn global_ctx(&self) -> Arc<GlobalContext> {
        self.global_ctx.clone()
    }

    pub(crate) fn version_set(&self) -> Arc<RwLock<VersionSet>> {
        self.version_set.clone()
    }

    pub(crate) fn summary_task_sender(&self) -> Sender<SummaryTask> {
        self.summary_task_sender.clone()
    }

    pub(crate) fn flush_task_sender(&self) -> Sender<FlushReq> {
        self.flush_task_sender.clone()
    }

    pub(crate) fn compact_task_sender(&self) -> Sender<CompactTask> {
        self.compact_task_sender.clone()
    }
}
