use std::collections::HashMap;
use std::panic;
use std::sync::Arc;
use std::time::Duration;

use memory_pool::{MemoryPool, MemoryPoolRef};
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::codec::Encoding;
use models::predicate::domain::{ColumnDomains, TimeRange};
use models::schema::{make_owner, DatabaseSchema, Precision, TableColumn};
use models::utils::unite_id;
use models::{ColumnId, SeriesId, SeriesKey};
use protos::kv_service::{WritePointsRequest, WritePointsResponse};
use protos::{get_db_from_fb_points, models as fb_models};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::broadcast::{self, Sender as BroadcastSender};
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{oneshot, RwLock};
use trace::{debug, error, info, warn};

use crate::compaction::{
    self, run_flush_memtable_job, CompactTask, FlushReq, LevelCompactionPicker, Picker,
};
use crate::context::{self, GlobalContext, GlobalSequenceContext, GlobalSequenceTask};
use crate::database::Database;
use crate::error::{self, Result};
use crate::file_system::file_manager;
use crate::index::ts_index;
use crate::kv_option::{Options, StorageOptions};
use crate::schema::error::SchemaError;
use crate::summary::{Summary, SummaryProcessor, SummaryTask, VersionEdit};
use crate::tseries_family::{SuperVersion, TseriesFamily};
use crate::tsm::codec::get_str_codec;
use crate::version_set::VersionSet;
use crate::wal::{WalEntryType, WalManager, WalTask};
use crate::{database, file_utils, tenant_name_from_request, Engine, Error, TseriesFamilyId};

// TODO: A small summay channel capacity can cause a block
pub const COMPACT_REQ_CHANNEL_CAP: usize = 1024;
pub const SUMMARY_REQ_CHANNEL_CAP: usize = 1024;
pub const GLOBAL_TASK_REQ_CHANNEL_CAP: usize = 1024;

#[derive(Debug)]
pub struct TsKv {
    options: Arc<Options>,
    global_ctx: Arc<GlobalContext>,
    global_seq_ctx: Arc<GlobalSequenceContext>,
    version_set: Arc<RwLock<VersionSet>>,
    meta_manager: MetaRef,

    runtime: Arc<Runtime>,
    memory_pool: Arc<dyn MemoryPool>,
    wal_sender: Sender<WalTask>,
    flush_task_sender: Sender<FlushReq>,
    compact_task_sender: Sender<CompactTask>,
    summary_task_sender: Sender<SummaryTask>,
    global_seq_task_sender: Sender<GlobalSequenceTask>,
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
        let (global_seq_task_sender, global_seq_task_receiver) =
            mpsc::channel::<GlobalSequenceTask>(GLOBAL_TASK_REQ_CHANNEL_CAP);
        let (close_sender, _close_receiver) = broadcast::channel(1);
        let (version_set, summary) = Self::recover_summary(
            runtime.clone(),
            memory_pool.clone(),
            meta_manager.clone(),
            shared_options.clone(),
            flush_task_sender.clone(),
            global_seq_task_sender.clone(),
            compact_task_sender.clone(),
            metrics.clone(),
        )
        .await;
        let global_seq_ctx = version_set.read().await.get_global_sequence_context().await;
        let global_seq_ctx = Arc::new(global_seq_ctx);

        let core = Self {
            options: shared_options.clone(),
            global_ctx: summary.global_context(),
            global_seq_ctx: global_seq_ctx.clone(),
            version_set,
            meta_manager,
            runtime: runtime.clone(),
            memory_pool,
            wal_sender,
            flush_task_sender: flush_task_sender.clone(),
            compact_task_sender: compact_task_sender.clone(),
            summary_task_sender: summary_task_sender.clone(),
            global_seq_task_sender: global_seq_task_sender.clone(),
            close_sender,
            metrics,
        };

        let wal_manager = core.recover_wal().await;
        core.run_wal_job(wal_manager, wal_receiver);
        core.run_flush_job(
            flush_task_receiver,
            summary.global_context(),
            global_seq_ctx.clone(),
            summary.version_set(),
            summary_task_sender.clone(),
            compact_task_sender.clone(),
        );
        compaction::job::run(
            shared_options.storage.clone(),
            runtime,
            compact_task_receiver,
            summary.global_context(),
            global_seq_ctx.clone(),
            summary.version_set(),
            summary_task_sender.clone(),
        );
        core.run_summary_job(summary, summary_task_receiver);
        context::run_global_context_job(
            core.runtime.clone(),
            global_seq_task_receiver,
            global_seq_ctx,
        );
        Ok(core)
    }

    #[allow(clippy::too_many_arguments)]
    async fn recover_summary(
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        meta: MetaRef,
        opt: Arc<Options>,
        flush_task_sender: Sender<FlushReq>,
        global_seq_task_sender: Sender<GlobalSequenceTask>,
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
                global_seq_task_sender,
                compact_task_sender,
                true,
                metrics.clone(),
            )
            .await
            .unwrap()
        } else {
            Summary::new(opt, runtime, memory_pool, global_seq_task_sender, metrics)
                .await
                .unwrap()
        };
        let version_set = summary.version_set();

        (version_set, summary)
    }

    async fn recover_wal(&self) -> WalManager {
        let wal_manager = WalManager::open(self.options.wal.clone(), self.global_seq_ctx.clone())
            .await
            .unwrap();

        wal_manager.recover(self).await.unwrap();

        wal_manager
    }

    pub(crate) fn run_wal_job(&self, mut wal_manager: WalManager, mut receiver: Receiver<WalTask>) {
        async fn on_write(
            wal_manager: &mut WalManager,
            points: Arc<Vec<u8>>,
            cb: oneshot::Sender<Result<(u64, usize)>>,
            id: TseriesFamilyId,
            tenant: Arc<Vec<u8>>,
            precision: Precision,
        ) {
            let ret = wal_manager
                .write(WalEntryType::Write, points, id, tenant, precision)
                .await;
            let send_ret = cb.send(ret);
            if let Err(e) = send_ret {
                // WAL job closed, leaving this write request.
                warn!("send WAL write result failed: {:?}", e);
            }
        }

        async fn on_tick_sync(wal_manager: &WalManager) {
            if let Err(e) = wal_manager.sync().await {
                error!("Failed flushing WAL file: {:?}", e);
            }
        }

        async fn on_tick_check_total_size(
            version_set: Arc<RwLock<VersionSet>>,
            wal_manager: &WalManager,
        ) {
            // TODO(zipper): This may cause flushing too frequent.
            if wal_manager.is_total_file_size_exceed() {
                version_set.read().await.send_flush_req().await;
            }
        }

        async fn on_cancel(wal_manager: WalManager) {
            info!("Job 'WAL' closing.");
            if let Err(e) = wal_manager.close().await {
                error!("Failed to close job 'WAL': {:?}", e);
            }
            info!("Job 'WAL' closed.");
        }

        info!("Job 'WAL' starting.");
        let version_set = self.version_set.clone();
        let mut close_receiver = self.close_sender.subscribe();
        self.runtime.spawn(async move {
            info!("Job 'WAL' started.");

            let sync_interval = wal_manager.sync_interval();
            let mut check_total_size_ticker = tokio::time::interval(Duration::from_secs(10));
            if sync_interval == Duration::ZERO {
                loop {
                    tokio::select! {
                        wal_task = receiver.recv() => {
                            match wal_task {
                                Some(WalTask::Write { id, points, precision,tenant, cb }) => on_write(&mut wal_manager, points, cb, id, tenant, precision).await,
                                _ => break
                            }
                        }
                        _ = check_total_size_ticker.tick() => {
                            on_tick_check_total_size(version_set.clone(), &wal_manager).await;
                        }
                        _ = close_receiver.recv() => {
                            on_cancel(wal_manager).await;
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
                                Some(WalTask::Write { id, points, precision,tenant, cb }) => on_write(&mut wal_manager, points, cb, id, tenant, precision).await,
                                _ => break
                            }
                        }
                        _ = sync_ticker.tick() => {
                            on_tick_sync(&wal_manager).await;
                        }
                        _ = check_total_size_ticker.tick() => {
                            on_tick_check_total_size(version_set.clone(), &wal_manager).await;
                        }
                        _ = close_receiver.recv() => {
                            on_cancel(wal_manager).await;
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
        seq_ctx: Arc<GlobalSequenceContext>,
        version_set: Arc<RwLock<VersionSet>>,
        summary_task_sender: Sender<SummaryTask>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let runtime = self.runtime.clone();
        let f = async move {
            while let Some(x) = receiver.recv().await {
                // TODO(zipper): this make config `flush_req_channel_cap` wasted
                runtime.spawn(run_flush_memtable_job(
                    x,
                    ctx.clone(),
                    seq_ctx.clone(),
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

    pub async fn get_db(&self, tenant: &str, database: &str) -> Result<Arc<RwLock<Database>>> {
        let db = self
            .version_set
            .read()
            .await
            .get_db(tenant, database)
            .ok_or(SchemaError::DatabaseNotFound {
                database: database.to_string(),
            })?;
        Ok(db)
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
        seq: u64,
        id: TseriesFamilyId,
        ve: Option<VersionEdit>,
        db: Arc<RwLock<Database>>,
    ) -> Result<Arc<RwLock<TseriesFamily>>> {
        let opt_tsf = db.read().await.get_tsfamily(id);
        match opt_tsf {
            Some(v) => Ok(v),
            None => Ok(db
                .write()
                .await
                .add_tsfamily(
                    id,
                    seq,
                    ve,
                    self.summary_task_sender.clone(),
                    self.flush_task_sender.clone(),
                    self.compact_task_sender.clone(),
                )
                .await),
        }
    }

    async fn delete_columns(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_ids: &[ColumnId],
    ) -> Result<()> {
        let db = self.get_db(tenant, database).await?;

        let series_ids = db.read().await.get_table_sids(table).await?;

        let storage_field_ids: Vec<u64> = series_ids
            .iter()
            .flat_map(|sid| column_ids.iter().map(|fid| unite_id(*fid, *sid)))
            .collect();

        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            for (_ts_family_id, ts_family) in db.read().await.ts_families().iter() {
                ts_family.read().await.delete_columns(&storage_field_ids);

                let version = ts_family.read().await.super_version();
                for column_file in version
                    .version
                    .column_files(&storage_field_ids, &TimeRange::all())
                {
                    self.runtime.block_on(
                        column_file.add_tombstone(&storage_field_ids, &TimeRange::all()),
                    )?;
                }
            }
        }

        Ok(())
    }

    async fn write_wal(
        &self,
        id: TseriesFamilyId,
        tenant: &str,
        precision: Precision,
        points: &[u8],
    ) -> Result<u64> {
        if !self.options.wal.enabled {
            return Ok(0);
        }

        let (cb, rx) = oneshot::channel();
        let mut enc_points = Vec::with_capacity(points.len() / 2);
        get_str_codec(Encoding::Zstd)
            .encode(&[points], &mut enc_points)
            .map_err(|_| Error::Send)?;
        self.wal_sender
            .send(WalTask::Write {
                id,
                precision,
                cb,
                points: Arc::new(enc_points),
                tenant: Arc::new(tenant.as_bytes().to_vec()),
            })
            .await
            .map_err(|_| Error::Send)?;
        let seq = rx.await.context(error::ReceiveSnafu)??.0;

        Ok(seq)
    }
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn write(
        &self,
        id: TseriesFamilyId,
        precision: Precision,
        write_batch: WritePointsRequest,
    ) -> Result<WritePointsResponse> {
        let tenant = tenant_name_from_request(&write_batch);
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = get_db_from_fb_points(&fb_points)?;
        let db = self.get_db_or_else_create(&tenant, &db_name).await?;
        let ts_index = self.get_ts_index_or_else_create(db.clone(), id).await?;

        let tables = fb_points.tables().ok_or(Error::CommonError {
            reason: "points missing table".to_string(),
        })?;

        let write_group = db
            .read()
            .await
            .build_write_group(&db_name, precision, tables, ts_index)
            .await?;

        let seq = self.write_wal(id, &tenant, precision, &points).await?;

        let tsf = self
            .get_tsfamily_or_else_create(seq, id, None, db.clone())
            .await?;

        let res = match tsf.read().await.put_points(seq, write_group) {
            Ok(points_number) => Ok(WritePointsResponse { points_number }),
            Err(err) => Err(err),
        };
        tsf.write().await.check_to_flush().await;
        res
    }

    async fn write_from_wal(
        &self,
        id: TseriesFamilyId,
        precision: Precision,
        write_batch: WritePointsRequest,
        seq: u64,
    ) -> Result<()> {
        let tenant = tenant_name_from_request(&write_batch);
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = get_db_from_fb_points(&fb_points)?;
        let db = self.get_db_or_else_create(&tenant, &db_name).await?;
        let ts_index = self.get_ts_index_or_else_create(db.clone(), id).await?;

        let write_group = db
            .read()
            .await
            .build_write_group(
                &db_name,
                precision,
                fb_points.tables().ok_or(Error::CommonError {
                    reason: "points missing table".to_string(),
                })?,
                ts_index,
            )
            .await?;

        let tsf = self
            .get_tsfamily_or_else_create(seq, id, None, db.clone())
            .await?;
        tsf.read().await.put_points(seq, write_group)?;
        return Ok(());
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
        // TODO Create global DropTable flag for droping the same table at the same time.
        let version_set = self.version_set.clone();
        let database = database.to_string();
        let table = table.to_string();
        let tenant = tenant.to_string();

        database::delete_table_async(tenant, database, table, version_set).await
    }

    async fn remove_tsfamily(&self, tenant: &str, database: &str, id: u32) -> Result<()> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            let mut db_wlock = db.write().await;

            db_wlock
                .del_tsfamily(id, self.summary_task_sender.clone())
                .await;

            let ts_dir = self
                .options
                .storage
                .ts_family_dir(&make_owner(tenant, database), id);
            let result = std::fs::remove_dir_all(&ts_dir);
            info!(
                "Remove TsFamily data '{}', result: {:?}",
                ts_dir.display(),
                result
            );
        }

        Ok(())
    }

    async fn flush_tsfamily(&self, tenant: &str, database: &str, id: u32) -> Result<()> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            if let Some(tsfamily) = db.read().await.get_tsfamily(id) {
                let request = {
                    let mut tsfamily = tsfamily.write().await;
                    tsfamily.switch_to_immutable();
                    tsfamily.build_flush_req(true)
                };

                if let Some(req) = request {
                    run_flush_memtable_job(
                        req,
                        self.global_ctx.clone(),
                        self.global_seq_ctx.clone(),
                        self.version_set.clone(),
                        self.summary_task_sender.clone(),
                        Some(self.compact_task_sender.clone()),
                    )
                    .await
                    .unwrap()
                }
            }

            if let Some(ts_index) = db.read().await.get_ts_index(id) {
                let _ = ts_index.flush().await;
            }
        }

        Ok(())
    }

    async fn add_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        new_column: TableColumn,
    ) -> Result<()> {
        let db = self.get_db(tenant, database).await?;
        let db = db.read().await;
        let sids = db.get_table_sids(table).await?;
        for (_ts_family_id, ts_family) in db.ts_families().iter() {
            ts_family.read().await.add_column(&sids, &new_column);
        }
        Ok(())
    }

    async fn drop_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_name: &str,
    ) -> Result<()> {
        let db = self.get_db(tenant, database).await?;
        let schema =
            db.read()
                .await
                .get_table_schema(table)?
                .ok_or(SchemaError::TableNotFound {
                    table: table.to_string(),
                })?;
        let column_id = schema
            .column(column_name)
            .ok_or(SchemaError::NotFoundField {
                field: column_name.to_string(),
            })?
            .id;
        self.delete_columns(tenant, database, table, &[column_id])
            .await?;
        Ok(())
    }

    async fn change_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_name: &str,
        new_column: TableColumn,
    ) -> Result<()> {
        let db = self.get_db(tenant, database).await?;
        let db = db.read().await;
        let sids = db.get_table_sids(table).await?;

        for (_ts_family_id, ts_family) in db.ts_families().iter() {
            ts_family
                .read()
                .await
                .change_column(&sids, column_name, &new_column);
        }
        Ok(())
    }

    async fn delete_series(
        &self,
        _tenant: &str,
        _database: &str,
        _series_ids: &[SeriesId],
        _field_ids: &[ColumnId],
        _time_range: &TimeRange,
    ) -> Result<()> {
        // let storage_field_ids: Vec<u64> = series_ids
        //     .iter()
        //     .flat_map(|sid| field_ids.iter().map(|fid| unite_id(*fid, *sid)))
        //     .collect();
        // let res = self.version_set.read().get_db(tenant, database);
        // if let Some(db) = res {
        //     let readable_db = db.read();
        //     for (ts_family_id, ts_family) in readable_db.ts_families() {
        //         // TODO Cancel current and prevent next flush or compaction in TseriesFamily provisionally.
        //         ts_family
        //             .write()
        //             .delete_series(&storage_field_ids, time_range);

        //         let version = ts_family.read().super_version();
        //         for column_file in version.version.column_files(&storage_field_ids, time_range) {
        //             self.runtime
        //                 .block_on(column_file.add_tombstone(&storage_field_ids, time_range))?;
        //         }
        //         // TODO Start next flush or compaction.
        //     }
        // }
        Ok(())
    }

    async fn get_series_id_by_filter(
        &self,
        tenant: &str,
        database: &str,
        tab: &str,
        id: SeriesId,
        filter: &ColumnDomains<String>,
    ) -> Result<Vec<SeriesId>> {
        let ts_index = match self.version_set.read().await.get_db(tenant, database) {
            Some(db) => match db.read().await.get_ts_index(id) {
                Some(ts_index) => ts_index,
                None => return Ok(vec![]),
            },
            None => return Ok(vec![]),
        };

        let res = if filter.is_all() {
            // Match all records
            debug!("pushed tags filter is All.");
            ts_index.get_series_id_list(tab, &[]).await?
        } else if filter.is_none() {
            // Does not match any record, return null
            debug!("pushed tags filter is None.");
            vec![]
        } else {
            // No error will be reported here
            debug!("pushed tags filter is {:?}.", filter);
            let domains = unsafe { filter.domains_unsafe() };
            ts_index.get_series_ids_by_domains(tab, domains).await?
        };

        Ok(res)
    }

    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
        sid: u32,
    ) -> Result<Option<SeriesKey>> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            return Ok(db.read().await.get_series_key(vnode_id, sid).await?);
        }

        Ok(None)
    }

    async fn get_db_version(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
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
            info!("ts_family with db name '{}' not found.", database);
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
        vnode_id: u32,
    ) -> Result<Option<VersionEdit>> {
        let version_set = self.version_set.read().await;
        if let Some(db) = version_set.get_db(tenant, database) {
            let db = db.read().await;
            // TODO: Send file_metas to the destination node.
            let mut file_metas = HashMap::new();
            if let Some(tsf) = db.get_tsfamily(vnode_id) {
                let ve = tsf.read().await.snapshot(db.owner(), &mut file_metas);
                Ok(Some(ve))
            } else {
                warn!(
                    "ts_family with db name '{}.{}' not found.",
                    tenant, database
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
        vnode_id: u32,
        summary: VersionEdit,
    ) -> Result<()> {
        info!("apply tsfamily summary: {:?}", summary);
        // It should be a version edit that add a vnode.
        if !summary.add_tsf {
            return Ok(());
        }

        let db = self.get_db_or_else_create(tenant, database).await?;
        self.get_ts_index_or_else_create(db.clone(), vnode_id)
            .await?;

        let mut db_wlock = db.write().await;
        // If there is a ts_family here, delete and re-build it.
        if db_wlock.get_tsfamily(vnode_id).is_some() {
            db_wlock
                .del_tsfamily(vnode_id, self.summary_task_sender.clone())
                .await;
        }

        db_wlock
            .add_tsfamily(
                vnode_id,
                0,
                Some(summary),
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
                self.compact_task_sender.clone(),
            )
            .await;
        Ok(())
    }

    async fn drop_vnode(&self, id: TseriesFamilyId) -> Result<()> {
        let r_version_set = self.version_set.read().await;
        let all_db = r_version_set.get_all_db();
        for (db_name, db) in all_db {
            if db.read().await.get_tsfamily(id).is_none() {
                continue;
            }
            {
                let mut db_wlock = db.write().await;
                db_wlock.del_ts_index(id);
                db_wlock
                    .del_tsfamily(id, self.summary_task_sender.clone())
                    .await;
            }
            let tsf_dir = self.options.storage.tsfamily_dir(db_name, id);
            if let Err(e) = std::fs::remove_dir_all(&tsf_dir) {
                error!("Failed to remove dir '{}', e: {}", tsf_dir.display(), e);
            }
            let index_dir = self.options.storage.index_dir(db_name, id);
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

                let mut tsf_wlock = ts_family.write().await;
                tsf_wlock.switch_to_immutable();
                let flush_req = tsf_wlock.build_flush_req(true);
                drop(tsf_wlock);
                if let Some(req) = flush_req {
                    if let Err(e) = run_flush_memtable_job(
                        req,
                        self.global_ctx.clone(),
                        self.global_seq_ctx.clone(),
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
