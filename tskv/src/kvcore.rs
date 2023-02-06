use std::time::Duration;
use std::{collections::HashMap, panic, sync::Arc};

use crate::context::{self, GlobalSequenceContext, GlobalSequenceTask};
use crate::error::MetaSnafu;
use crate::kv_option::StorageOptions;
use crate::tsm::codec::get_str_codec;
use config::ClusterConfig;
use datafusion::prelude::Column;
use flatbuffers::FlatBufferBuilder;
use futures::stream::SelectNextSome;
use futures::FutureExt;
use libc::printf;
use meta::meta_manager::RemoteMetaManager;
use meta::MetaRef;
use models::predicate::domain::{ColumnDomains, PredicateRef};
use snafu::{OptionExt, ResultExt};
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::{
    runtime::Runtime,
    sync::{
        broadcast::{self, Receiver as BroadcastReceiver, Sender as BroadcastSender},
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::Instant,
};

use crate::error::SendSnafu;
use metrics::{incr_compaction_failed, incr_compaction_success, sample_tskv_compaction_duration};
use models::codec::Encoding;
use models::schema::{
    make_owner, DatabaseSchema, TableColumn, TableSchema, TskvTableSchema, DEFAULT_CATALOG,
};
use models::{
    utils::unite_id, ColumnId, FieldId, FieldInfo, InMemPoint, SeriesId, SeriesKey, Tag, Timestamp,
    ValueType,
};
use protos::{
    kv_service::{WritePointsRpcRequest, WritePointsRpcResponse, WriteRowsRpcRequest},
    models as fb_models,
};
use trace::{debug, error, info, trace, warn};

use crate::database::Database;
use crate::error::SchemaSnafu;
use crate::file_system::file_manager::{self, FileManager};
use crate::schema::error::SchemaError;
use crate::tseries_family::TseriesFamily;
use crate::{
    compaction::{self, run_flush_memtable_job, CompactReq, FlushReq},
    context::GlobalContext,
    database,
    engine::Engine,
    error::{self, IndexErrSnafu, Result},
    file_utils,
    index::IndexResult,
    kv_option::Options,
    memcache::{DataType, MemCache},
    record_file::Reader,
    summary::{self, Summary, SummaryProcessor, SummaryTask, VersionEdit, WriteSummaryRequest},
    tseries_family::{SuperVersion, TimeRange, Version},
    tsm::{DataBlock, TsmTombstone, MAX_BLOCK_VALUES},
    version_set,
    version_set::VersionSet,
    wal::{self, WalEntryType, WalManager, WalTask},
    Error, Task, TseriesFamilyId,
};

#[derive(Debug)]
pub struct TsKv {
    options: Arc<Options>,
    global_ctx: Arc<GlobalContext>,
    global_seq_ctx: Arc<GlobalSequenceContext>,
    version_set: Arc<RwLock<VersionSet>>,
    meta_manager: MetaRef,

    runtime: Arc<Runtime>,
    wal_sender: UnboundedSender<WalTask>,
    flush_task_sender: UnboundedSender<FlushReq>,
    compact_task_sender: UnboundedSender<TseriesFamilyId>,
    summary_task_sender: UnboundedSender<SummaryTask>,
    global_seq_task_sender: UnboundedSender<GlobalSequenceTask>,
    close_sender: BroadcastSender<UnboundedSender<()>>,
}

impl TsKv {
    pub async fn open(meta_manager: MetaRef, opt: Options, runtime: Arc<Runtime>) -> Result<TsKv> {
        let shared_options = Arc::new(opt);
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel::<FlushReq>();
        let (compact_task_sender, compact_task_receiver) =
            mpsc::unbounded_channel::<TseriesFamilyId>();
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel::<WalTask>();
        let (summary_task_sender, summary_task_receiver) = mpsc::unbounded_channel::<SummaryTask>();
        let (global_seq_task_sender, global_seq_task_receiver) =
            mpsc::unbounded_channel::<GlobalSequenceTask>();
        let (close_sender, _close_receiver) = broadcast::channel(1);
        let (version_set, summary) = Self::recover_summary(
            runtime.clone(),
            meta_manager.clone(),
            shared_options.clone(),
            flush_task_sender.clone(),
            global_seq_task_sender.clone(),
            compact_task_sender.clone(),
        )
        .await;
        let global_seq_ctx = version_set.read().await.get_global_sequence_context().await;
        let global_seq_ctx = Arc::new(global_seq_ctx);
        let wal_cfg = shared_options.wal.clone();
        let core = Self {
            options: shared_options.clone(),
            global_ctx: summary.global_context(),
            global_seq_ctx: global_seq_ctx.clone(),
            version_set,
            meta_manager,
            runtime: runtime.clone(),
            wal_sender,
            flush_task_sender: flush_task_sender.clone(),
            compact_task_sender: compact_task_sender.clone(),
            summary_task_sender: summary_task_sender.clone(),
            global_seq_task_sender: global_seq_task_sender.clone(),
            close_sender,
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
        let _ = compaction::job::run(
            shared_options.storage.clone(),
            runtime,
            compact_task_receiver,
            summary.global_context(),
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

    pub async fn close(&self) {
        let (tx, mut rx) = mpsc::unbounded_channel();
        if let Err(e) = self.close_sender.send(tx) {
            error!("Failed to broadcast close signal: {:?}", e);
        }
        while let Some(_x) = rx.recv().await {
            continue;
        }
        info!("TsKv closed");
    }

    async fn recover_summary(
        runtime: Arc<Runtime>,
        meta: MetaRef,
        opt: Arc<Options>,
        flush_task_sender: UnboundedSender<FlushReq>,
        global_seq_task_sender: UnboundedSender<GlobalSequenceTask>,
        compact_task_sender: UnboundedSender<TseriesFamilyId>,
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
                flush_task_sender,
                global_seq_task_sender,
                compact_task_sender,
                true,
            )
            .await
            .unwrap()
        } else {
            Summary::new(opt, runtime, global_seq_task_sender)
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

        wal_manager
            .recover(self, self.global_ctx.clone())
            .await
            .unwrap();

        wal_manager
    }

    fn run_wal_job(&self, mut wal_manager: WalManager, mut receiver: UnboundedReceiver<WalTask>) {
        warn!("job 'WAL' starting.");
        let mut close_receiver = self.close_sender.subscribe();
        let f = async move {
            loop {
                tokio::select! {
                    wal_task = receiver.recv() => {
                        match wal_task {
                            Some(WalTask::Write { id, points, tenant, cb }) => {
                                // write wal
                                let ret = wal_manager.write(WalEntryType::Write, points, id, tenant).await;
                                let send_ret = cb.send(ret);
                                match send_ret {
                                    Ok(wal_result) => {}
                                    Err(err) => {
                                        warn!("send WAL write result failed.")
                                    }
                                }
                            }
                            _ => {
                                break;
                            }
                        }
                    }
                    close_task = close_receiver.recv() => {
                        info!("job 'WAL' closing.");
                        if let Err(e) = wal_manager.close().await {
                            error!("Failed to close wal: {:?}", e);
                        }
                        info!("job 'WAL' closed.");
                        if let Ok(tx) = close_task {
                            if let Err(e) = tx.send(()) {
                                error!("Failed to send wal closed signal: {:?}", e);
                            }
                        }
                        break;
                    }
                }
            }
        };
        self.runtime.spawn(f);
        info!("job 'WAL' started.");
    }

    fn run_flush_job(
        &self,
        mut receiver: UnboundedReceiver<FlushReq>,
        ctx: Arc<GlobalContext>,
        version_set: Arc<RwLock<VersionSet>>,
        summary_task_sender: UnboundedSender<SummaryTask>,
        compact_task_sender: UnboundedSender<TseriesFamilyId>,
    ) {
        let runtime = self.runtime.clone();
        let f = async move {
            while let Some(x) = receiver.recv().await {
                runtime.spawn(run_flush_memtable_job(
                    x,
                    ctx.clone(),
                    version_set.clone(),
                    summary_task_sender.clone(),
                    compact_task_sender.clone(),
                ));
            }
        };
        self.runtime.spawn(f);
        info!("Flush task handler started");
    }

    fn run_summary_job(
        &self,
        summary: Summary,
        mut summary_task_receiver: UnboundedReceiver<SummaryTask>,
    ) {
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

    // fn run_timer_job(&self, pub_sender: Sender<()>) {
    //     let f = async move {
    //         let interval = Duration::from_secs(1);
    //         let ticker = crossbeam_channel::tick(interval);
    //         loop {
    //             ticker.recv().unwrap();
    //             let _ = pub_sender.send(());
    //         }
    //     };
    //     self.runtime.spawn(f);
    // }

    // pub async fn query(&self, _opt: QueryOption) -> Result<Option<Entry>> {
    //     Ok(None)
    // }

    // Compact TSM files in database into bigger TSM files.

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

    pub(crate) async fn create_database(
        &self,
        schema: &DatabaseSchema,
    ) -> Result<Arc<RwLock<Database>>> {
        if self
            .version_set
            .read()
            .await
            .db_exists(schema.tenant_name(), schema.database_name())
        {
            return Err(SchemaError::DatabaseAlreadyExists {
                database: schema.database_name().to_string(),
            }
            .into());
        }
        let db = self
            .version_set
            .write()
            .await
            .create_db(schema.clone(), self.meta_manager.clone())
            .await?;
        Ok(db)
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
            for (ts_family_id, ts_family) in db.read().await.ts_families().iter() {
                ts_family.read().delete_columns(&storage_field_ids);

                let version = ts_family.read().super_version();
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
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn write(
        &self,
        id: TseriesFamilyId,
        write_batch: WritePointsRpcRequest,
    ) -> Result<WritePointsRpcResponse> {
        let tenant_name = write_batch
            .meta
            .map(|meta| meta.tenant)
            .ok_or(Error::CommonError {
                reason: "Write data missing tenant".to_string(),
            })?;
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = String::from_utf8(fb_points.db().unwrap().bytes().to_vec())
            .map_err(|err| Error::ErrCharacterSet)?;

        let db_warp = self.version_set.read().await.get_db(&tenant_name, &db_name);
        let db = match db_warp {
            Some(database) => database,
            None => {
                self.create_database(&DatabaseSchema::new(&tenant_name, &db_name))
                    .await?
            }
        };

        let opt_index = db.read().await.get_ts_index(id);
        let ts_index = match opt_index {
            Some(v) => v,
            None => db.write().await.get_ts_index_or_add(id).await?,
        };

        let write_group = db
            .read()
            .await
            .build_write_group(fb_points.points().unwrap(), ts_index)
            .await?;

        let mut seq = 0;
        if self.options.wal.enabled {
            let (cb, rx) = oneshot::channel();
            let mut enc_points = Vec::with_capacity(points.len() / 2);
            get_str_codec(Encoding::Zstd)
                .encode(&[&points], &mut enc_points)
                .map_err(|_| Error::Send)?;
            self.wal_sender
                .send(WalTask::Write {
                    id,
                    cb,
                    points: Arc::new(enc_points),
                    tenant: Arc::new(tenant_name.as_bytes().to_vec()),
                })
                .map_err(|err| Error::Send)?;
            seq = rx.await.context(error::ReceiveSnafu)??.0;
        }

        let opt_tsf = db.read().await.get_tsfamily(id);
        let tsf = match opt_tsf {
            Some(v) => v,
            None => db.write().await.add_tsfamily(
                id,
                seq,
                None,
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
                self.compact_task_sender.clone(),
            ),
        };

        tsf.read().put_points(seq, write_group);
        tsf.write().check_to_flush();
        Ok(WritePointsRpcResponse {
            version: 1,
            points: vec![],
        })
    }

    async fn write_from_wal(
        &self,
        id: TseriesFamilyId,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse> {
        let tenant_name = write_batch
            .meta
            .map(|meta| meta.tenant)
            .unwrap_or_else(|| DEFAULT_CATALOG.to_string());
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = String::from_utf8(fb_points.db().unwrap().bytes().to_vec())
            .map_err(|err| Error::ErrCharacterSet)?;

        let db = self
            .version_set
            .write()
            .await
            .create_db(
                DatabaseSchema::new(&tenant_name, &db_name),
                self.meta_manager.clone(),
            )
            .await?;

        let opt_index = db.read().await.get_ts_index(id);
        let ts_index = match opt_index {
            Some(v) => v,
            None => db.write().await.get_ts_index_or_add(id).await?,
        };

        let write_group = db
            .read()
            .await
            .build_write_group(fb_points.points().unwrap(), ts_index)
            .await?;

        let opt_tsf = db.read().await.get_tsfamily(id);
        let tsf = match opt_tsf {
            Some(v) => v,
            None => db.write().await.add_tsfamily(
                id,
                seq,
                None,
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
                self.compact_task_sender.clone(),
            ),
        };

        tsf.read().put_points(seq, write_group);

        return Ok(WritePointsRpcResponse {
            version: 1,
            points: vec![],
        });
    }

    async fn remove_tsfamily(&self, tenant: &str, database: &str, id: u32) -> Result<()> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            let mut db_wlock = db.write().await;

            db_wlock.del_tsfamily(id, self.summary_task_sender.clone());

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
                    let mut tsfamily = tsfamily.write();
                    tsfamily.switch_to_immutable();
                    tsfamily.flush_req(true)
                };

                if let Some(req) = request {
                    run_flush_memtable_job(
                        req,
                        self.global_ctx.clone(),
                        self.version_set.clone(),
                        self.summary_task_sender.clone(),
                        self.compact_task_sender.clone(),
                    )
                    .await
                    .unwrap()
                }
            }
        }

        Ok(())
    }

    async fn drop_database(&self, tenant: &str, database: &str) -> Result<()> {
        if let Some(db) = self.version_set.write().await.delete_db(tenant, database) {
            let mut db_wlock = db.write().await;
            let ts_family_ids: Vec<TseriesFamilyId> = db_wlock
                .ts_families()
                .iter()
                .map(|(tsf_id, tsf)| *tsf_id)
                .collect();
            for ts_family_id in ts_family_ids {
                db_wlock.del_ts_index(ts_family_id);
                db_wlock.del_tsfamily(ts_family_id, self.summary_task_sender.clone());
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

    async fn delete_series(
        &self,
        tenant: &str,
        database: &str,
        series_ids: &[SeriesId],
        field_ids: &[ColumnId],
        time_range: &TimeRange,
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

    async fn get_table_schema(
        &self,
        tenant: &str,
        database: &str,
        tab: &str,
    ) -> Result<Option<TskvTableSchema>> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            let val = db.read().await.get_table_schema(tab)?;
            return Ok(val);
        }
        Ok(None)
    }

    async fn get_series_id_by_filter(
        &self,
        id: u32,
        tenant: &str,
        database: &str,
        tab: &str,
        filter: &ColumnDomains<String>,
    ) -> IndexResult<Vec<u32>> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            let opt_index = db.read().await.get_ts_index(id);
            if let Some(ts_index) = opt_index {
                if filter.is_all() {
                    // Match all records
                    debug!("pushed tags filter is All.");
                    return ts_index.read().await.get_series_id_list(tab, &[]);
                } else if filter.is_none() {
                    // Does not match any record, return null
                    debug!("pushed tags filter is None.");
                    return Ok(vec![]);
                } else {
                    // No error will be reported here
                    debug!("pushed tags filter is {:?}.", filter);
                    let domains = unsafe { filter.domains_unsafe() };
                    return ts_index
                        .read()
                        .await
                        .get_series_ids_by_domains(tab, domains);
                }
            }
        }

        Ok(vec![])
    }

    async fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
        sid: u32,
    ) -> IndexResult<Option<SeriesKey>> {
        if let Some(db) = self.version_set.read().await.get_db(tenant, database) {
            return db.read().await.get_series_key(vnode_id, sid).await;
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
        if !version_set.db_exists(tenant, database) {
            return Err(SchemaError::DatabaseNotFound {
                database: database.to_string(),
            }
            .into());
        }
        if let Some(tsf) = version_set
            .get_tsfamily_by_name_id(tenant, database, vnode_id)
            .await
        {
            Ok(Some(tsf.read().super_version()))
        } else {
            warn!("ts_family with db name '{}' not found.", database);
            Ok(None)
        }
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
            let mut file_metas = HashMap::new();
            if let Some(tsf) = db.get_tsfamily(vnode_id) {
                let ve =
                    tsf.read()
                        .snapshot(self.global_ctx.last_seq(), db.owner(), &mut file_metas);
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
        mut summary: VersionEdit,
    ) -> Result<()> {
        info!("apply tsfamily summary: {:?}", summary);
        // It should be a version edit that add a vnode.
        if !summary.add_tsf {
            return Ok(());
        }

        summary.tsf_id = vnode_id;
        let version_set = self.version_set.read().await;
        if let Some(db) = version_set.get_db(tenant, database) {
            let mut db_wlock = db.write().await;
            // If there is a ts_family here, delete and re-build it.
            if let Some(tsf) = db_wlock.get_tsfamily(vnode_id) {
                db_wlock.del_tsfamily(vnode_id, self.summary_task_sender.clone());
            }

            db_wlock.get_ts_index_or_add(vnode_id).await?;

            db_wlock.add_tsfamily(
                vnode_id,
                0,
                Some(summary),
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
                self.compact_task_sender.clone(),
            );
            Ok(())
        } else {
            return Err(SchemaError::DatabaseNotFound {
                database: database.to_string(),
            }
            .into());
        }
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
        for (ts_family_id, ts_family) in db.ts_families().iter() {
            ts_family.read().add_column(&sids, &new_column);
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

        for (ts_family_id, ts_family) in db.ts_families().iter() {
            ts_family
                .read()
                .change_column(&sids, column_name, &new_column);
        }
        Ok(())
    }

    fn get_storage_options(&self) -> Arc<StorageOptions> {
        self.options.storage.clone()
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
                db_wlock.del_tsfamily(id, self.summary_task_sender.clone());
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

    async fn compact(&self, tenant: &str, database: &str) {
        let database = self.version_set.read().await.get_db(tenant, database);
        if let Some(db) = database {
            // TODO: stop current and prevent next flush and compaction.
            for (ts_family_id, ts_family) in db.read().await.ts_families() {
                let compact_req = ts_family.read().pick_compaction();
                if let Some(req) = compact_req {
                    match compaction::run_compaction_job(req, self.global_ctx.clone()).await {
                        Ok(Some((version_edit, file_metas))) => {
                            let (summary_tx, summary_rx) = oneshot::channel();
                            let ret =
                                self.summary_task_sender
                                    .send(SummaryTask::new_column_file_task(
                                        file_metas,
                                        vec![version_edit],
                                        summary_tx,
                                    ));

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
    }
}

#[cfg(test)]
impl TsKv {
    pub(crate) fn summary_task_sender(&self) -> UnboundedSender<SummaryTask> {
        self.summary_task_sender.clone()
    }

    pub(crate) fn flush_task_sender(&self) -> UnboundedSender<FlushReq> {
        self.flush_task_sender.clone()
    }

    pub(crate) fn compact_task_sender(&self) -> UnboundedSender<TseriesFamilyId> {
        self.compact_task_sender.clone()
    }
}

#[cfg(test)]
mod test {
    use config::get_config;
    use flatbuffers::{FlatBufferBuilder, WIPOffset};
    use meta::meta_manager::RemoteMetaManager;
    use meta::MetaRef;
    use models::utils::now_timestamp;
    use models::{ColumnId, InMemPoint, SeriesId, SeriesKey, Timestamp};
    use protos::{models::Points, models_helper};
    use std::collections::HashMap;
    use std::sync::{atomic, Arc};
    use tokio::runtime::{self, Runtime};

    use crate::{engine::Engine, error, tsm::DataBlock, Options, TimeRange, TsKv};
    use std::sync::atomic::{AtomicI64, Ordering};
    use tokio::sync::watch;

    #[tokio::test]
    #[ignore]
    async fn test_compact() {
        trace::init_default_global_tracing("tskv_log", "tskv.log", "info");
        let config = get_config("/tmp/test/config/config.toml");
        let opt = Options::from(&config);
        let meta_manager: MetaRef = RemoteMetaManager::new(config.cluster.clone()).await;
        meta_manager.admin_meta().add_data_node().await.unwrap();
        let tskv = TsKv::open(meta_manager, opt, Arc::new(Runtime::new().unwrap()))
            .await
            .unwrap();
        tskv.compact("cnosdb", "public").await;
    }
}
