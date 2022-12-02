use std::time::Duration;
use std::{collections::HashMap, panic, sync::Arc};

use crate::tsm::codec::get_str_codec;
use config::ClusterConfig;
use datafusion::prelude::Column;
use flatbuffers::FlatBufferBuilder;
use futures::stream::SelectNextSome;
use futures::FutureExt;
use libc::printf;
use meta::meta_client::{MetaRef, RemoteMetaManager};
use models::predicate::domain::{ColumnDomains, PredicateRef};
use parking_lot::{Mutex, RwLock};
use snafu::ResultExt;
use tokio::sync::watch;
use tokio::sync::watch::{Receiver, Sender};
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
use models::schema::{DatabaseSchema, TableColumn, TableSchema, TskvTableSchema};
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
use crate::file_system::file_manager::{self, init_file_manager, FileManager};
use crate::file_system::Options as FileOptions;
use crate::index::index_manger;
use crate::tseries_family::TseriesFamily;
use crate::Error::{DatabaseNotFound, IndexErr};
use crate::{
    compaction::{self, run_flush_memtable_job, CompactReq, FlushReq},
    context::GlobalContext,
    database,
    engine::Engine,
    error::{self, IndexErrSnafu, Result},
    file_utils,
    index::{db_index, IndexResult},
    kv_option::Options,
    memcache::{DataType, MemCache},
    record_file::Reader,
    summary,
    summary::{Summary, SummaryProcessor, SummaryTask, VersionEdit},
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
    version_set: Arc<RwLock<VersionSet>>,
    meta_manager: MetaRef,

    runtime: Arc<Runtime>,
    wal_sender: UnboundedSender<WalTask>,
    flush_task_sender: UnboundedSender<FlushReq>,
    compact_task_sender: UnboundedSender<TseriesFamilyId>,
    summary_task_sender: UnboundedSender<SummaryTask>,
    close_sender: BroadcastSender<UnboundedSender<()>>,
}

impl TsKv {
    pub async fn open(
        cluster_options: ClusterConfig,
        opt: Options,
        runtime: Arc<Runtime>,
    ) -> Result<TsKv> {
        let meta_manager: MetaRef = Arc::new(RemoteMetaManager::new(cluster_options));
        let shared_options = Arc::new(opt);
        init_file_manager(
            FileOptions::default()
                .max_resident(shared_options.storage.dio_max_resident)
                .max_non_resident(shared_options.storage.dio_max_non_resident)
                .page_len_scale(shared_options.storage.dio_page_len_scale),
        );
        let (flush_task_sender, flush_task_receiver) = mpsc::unbounded_channel();
        let (compact_task_sender, compact_task_receiver) = mpsc::unbounded_channel();
        let (wal_sender, wal_receiver) = mpsc::unbounded_channel();
        let (summary_task_sender, summary_task_receiver) = mpsc::unbounded_channel();
        let (close_sender, _close_receiver) = broadcast::channel(1);
        let (version_set, summary) = Self::recover_summary(
            meta_manager.clone(),
            shared_options.clone(),
            flush_task_sender.clone(),
        )
        .await;
        let wal_cfg = shared_options.wal.clone();
        let core = Self {
            version_set,
            global_ctx: summary.global_context(),
            runtime,
            wal_sender,
            options: shared_options,
            flush_task_sender: flush_task_sender.clone(),
            compact_task_sender: compact_task_sender.clone(),
            summary_task_sender: summary_task_sender.clone(),
            close_sender,
            meta_manager,
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
        core.run_compact_job(
            compact_task_receiver,
            summary.global_context(),
            summary.version_set(),
            summary_task_sender.clone(),
        );
        core.run_summary_job(summary, summary_task_receiver);
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
        meta: MetaRef,
        opt: Arc<Options>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) -> (Arc<RwLock<VersionSet>>, Summary) {
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir)
                .context(error::IOSnafu)
                .unwrap();
        }
        let summary_file = file_utils::make_summary_file(&summary_dir, 0);
        let summary = if file_manager::try_exists(&summary_file) {
            Summary::recover(meta, opt.clone(), flush_task_sender)
                .await
                .unwrap()
        } else {
            Summary::new(opt.clone(), flush_task_sender).await.unwrap()
        };
        let version_set = summary.version_set();

        (version_set, summary)
    }

    async fn recover_wal(&self) -> WalManager {
        let wal_manager = WalManager::new(self.options.wal.clone());

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
                                let ret = wal_manager.write(WalEntryType::Write, &points, id, &tenant).await;
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
        let f = async move {
            while let Some(x) = receiver.recv().await {
                run_flush_memtable_job(
                    x,
                    ctx.clone(),
                    version_set.clone(),
                    summary_task_sender.clone(),
                    compact_task_sender.clone(),
                )
                .unwrap();
            }
        };
        self.runtime.spawn(f);
        info!("Flush task handler started");
    }

    fn run_compact_job(
        &self,
        mut receiver: UnboundedReceiver<TseriesFamilyId>,
        ctx: Arc<GlobalContext>,
        version_set: Arc<RwLock<VersionSet>>,
        summary_task_sender: UnboundedSender<SummaryTask>,
    ) {
        self.runtime.spawn(async move {
            while let Some(ts_family_id) = receiver.recv().await {
                let ts_family = version_set.read().get_tsfamily_by_tf_id(ts_family_id);
                if let Some(tsf) = ts_family {
                    info!("Starting compaction on ts_family {}", ts_family_id);
                    let start = Instant::now();
                    let compact_req = tsf.read().pick_compaction();
                    if let Some(req) = compact_req {
                        let database = req.database.clone();
                        let compact_ts_family = req.ts_family_id;
                        let out_level = req.out_level;
                        match compaction::run_compaction_job(req, ctx.clone()) {
                            Ok(Some(version_edit)) => {
                                incr_compaction_success();
                                let (summary_tx, summary_rx) = oneshot::channel();
                                let ret = summary_task_sender.send(SummaryTask {
                                    edits: vec![version_edit],
                                    cb: summary_tx,
                                });
                                sample_tskv_compaction_duration(
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
                                incr_compaction_failed();
                                error!("Compaction job failed: {}", e);
                            }
                        }
                    }
                }
            }
        });
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
    pub fn compact(&self, tenant: &str, database: &str) {
        let database = self.version_set.read().get_db(tenant, database);
        if let Some(db) = database {
            // TODO: stop current and prevent next flush and compaction.
            for (ts_family_id, ts_family) in db.read().ts_families() {
                let compact_req = ts_family.read().pick_compaction();
                if let Some(req) = compact_req {
                    match compaction::run_compaction_job(req, self.global_ctx.clone()) {
                        Ok(Some(version_edit)) => {
                            let (summary_tx, summary_rx) = oneshot::channel();
                            let ret = self.summary_task_sender.send(SummaryTask {
                                edits: vec![version_edit],
                                cb: summary_tx,
                            });

                            // let _ = summary_rx.await;
                        }
                        Ok(None) => {
                            info!("There is nothing to compact.");
                        }
                        Err(e) => {
                            error!("Compaction job failed: {}", e);
                        }
                    }
                }
            }
        }
    }

    pub fn get_db(&self, tenant: &str, database: &str) -> Result<Arc<RwLock<Database>>> {
        let db = self
            .version_set
            .read()
            .get_db(tenant, database)
            .ok_or(DatabaseNotFound {
                database: database.to_string(),
            })?;
        Ok(db)
    }
}

#[async_trait::async_trait]
impl Engine for TsKv {
    async fn write(
        &self,
        id: TseriesFamilyId,
        tenant_name: &str,
        write_batch: WritePointsRpcRequest,
    ) -> Result<WritePointsRpcResponse> {
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = String::from_utf8(fb_points.db().unwrap().to_vec())
            .map_err(|err| Error::ErrCharacterSet)?;

        let db_warp = self.version_set.read().get_db(tenant_name, &db_name);
        let db = match db_warp {
            Some(database) => database,
            None => self.version_set.write().create_db(
                DatabaseSchema::new(tenant_name, &db_name),
                self.meta_manager.clone(),
            )?,
        };
        let write_group = db.read().build_write_group(fb_points.points().unwrap())?;

        let mut seq = 0;
        if self.options.wal.enabled {
            let (cb, rx) = oneshot::channel();
            let mut enc_points = Vec::new();
            let coder = get_str_codec(Encoding::Zstd);
            coder
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

        let opt_tsf = db.read().get_tsfamily(id);
        let tsf = match opt_tsf {
            Some(v) => v,
            None => db.write().add_tsfamily(
                id,
                seq,
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
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
        tenant_name: &str,
        write_batch: WritePointsRpcRequest,
        seq: u64,
    ) -> Result<WritePointsRpcResponse> {
        let points = Arc::new(write_batch.points);
        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(error::InvalidFlatbufferSnafu)?;

        let db_name = String::from_utf8(fb_points.db().unwrap().to_vec())
            .map_err(|err| Error::ErrCharacterSet)?;

        let db = self.version_set.write().create_db(
            DatabaseSchema::new(tenant_name, &db_name),
            self.meta_manager.clone(),
        )?;

        let write_group = db.read().build_write_group(fb_points.points().unwrap())?;

        let opt_tsf = db.read().get_tsfamily(id);
        let tsf = match opt_tsf {
            Some(v) => v,
            None => db.write().add_tsfamily(
                id,
                seq,
                self.summary_task_sender.clone(),
                self.flush_task_sender.clone(),
            ),
        };

        tsf.read().put_points(seq, write_group);

        return Ok(WritePointsRpcResponse {
            version: 1,
            points: vec![],
        });
    }

    fn create_database(&self, schema: &DatabaseSchema) -> Result<()> {
        if self
            .version_set
            .read()
            .db_exists(schema.tenant_name(), schema.database_name())
        {
            return Err(Error::DatabaseAlreadyExists {
                database: schema.database_name().to_string(),
            });
        }
        self.version_set
            .write()
            .create_db(schema.clone(), self.meta_manager.clone())?;
        Ok(())
    }

    fn alter_database(&self, schema: &DatabaseSchema) -> Result<()> {
        let db = self
            .version_set
            .write()
            .get_db(schema.tenant_name(), schema.database_name())
            .ok_or(DatabaseNotFound {
                database: schema.database_name().to_string(),
            })?;
        db.write().alter_db_schema(schema.clone())?;
        Ok(())
    }

    fn list_databases(&self) -> Result<Vec<String>> {
        let version_set = self.version_set.read();
        let dbs = version_set.get_all_db();

        let mut db = Vec::new();
        for (name, _) in dbs.iter() {
            db.push(name.clone())
        }

        Ok(db)
    }

    fn list_tables(&self, tenant_name: &str, database: &str) -> Result<Vec<String>> {
        if let Some(db) = self.version_set.read().get_db(tenant_name, database) {
            Ok(db.read().get_schemas().list_tables())
        } else {
            error!("Database {}, not found", database);
            Err(Error::DatabaseNotFound {
                database: database.to_string(),
            })
        }
    }

    fn get_db_schema(&self, tenant: &str, database: &str) -> Option<DatabaseSchema> {
        self.version_set.read().get_db_schema(tenant, database)
    }

    fn drop_database(&self, tenant: &str, database: &str) -> Result<()> {
        if let Some(db) = self.version_set.write().delete_db(tenant, database) {
            let mut db_wlock = db.write();
            let ts_family_ids: Vec<TseriesFamilyId> = db_wlock
                .ts_families()
                .iter()
                .map(|(tsf_id, tsf)| *tsf_id)
                .collect();
            for ts_family_id in ts_family_ids {
                db_wlock.del_tsfamily(ts_family_id, self.summary_task_sender.clone());
            }
            index_manger(db_wlock.get_index().path())
                .write()
                .remove_db_index(database);
        }

        let idx_dir = self.options.storage.index_dir(database);
        if let Err(e) = std::fs::remove_dir_all(&idx_dir) {
            error!("Failed to remove dir '{}', e: {}", idx_dir.display(), e);
        }
        let db_dir = self.options.storage.database_dir(database);
        if let Err(e) = std::fs::remove_dir_all(&db_dir) {
            error!("Failed to remove dir '{}', e: {}", db_dir.display(), e);
        }

        Ok(())
    }

    fn create_table(&self, schema: &TableSchema) -> Result<()> {
        // todo: remove this
        let schema = match schema {
            TableSchema::TsKvTableSchema(schema) => schema,
            TableSchema::ExternalTableSchema(_) => return Err(Error::Cancel),
        };
        if let Some(db) = self.version_set.write().get_db(&schema.tenant, &schema.db) {
            db.read()
                .get_schemas()
                .create_table(schema)
                .map_err(|e| {
                    error!("failed create database '{}'", e);
                    e
                })
                .context(SchemaSnafu)
        } else {
            error!("Database {}, not found", schema.db);
            Err(Error::DatabaseNotFound {
                database: schema.db.clone(),
            })
        }
    }

    fn drop_table(&self, tenant: &str, database: &str, table: &str) -> Result<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.

        let version_set = self.version_set.clone();
        let database = database.to_string();
        let table = table.to_string();
        let tenant = tenant.to_string();
        let handle = std::thread::spawn(move || {
            database::delete_table_async(tenant, database, table, version_set)
        });
        let recv_ret = match handle.join() {
            Ok(ret) => ret,
            Err(e) => panic::resume_unwind(e),
        };

        // TODO Release global DropTable flag.
        recv_ret
    }

    fn delete_columns(
        &self,
        tenant: &str,
        database: &str,
        series_ids: &[SeriesId],
        field_ids: &[ColumnId],
    ) -> Result<()> {
        let storage_field_ids: Vec<u64> = series_ids
            .iter()
            .flat_map(|sid| field_ids.iter().map(|fid| unite_id(*fid as u64, *sid)))
            .collect();

        if let Some(db) = self.version_set.read().get_db(tenant, database) {
            for (ts_family_id, ts_family) in db.read().ts_families().iter() {
                ts_family.write().delete_columns(&storage_field_ids);

                let version = ts_family.read().super_version();
                for column_file in version
                    .version
                    .column_files(&storage_field_ids, &TimeRange::all())
                {
                    column_file.add_tombstone(&storage_field_ids, &TimeRange::all())?;
                }
            }
        }

        Ok(())
    }

    fn delete_series(
        &self,
        tenant: &str,
        database: &str,
        series_ids: &[SeriesId],
        field_ids: &[ColumnId],
        time_range: &TimeRange,
    ) -> Result<()> {
        let storage_field_ids: Vec<u64> = series_ids
            .iter()
            .flat_map(|sid| field_ids.iter().map(|fid| unite_id(*fid as u64, *sid)))
            .collect();

        if let Some(db) = self.version_set.read().get_db(tenant, database) {
            for (ts_family_id, ts_family) in db.read().ts_families().iter() {
                // TODO Cancel current and prevent next flush or compaction in TseriesFamily provisionally.
                ts_family
                    .write()
                    .delete_series(&storage_field_ids, time_range);

                let version = ts_family.read().super_version();
                for column_file in version.version.column_files(&storage_field_ids, time_range) {
                    column_file.add_tombstone(&storage_field_ids, time_range)?;
                }
                // TODO Start next flush or compaction.
            }
        }
        Ok(())
    }

    fn get_table_schema(
        &self,
        tenant: &str,
        database: &str,
        tab: &str,
    ) -> Result<Option<TableSchema>> {
        if let Some(db) = self.version_set.read().get_db(tenant, database) {
            let val = db.read().get_table_schema(tab)?;
            // todo: remove this
            return match val {
                None => Ok(None),
                Some(schema) => Ok(Some(TableSchema::TsKvTableSchema(schema))),
            };
        }
        Ok(None)
    }

    fn get_series_id_list(
        &self,
        tenant: &str,
        database: &str,
        tab: &str,
        tags: &[Tag],
    ) -> IndexResult<Vec<u64>> {
        if let Some(db) = self.version_set.read().get_db(tenant, database) {
            return db.read().get_index().get_series_id_list(tab, tags);
        }

        Ok(vec![])
    }

    fn get_series_id_by_filter(
        &self,
        tenant: &str,
        database: &str,
        tab: &str,
        filter: &ColumnDomains<String>,
    ) -> IndexResult<Vec<u64>> {
        let result = if let Some(db) = self.version_set.read().get_db(tenant, database) {
            if filter.is_all() {
                // Match all records
                debug!("pushed tags filter is All.");
                db.read().get_index().get_series_id_list(tab, &[])
            } else if filter.is_none() {
                // Does not match any record, return null
                debug!("pushed tags filter is None.");
                Ok(vec![])
            } else {
                // No error will be reported here
                debug!("pushed tags filter is {:?}.", filter);
                let domains = unsafe { filter.domains_unsafe() };
                db.read()
                    .get_index()
                    .get_series_ids_by_domains(tab, domains)
            }
        } else {
            Ok(vec![])
        };

        result
    }

    fn get_series_key(
        &self,
        tenant: &str,
        database: &str,
        sid: u64,
    ) -> IndexResult<Option<SeriesKey>> {
        if let Some(db) = self.version_set.read().get_db(tenant, database) {
            return db.read().get_series_key(sid);
        }

        Ok(None)
    }

    fn get_db_version(
        &self,
        tenant: &str,
        database: &str,
        vnode_id: u32,
    ) -> Result<Option<Arc<SuperVersion>>> {
        let version_set = self.version_set.read();
        if !version_set.db_exists(tenant, database) {
            return Err(Error::DatabaseNotFound {
                database: database.to_string(),
            });
        }
        if let Some(tsf) = version_set.get_tsfamily_by_name_id(tenant, database, vnode_id) {
            Ok(Some(tsf.read().super_version()))
        } else {
            warn!("ts_family with db name '{}' not found.", database);
            Ok(None)
        }
    }

    fn add_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column: TableColumn,
    ) -> Result<()> {
        let db = self.get_db(tenant, database)?;
        db.read().add_table_column(table, column)?;
        Ok(())
    }

    fn drop_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_name: &str,
    ) -> Result<()> {
        let db = self.get_db(tenant, database)?;
        let schema = db
            .read()
            .get_table_schema(table)?
            .ok_or(Error::NotFoundTable {
                table_name: table.to_string(),
            })?;
        let column_id = schema
            .column(column_name)
            .ok_or(Error::NotFoundField {
                reason: column_name.to_string(),
            })?
            .id;
        db.read().drop_table_column(table, column_name)?;
        let sid = db
            .read()
            .get_index()
            .get_series_id_list(table, &[])
            .context(IndexErrSnafu)?;
        self.delete_columns(tenant, database, &sid, &[column_id])?;
        Ok(())
    }

    fn change_table_column(
        &self,
        tenant: &str,
        database: &str,
        table: &str,
        column_name: &str,
        new_column: TableColumn,
    ) -> Result<()> {
        let db = self.get_db(tenant, database)?;
        db.read()
            .change_table_column(table, column_name, new_column)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use config::get_config;
    use flatbuffers::{FlatBufferBuilder, WIPOffset};
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
        let tskv = TsKv::open(
            config.cluster.clone(),
            opt,
            Arc::new(Runtime::new().unwrap()),
        )
        .await
        .unwrap();
        tskv.compact("cnosdb", "public");
    }
}
