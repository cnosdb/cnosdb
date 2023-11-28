use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use models::predicate::domain::{ResolvedPredicate, TimeRange, TimeRanges};
use models::schema::Precision;
use models::utils::unite_id;
use models::{ColumnId, SeriesId, SeriesKey, TagKey, TagValue};
use protos::kv_service::{raft_write_command, WritePointsResponse};
use protos::models as fb_models;
use snafu::ResultExt;
use tokio::sync::RwLock;
use trace::{debug, error, info, warn, SpanContext, SpanExt, SpanRecorder};

use crate::database::Database;
use crate::error::Result;
use crate::index::ts_index::TSIndex;
use crate::schema::error::SchemaError;
use crate::summary::CompactMeta;
use crate::tseries_family::TseriesFamily;
use crate::{
    file_utils, Error, SnapshotFileMeta, TsKvContext, UpdateSetValue, VersionEdit, VnodeSnapshot,
};

#[derive(Clone)]
pub struct VnodeStorage {
    pub ctx: Arc<TsKvContext>,
    pub db: Arc<RwLock<Database>>,
    pub ts_index: Arc<TSIndex>,
    pub ts_family: Arc<RwLock<TseriesFamily>>,
}

impl VnodeStorage {
    pub async fn apply(
        &self,
        ctx: &replication::ApplyContext,
        command: raft_write_command::Command,
    ) -> Result<Vec<u8>> {
        match command {
            raft_write_command::Command::WriteData(cmd) => {
                let precision = Precision::from(cmd.precision as u8);
                self.write(ctx.index, cmd.data, precision, None).await?;
                Ok(vec![])
            }

            raft_write_command::Command::DropTable(cmd) => {
                self.drop_table(&cmd.table).await?;
                Ok(vec![])
            }

            raft_write_command::Command::DropColumn(cmd) => {
                self.drop_table_column(&cmd.table, &cmd.column).await?;
                Ok(vec![])
            }

            raft_write_command::Command::UpdateTags(cmd) => {
                let new_tags = cmd
                    .new_tags
                    .iter()
                    .cloned()
                    .map(|protos::kv_service::UpdateSetValue { key, value }| {
                        crate::UpdateSetValue { key, value }
                    })
                    .collect::<Vec<_>>();

                let mut series = Vec::with_capacity(cmd.matched_series.len());
                for key in cmd.matched_series.iter() {
                    let ss = SeriesKey::decode(key).map_err(|_| Error::InvalidParam {
                        reason:
                            "Deserialize 'matched_series' of 'UpdateTagsRequest' failed, expected: SeriesKey"
                                .to_string(),
                    })?;
                    series.push(ss);
                }

                self.update_tags_value(&new_tags, &series, cmd.dry_run)
                    .await?;
                Ok(vec![])
            }

            raft_write_command::Command::DeleteFromTable(cmd) => {
                let predicate =
                    bincode::deserialize::<ResolvedPredicate>(&cmd.predicate).map_err(|err| {
                        Error::InvalidParam {
                            reason: format!(
                                "Predicate of delete_from_table is invalid, error: {err}"
                            ),
                        }
                    })?;

                self.delete_from_table(&cmd.table, &predicate).await?;
                Ok(vec![])
            }
        }
    }

    async fn write(
        &self,
        index: u64,
        points: Vec<u8>,
        precision: Precision,
        span_ctx: Option<&SpanContext>,
    ) -> Result<WritePointsResponse> {
        let span_recorder = SpanRecorder::new(span_ctx.child_span("tskv engine write cache"));

        let fb_points = flatbuffers::root::<fb_models::Points>(&points)
            .context(crate::error::InvalidFlatbufferSnafu)?;

        let tables = fb_points.tables().ok_or(Error::InvalidPointTable)?;

        let write_group = {
            let mut span_recorder = span_recorder.child("build write group");
            self.db
                .read()
                .await
                .build_write_group(precision, tables, self.ts_index.clone(), false, None)
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        let res = {
            let mut span_recorder = span_recorder.child("put points");
            match self.ts_family.read().await.put_points(index, write_group) {
                Ok(points_number) => Ok(WritePointsResponse { points_number }),
                Err(err) => {
                    span_recorder.error(err.to_string());
                    Err(err)
                }
            }
        };

        let sender = self.ctx.flush_task_sender.clone();
        self.ts_family.write().await.check_to_flush(sender).await;

        res
    }

    pub async fn drop_table(&self, table: &str) -> Result<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.
        let db_owner = self.db.read().await.owner();
        let vnode_id = self.ts_family.read().await.tf_id();

        let schemas = self.db.read().await.get_schemas();
        if let Some(fields) = schemas.get_table_schema(table)? {
            let column_ids: Vec<ColumnId> = fields.columns().iter().map(|f| f.id).collect();
            info!(
                "Drop table: deleting {} columns in table: {db_owner}.{table}",
                column_ids.len()
            );

            let series_ids = self.ts_index.get_series_id_list(table, &[]).await?;
            self.ts_family
                .write()
                .await
                .delete_series(&series_ids, &TimeRange::all());

            let field_ids: Vec<u64> = series_ids
                .iter()
                .flat_map(|sid| column_ids.iter().map(|fid| unite_id(*fid, *sid)))
                .collect();
            info!(
                "Drop table: vnode {vnode_id} deleting {} fields in table: {db_owner}.{table}",
                field_ids.len()
            );

            let version = self.ts_family.read().await.super_version();
            version
                .add_tsm_tombstone(&field_ids, &TimeRanges::all())
                .await?;

            info!(
                "Drop table: index {vnode_id} deleting {} fields in table: {db_owner}.{table}",
                series_ids.len()
            );

            for sid in series_ids {
                self.ts_index.del_series_info(sid).await?;
            }
        }

        Ok(())
    }

    pub async fn drop_table_column(&self, table: &str, column_name: &str) -> Result<()> {
        let db_name = self.db.read().await.db_name();
        let schema = self
            .db
            .read()
            .await
            .get_table_schema(table)?
            .ok_or_else(|| SchemaError::TableNotFound {
                database: db_name.to_string(),
                table: table.to_string(),
            })?;

        let column_id = schema
            .column(column_name)
            .ok_or_else(|| SchemaError::FieldNotFound {
                database: db_name.to_string(),
                table: table.to_string(),
                field: column_name.to_string(),
            })?
            .id;

        self.drop_table_columns(table, &[column_id]).await?;

        Ok(())
    }

    async fn update_tags_value(
        &self,
        new_tags: &[UpdateSetValue<TagKey, TagValue>],
        matched_series: &[SeriesKey],
        dry_run: bool,
    ) -> Result<()> {
        // 准备数据
        // 获取待更新的 series key，更新后的 series key 及其对应的 series id
        let (old_series_keys, new_series_keys, sids) = self
            .ts_index
            .prepare_update_tags_value(new_tags, matched_series)
            .await?;

        if dry_run {
            return Ok(());
        }

        // 更新索引
        if let Err(err) = self
            .ts_index
            .update_series_key(old_series_keys, new_series_keys, sids, false)
            .await
        {
            error!(
                "Update tags value tag of TSIndex({}): {}",
                self.ts_index.path().display(),
                err
            );

            return Err(crate::error::Error::IndexErr { source: err });
        }

        Ok(())
    }

    async fn delete_from_table(&self, table: &str, predicate: &ResolvedPredicate) -> Result<()> {
        let tag_domains = predicate.tags_filter();
        let time_ranges = predicate.time_ranges();

        let vnode_id = self.ts_family.read().await.tf_id();
        let series_ids = {
            let db = self.db.read().await;

            let table_schema = match db.get_table_schema(table)? {
                None => return Ok(()),
                Some(schema) => schema,
            };

            let vnode_index = match db.get_ts_index(vnode_id) {
                Some(vnode) => vnode,
                None => return Ok(()),
            };
            drop(db);

            vnode_index
                .get_series_ids_by_domains(table_schema.as_ref(), tag_domains)
                .await?
        };

        // 执行delete，删除缓存 & 写墓碑文件
        self.delete(table, &series_ids, &time_ranges).await
    }

    /// Flush caches into TSM file, create a new Version of the Vnode, then:
    /// 1. Make hard links point to all TSM files in the Version in snapshot directory,
    /// 2. Copy series index in Vnode into snapshot directory,
    /// 3. Save current Version as a summary file in snapshot directory.
    /// Then return VnodeSnapshot.
    ///
    /// For one Vnode, multi snapshot may exist at a time.
    pub async fn create_snapshot(&self) -> Result<VnodeSnapshot> {
        let vnode_id = self.ts_family.read().await.tf_id();
        debug!("Snapshot: create snapshot on vnode: {vnode_id}");

        let (vnode_optional, vnode_index_optional) = self
            .ctx
            .version_set
            .read()
            .await
            .get_tsfamily_tsindex_by_tf_id(vnode_id)
            .await;
        if let Some(vnode) = vnode_optional {
            // Get snapshot directory.
            let storage_opt = self.ctx.options.storage.clone();
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
                    if let Some(ve_flushed_files) = crate::compaction::run_flush_memtable_job(
                        flush_req,
                        self.ctx.clone(),
                        false,
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
                        crate::error::CreateFileSnafu { path: dir.clone() }
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
                    if let Err(e) = std::fs::hard_link(&file_path, &snapshot_path)
                        .context(crate::error::IOSnafu)
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

            let (tenant, database) = models::schema::split_owner(tenant_database.as_str());
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

    /// Build a new Vnode from the VersionSnapshot, existing Vnode with the same VnodeId
    /// will be deleted.
    pub async fn apply_snapshot(&self, snapshot: VnodeSnapshot) -> Result<()> {
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
        let tenant_database = models::schema::make_owner(&tenant, &database);
        let storage_opt = self.ctx.options.storage.clone();

        let mut db_wlock = self.db.write().await;
        if db_wlock.get_tsfamily(vnode_id).is_some() {
            warn!("Snapshot: removing existing vnode {vnode_id}.");
            db_wlock
                .del_tsfamily(vnode_id, self.ctx.summary_task_sender.clone())
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
            .add_tsfamily(vnode_id, Some(version_edit), self.ctx.clone())
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

    /// Delete the snapshot directory of a Vnode, all snapshots will be deleted.
    pub async fn delete_snapshot(&self) -> Result<()> {
        let vnode_id = self.ts_family.read().await.tf_id();
        debug!("Snapshot: create snapshot on vnode: {vnode_id}");
        let vnode_optional = self
            .ctx
            .version_set
            .read()
            .await
            .get_tsfamily_by_tf_id(vnode_id)
            .await;
        if let Some(vnode) = vnode_optional {
            let tenant_database = vnode.read().await.tenant_database();
            let storage_opt = self.ctx.options.storage.clone();
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
                crate::error::DeleteFileSnafu { path: snapshot_dir }
            })?;
            Ok(())
        } else {
            // Vnode not found
            warn!("Snapshot: vnode {vnode_id} not found.");
            Err(Error::VnodeNotFound { vnode_id })
        }
    }

    async fn drop_table_columns(&self, table: &str, column_ids: &[ColumnId]) -> Result<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.
        let db_rlock = self.db.read().await;
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

            let time_ranges = TimeRanges::all();
            for (ts_family_id, ts_family) in db_rlock.ts_families().iter() {
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
                    version.add_tsm_tombstone(&field_ids, &time_ranges).await?;
                } else {
                    continue;
                }
            }
        }

        Ok(())
    }

    pub async fn delete(
        &self,
        table: &str,
        series_ids: &[SeriesId],
        time_ranges: &TimeRanges,
    ) -> Result<()> {
        let vnode = self.ts_family.read().await;
        let db_name = self.db.read().await.db_name();
        vnode.delete_series_by_time_ranges(series_ids, time_ranges);

        let vnode_id = self.ts_family.read().await.tf_id();
        let column_ids = self
            .db
            .read()
            .await
            .get_table_schema(table)?
            .ok_or_else(|| SchemaError::TableNotFound {
                database: db_name.to_string(),
                table: table.to_string(),
            })?
            .column_ids();

        let field_ids = series_ids
            .iter()
            .flat_map(|sid| column_ids.iter().map(|fid| unite_id(*fid, *sid)))
            .collect::<Vec<_>>();

        trace::debug!(
            "Drop table: vnode {vnode_id} deleting {} fields in table: {table}",
            field_ids.len()
        );

        let version = vnode.super_version();

        // Stop compaction when doing delete TODO

        version.add_tsm_tombstone(&field_ids, time_ranges).await?;

        Ok(())
    }
}
