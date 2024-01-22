use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use models::meta_data::VnodeId;
use models::predicate::domain::{ResolvedPredicate, TimeRange, TimeRanges};
use models::schema::Precision;
use models::{ColumnId, SeriesId, SeriesKey};
use protos::kv_service::{raft_write_command, WritePointsResponse, *};
use snafu::ResultExt;
use tokio::sync::RwLock;
use trace::{debug, error, info, warn, SpanContext, SpanExt, SpanRecorder};

use crate::compaction::run_flush_memtable_job;
use crate::database::Database;
use crate::error::Result;
use crate::file_system::file_info;
use crate::index::ts_index::TSIndex;
use crate::schema::error::SchemaError;
use crate::tseries_family::TseriesFamily;
use crate::{Error, TsKvContext, VnodeSnapshot};

#[derive(Clone)]
pub struct VnodeStorage {
    pub id: VnodeId,
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
                if let Err(err) = self.write(ctx, cmd.data, precision, None).await {
                    if ctx.apply_type == replication::APPLY_TYPE_WAL {
                        info!("recover: write points: {}", err);
                    } else {
                        return Err(err);
                    }
                }

                Ok(vec![])
            }

            raft_write_command::Command::DropTable(cmd) => {
                self.drop_table(&cmd.table).await?;
                Ok(vec![])
            }

            raft_write_command::Command::DropColumn(cmd) => {
                if let Err(err) = self.drop_table_column(&cmd.table, &cmd.column).await {
                    if ctx.apply_type == replication::APPLY_TYPE_WAL {
                        info!("recover: drop column: {}", err);
                    } else {
                        return Err(err);
                    }
                }
                Ok(vec![])
            }

            raft_write_command::Command::UpdateTags(cmd) => {
                self.update_tags_value(ctx, &cmd).await?;
                Ok(vec![])
            }

            raft_write_command::Command::DeleteFromTable(cmd) => {
                self.delete_from_table(&cmd).await?;
                Ok(vec![])
            }
        }
    }

    async fn write(
        &self,
        ctx: &replication::ApplyContext,
        points: Vec<u8>,
        precision: Precision,
        span_ctx: Option<&SpanContext>,
    ) -> Result<WritePointsResponse> {
        let span_recorder = SpanRecorder::new(span_ctx.child_span("tskv engine write cache"));
        let fb_points = flatbuffers::root::<protos::models::Points>(&points)
            .context(crate::error::InvalidFlatbufferSnafu)?;
        let tables = fb_points.tables().ok_or(Error::InvalidPointTable)?;

        let (mut recover_from_wal, mut strict_write) = (false, None);
        if ctx.apply_type == replication::APPLY_TYPE_WAL {
            (recover_from_wal, strict_write) = (true, Some(true));
        }

        let write_group = {
            let mut span_recorder = span_recorder.child("build write group");
            self.db
                .read()
                .await
                .build_write_group(
                    precision,
                    tables,
                    self.ts_index.clone(),
                    recover_from_wal,
                    strict_write,
                )
                .await
                .map_err(|err| {
                    span_recorder.error(err.to_string());
                    err
                })?
        };

        let res = {
            let mut span_recorder = span_recorder.child("put points");
            match self
                .ts_family
                .read()
                .await
                .put_points(ctx.index, write_group)
            {
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

    async fn drop_table(&self, table: &str) -> Result<()> {
        // TODO Create global DropTable flag for droping the same table at the same time.
        let db_owner = self.db.read().await.owner();
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

            info!(
                "Drop table: vnode {} deleting {} fields in table: {db_owner}.{table}",
                self.id,
                column_ids.len() * series_ids.len()
            );

            let version = self.ts_family.read().await.super_version();
            version
                .add_tombstone(&series_ids, &column_ids, &TimeRange::all())
                .await?;

            info!(
                "Drop table: index {} deleting {} fields in table: {db_owner}.{table}",
                self.id,
                series_ids.len()
            );

            for sid in series_ids {
                self.ts_index.del_series_info(sid).await?;
            }
        }

        Ok(())
    }

    async fn drop_table_column(&self, table: &str, column_name: &str) -> Result<()> {
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

    /// Update the value of the tag type columns of the specified table
    ///
    /// `new_tags` is the new tags, and the tag key must be included in all series
    ///
    /// # Parameters
    /// - `tenant` - The tenant name.
    /// - `database` - The database name.
    /// - `new_tags` - The tags and its new tag value.
    /// - `matched_series` - The series that need to be updated.
    /// - `dry_run` - Whether to only check if the `update_tags_value` is successful, if it is true, the update will not be performed.
    ///
    /// # Examples
    ///
    /// We have a table `tbl` as follows
    ///
    /// ```text
    /// +----+-----+-----+-----+
    /// | ts | tag1| tag2|field|
    /// +----+-----+-----+-----+
    /// | 1  | t1a | t2b | f1  |
    /// +----+-----+-----+-----+
    /// | 2  | t1a | t2c | f2  |
    /// +----+-----+-----+-----+
    /// | 3  | t1b | t2c | f3  |
    /// +----+-----+-----+-----+
    /// ```
    ///
    /// Execute the following update statement
    ///
    /// ```sql
    /// UPDATE tbl SET tag1 = 't1c' WHERE tag2 = 't2c';
    /// ```
    ///
    /// The `new_tags` is `[tag1 = 't1c']`, and the `matched_series` is `[(tag1 = 't1a', tag2 = 't2c'), (tag1 = 't1b', tag2 = 't2c')]`
    ///
    /// TODO Specify vnode id
    async fn update_tags_value(
        &self,
        ctx: &replication::ApplyContext,
        cmd: &UpdateTagsRequest,
    ) -> Result<()> {
        let new_tags = cmd
            .new_tags
            .iter()
            .cloned()
            .map(
                |protos::kv_service::UpdateSetValue { key, value }| crate::UpdateSetValue {
                    key,
                    value,
                },
            )
            .collect::<Vec<_>>();

        let mut series = Vec::with_capacity(cmd.matched_series.len());
        for key in cmd.matched_series.iter() {
            let ss = SeriesKey::decode(key).map_err(|_| {
                Error::InvalidParam {
            reason:
                "Deserialize 'matched_series' of 'UpdateTagsRequest' failed, expected: SeriesKey"
                    .to_string(),
        }
            })?;
            series.push(ss);
        }

        // 准备数据
        // 获取待更新的 series key，更新后的 series key 及其对应的 series id
        let mut check_conflict = true;
        if ctx.apply_type == replication::APPLY_TYPE_WAL {
            check_conflict = false;
        }
        let (old_series_keys, new_series_keys, sids) = self
            .ts_index
            .prepare_update_tags_value(&new_tags, &series, check_conflict)
            .await?;

        if cmd.dry_run {
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

    async fn delete_from_table(&self, cmd: &DeleteFromTableRequest) -> Result<()> {
        let predicate =
            bincode::deserialize::<ResolvedPredicate>(&cmd.predicate).map_err(|err| {
                Error::InvalidParam {
                    reason: format!("Predicate of delete_from_table is invalid, error: {err}"),
                }
            })?;

        let tag_domains = predicate.tags_filter();
        let series_ids = {
            let table_schema = match self.db.read().await.get_table_schema(&cmd.table)? {
                None => return Ok(()),
                Some(schema) => schema,
            };

            self.ts_index
                .get_series_ids_by_domains(table_schema.as_ref(), tag_domains)
                .await?
        };

        // 执行delete，删除缓存 & 写墓碑文件
        let time_ranges = predicate.time_ranges();
        self.delete(&cmd.table, &series_ids, &time_ranges).await
    }

    /// Flush caches into TSM file, create a new Version of the Vnode, then:
    /// 1. Make hard links point to all TSM files in the Version in snapshot directory,
    /// 2. Copy series index in Vnode into snapshot directory,
    /// 3. Save current Version as a summary file in snapshot directory.
    /// Then return VnodeSnapshot.
    ///
    /// For one Vnode, multi snapshot may exist at a time.
    pub async fn create_snapshot(&self) -> Result<VnodeSnapshot> {
        debug!("Snapshot: create snapshot on vnode: {}", self.id);
        let vnode_id = self.id;

        // Get snapshot directory.
        let opt = self.ctx.options.storage.clone();
        let owner = self.ts_family.read().await.tenant_database();

        let time_str = chrono::Local::now().format("%Y%m%d_%H%M%S_%3f").to_string();
        let snapshot_id = format!("snap_{}_{}", vnode_id, time_str);
        let snapshot_dir = opt.snapshot_sub_dir(owner.as_str(), vnode_id, &snapshot_id);
        let _ = std::fs::remove_dir_all(&snapshot_dir);

        let (flush_req_optional, mut ve_summary_snapshot) = {
            let mut vnode_wlock = self.ts_family.write().await;
            vnode_wlock.switch_to_immutable();
            let flush_req_optional = vnode_wlock.build_flush_req(true);

            let mut _file_metas = HashMap::new();
            let ve_summary_snapshot = vnode_wlock.build_version_edit(&mut _file_metas);
            (flush_req_optional, ve_summary_snapshot)
        };

        // Run force flush
        let mut last_seq_no = 0;
        if let Some(flush_req) = flush_req_optional {
            last_seq_no = flush_req.high_seq_no;
            if let Some(flushed_ve) =
                run_flush_memtable_job(flush_req, self.ctx.clone(), false).await?
            {
                // Normally flushed, and generated some tsm/delta files.
                debug!("Snapshot: flush vnode {vnode_id} succeed.");
                ve_summary_snapshot.add_files.extend(flushed_ve.add_files);
            } else {
                // Flushed but not generate any file.
                warn!("Snapshot: flush vnode {vnode_id} did not generated any file.");
            }
        };

        // Copy index directory.
        let index_dir = opt.index_dir(owner.as_str(), vnode_id);
        let snap_index_dir = opt.snapshot_index_dir(owner.as_str(), vnode_id, &snapshot_id);
        if let Err(e) = self.ts_index.flush().await {
            error!("Snapshot: failed to flush vnode index: {e}.");
            return Err(Error::IndexErr { source: e });
        }
        if let Err(e) = dircpy::copy_dir(&index_dir, &snap_index_dir) {
            error!(
                "Snapshot: failed to copy index dir {:?} to {:?}: {e}",
                index_dir, snap_index_dir
            );
            return Err(Error::IO { source: e });
        }

        // Do snapshot, tsm file system operations.
        self.ts_family
            .read()
            .await
            .backup(&ve_summary_snapshot, &snapshot_id)
            .await?;

        let mut files_info = file_info::get_files_info(&snapshot_dir).await?;
        for info in files_info.iter_mut() {
            if let Ok(value) = PathBuf::from(&info.name).strip_prefix(&snapshot_dir) {
                info.name = value.to_string_lossy().to_string();
            } else {
                error!("strip snapshot file failed: {}", info.name);
            }
        }

        let snapshot = VnodeSnapshot {
            snapshot_id,
            vnode_id,
            last_seq_no,
            files_info,
            node_id: opt.node_id,
            //tsm_files: backup_tsm_files,
            version_edit: ve_summary_snapshot,
        };
        debug!("Snapshot: created snapshot: {snapshot:?}");

        Ok(snapshot)
    }

    /// Build a new Vnode from the VersionSnapshot, existing Vnode with the same VnodeId
    /// will be deleted.
    pub async fn apply_snapshot(
        &mut self,
        snapshot: VnodeSnapshot,
        shapshot_dir: &PathBuf,
    ) -> Result<()> {
        debug!("Snapshot: apply snapshot {snapshot:?} to create new vnode.");

        let vnode_id = self.id;
        let owner = self.ts_family.read().await.tenant_database();
        let storage_opt = self.ctx.options.storage.clone();

        // delete already exist data
        let mut db_wlock = self.db.write().await;
        let summary_sender = self.ctx.summary_task_sender.clone();
        db_wlock.del_tsfamily(vnode_id, summary_sender).await;
        db_wlock.del_ts_index(vnode_id);
        let vnode_dir = storage_opt.ts_family_dir(&owner, vnode_id);
        let _ = std::fs::remove_dir_all(&vnode_dir);

        // move snashot data to vnode move dir
        let move_dir = storage_opt.move_dir(&owner, vnode_id);
        info!("apply snapshot rename {:?} -> {:?}", shapshot_dir, move_dir);
        tokio::fs::create_dir_all(&move_dir).await?;
        tokio::fs::rename(&shapshot_dir, &move_dir).await?;

        // apply data and reopen
        let mut version_edit = snapshot.version_edit.clone();
        version_edit.update_vnode_id(vnode_id);
        let ts_family = db_wlock
            .add_tsfamily(vnode_id, Some(version_edit), self.ctx.clone())
            .await?;
        let ts_index = db_wlock.get_ts_index_or_add(vnode_id).await?;

        self.ts_index = ts_index;
        self.ts_family = ts_family;

        Ok(())
    }

    /// Delete the snapshot directory of a Vnode, all snapshots will be deleted.
    pub async fn delete_snapshot(&self) -> Result<()> {
        debug!("Snapshot: delete snapshot on vnode: {}", self.id);

        let owner = self.ts_family.read().await.tenant_database();
        let storage_opt = self.ctx.options.storage.clone();

        let snapshot_dir = storage_opt.snapshot_dir(owner.as_str(), self.id);
        debug!("Snapshot: removing snapshot directory {:?}", snapshot_dir);
        std::fs::remove_dir_all(&snapshot_dir).with_context(|_| {
            error!(
                "Snapshot: failed to remove snapshot directory {:?}",
                snapshot_dir
            );
            crate::error::DeleteFileSnafu { path: snapshot_dir }
        })?;
        Ok(())
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

            let time_range = TimeRange::all();
            let series_ids = self.ts_index.get_series_id_list(table, &[]).await?;
            info!(
                "drop table column: vnode: {} deleting {} fields in table: {db_owner}.{table}",
                self.id,
                series_ids.len() * to_drop_column_ids.len()
            );

            self.ts_family
                .write()
                .await
                .drop_columns(&series_ids, &to_drop_column_ids);
            let version = self.ts_family.read().await.super_version();
            version
                .add_tombstone(&series_ids, &to_drop_column_ids, &time_range)
                .await?;
        }

        Ok(())
    }

    async fn delete(
        &self,
        table: &str,
        series_ids: &[SeriesId],
        time_ranges: &TimeRanges,
    ) -> Result<()> {
        let vnode = self.ts_family.read().await;
        let db_name = self.db.read().await.db_name();
        vnode.delete_series_by_time_ranges(series_ids, time_ranges);

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

        let version = vnode.super_version();

        // Stop compaction when doing delete TODO

        for time_range in time_ranges.time_ranges() {
            version
                .add_tombstone(series_ids, &column_ids, time_range)
                .await?;
        }

        Ok(())
    }
}
