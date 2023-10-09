use std::borrow::Cow;
use std::collections::{HashMap, LinkedList};
use std::mem::size_of;
use std::sync::Arc;

use flatbuffers::{ForwardsUOffset, Vector};
use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::predicate::domain::TimeRange;
use models::schema::{DatabaseSchema, Precision, TskvTableSchema, TskvTableSchemaRef};
use models::{SeriesId, SeriesKey};
use protos::models::{Column, ColumnType, FieldType, Table};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, RwLock};
use trace::{error, info};
use utils::BloomFilter;

use crate::compaction::CompactTask;
use crate::error::{Result, SchemaSnafu};
use crate::index::{self, IndexResult};
use crate::kv_option::{Options, INDEX_PATH};
use crate::memcache::{RowData, RowGroup};
use crate::schema::schemas::DBschemas;
use crate::summary::{SummaryTask, VersionEdit};
use crate::tseries_family::{
    schedule_vnode_compaction, LevelInfo, TseriesFamily, TsfFactory, Version,
};
use crate::{file_utils, ColumnFileId, Error, TsKvContext, TseriesFamilyId};

pub type FlatBufferTable<'a> = flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Table<'a>>>;

#[derive(Debug)]
pub struct Database {
    //tenant_name.database_name => owner
    owner: Arc<String>,
    opt: Arc<Options>,
    runtime: Arc<Runtime>,
    db_name: Arc<String>,

    schemas: Arc<DBschemas>,
    ts_indexes: HashMap<TseriesFamilyId, Arc<index::ts_index::TSIndex>>,
    ts_families: HashMap<TseriesFamilyId, Arc<RwLock<TseriesFamily>>>,
    tsf_factory: TsfFactory,
}

#[derive(Debug)]
pub struct DatabaseFactory {
    meta: MetaRef,
    memory_pool: MemoryPoolRef,
    metrics_register: Arc<MetricsRegister>,
    opt: Arc<Options>,
    runtime: Arc<Runtime>,
}

impl DatabaseFactory {
    pub fn new(
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            meta,
            memory_pool,
            metrics_register,
            opt,
            runtime,
        }
    }

    pub async fn create_database(&self, schema: DatabaseSchema) -> Result<Database> {
        Database::new(
            schema,
            self.opt.clone(),
            self.runtime.clone(),
            self.meta.clone(),
            self.memory_pool.clone(),
            self.metrics_register.clone(),
        )
        .await
    }
}

impl Database {
    pub async fn new(
        schema: DatabaseSchema,
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<Self> {
        let owner = Arc::new(schema.owner());
        let tsf_factory = TsfFactory::new(
            owner.clone(),
            opt.clone(),
            memory_pool.clone(),
            metrics_register.clone(),
        );

        let db = Self {
            opt,
            runtime,

            owner: Arc::new(schema.owner()),
            db_name: Arc::new(schema.database_name().to_owned()),
            schemas: Arc::new(DBschemas::new(schema, meta).await.context(SchemaSnafu)?),
            ts_indexes: HashMap::new(),
            ts_families: HashMap::new(),
            tsf_factory,
        };

        Ok(db)
    }

    pub fn open_tsfamily(
        &mut self,
        runtime: Arc<Runtime>,
        ver: Arc<Version>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let tf_id = ver.tf_id();
        let tf = self.tsf_factory.create_tsf(tf_id, ver.clone());

        let tf_ref = Arc::new(RwLock::new(tf));
        schedule_vnode_compaction(runtime, tf_ref.clone(), compact_task_sender);

        self.ts_families.insert(ver.tf_id(), tf_ref);
    }

    // todo: Maybe TseriesFamily::new() should be refactored.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_tsfamily(
        &mut self,
        tsf_id: TseriesFamilyId,
        version_edit: Option<VersionEdit>,
        ctx: Arc<TsKvContext>,
    ) -> Result<Arc<RwLock<TseriesFamily>>> {
        let new_version_edit_seq_no = 0;
        let (seq_no, version_edits, file_metas) = match version_edit {
            Some(mut ve) => {
                ve.tsf_id = tsf_id;
                ve.has_seq_no = true;
                ve.seq_no = new_version_edit_seq_no;
                let mut file_metas = HashMap::with_capacity(ve.add_files.len());
                for f in ve.add_files.iter_mut() {
                    let new_file_id = ctx.global_ctx.file_id_next();
                    f.tsf_id = tsf_id;
                    let file_path = f
                        .rename_file(&self.opt.storage, &self.owner, f.tsf_id, new_file_id)
                        .await?;
                    let file_reader = crate::tsm::TsmReader::open(file_path).await?;
                    file_metas.insert(new_file_id, file_reader.bloom_filter());
                }
                for f in ve.del_files.iter_mut() {
                    let new_file_id = ctx.global_ctx.file_id_next();
                    f.tsf_id = tsf_id;
                    f.rename_file(&self.opt.storage, &self.owner, f.tsf_id, new_file_id)
                        .await?;
                }
                //move index
                let origin_index = self
                    .opt
                    .storage
                    .move_dir(&self.owner, tsf_id)
                    .join(INDEX_PATH);
                let new_index = self.opt.storage.index_dir(&self.owner, tsf_id);
                info!("move index from {:?} to {:?}", &origin_index, &new_index);
                file_utils::rename(origin_index, new_index).await?;
                tokio::fs::remove_dir_all(self.opt.storage.move_dir(&self.owner, tsf_id)).await?;
                (ve.seq_no, vec![ve], Some(file_metas))
            }
            None => (
                new_version_edit_seq_no,
                vec![VersionEdit::new_add_vnode(
                    tsf_id,
                    self.owner.as_ref().clone(),
                    new_version_edit_seq_no,
                )],
                None,
            ),
        };

        let cache =
            cache::ShardedAsyncCache::create_lru_sharded_cache(self.opt.storage.max_cached_readers);

        let ver = Arc::new(Version::new(
            tsf_id,
            self.owner.clone(),
            self.opt.storage.clone(),
            seq_no,
            LevelInfo::init_levels(self.owner.clone(), tsf_id, self.opt.storage.clone()),
            i64::MIN,
            cache.into(),
        ));

        let tf = self.tsf_factory.create_tsf(tsf_id, ver.clone());
        let tf = Arc::new(RwLock::new(tf));
        if let Some(tsf) = self.ts_families.get(&tsf_id) {
            return Ok(tsf.clone());
        }
        schedule_vnode_compaction(
            self.runtime.clone(),
            tf.clone(),
            ctx.compact_task_sender.clone(),
        );
        self.ts_families.insert(tsf_id, tf.clone());

        let (task_state_sender, _task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new(version_edits, file_metas, None, task_state_sender);
        if let Err(e) = ctx.summary_task_sender.send(task).await {
            error!("failed to send Summary task, {:?}", e);
        }

        Ok(tf)
    }

    pub async fn del_tsfamily(&mut self, tf_id: u32, summary_task_sender: Sender<SummaryTask>) {
        if let Some(tf) = self.ts_families.remove(&tf_id) {
            tf.read().await.close();
        }

        // TODO(zipper): If no ts_family recovered from summary, do not write summary.
        let edits = vec![VersionEdit::new_del_vnode(tf_id)];
        let (task_state_sender, _task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new(edits, None, None, task_state_sender);
        if let Err(e) = summary_task_sender.send(task).await {
            error!("failed to send Summary task, {:?}", e);
        }
    }

    pub async fn build_write_group(
        &self,
        precision: Precision,
        tables: FlatBufferTable<'_>,
        ts_index: Arc<index::ts_index::TSIndex>,
        recover_from_wal: bool,
        strict_write: Option<bool>,
    ) -> Result<HashMap<SeriesId, RowGroup>> {
        let strict_write = strict_write.unwrap_or(self.opt.storage.strict_write);

        // (series id, schema id) -> RowGroup
        let mut map = HashMap::new();
        for table in tables {
            let table_name = table.tab_ext()?;
            let columns = table.columns().ok_or(Error::CommonError {
                reason: "table missing columns".to_string(),
            })?;
            let num_rows = table.num_rows() as usize;

            let fb_schema = FbSchema::from_fb_column(table_name, columns)?;
            let schema = if strict_write {
                let schema = self.schemas.get_table_schema(fb_schema.table)?;

                schema.ok_or_else(|| Error::TableNotFound {
                    table: fb_schema.table.to_string(),
                })?
            } else {
                self.schemas
                    .check_field_type_or_else_add(&fb_schema)
                    .await?
            };

            let sids = Self::build_index(
                &fb_schema,
                &columns,
                &schema,
                num_rows,
                ts_index.clone(),
                recover_from_wal,
            )
            .await?;
            // every row produces a sid
            debug_assert_eq!(num_rows, sids.len());
            self.build_row_data(
                &fb_schema,
                &columns,
                schema.clone(),
                &mut map,
                precision,
                &sids,
            )?;
        }
        Ok(map)
    }

    fn build_row_data(
        &self,
        fb_schema: &FbSchema<'_>,
        columns: &Vector<ForwardsUOffset<Column>>,
        table_schema: TskvTableSchemaRef,
        map: &mut HashMap<SeriesId, RowGroup>,
        precision: Precision,
        sids: &[u32],
    ) -> Result<()> {
        let mut sid_map: HashMap<u32, Vec<usize>> = HashMap::new();
        for (row_count, sid) in sids.iter().enumerate() {
            let row_idx = sid_map.entry(*sid).or_default();
            row_idx.push(row_count);
        }
        for (sid, row_idx) in sid_map.into_iter() {
            let rows = RowData::point_to_row_data(
                table_schema.as_ref(),
                precision,
                columns,
                fb_schema,
                row_idx,
            )?;
            let mut row_group = RowGroup {
                schema: table_schema.clone(),
                rows: LinkedList::new(),
                range: TimeRange::none(),
                size: size_of::<RowGroup>(),
            };
            for row in rows {
                row_group.range.merge(&TimeRange::new(row.ts, row.ts));
                row_group.size += row.size();
                row_group.rows.push_back(row);
            }
            let res = map.insert(sid, row_group);
            // every sid of different table is different
            debug_assert!(res.is_none())
        }
        Ok(())
    }

    async fn build_index<'a>(
        fb_schema: &'a FbSchema<'a>,
        columns: &Vector<'a, ForwardsUOffset<Column<'a>>>,
        table_column: &TskvTableSchema,
        row_num: usize,
        ts_index: Arc<index::ts_index::TSIndex>,
        recover_from_wal: bool,
    ) -> Result<Vec<u32>> {
        let mut res_sids = Vec::with_capacity(row_num);
        let mut series_keys = Vec::with_capacity(row_num);
        for row_count in 0..row_num {
            let series_key = SeriesKey::build_series_key(
                fb_schema.table,
                columns,
                table_column,
                &fb_schema.tag_indexes,
                row_count,
            )
            .map_err(|e| Error::CommonError {
                reason: e.to_string(),
            })?;
            if let Some(id) = ts_index.get_series_id(&series_key).await? {
                res_sids.push(Some(id));
                continue;
            }

            if recover_from_wal {
                if let Some(id) = ts_index.get_deleted_series_id(&series_key).await? {
                    // 仅在 recover wal的时候有用
                    res_sids.push(Some(id));
                    continue;
                }
            }

            res_sids.push(None);
            series_keys.push(series_key);
        }

        let mut ids = ts_index
            .add_series_if_not_exists(series_keys)
            .await?
            .into_iter();
        for item in res_sids.iter_mut() {
            if item.is_none() {
                *item = Some(ids.next().ok_or(Error::CommonError {
                    reason: "add series failed, new series id is missing".to_string(),
                })?);
            }
        }
        let res_sids = res_sids.into_iter().flatten().collect::<Vec<_>>();

        Ok(res_sids)
    }

    /// Snapshots last version before `last_seq` of this database's all vnodes
    /// or specified vnode by `vnode_id`.
    ///
    /// Generated version data will be inserted into `version_edits` and `file_metas`.
    ///
    /// - `version_edits` are for all vnodes and db-files,
    /// - `file_metas` is for index data
    /// (field-id filter) of db-files.
    pub async fn snapshot(
        &self,
        vnode_id: Option<TseriesFamilyId>,
        version_edits: &mut Vec<VersionEdit>,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) {
        if let Some(tsf_id) = vnode_id.as_ref() {
            if let Some(tsf) = self.ts_families.get(tsf_id) {
                let ve = tsf.read().await.build_version_edit(file_metas);
                version_edits.push(ve);
            }
        } else {
            for tsf in self.ts_families.values() {
                let ve = tsf.read().await.build_version_edit(file_metas);
                version_edits.push(ve);
            }
        }
    }

    pub async fn get_series_key(
        &self,
        vnode_id: u32,
        sids: &[SeriesId],
    ) -> IndexResult<Vec<SeriesKey>> {
        let mut res = vec![];
        if let Some(idx) = self.get_ts_index(vnode_id) {
            for sid in sids {
                if let Some(key) = idx.get_series_key(*sid).await? {
                    res.push(key)
                }
            }
        }

        Ok(res)
    }

    pub fn get_table_schema(&self, table_name: &str) -> Result<Option<TskvTableSchemaRef>> {
        Ok(self.schemas.get_table_schema(table_name)?)
    }

    pub fn get_tsfamily(&self, id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some(v) = self.ts_families.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_families(&self) -> &HashMap<TseriesFamilyId, Arc<RwLock<TseriesFamily>>> {
        &self.ts_families
    }

    pub fn for_each_ts_family<F>(&self, func: F)
    where
        F: FnMut((&TseriesFamilyId, &Arc<RwLock<TseriesFamily>>)),
    {
        self.ts_families.iter().for_each(func);
    }

    pub fn del_ts_index(&mut self, id: TseriesFamilyId) {
        self.ts_indexes.remove(&id);
    }

    pub fn get_ts_index(&self, id: TseriesFamilyId) -> Option<Arc<index::ts_index::TSIndex>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_indexes(&self) -> HashMap<TseriesFamilyId, Arc<index::ts_index::TSIndex>> {
        self.ts_indexes.clone()
    }

    pub async fn get_ts_index_or_add(&mut self, id: u32) -> Result<Arc<index::ts_index::TSIndex>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Ok(v.clone());
        }

        let path = self.opt.storage.index_dir(&self.owner, id);

        let idx = index::ts_index::TSIndex::new(path).await?;

        self.ts_indexes.insert(id, idx.clone());

        Ok(idx)
    }

    pub fn get_schemas(&self) -> Arc<DBschemas> {
        self.schemas.clone()
    }

    pub fn get_schema(&self) -> Result<DatabaseSchema> {
        Ok(self.schemas.db_schema()?)
    }

    pub fn owner(&self) -> Arc<String> {
        self.owner.clone()
    }

    pub fn db_name(&self) -> Arc<String> {
        self.db_name.clone()
    }
}

#[cfg(test)]
impl Database {
    pub fn tsf_num(&self) -> usize {
        self.ts_families.len()
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let vnodes: Vec<Arc<RwLock<TseriesFamily>>> = self.ts_families.values().cloned().collect();
        self.runtime.spawn(async move {
            for vnode in vnodes {
                vnode.write().await.close();
            }
        });
    }
}

#[derive(Debug)]
pub struct FbSchema<'a> {
    pub table: &'a str,
    pub time_index: usize,
    pub tag_indexes: Vec<usize>,
    pub tag_names: Vec<Cow<'a, str>>,
    pub field_indexes: Vec<usize>,
    pub field_names: Vec<&'a str>,
    pub field_types: Vec<FieldType>,
}

impl<'a> FbSchema<'a> {
    pub fn from_fb_column(
        table: &'a str,
        columns: Vector<'a, ForwardsUOffset<Column<'a>>>,
    ) -> Result<FbSchema<'a>> {
        let mut time_index = usize::MAX;
        let mut tag_indexes = vec![];
        let mut tag_names = vec![];
        let mut field_indexes = vec![];
        let mut field_names = vec![];
        let mut field_types = vec![];

        for (index, column) in columns.iter().enumerate() {
            match column.column_type() {
                ColumnType::Time => {
                    time_index = index;
                }
                ColumnType::Tag => {
                    tag_indexes.push(index);
                    let column_name = column.name().ok_or(Error::CommonError {
                        reason: "Tag column name not found in flatbuffer columns".to_string(),
                    })?;

                    tag_names.push(Cow::Borrowed(column_name));
                }
                ColumnType::Field => {
                    field_indexes.push(index);
                    field_names.push(column.name().ok_or(Error::CommonError {
                        reason: "Field column name not found in flatbuffer columns".to_string(),
                    })?);
                    field_types.push(column.field_type());
                }
                _ => {}
            }
        }

        if time_index == usize::MAX {
            return Err(Error::CommonError {
                reason: "Time column not found in flatbuffer columns".to_string(),
            });
        }

        if field_indexes.is_empty() {
            return Err(Error::CommonError {
                reason: "Field column not found in flatbuffer columns".to_string(),
            });
        }

        Ok(Self {
            table,
            time_index,
            tag_indexes,
            tag_names,
            field_indexes,
            field_names,
            field_types,
        })
    }
}
