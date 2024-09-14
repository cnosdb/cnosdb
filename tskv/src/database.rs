use std::borrow::Cow;
use std::collections::HashMap;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;

use flatbuffers::{ForwardsUOffset, Vector};
use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::predicate::domain::TimeRange;
use models::schema::database_schema::{DatabaseConfig, DatabaseSchema};
use models::schema::tskv_table_schema::{TskvTableSchema, TskvTableSchemaRef};
use models::{SeriesId, SeriesKey};
use protos::models::{Column, ColumnType, FieldType, Table};
use snafu::{OptionExt, ResultExt};
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, RwLock};
use trace::error;
use utils::precision::Precision;

use crate::context::GlobalContext;
use crate::error::{
    CommonSnafu, IndexErrSnafu, ModelSnafu, SchemaSnafu, TableNotFoundSnafu, TskvResult,
};
use crate::index::ts_index::TSIndex;
use crate::index::IndexResult;
use crate::kv_option::Options;
use crate::mem_cache::row_data::{OrderedRowsData, RowData};
use crate::mem_cache::series_data::RowGroup;
use crate::schema::schemas::DBschemas;
use crate::summary::{SummaryTask, VersionEdit};
use crate::tsfamily::level_info::LevelInfo;
use crate::tsfamily::tseries_family::{TseriesFamily, TsfFactory};
use crate::tsfamily::version::Version;
use crate::tsm::reader::TsmReader;
use crate::{TsKvContext, VnodeId};

pub type FlatBufferTable<'a> = flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Table<'a>>>;

#[derive(Debug)]
pub struct Database {
    //tenant_name.database_name => owner
    owner: Arc<String>,
    opt: Arc<Options>,
    config: Arc<DatabaseConfig>,
    db_name: Arc<String>,

    schemas: Arc<DBschemas>,
    ts_indexes: HashMap<VnodeId, Arc<RwLock<TSIndex>>>,
    ts_families: HashMap<VnodeId, Arc<RwLock<TseriesFamily>>>,
    tsf_factory: TsfFactory,
}

#[derive(Debug)]
pub struct DatabaseFactory {
    meta: MetaRef,
    memory_pool: MemoryPoolRef,
    metrics_register: Arc<MetricsRegister>,
    opt: Arc<Options>,
    ctx: Arc<GlobalContext>,
}

impl DatabaseFactory {
    pub fn new(
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
        opt: Arc<Options>,
        ctx: Arc<GlobalContext>,
    ) -> Self {
        Self {
            meta,
            memory_pool,
            metrics_register,
            opt,
            ctx,
        }
    }

    pub async fn create_database(&self, schema: DatabaseSchema) -> TskvResult<Database> {
        Database::new(
            schema,
            self.opt.clone(),
            self.ctx.clone(),
            self.meta.clone(),
            self.memory_pool.clone(),
            self.metrics_register.clone(),
        )
        .await
    }
}

impl Database {
    pub fn config(&self) -> Arc<DatabaseConfig> {
        self.config.clone()
    }

    pub async fn new(
        schema: DatabaseSchema,
        opt: Arc<Options>,
        ctx: Arc<GlobalContext>,
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> TskvResult<Self> {
        let owner = Arc::new(schema.owner());
        let tsf_factory = TsfFactory::new(
            owner.clone(),
            opt.clone(),
            ctx.clone(),
            schema.config().clone(),
            memory_pool.clone(),
            metrics_register.clone(),
        );

        let db = Self {
            opt,
            config: schema.config().clone(),
            owner: Arc::new(schema.owner()),
            db_name: Arc::new(schema.database_name().to_owned()),
            schemas: Arc::new(DBschemas::new(schema, meta).await.context(SchemaSnafu)?),
            ts_indexes: HashMap::new(),
            ts_families: HashMap::new(),
            tsf_factory,
        };

        Ok(db)
    }

    pub fn open_tsfamily(&mut self, ver: Arc<Version>) {
        let tf_id = ver.tf_id();
        let tf = self.tsf_factory.create_tsf(tf_id, ver.clone());
        self.ts_families.insert(ver.tf_id(), tf);
    }

    pub async fn create_tsfamily(
        &mut self,
        tsf_id: VnodeId,
        ctx: Arc<TsKvContext>,
    ) -> TskvResult<Arc<RwLock<TseriesFamily>>> {
        let tsm_reader_cache = Arc::new(cache::ShardedAsyncCache::create_lru_sharded_cache(
            self.config.max_cache_readers() as usize,
        ));
        let levels = LevelInfo::init_levels(self.owner.clone(), tsf_id, self.opt.storage.clone());
        let version_edit = VersionEdit::new_add_vnode(tsf_id, self.owner.as_ref().clone(), 0);

        let ver = Arc::new(Version::new(
            tsf_id,
            self.owner.clone(),
            self.opt.storage.clone(),
            0,
            levels,
            i64::MIN,
            tsm_reader_cache,
        ));

        let tf = self.tsf_factory.create_tsf(tsf_id, ver.clone());
        self.ts_families.insert(tsf_id, tf.clone());

        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new(tf.clone(), version_edit, None, None, task_state_sender);
        if let Err(e) = ctx.summary_task_sender.send(task).await {
            error!("failed to send Summary task, {:?}", e);
        }
        let _ = task_state_receiver.await;

        Ok(tf)
    }

    // todo: Maybe TseriesFamily::new() should be refactored.
    pub async fn add_tsfamily(
        &mut self,
        mut ve: VersionEdit,
        data_dir: &Path,
        ctx: Arc<TsKvContext>,
    ) -> TskvResult<Arc<RwLock<TseriesFamily>>> {
        let new_dir = self.opt.storage.ts_family_dir(&self.owner, ve.tsf_id);
        let new_dir = new_dir.as_path();

        let mut file_metas = HashMap::with_capacity(ve.add_files.len());
        for f in ve.add_files.iter_mut() {
            let new_file_id = ctx.global_ctx.file_id_next();
            let file_path = f.rename_file(data_dir, new_dir, new_file_id).await?;

            let file_reader = TsmReader::open(file_path).await?;
            let bloom_filter = Arc::new(file_reader.footer().series().bloom_filter().clone());
            file_metas.insert(new_file_id, bloom_filter.clone());

            f.file_id = new_file_id;
        }
        for f in ve.del_files.iter_mut() {
            let new_file_id = ctx.global_ctx.file_id_next();
            f.rename_file(data_dir, new_dir, new_file_id).await?;

            f.file_id = new_file_id;
        }

        let levels =
            LevelInfo::init_levels(self.owner.clone(), ve.tsf_id, self.opt.storage.clone());
        let tsm_reader_cache = Arc::new(cache::ShardedAsyncCache::create_lru_sharded_cache(
            self.config.max_cache_readers() as usize,
        ));
        let ver = Arc::new(Version::new(
            ve.tsf_id,
            self.owner.clone(),
            self.opt.storage.clone(),
            ve.seq_no,
            levels,
            ve.max_level_ts,
            tsm_reader_cache,
        ));

        let tf = self.tsf_factory.create_tsf(ve.tsf_id, ver.clone());
        self.ts_families.insert(ve.tsf_id, tf.clone());

        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new(tf.clone(), ve, Some(file_metas), None, task_state_sender);
        if let Err(e) = ctx.summary_task_sender.send(task).await {
            error!("failed to send Summary task, {:?}", e);
        }

        let _ = task_state_receiver.await;

        Ok(tf)
    }

    pub async fn del_tsfamily(&mut self, tf_id: u32, summary_task_sender: Sender<SummaryTask>) {
        if let Some(tf) = self.ts_families.remove(&tf_id) {
            let owner = tf.read().await.owner();
            let seq = tf.read().await.version().last_seq();
            self.tsf_factory.drop_tsf(tf_id);
            let edit = VersionEdit::new_del_vnode(tf_id, owner.to_string(), seq);
            let (task_state_sender, task_state_receiver) = oneshot::channel();
            let task = SummaryTask::new(tf.clone(), edit, None, None, task_state_sender);
            if let Err(e) = summary_task_sender.send(task).await {
                error!("failed to send Summary task, {:?}", e);
            }

            let _ = task_state_receiver.await;
        }
    }

    pub async fn build_write_group(
        &self,
        precision: Precision,
        tables: FlatBufferTable<'_>,
        ts_index: Arc<RwLock<TSIndex>>,
        recover_from_wal: bool,
        strict_write: Option<bool>,
    ) -> TskvResult<HashMap<SeriesId, (SeriesKey, RowGroup)>> {
        let strict_write = strict_write.unwrap_or(self.config.strict_write());

        // (series id, schema id) -> RowGroup
        let mut map = HashMap::new();
        for table in tables {
            let table_name = table.tab_ext()?;
            let columns = table.columns().context(CommonSnafu {
                reason: "table missing columns".to_string(),
            })?;
            let num_rows = table.num_rows() as usize;

            let fb_schema = FbSchema::from_fb_column(table_name, columns)?;
            let schema = if strict_write {
                self.schemas
                    .get_table_schema(fb_schema.table)
                    .await
                    .context(SchemaSnafu)?
                    .context(TableNotFoundSnafu {
                        table: fb_schema.table.to_string(),
                    })?
            } else {
                self.schemas
                    .check_field_type_or_else_add(&fb_schema)
                    .await
                    .context(SchemaSnafu)?
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
            self.build_row_data(&columns, schema.clone(), &mut map, precision, &sids)?;
        }
        Ok(map)
    }

    fn build_row_data(
        &self,
        columns: &Vector<ForwardsUOffset<Column>>,
        table_schema: TskvTableSchemaRef,
        map: &mut HashMap<SeriesId, (SeriesKey, RowGroup)>,
        precision: Precision,
        sids: &[(u32, SeriesKey)],
    ) -> TskvResult<()> {
        let mut sid_map: HashMap<u32, (SeriesKey, Vec<usize>)> = HashMap::new();
        for (row_count, (sid, series_key)) in sids.iter().enumerate() {
            let buf_and_row_idx = sid_map.entry(*sid).or_default();
            if buf_and_row_idx.0.table().is_empty() && buf_and_row_idx.0.tags().is_empty() {
                buf_and_row_idx.0 = series_key.clone();
            }
            buf_and_row_idx.1.push(row_count);
        }
        for (sid, (series_key_buf, row_idx)) in sid_map.into_iter() {
            let rows =
                RowData::point_to_row_data(table_schema.as_ref(), precision, columns, row_idx)?;
            let mut row_group = RowGroup {
                schema: table_schema.clone(),
                rows: OrderedRowsData::new(),
                range: TimeRange::none(),
                size: size_of::<RowGroup>(),
            };
            for row in rows {
                row_group.range.merge(&TimeRange::new(row.ts, row.ts));
                row_group.size += row.size();
                row_group.rows.insert(row);
            }
            let res = map.insert(sid, (series_key_buf, row_group));
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
        ts_index: Arc<RwLock<TSIndex>>,
        recover_from_wal: bool,
    ) -> TskvResult<Vec<(u32, SeriesKey)>> {
        let mut res_sids = Vec::with_capacity(row_num);
        let mut series_keys = Vec::with_capacity(row_num);
        let ts_index_r = ts_index.read().await;
        for row_count in 0..row_num {
            let series_key = SeriesKey::build_series_key(
                fb_schema.table,
                columns,
                table_column,
                &fb_schema.tag_indexes,
                row_count,
            )
            .context(ModelSnafu)?;
            if let Some(id) = ts_index_r
                .get_series_id(&series_key)
                .await
                .context(IndexErrSnafu)?
            {
                res_sids.push(Some((id, series_key)));
                continue;
            }

            if recover_from_wal {
                if let Some(id) = ts_index_r
                    .get_tombstone_series_id(&series_key)
                    .await
                    .context(IndexErrSnafu)?
                {
                    // 仅在 recover wal的时候有用
                    res_sids.push(Some((id, series_key)));
                    continue;
                }
            }

            res_sids.push(None);
            series_keys.push(series_key);
        }
        drop(ts_index_r);

        let mut ids = ts_index
            .write()
            .await
            .add_series_if_not_exists(series_keys)
            .await
            .context(IndexErrSnafu)?
            .into_iter();
        for item in res_sids.iter_mut() {
            if item.is_none() {
                *item = Some(ids.next().context(CommonSnafu {
                    reason: "add series failed, new series id is missing".to_string(),
                })?);
            }
        }
        let res_sids = res_sids.into_iter().flatten().collect::<Vec<_>>();

        Ok(res_sids)
    }

    pub async fn get_series_key(
        &self,
        vnode_id: u32,
        sids: &[SeriesId],
    ) -> IndexResult<Vec<SeriesKey>> {
        let mut res = vec![];
        if let Some(idx) = self.get_ts_index(vnode_id) {
            let idx = idx.read().await;
            for sid in sids {
                if let Some(key) = idx.get_series_key(*sid).await? {
                    res.push(key)
                }
            }
        }

        Ok(res)
    }

    pub async fn rebuild_tsfamily_index(
        &mut self,
        ts_family: Arc<RwLock<TseriesFamily>>,
    ) -> TskvResult<Arc<RwLock<TSIndex>>> {
        let id = ts_family.read().await.tf_id();
        let ts_index = ts_family.read().await.rebuild_index().await?;

        self.ts_indexes.insert(id, ts_index.clone());

        Ok(ts_index)
    }

    pub async fn get_table_schema(
        &self,
        table_name: &str,
    ) -> TskvResult<Option<TskvTableSchemaRef>> {
        Ok(self.schemas.get_table_schema(table_name).await?)
    }

    pub fn get_tsfamily(&self, id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some(v) = self.ts_families.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_families(&self) -> &HashMap<VnodeId, Arc<RwLock<TseriesFamily>>> {
        &self.ts_families
    }

    pub fn for_each_ts_family<F>(&self, func: F)
    where
        F: FnMut((&VnodeId, &Arc<RwLock<TseriesFamily>>)),
    {
        self.ts_families.iter().for_each(func);
    }

    pub fn del_ts_index(&mut self, id: VnodeId) {
        self.ts_indexes.remove(&id);
    }

    pub fn get_ts_index(&self, id: VnodeId) -> Option<Arc<RwLock<TSIndex>>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_indexes(&self) -> HashMap<VnodeId, Arc<RwLock<TSIndex>>> {
        self.ts_indexes.clone()
    }

    pub async fn get_ts_index_or_add(&mut self, id: u32) -> TskvResult<Arc<RwLock<TSIndex>>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Ok(v.clone());
        }

        let path = self.opt.storage.index_dir(self.owner.as_str(), id);
        let idx = TSIndex::new(path, self.opt.storage.index_cache_capacity)
            .await
            .context(IndexErrSnafu)?;

        self.ts_indexes.insert(id, idx.clone());

        Ok(idx)
    }

    pub fn get_schemas(&self) -> Arc<DBschemas> {
        self.schemas.clone()
    }

    pub async fn get_schema(&self) -> TskvResult<DatabaseSchema> {
        Ok(self.schemas.db_schema().await?)
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
    ) -> TskvResult<FbSchema<'a>> {
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
                    let column_name = column.name().context(CommonSnafu {
                        reason: "Tag column name not found in flatbuffer columns".to_string(),
                    })?;

                    tag_names.push(Cow::Borrowed(column_name));
                }
                ColumnType::Field => {
                    field_indexes.push(index);
                    field_names.push(column.name().context(CommonSnafu {
                        reason: "Field column name not found in flatbuffer columns".to_string(),
                    })?);
                    field_types.push(column.field_type());
                }
                _ => {}
            }
        }

        if time_index == usize::MAX {
            return Err(CommonSnafu {
                reason: "Time column not found in flatbuffer columns".to_string(),
            }
            .build());
        }

        if field_indexes.is_empty() {
            return Err(CommonSnafu {
                reason: "Field column not found in flatbuffer columns".to_string(),
            }
            .build());
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
