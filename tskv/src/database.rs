use std::collections::HashMap;
use std::mem::size_of;
use std::path::{self, Path};
use std::ptr::read;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::sql::sqlparser::test_utils::table;
use futures::executor::block_on;
use lru_cache::asynchronous::ShardedCache;
use memory_pool::{MemoryPool, MemoryPoolRef};
use meta::MetaRef;
use metrics::gauge::{GaugeWrap, U64Gauge};
use metrics::metric::Metric;
use metrics::metric_register::MetricsRegister;
use models::schema::{DatabaseSchema, TableColumn, TableSchema, TskvTableSchema};
use models::utils::{split_id, unite_id};
use models::{
    ColumnId, FieldInfo, InMemPoint, SchemaId, SeriesId, SeriesKey, Tag, Timestamp, ValueType,
};
use parking_lot::RwLock as SyncRwLock;
use protos::models::{Point, Points};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::{oneshot, RwLock};
use trace::{debug, error, info};
use utils::BloomFilter;

use crate::compaction::{check, CompactTask, FlushReq};
use crate::error::{self, IndexErrSnafu, Result, SchemaSnafu};
use crate::index::{self, IndexResult};
use crate::kv_option::Options;
use crate::memcache::{MemCache, RowData, RowGroup};
use crate::schema::schemas::DBschemas;
use crate::summary::{SummaryTask, VersionEdit};
use crate::tseries_family::{LevelInfo, TseriesFamily, Version};
use crate::version_set::VersionSet;
use crate::Error::{self, InvalidPoint};
use crate::{ColumnFileId, TimeRange, TseriesFamilyId};

pub type FlatBufferPoint<'a> = flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Point<'a>>>;

#[derive(Debug)]
pub struct Database {
    //tenant_name.database_name => owner
    owner: Arc<String>,
    opt: Arc<Options>,

    schemas: Arc<DBschemas>,
    ts_indexes: HashMap<TseriesFamilyId, Arc<RwLock<index::ts_index::TSIndex>>>,
    ts_families: HashMap<TseriesFamilyId, Arc<RwLock<TseriesFamily>>>,
    runtime: Arc<Runtime>,
    memory_pool: MemoryPoolRef,
    metrics_register: Arc<MetricsRegister>,
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
        let db = Self {
            opt,
            owner: Arc::new(schema.owner()),
            schemas: Arc::new(DBschemas::new(schema, meta).await.context(SchemaSnafu)?),
            ts_indexes: HashMap::new(),
            ts_families: HashMap::new(),
            runtime,
            memory_pool,
            metrics_register,
        };

        Ok(db)
    }

    pub fn open_tsfamily(
        &mut self,
        ver: Arc<Version>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
    ) {
        let opt = ver.storage_opt();

        let tf = TseriesFamily::new(
            ver.tf_id(),
            ver.database(),
            MemCache::new(
                ver.tf_id(),
                self.opt.cache.max_buffer_size,
                ver.last_seq,
                &self.memory_pool,
            ),
            ver.clone(),
            self.opt.cache.clone(),
            self.opt.storage.clone(),
            flush_task_sender,
            compact_task_sender,
            self.memory_pool.clone(),
            &self.metrics_register,
        );
        tf.schedule_compaction(self.runtime.clone());

        self.ts_families
            .insert(ver.tf_id(), Arc::new(RwLock::new(tf)));
    }

    pub async fn switch_memcache(&self, tf_id: u32, seq: u64) {
        if let Some(tf) = self.ts_families.get(&tf_id) {
            let mem = Arc::new(parking_lot::RwLock::new(MemCache::new(
                tf_id,
                self.opt.cache.max_buffer_size,
                seq,
                &self.memory_pool,
            )));
            let mut tf = tf.write().await;
            tf.switch_memcache(mem);
        }
    }

    // todo: Maybe TseriesFamily::new() should be refactored.
    #[allow(clippy::too_many_arguments)]
    pub async fn add_tsfamily(
        &mut self,
        tsf_id: u32,
        seq_no: u64,
        version_edit: Option<VersionEdit>,
        summary_task_sender: Sender<SummaryTask>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
    ) -> Arc<RwLock<TseriesFamily>> {
        let seq_no = version_edit.as_ref().map(|v| v.seq_no).unwrap_or(seq_no);

        let ver = Arc::new(Version::new(
            tsf_id,
            self.owner.clone(),
            self.opt.storage.clone(),
            seq_no,
            LevelInfo::init_levels(self.owner.clone(), tsf_id, self.opt.storage.clone()),
            i64::MIN,
            Arc::new(ShardedCache::default()),
        ));

        let tf = TseriesFamily::new(
            tsf_id,
            self.owner.clone(),
            MemCache::new(
                tsf_id,
                self.opt.cache.max_buffer_size,
                seq_no,
                &self.memory_pool,
            ),
            ver,
            self.opt.cache.clone(),
            self.opt.storage.clone(),
            flush_task_sender,
            compact_task_sender,
            self.memory_pool.clone(),
            &self.metrics_register,
        );

        let tf = Arc::new(RwLock::new(tf));
        self.ts_families.insert(tsf_id, tf.clone());

        let edits = version_edit.map(|v| vec![v]).unwrap_or_else(|| {
            vec![VersionEdit::new_add_vnode(
                tsf_id,
                self.owner.as_ref().clone(),
            )]
        });
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new_vnode_task(edits, task_state_sender);
        if let Err(e) = summary_task_sender.send(task).await {
            error!("failed to send Summary task, {:?}", e);
        }

        tf
    }

    pub async fn del_tsfamily(&mut self, tf_id: u32, summary_task_sender: Sender<SummaryTask>) {
        if let Some(tf) = self.ts_families.remove(&tf_id) {
            tf.read().await.close();
        }

        let edits = vec![VersionEdit::new_del_vnode(tf_id)];
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new_vnode_task(edits, task_state_sender);
        if let Err(e) = summary_task_sender.send(task).await {
            error!("failed to send Summary task, {:?}", e);
        }
    }

    pub async fn build_write_group(
        &self,
        points: FlatBufferPoint<'_>,
        ts_index: Arc<RwLock<index::ts_index::TSIndex>>,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        if self.opt.storage.strict_write {
            self.build_write_group_strict_mode(points, ts_index).await
        } else {
            self.build_write_group_loose_mode(points, ts_index).await
        }
    }

    pub async fn build_write_group_strict_mode(
        &self,
        points: FlatBufferPoint<'_>,
        ts_index: Arc<RwLock<index::ts_index::TSIndex>>,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        // (series id, schema id) -> RowGroup
        let mut map = HashMap::new();
        for point in points {
            let sid = Self::build_index(&point, ts_index.clone()).await?;
            self.build_row_data(&mut map, point, sid)?
        }
        Ok(map)
    }

    pub async fn build_write_group_loose_mode(
        &self,
        points: FlatBufferPoint<'_>,
        ts_index: Arc<RwLock<index::ts_index::TSIndex>>,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        let mut map = HashMap::new();
        for point in points {
            let sid = Self::build_index(&point, ts_index.clone()).await?;
            if self.schemas.check_field_type_from_cache(&point).is_err() {
                self.schemas.check_field_type_or_else_add(&point).await?;
            }

            self.build_row_data(&mut map, point, sid)?
        }
        Ok(map)
    }

    fn build_row_data(
        &self,
        map: &mut HashMap<(SeriesId, SchemaId), RowGroup>,
        point: Point,
        sid: u32,
    ) -> Result<()> {
        let table_name = String::from_utf8(point.tab().unwrap().bytes().to_vec()).unwrap();
        let table_schema = self.schemas.get_table_schema(&table_name)?;
        let table_schema = match table_schema {
            Some(v) => v,
            None => return Ok(()),
        };

        let row = RowData::point_to_row_data(point, &table_schema);
        let schema_size = table_schema.size();
        let schema_id = table_schema.schema_id;
        let entry = map.entry((sid, schema_id)).or_insert(RowGroup {
            schema: Arc::new(TskvTableSchema::default()),
            rows: vec![],
            range: TimeRange {
                min_ts: i64::MAX,
                max_ts: i64::MIN,
            },
            size: size_of::<RowGroup>(),
        });
        entry.schema = table_schema;
        entry.size += schema_size;
        entry.range.merge(&TimeRange {
            min_ts: row.ts,
            max_ts: row.ts,
        });
        entry.size += row.size();
        //todo: remove this copy
        entry.rows.push(row);
        Ok(())
    }

    async fn build_index(
        info: &Point<'_>,
        ts_index: Arc<RwLock<index::ts_index::TSIndex>>,
    ) -> Result<u32> {
        if info.fields().ok_or(InvalidPoint)?.is_empty() {
            return Err(InvalidPoint);
        }

        let mut series_key = SeriesKey::from_flatbuffer(info).map_err(|e| Error::CommonError {
            reason: e.to_string(),
        })?;

        if let Some(id) = ts_index.read().await.get_series_id(&series_key)? {
            return Ok(id);
        }

        let id = ts_index
            .write()
            .await
            .add_series_if_not_exists(&mut series_key)
            .await?;

        Ok(id)
    }

    /// Snashots last version before `last_seq` of this database's all vnodes
    /// or specified vnode by `vnode_id`.
    ///
    /// Generated version data will be inserted into `version_edits` and `file_metas`.
    ///
    /// - `version_edits` are for all vnodes and db-files,
    /// - `file_metas` is for index data
    /// (field-id filter) of db-files.
    pub async fn snapshot(
        &self,
        last_seq: u64,
        vnode_id: Option<TseriesFamilyId>,
        version_edits: &mut Vec<VersionEdit>,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) {
        if let Some(tsf_id) = vnode_id.as_ref() {
            if let Some(tsf) = self.ts_families.get(tsf_id) {
                let ve = tsf
                    .read()
                    .await
                    .snapshot(last_seq, self.owner.clone(), file_metas);
                version_edits.push(ve);
            }
        } else {
            for (id, tsf) in &self.ts_families {
                let ve = tsf
                    .read()
                    .await
                    .snapshot(last_seq, self.owner.clone(), file_metas);
                version_edits.push(ve);
            }
        }
    }

    pub async fn get_series_key(&self, vnode_id: u32, sid: u32) -> IndexResult<Option<SeriesKey>> {
        if let Some(idx) = self.get_ts_index(vnode_id) {
            return idx.read().await.get_series_key(sid);
        }

        Ok(None)
    }

    pub fn get_table_schema(&self, table_name: &str) -> Result<Option<Arc<TskvTableSchema>>> {
        Ok(self.schemas.get_table_schema(table_name)?)
    }

    pub fn get_tsfamily(&self, id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some(v) = self.ts_families.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn tsf_num(&self) -> usize {
        self.ts_families.len()
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

    pub fn del_ts_index(&mut self, id: u32) {
        self.ts_indexes.remove(&id);
    }

    pub fn get_ts_index(&self, id: u32) -> Option<Arc<RwLock<index::ts_index::TSIndex>>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Some(v.clone());
        }

        None
    }

    pub fn ts_indexes(&self) -> &HashMap<TseriesFamilyId, Arc<RwLock<index::ts_index::TSIndex>>> {
        &self.ts_indexes
    }

    pub async fn get_table_sids(&self, table: &str) -> IndexResult<Vec<SeriesId>> {
        let mut res = vec![];
        for (_, ts_index) in self.ts_indexes.iter() {
            let mut list = ts_index.read().await.get_series_id_list(table, &[])?;
            res.append(&mut list);
        }
        Ok(res)
    }

    pub async fn get_ts_index_or_add(
        &mut self,
        id: u32,
    ) -> Result<Arc<RwLock<index::ts_index::TSIndex>>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Ok(v.clone());
        }

        let path = self.opt.storage.index_dir(&self.owner, id);

        let idx = index::ts_index::TSIndex::new(path).await?;
        let idx = Arc::new(RwLock::new(idx));

        self.ts_indexes.insert(id, idx.clone());

        Ok(idx)
    }

    pub fn get_schemas(&self) -> Arc<DBschemas> {
        self.schemas.clone()
    }

    // todo: will delete in cluster version
    pub fn get_tsfamily_random(&self) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some((_, v)) = self.ts_families.iter().next() {
            return Some(v.clone());
        }

        None
    }

    pub fn get_schema(&self) -> Result<DatabaseSchema> {
        Ok(self.schemas.db_schema()?)
    }

    pub async fn get_ts_family_hash_tree(
        &self,
        ts_family_id: TseriesFamilyId,
    ) -> Result<Vec<check::TableHashTreeNode>> {
        check::get_ts_family_hash_tree(self, ts_family_id).await
    }

    pub fn owner(&self) -> Arc<String> {
        self.owner.clone()
    }
}

pub(crate) async fn delete_table_async(
    tenant: String,
    database: String,
    table: String,
    version_set: Arc<RwLock<VersionSet>>,
) -> Result<()> {
    info!("Drop table: '{}.{}'", &database, &table);
    let version_set_rlock = version_set.read().await;
    let options = version_set_rlock.options();
    let db_instance = version_set_rlock.get_db(&tenant, &database);
    drop(version_set_rlock);

    if let Some(db) = db_instance {
        let schemas = db.read().await.get_schemas();
        let field_infos = schemas.get_table_schema(&table)?;
        schemas.del_table_schema(&table).await?;

        let mut sids = vec![];
        for (id, index) in db.read().await.ts_indexes().iter() {
            let mut index = index.write().await;

            let mut ids = index
                .get_series_id_list(&table, &[])
                .context(error::IndexErrSnafu)?;

            for sid in ids.iter() {
                index
                    .del_series_info(*sid)
                    .await
                    .context(error::IndexErrSnafu)?;
            }
            index.flush().await.context(error::IndexErrSnafu)?;

            sids.append(&mut ids);
        }

        if let Some(fields) = field_infos {
            debug!(
                "Drop table: deleting series in table: {}.{}",
                &database, &table
            );
            let fids: Vec<ColumnId> = fields.columns().iter().map(|f| f.id).collect();
            let storage_fids: Vec<u64> = sids
                .iter()
                .flat_map(|sid| fids.iter().map(|fid| unite_id(*fid, *sid)))
                .collect();
            let time_range = &TimeRange {
                min_ts: Timestamp::MIN,
                max_ts: Timestamp::MAX,
            };

            for (ts_family_id, ts_family) in db.read().await.ts_families().iter() {
                // TODO: Concurrent delete on ts_family.
                // TODO: Limit parallel delete to 1.
                ts_family.write().await.delete_series(&sids, time_range);
                let version = ts_family.read().await.super_version();
                for column_file in version.version.column_files(&storage_fids, time_range) {
                    column_file.add_tombstone(&storage_fids, time_range).await?;
                }
            }
        }
    }
    Ok(())
}
