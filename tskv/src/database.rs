use std::mem::size_of;
use std::mem::size_of_val;
use std::{
    collections::{BTreeMap, HashMap},
    path::{self, Path},
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use parking_lot::RwLock;
use snafu::ResultExt;
use tokio::sync::watch::Receiver;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

use ::models::{FieldInfo, InMemPoint, Tag, ValueType};
use meta::meta_client::MetaRef;
use models::schema::{DatabaseSchema, TableColumn, TableSchema, TskvTableSchema};
use models::utils::{split_id, unite_id};
use models::{ColumnId, SchemaId, SeriesId, SeriesKey, Timestamp};
use protos::models::{Point, Points};
use trace::{debug, error, info};

use crate::compaction::FlushReq;
use crate::error::SchemaSnafu;
use crate::index::{self, IndexError, IndexResult};
use crate::schema::schemas::DBschemas;
use crate::tseries_family::LevelInfo;
use crate::Error::{IndexErr, InvalidPoint};
use crate::{
    error::{self, IndexErrSnafu, Result},
    memcache::{RowData, RowGroup},
    version_set::VersionSet,
    Error, TimeRange, TseriesFamilyId,
};
use crate::{
    kv_option::Options,
    memcache::MemCache,
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::{TseriesFamily, Version},
};

pub type FlatBufferPoint<'a> = flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Point<'a>>>;

#[derive(Debug)]
pub struct Database {
    //tenant_name.database_name => owner
    owner: String,
    opt: Arc<Options>,

    schemas: Arc<DBschemas>,
    ts_indexes: HashMap<TseriesFamilyId, Arc<RwLock<index::ts_index::TSIndex>>>,
    ts_families: HashMap<TseriesFamilyId, Arc<RwLock<TseriesFamily>>>,
}

impl Database {
    pub fn new(schema: DatabaseSchema, opt: Arc<Options>, meta: MetaRef) -> Result<Self> {
        let db = Self {
            opt,
            owner: schema.owner(),
            schemas: Arc::new(DBschemas::new(schema, meta).context(SchemaSnafu)?),
            ts_indexes: HashMap::new(),
            ts_families: HashMap::new(),
        };

        Ok(db)
    }

    pub fn alter_db_schema(&self, schema: DatabaseSchema) -> Result<()> {
        self.schemas.alter_db_schema(schema).context(SchemaSnafu)
    }

    pub fn open_tsfamily(
        &mut self,
        ver: Arc<Version>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) {
        let opt = ver.storage_opt();

        let tf = TseriesFamily::new(
            ver.tf_id(),
            ver.database().to_string(),
            MemCache::new(ver.tf_id(), self.opt.cache.max_buffer_size, ver.last_seq),
            ver.clone(),
            self.opt.cache.clone(),
            self.opt.storage.clone(),
            flush_task_sender,
        );
        self.ts_families
            .insert(ver.tf_id(), Arc::new(RwLock::new(tf)));
    }

    pub fn switch_memcache(&self, tf_id: u32, seq: u64) {
        if let Some(tf) = self.ts_families.get(&tf_id) {
            let mem = Arc::new(RwLock::new(MemCache::new(
                tf_id,
                self.opt.cache.max_buffer_size,
                seq,
            )));
            let mut tf = tf.write();
            tf.switch_memcache(mem);
        }
    }

    // todo: Maybe TseriesFamily::new() should be refactored.
    #[allow(clippy::too_many_arguments)]
    pub fn add_tsfamily(
        &mut self,
        tsf_id: u32,
        seq_no: u64,
        summary_task_sender: UnboundedSender<SummaryTask>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) -> Arc<RwLock<TseriesFamily>> {
        let ver = Arc::new(Version::new(
            tsf_id,
            self.owner.clone(),
            self.opt.storage.clone(),
            seq_no,
            LevelInfo::init_levels(self.owner.clone(), self.opt.storage.clone()),
            i64::MIN,
        ));

        let tf = TseriesFamily::new(
            tsf_id,
            self.owner.clone(),
            MemCache::new(tsf_id, self.opt.cache.max_buffer_size, seq_no),
            ver,
            self.opt.cache.clone(),
            self.opt.storage.clone(),
            flush_task_sender,
        );
        let tf = Arc::new(RwLock::new(tf));
        self.ts_families.insert(tsf_id, tf.clone());

        let mut edit = VersionEdit::new();
        edit.add_tsfamily(tsf_id, self.owner.clone());

        let edits = vec![edit];
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask {
            edits,
            cb: task_state_sender,
        };
        if let Err(e) = summary_task_sender.send(task) {
            error!("failed to send Summary task, {:?}", e);
        }

        tf
    }

    pub fn del_tsfamily(&mut self, tf_id: u32, summary_task_sender: UnboundedSender<SummaryTask>) {
        self.ts_families.remove(&tf_id);

        let mut edits = vec![];
        let mut edit = VersionEdit::new();
        edit.del_tsfamily(tf_id);
        edits.push(edit);
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask {
            edits,
            cb: task_state_sender,
        };
        if let Err(e) = summary_task_sender.send(task) {
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
            let sid = self.build_index(&point, ts_index.clone()).await?;
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
            let sid = self.build_index(&point, ts_index.clone()).await?;
            match self.schemas.check_field_type_from_cache(&point) {
                Ok(_) => {}
                Err(_) => {
                    self.schemas
                        .check_field_type_or_else_add(&point)
                        .context(error::SchemaSnafu)?;
                }
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
        let table_schema = self
            .schemas
            .get_table_schema(&table_name)
            .context(SchemaSnafu)?;
        let table_schema = match table_schema {
            Some(v) => v,
            None => return Ok(()),
        };

        let row = RowData::point_to_row_data(point, &table_schema);
        let schema_size = table_schema.size();
        let schema_id = table_schema.schema_id;
        let entry = map.entry((sid, schema_id)).or_insert(RowGroup {
            schema: TskvTableSchema::default(),
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
        &self,
        info: &Point<'_>,
        ts_index: Arc<RwLock<index::ts_index::TSIndex>>,
    ) -> Result<u32> {
        if info.tags().ok_or(InvalidPoint)?.is_empty()
            || info.fields().ok_or(InvalidPoint)?.is_empty()
        {
            return Err(InvalidPoint);
        }

        let mut series_key = SeriesKey::from_flatbuffer(info).map_err(|e| Error::CommonError {
            reason: e.to_string(),
        })?;

        if let Some(id) = ts_index.read().get_series_id(&series_key)? {
            return Ok(id);
        }

        let id = ts_index
            .write()
            .add_series_if_not_exists(&mut series_key)
            .await?;

        Ok(id)
    }

    pub fn version_edit(&self, last_seq: u64) -> (Vec<VersionEdit>, Vec<VersionEdit>) {
        let mut edits = vec![];
        let mut files = vec![];

        for (id, ts) in &self.ts_families {
            //tsfamily edit
            let mut edit = VersionEdit::new();
            edit.add_tsfamily(*id, self.owner.clone());
            edits.push(edit);

            // file edit
            let mut edit = VersionEdit::new();
            let version = ts.read().version().clone();
            let max_level_ts = version.max_level_ts;
            for files in version.levels_info.iter() {
                for file in files.files.iter() {
                    let mut meta = CompactMeta::from(file.as_ref());
                    meta.tsf_id = files.tsf_id;
                    meta.high_seq = last_seq;
                    edit.add_file(meta, max_level_ts);
                }
            }
            files.push(edit);
        }

        (edits, files)
    }

    pub fn add_table_column(&self, table: &str, column: TableColumn) -> Result<()> {
        self.schemas
            .add_table_column(table, column)
            .context(SchemaSnafu)?;
        Ok(())
    }

    pub fn drop_table_column(&self, table: &str, column_name: &str) -> Result<()> {
        self.schemas
            .drop_table_column(table, column_name)
            .context(SchemaSnafu)?;
        Ok(())
    }

    pub fn change_table_column(
        &self,
        table: &str,
        column_name: &str,
        new_column: TableColumn,
    ) -> Result<()> {
        self.schemas
            .change_table_column(table, column_name, new_column)
            .context(SchemaSnafu)?;
        Ok(())
    }

    pub fn get_series_key(&self, vnode_id: u32, sid: u32) -> IndexResult<Option<SeriesKey>> {
        if let Some(idx) = self.get_ts_index(vnode_id) {
            return idx.read().get_series_key(sid);
        }

        Ok(None)
    }

    pub fn get_table_schema(&self, table_name: &str) -> Result<Option<TskvTableSchema>> {
        self.schemas
            .get_table_schema(table_name)
            .context(SchemaSnafu)
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

    pub async fn get_ts_index_or_add(
        &mut self,
        id: u32,
    ) -> Result<Arc<RwLock<index::ts_index::TSIndex>>> {
        if let Some(v) = self.ts_indexes.get(&id) {
            return Ok(v.clone());
        }

        let path = self
            .opt
            .storage
            .index_dir(&self.schemas.database_name(), id);

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
        self.schemas.db_schema().context(SchemaSnafu)
    }
}

#[allow(clippy::await_holding_lock)]
pub(crate) async fn delete_table_async(
    tenant: String,
    database: String,
    table: String,
    version_set: Arc<RwLock<VersionSet>>,
) -> Result<()> {
    info!("Drop table: '{}.{}'", &database, &table);
    let version_set_rlock = version_set.read();
    let options = version_set_rlock.options();
    let db_instance = version_set_rlock.get_db(&tenant, &database);
    drop(version_set_rlock);

    if let Some(db) = db_instance {
        let schemas = db.read().get_schemas();
        let field_infos = schemas
            .get_table_schema(&table)
            .context(error::SchemaSnafu)?;
        schemas
            .del_table_schema(&table)
            .context(error::SchemaSnafu)?;

        let mut sids = vec![];
        for (id, index) in db.read().ts_indexes().iter() {
            let mut index = index.write();

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

            for (ts_family_id, ts_family) in db.read().ts_families().iter() {
                // TODO: Concurrent delete on ts_family.
                // TODO: Limit parallel delete to 1.
                ts_family.write().delete_series(&sids, time_range);
                let version = ts_family.read().super_version();
                for column_file in version.version.column_files(&storage_fids, time_range) {
                    column_file.add_tombstone(&storage_fids, time_range).await?;
                }
            }
        }
    }
    Ok(())
}
