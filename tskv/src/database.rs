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
use models::schema::TableSchema;
use models::utils::{split_id, unite_id};
use models::{SchemaId, SeriesId, SeriesKey, Timestamp};
use protos::models::{Point, Points};
use trace::{debug, error, info};

use crate::compaction::FlushReq;
use crate::index::{IndexError, IndexResult};
use crate::tseries_family::LevelInfo;
use crate::{
    error::{self, Result},
    memcache::{RowData, RowGroup},
    version_set::VersionSet,
    Error, TimeRange, TseriesFamilyId,
};
use crate::{
    index::db_index,
    kv_option::Options,
    memcache::MemCache,
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::{TseriesFamily, Version},
};

pub type FlatBufferPoint<'a> = flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<Point<'a>>>;

#[derive(Debug)]
pub struct Database {
    name: String,
    index: Arc<db_index::DBIndex>,
    ts_families: HashMap<u32, Arc<RwLock<TseriesFamily>>>,
    opt: Arc<Options>,
}

impl Database {
    pub fn new(name: &String, opt: Arc<Options>) -> Self {
        Self {
            index: db_index::index_manger(opt.storage.index_base_dir())
                .write()
                .get_db_index(name),
            name: name.to_string(),
            ts_families: HashMap::new(),
            opt,
        }
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
        file_id: u64,
        summary_task_sender: UnboundedSender<SummaryTask>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) -> Arc<RwLock<TseriesFamily>> {
        let ver = Arc::new(Version::new(
            tsf_id,
            self.name.clone(),
            self.opt.storage.clone(),
            file_id,
            LevelInfo::init_levels(self.name.clone(), self.opt.storage.clone()),
            i64::MIN,
        ));

        let tf = TseriesFamily::new(
            tsf_id,
            self.name.clone(),
            MemCache::new(tsf_id, self.opt.cache.max_buffer_size, seq_no),
            ver,
            self.opt.cache.clone(),
            self.opt.storage.clone(),
            flush_task_sender,
        );
        let tf = Arc::new(RwLock::new(tf));
        self.ts_families.insert(tsf_id, tf.clone());

        let mut edit = VersionEdit::new();
        edit.add_tsfamily(tsf_id, self.name.clone());

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

    pub fn build_write_group(
        &self,
        points: FlatBufferPoint,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        if self.opt.storage.strict_write {
            self.build_write_group_strict_mode(points)
        } else {
            self.build_write_group_loose_mode(points)
        }
    }

    pub fn build_write_group_strict_mode(
        &self,
        points: FlatBufferPoint,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        // (series id, schema id) -> RowGroup
        let mut map = HashMap::new();
        for point in points {
            let sid = self.build_index(&point)?;
            self.build_row_data(&mut map, point, sid)
        }
        Ok(map)
    }

    pub fn build_write_group_loose_mode(
        &self,
        points: FlatBufferPoint,
    ) -> Result<HashMap<(SeriesId, SchemaId), RowGroup>> {
        let mut map = HashMap::new();
        for point in points {
            let sid = self.build_index(&point)?;
            match self.index.check_field_type_from_cache(sid, &point) {
                Ok(_) => {}
                Err(_) => {
                    self.index
                        .check_field_type_or_else_add(sid, &point)
                        .context(error::IndexErrSnafu)?;
                }
            }

            self.build_row_data(&mut map, point, sid)
        }
        Ok(map)
    }

    fn build_row_data(
        &self,
        map: &mut HashMap<(SeriesId, SchemaId), RowGroup>,
        point: Point,
        sid: u64,
    ) {
        let table_name = String::from_utf8(point.table().unwrap().to_vec()).unwrap();
        let table_schema = match self.index.get_table_schema(&table_name) {
            Ok(schema) => match schema {
                None => {
                    error!("failed get schema for table {}", table_name);
                    return;
                }
                Some(schema) => schema,
            },
            Err(_) => {
                error!("failed get schema for table {}", table_name);
                return;
            }
        };

        let (row, mut schema) = RowData::point_to_row_data(point, table_schema, sid);
        let schema_size = if !schema.is_empty() {
            size_of_val(&schema) + schema.capacity() * size_of_val(&schema[0])
        } else {
            size_of_val(&schema)
        };
        let schema_id = 0;
        let entry = map.entry((sid, schema_id)).or_insert(RowGroup {
            schema_id,
            schema: vec![],
            rows: vec![],
            range: TimeRange {
                min_ts: i64::MAX,
                max_ts: i64::MIN,
            },
            size: size_of::<RowGroup>() + schema_size,
        });

        entry.schema.append(&mut schema);
        entry.schema.sort();
        entry.schema.dedup();
        entry.range.merge(&TimeRange {
            min_ts: row.ts,
            max_ts: row.ts,
        });
        entry.size += row.size();
        //todo: remove this copy
        entry.rows.push(row);
    }

    fn build_index(&self, info: &Point) -> Result<u64> {
        if let Some(id) = self
            .index
            .get_sid_from_cache(info)
            .context(error::IndexErrSnafu)?
        {
            return Ok(id);
        }

        let id = self
            .index
            .add_series_if_not_exists(info)
            .context(error::IndexErrSnafu)?;

        Ok(id)
    }

    pub fn version_edit(&self, last_seq: u64) -> (Vec<VersionEdit>, Vec<VersionEdit>) {
        let mut edits = vec![];
        let mut files = vec![];

        for (id, ts) in &self.ts_families {
            //tsfamily edit
            let mut edit = VersionEdit::new();
            edit.add_tsfamily(*id, self.name.clone());
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

    pub fn get_series_key(&self, sid: u64) -> IndexResult<Option<SeriesKey>> {
        self.index.get_series_key(sid)
    }

    pub fn get_table_schema(&self, table_name: &str) -> IndexResult<Option<TableSchema>> {
        self.index.get_table_schema(table_name)
    }

    pub fn get_table_schema_by_series_id(&self, sid: u64) -> IndexResult<Option<TableSchema>> {
        self.index.get_table_schema_by_series_id(sid)
    }

    pub fn get_tsfamily(&self, id: u32) -> Option<&Arc<RwLock<TseriesFamily>>> {
        self.ts_families.get(&id)
    }

    pub fn tsf_num(&self) -> usize {
        self.ts_families.len()
    }

    pub fn ts_families(&self) -> &HashMap<u32, Arc<RwLock<TseriesFamily>>> {
        &self.ts_families
    }

    pub fn for_each_ts_family<F>(&self, func: F)
    where
        F: FnMut((&TseriesFamilyId, &Arc<RwLock<TseriesFamily>>)),
    {
        self.ts_families.iter().for_each(func);
    }

    pub fn get_index(&self) -> Arc<db_index::DBIndex> {
        self.index.clone()
    }

    // todo: will delete in cluster version
    pub fn get_tsfamily_random(&self) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some((_, v)) = self.ts_families.iter().next() {
            return Some(v.clone());
        }

        None
    }
}

pub(crate) fn delete_table_async(
    database: String,
    table: String,
    version_set: Arc<RwLock<VersionSet>>,
) -> Result<()> {
    info!("Drop table: '{}.{}'", &database, &table);
    let version_set_rlock = version_set.read();
    let options = version_set_rlock.options();
    let db_instance = version_set_rlock.get_db(&database);
    drop(version_set_rlock);

    if let Some(db) = db_instance {
        let index = db.read().get_index();
        let sids = index
            .get_series_id_list(&table, &[])
            .context(error::IndexErrSnafu)?;
        debug!(
            "Drop table: deleting index in table: {}.{}",
            &database, &table
        );
        let field_infos = index
            .get_table_schema(&table)
            .context(error::IndexErrSnafu)?;
        index
            .del_table_schema(&table)
            .context(error::IndexErrSnafu)?;

        println!("{:?}", &sids);
        for sid in sids.iter() {
            index.del_series_info(*sid).context(error::IndexErrSnafu)?;
        }
        index.flush().context(error::IndexErrSnafu)?;

        if let Some(fields) = field_infos {
            debug!(
                "Drop table: deleting series in table: {}.{}",
                &database, &table
            );
            let fids: Vec<u64> = fields.fields.iter().map(|f| f.1.id).collect();
            let storage_fids: Vec<u64> = sids
                .iter()
                .flat_map(|sid| fids.iter().map(|fid| unite_id(*fid, *sid)))
                .collect();
            let time_range = &TimeRange {
                min_ts: Timestamp::MIN,
                max_ts: Timestamp::MAX,
            };

            for (ts_family_id, ts_family) in db.read().ts_families().iter() {
                ts_family.write().delete_cache(&storage_fids, time_range);
                let version = ts_family.read().super_version();
                for column_file in version.version.column_files(&storage_fids, time_range) {
                    column_file.add_tombstone(&storage_fids, time_range)?;
                }
            }
        }
    }
    Ok(())
}
