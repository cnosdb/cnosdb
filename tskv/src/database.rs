use std::{
    collections::{BTreeMap, HashMap},
    mem::{size_of, size_of_val},
    path::{self, Path},
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use models::utils::{split_id, unite_id};
use models::{SeriesKey, Timestamp};

use parking_lot::RwLock;
use snafu::ResultExt;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

use ::models::{FieldInfo, InMemPoint, SeriesInfo, Tag, ValueType};
use models::SchemaId;
use protos::models::{Point, Points};
use trace::{debug, error, info};

use crate::{
    error::{self, Result},
    index::db_index,
    index::IndexResult,
    kv_option::Options,
    memcache::{FieldVal, MemCache},
    memcache::{RowData, RowGroup},
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::LevelInfo,
    tseries_family::{TseriesFamily, Version},
    version_set::VersionSet,
    TimeRange, TseriesFamilyId,
};

#[derive(Debug)]
pub struct Database {
    name: String,
    index: Arc<RwLock<db_index::DBIndex>>,
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

    pub fn open_tsfamily(&mut self, ver: Arc<Version>) {
        let opt = ver.storage_opt();

        let tf = TseriesFamily::new(
            ver.tf_id(),
            ver.database().to_string(),
            MemCache::new(ver.tf_id(), self.opt.cache.max_buffer_size, ver.last_seq),
            ver.clone(),
            self.opt.cache.clone(),
            self.opt.storage.clone(),
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
        points: flatbuffers::Vector<flatbuffers::ForwardsUOffset<Point>>,
    ) -> Result<HashMap<(u64, u32), RowGroup>> {
        // (series id, schema id) -> RowGroup
        let mut map = HashMap::new();
        for point in points {
            let (mut info, row) = {
                (
                    SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu)?,
                    RowData::from(point),
                )
            };

            let sid = self.build_index_and_check_type(&mut info)?;

            let all_fileds = {
                let mut all_fileds = BTreeMap::new();
                let fields = info.field_infos();
                for (i, f) in row.fields.into_iter().enumerate() {
                    let (fid, _) = split_id(fields[i].field_id());
                    all_fileds.insert(fid, f);
                }

                for field in info.field_fill() {
                    all_fileds.insert(field.field_id() as u32, None);
                }
                all_fileds
            };

            let ts = row.ts;
            let schema_id = info.get_schema_id();
            let (schema, all_row, schema_size, row_size) = {
                let mut schema = Vec::with_capacity(all_fileds.len());
                let mut all_row: Vec<Option<FieldVal>> = Vec::with_capacity(all_fileds.len());
                let schema_size = schema.capacity() * size_of::<u32>();
                let mut row_size = all_row.capacity() * size_of::<Option<FieldVal>>();

                for (fid, field_val) in all_fileds {
                    schema.push(fid);

                    row_size += size_of_val(&field_val);
                    if let Some(val) = &field_val {
                        row_size += val.heap_size();
                    }

                    all_row.push(field_val);
                }
                (schema, all_row, schema_size, row_size)
            };

            let (_, sid) = split_id(sid);
            let entry = map.entry((sid, schema_id)).or_insert(RowGroup {
                schema_id,
                schema,
                rows: vec![],
                range: TimeRange {
                    min_ts: i64::MAX,
                    max_ts: i64::MIN,
                },
                size: size_of::<RowGroup>() + schema_size,
            });

            entry.range.merge(&TimeRange {
                min_ts: ts,
                max_ts: ts,
            });
            entry.size += row_size + size_of::<u64>();
            entry.rows.push(RowData {
                ts,
                fields: all_row,
            });
        }

        Ok(map)
    }

    fn build_index_and_check_type(&self, info: &mut SeriesInfo) -> Result<u64> {
        if let Some(id) = self.index.read().get_from_cache(info) {
            return Ok(id);
        }

        let id = self
            .index
            .write()
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
        self.index.write().get_series_key(sid)
    }

    pub fn get_table_schema(&self, table_name: &str) -> IndexResult<Option<Vec<FieldInfo>>> {
        self.index.write().get_table_schema(table_name)
    }

    pub fn get_table_schema_by_series_id(&self, sid: u64) -> IndexResult<Option<Vec<FieldInfo>>> {
        self.index.write().get_table_schema_by_series_id(sid)
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

    pub fn get_index(&self) -> Arc<RwLock<db_index::DBIndex>> {
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
            .read()
            .get_series_id_list(&table, &[])
            .context(error::IndexErrSnafu)?;

        let mut index_wlock = index.write();

        debug!(
            "Drop table: deleting index in table: {}.{}",
            &database, &table
        );
        let field_infos = index_wlock
            .get_table_schema(&table)
            .context(error::IndexErrSnafu)?;
        index_wlock
            .del_table_schema(&table)
            .context(error::IndexErrSnafu)?;

        println!("{:?}", &sids);
        for sid in sids.iter() {
            index_wlock
                .del_series_info(*sid)
                .context(error::IndexErrSnafu)?;
        }
        index_wlock.flush().context(error::IndexErrSnafu)?;

        drop(index_wlock);

        if let Some(fields) = field_infos {
            debug!(
                "Drop table: deleting series in table: {}.{}",
                &database, &table
            );
            let fids: Vec<u64> = fields.iter().map(|f| f.field_id()).collect();
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
