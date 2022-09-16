use std::{
    collections::{BTreeMap, HashMap},
    path::{self, Path},
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use crate::{
    error::{self, Result},
    memcache::{RowData, RowGroup},
    version_set::VersionSet,
    TimeRange, TseriesFamilyId,
};
use models::utils::{split_id, unite_id};
use models::Timestamp;

use parking_lot::RwLock;
use protos::models::{Point, Points};
use snafu::ResultExt;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use trace::{debug, error, info};

use ::models::{FieldInfo, InMemPoint, SeriesInfo, Tag, ValueType};

use crate::tseries_family::LevelInfo;
use crate::{
    index::db_index,
    kv_option::Options,
    memcache::MemCache,
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::{TseriesFamily, Version},
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

    pub async fn switch_memcache(&self, tf_id: u32, seq: u64) {
        if let Some(tf) = self.ts_families.get(&tf_id) {
            let mut tf = tf.write();
            let mem = Arc::new(RwLock::new(MemCache::new(
                tf_id,
                self.opt.cache.max_buffer_size,
                seq,
            )));

            tf.switch_memcache(mem).await;
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
            let mut info =
                SeriesInfo::from_flatbuffers(&point).context(error::InvalidModelSnafu)?;

            let sid = self.build_index_and_check_type(&mut info)?;

            let row = RowData::from(point);
            let mut all_fileds = BTreeMap::new();
            let fields = info.field_infos();
            for (i, f) in row.fields.into_iter().enumerate() {
                all_fileds.insert(fields[i].field_id(), f);
            }

            let fields = info.field_fill();
            for i in 0..fields.len() {
                all_fileds.insert(fields[i].field_id(), None);
            }

            let ts = row.ts;
            let schema_id = info.get_schema_id();
            let mut schema = Vec::with_capacity(all_fileds.len());
            let mut all_row = Vec::with_capacity(all_fileds.len());
            for (k, v) in all_fileds {
                let (fid, _) = split_id(k);

                all_row.push(v);
                schema.push(fid);
            }

            let (_, sid) = split_id(sid);
            let entry = map.entry((sid, schema_id)).or_insert(RowGroup {
                schema_id,
                schema,
                rows: vec![],
                range: TimeRange {
                    min_ts: i64::MAX,
                    max_ts: i64::MIN,
                },
            });

            entry.range.merge(&TimeRange {
                min_ts: ts,
                max_ts: ts,
            });
            entry.rows.push(RowData {
                ts,
                fields: all_row,
            });
        }

        return Ok(map);
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
        for (_, v) in &self.ts_families {
            return Some(v.clone());
        }

        None
    }
}

pub(crate) async fn delete_table_async(
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
            .get_series_id_list(&table, &vec![])
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
            .await
            .context(error::IndexErrSnafu)?;

        println!("{:?}", &sids);
        for sid in sids.iter() {
            index_wlock
                .del_series_info(*sid)
                .await
                .context(error::IndexErrSnafu)?;
        }
        index_wlock.flush().await.context(error::IndexErrSnafu)?;

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
