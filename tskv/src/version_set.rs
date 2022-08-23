use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use parking_lot::RwLock;
use tokio::sync::{mpsc::UnboundedSender, oneshot};
use trace::error;

use crate::tseries_family::LevelInfo;
use crate::{
    database,
    index::db_index,
    kv_option::{TseriesFamDesc, TseriesFamOpt},
    memcache::MemCache,
    summary::{SummaryTask, VersionEdit},
    tseries_family::{TseriesFamily, Version},
};

#[derive(Debug)]
pub struct VersionSet {
    ts_opt: Arc<TseriesFamOpt>,

    incr_id: AtomicU32, // todo: will delete in cluster version

    dbs: HashMap<String, Arc<RwLock<database::Database>>>,
}

impl VersionSet {
    pub fn new(opt: Arc<TseriesFamOpt>, ver_set: HashMap<u32, Arc<Version>>) -> Self {
        let mut max_id = 0_u32;
        let mut dbs = HashMap::new();

        for (id, ver) in ver_set {
            let name = ver.tf_name().to_string();
            let seq = ver.last_seq;

            let db = dbs.entry(name.clone()).or_insert_with(|| {
                Arc::new(RwLock::new(database::Database::new(&name, &opt.index_path)))
            });

            db.write().open_tsfamily(ver.clone());

            if max_id < ver.tf_id() {
                max_id = id;
            }
        }

        Self {
            dbs,
            ts_opt: opt,
            incr_id: AtomicU32::new(max_id + 1),
        }
    }

    pub fn create_db(&mut self, name: &String) -> Arc<RwLock<database::Database>> {
        self.dbs
            .entry(name.clone())
            .or_insert_with(|| {
                Arc::new(RwLock::new(database::Database::new(
                    name,
                    &self.ts_opt.index_path,
                )))
            })
            .clone()
    }

    pub fn get_all_db(&self) -> &HashMap<String, Arc<RwLock<database::Database>>> {
        return &self.dbs;
    }

    pub fn get_db(&self, name: &String) -> Option<Arc<RwLock<database::Database>>> {
        if let Some(v) = self.dbs.get(name) {
            return Some(v.clone());
        }

        None
    }

    pub fn tsf_num(&self) -> usize {
        let mut size = 0;
        for (_, db) in &self.dbs {
            size += db.read().tsf_num();
        }

        return size;
    }

    pub fn get_tsfamily_by_tf_id(&self, tf_id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        for (_, db) in &self.dbs {
            if let Some(v) = db.read().get_tsfamily(tf_id) {
                return Some(v.clone());
            }
        }

        None
    }

    // will delete in cluster version
    pub fn get_tsfamily_by_name(&self, name: &String) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some(db) = self.dbs.get(name) {
            return db.read().get_tsfamily_random();
        }

        None
    }
}
