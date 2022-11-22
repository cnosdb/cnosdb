use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use models::schema::DatabaseSchema;
use parking_lot::RwLock;
use tokio::sync::watch::Receiver;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

use trace::error;

use crate::compaction::FlushReq;
use crate::{
    database::Database,
    error::Result,
    index::db_index,
    kv_option::StorageOptions,
    memcache::MemCache,
    summary::{SummaryTask, VersionEdit},
    tseries_family::{LevelInfo, TseriesFamily, Version},
    Options, TseriesFamilyId,
};

#[derive(Debug)]
pub struct VersionSet {
    opt: Arc<Options>,
    // DBName -> DB
    dbs: HashMap<String, Arc<RwLock<Database>>>,
}

impl VersionSet {
    pub fn new(
        opt: Arc<Options>,
        ver_set: HashMap<TseriesFamilyId, Arc<Version>>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) -> Result<Self> {
        let mut dbs = HashMap::new();

        for (id, ver) in ver_set {
            let name = ver.database().to_string();
            let seq = ver.last_seq;

            let db: &mut Arc<RwLock<Database>> =
                dbs.entry(name.clone())
                    .or_insert(Arc::new(RwLock::new(Database::new(
                        DatabaseSchema::new(&name),
                        opt.clone(),
                    )?)));

            db.write().open_tsfamily(ver, flush_task_sender.clone());
        }

        Ok(Self { dbs, opt })
    }

    pub fn options(&self) -> Arc<Options> {
        self.opt.clone()
    }

    pub fn create_db(&mut self, schema: DatabaseSchema) -> Result<Arc<RwLock<Database>>> {
        let db = self
            .dbs
            .entry(schema.name.clone())
            .or_insert(Arc::new(RwLock::new(Database::new(
                schema,
                self.opt.clone(),
            )?)))
            .clone();
        Ok(db)
    }

    pub fn delete_db(&mut self, name: &String) -> Option<Arc<RwLock<Database>>> {
        self.dbs.remove(name)
    }

    pub fn db_exists(&self, name: &str) -> bool {
        self.dbs.get(name).is_some()
    }

    pub fn get_db_schema(&self, name: &str) -> Option<DatabaseSchema> {
        self.dbs.get(name).map(|db| db.read().get_schema())
    }

    pub fn get_all_db(&self) -> &HashMap<String, Arc<RwLock<Database>>> {
        &self.dbs
    }

    pub fn get_db(&self, name: &str) -> Option<Arc<RwLock<Database>>> {
        if let Some(v) = self.dbs.get(name) {
            return Some(v.clone());
        }

        None
    }

    pub fn tsf_num(&self) -> usize {
        let mut size = 0;
        for db in self.dbs.values() {
            size += db.read().tsf_num();
        }

        size
    }

    pub fn get_tsfamily_by_tf_id(&self, tf_id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        for db in self.dbs.values() {
            if let Some(v) = db.read().get_tsfamily(tf_id) {
                return Some(v.clone());
            }
        }

        None
    }

    // will delete in cluster version
    pub fn get_tsfamily_by_name(&self, name: &str) -> Option<Arc<RwLock<TseriesFamily>>> {
        if let Some(db) = self.dbs.get(name) {
            return db.read().get_tsfamily_random();
        }

        None
    }
}
