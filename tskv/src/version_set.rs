use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, atomic::Ordering, Arc, Mutex},
};

use meta::meta_client::{MetaClientRef, MetaRef};
use models::schema::{make_owner, split_owner, DatabaseSchema};
use parking_lot::RwLock;
use snafu::ResultExt;
use tokio::sync::watch::Receiver;
use tokio::sync::{mpsc::UnboundedSender, oneshot};

use trace::error;

use crate::compaction::FlushReq;
use crate::{
    database::Database,
    error::MetaSnafu,
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
    pub fn empty(opt: Arc<Options>) -> Self {
        Self {
            opt,
            dbs: HashMap::new(),
        }
    }

    pub fn new(
        meta: MetaRef,
        opt: Arc<Options>,
        ver_set: HashMap<TseriesFamilyId, Arc<Version>>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) -> Result<Self> {
        let mut dbs = HashMap::new();

        for (id, ver) in ver_set {
            let owner = ver.database().to_string();
            let (tenant, database) = split_owner(&owner);
            let seq = ver.last_seq;

            let schema = match meta.tenant_manager().tenant_meta(tenant) {
                None => DatabaseSchema::new(tenant, database),
                Some(client) => match client.get_db_schema(database).context(MetaSnafu)? {
                    None => DatabaseSchema::new(tenant, database),
                    Some(schema) => schema,
                },
            };
            let db: &mut Arc<RwLock<Database>> =
                dbs.entry(owner)
                    .or_insert(Arc::new(RwLock::new(Database::new(
                        schema,
                        opt.clone(),
                        meta.clone(),
                    )?)));

            db.write().open_tsfamily(ver, flush_task_sender.clone());
        }

        Ok(Self { dbs, opt })
    }

    pub fn options(&self) -> Arc<Options> {
        self.opt.clone()
    }

    pub fn create_db(
        &mut self,
        schema: DatabaseSchema,
        meta: MetaRef,
    ) -> Result<Arc<RwLock<Database>>> {
        let db = self
            .dbs
            .entry(schema.owner())
            .or_insert(Arc::new(RwLock::new(Database::new(
                schema,
                self.opt.clone(),
                meta.clone(),
            )?)))
            .clone();
        Ok(db)
    }

    pub fn delete_db(&mut self, tenant: &str, database: &str) -> Option<Arc<RwLock<Database>>> {
        let owner = make_owner(tenant, database);
        self.dbs.remove(&owner)
    }

    pub fn db_exists(&self, tenant: &str, database: &str) -> bool {
        let owner = make_owner(tenant, database);
        self.dbs.get(&owner).is_some()
    }

    pub fn get_db_schema(&self, tenant: &str, database: &str) -> Result<Option<DatabaseSchema>> {
        let owner = make_owner(tenant, database);
        let db = self.dbs.get(&owner);
        match db {
            None => Ok(None),
            Some(db) => Ok(Some(db.read().get_schema()?)),
        }
    }

    pub fn get_all_db(&self) -> &HashMap<String, Arc<RwLock<Database>>> {
        &self.dbs
    }

    pub fn get_db(&self, tenant_name: &str, db_name: &str) -> Option<Arc<RwLock<Database>>> {
        let owner_name = make_owner(tenant_name, db_name);
        if let Some(v) = self.dbs.get(&owner_name) {
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
                return Some(v);
            }
        }

        None
    }

    pub fn get_tsfamily_by_name_id(
        &self,
        tenant: &str,
        database: &str,
        tf_id: u32,
    ) -> Option<Arc<RwLock<TseriesFamily>>> {
        let owner = make_owner(tenant, database);
        if let Some(db) = self.dbs.get(&owner) {
            return db.read().get_tsfamily(tf_id);
        }

        None
    }

    // will delete in cluster version
    pub fn get_tsfamily_by_name(
        &self,
        tenant: &str,
        database: &str,
    ) -> Option<Arc<RwLock<TseriesFamily>>> {
        let owner = make_owner(tenant, database);
        if let Some(db) = self.dbs.get(&owner) {
            return db.read().get_tsfamily_random();
        }

        None
    }
}
