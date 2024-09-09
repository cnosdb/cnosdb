use std::collections::HashMap;
use std::sync::Arc;

use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::schema::database_schema::{make_owner, DatabaseSchema};
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use crate::context::GlobalContext;
use crate::database::{Database, DatabaseFactory};
use crate::error::TskvResult;
use crate::index::ts_index::TSIndex;
use crate::summary::SummaryRequest;
use crate::tsfamily::tseries_family::TseriesFamily;
use crate::tsfamily::version::Version;
use crate::{Options, VnodeId};

#[derive(Debug)]
pub struct VersionSet {
    /// Maps DBName -> DB
    dbs: HashMap<String, Arc<RwLock<Database>>>,
    _runtime: Arc<Runtime>,
    db_factory: DatabaseFactory,
}

impl VersionSet {
    pub fn empty(
        meta: MetaRef,
        opt: Arc<Options>,
        ctx: Arc<GlobalContext>,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Self {
        let db_factory =
            DatabaseFactory::new(meta, memory_pool.clone(), metrics_register, opt, ctx);

        Self {
            dbs: HashMap::new(),
            _runtime: runtime,
            db_factory,
        }
    }

    pub async fn new(
        meta: MetaRef,
        opt: Arc<Options>,
        ctx: Arc<GlobalContext>,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        ver_set: HashMap<VnodeId, (DatabaseSchema, Arc<Version>)>,
        metrics_register: Arc<MetricsRegister>,
    ) -> TskvResult<Self> {
        let mut dbs = HashMap::new();
        let db_factory =
            DatabaseFactory::new(meta.clone(), memory_pool, metrics_register, opt, ctx);

        for (schema, ver) in ver_set.into_values() {
            let owner = (*ver.owner()).clone();

            let db: &mut Arc<RwLock<Database>> = dbs.entry(owner).or_insert(Arc::new(RwLock::new(
                db_factory.create_database(schema).await?,
            )));

            let tf_id = ver.tf_id();
            db.write().await.open_tsfamily(ver);
            db.write().await.get_ts_index_or_add(tf_id).await?;
        }

        Ok(Self {
            dbs,
            _runtime: runtime,
            db_factory,
        })
    }

    pub async fn create_db(&mut self, schema: DatabaseSchema) -> TskvResult<Arc<RwLock<Database>>> {
        let db = self
            .dbs
            .entry(schema.owner())
            .or_insert(Arc::new(RwLock::new(
                self.db_factory.create_database(schema).await?,
            )))
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

    pub async fn get_tsfamily_by_tf_id(&self, tf_id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        // FIXME: add tsf_id -> db HashTable
        for db in self.dbs.values() {
            if let Some(v) = db.read().await.get_tsfamily(tf_id) {
                return Some(v);
            }
        }

        None
    }

    pub async fn get_tsfamily_tsindex_by_tf_id(
        &self,
        tf_id: u32,
    ) -> (
        Option<Arc<RwLock<TseriesFamily>>>,
        Option<Arc<RwLock<TSIndex>>>,
    ) {
        let mut vnode = None;
        let mut vnode_index = None;
        for db in self.dbs.values() {
            let db = db.read().await;
            if let Some(v) = db.get_tsfamily(tf_id) {
                vnode = Some(v);
                if let Some(v) = db.get_ts_index(tf_id) {
                    vnode_index = Some(v);
                }
                break;
            }
        }

        (vnode, vnode_index)
    }

    pub async fn get_tsfamily_by_name_id(
        &self,
        tenant: &str,
        database: &str,
        tf_id: u32,
    ) -> Option<Arc<RwLock<TseriesFamily>>> {
        let owner = make_owner(tenant, database);
        if let Some(db) = self.dbs.get(&owner) {
            return db.read().await.get_tsfamily(tf_id);
        }

        None
    }

    /// Generated data is `VersionEdit`s for all vnodes and db-files,
    /// and `HashMap<ColumnFileId, Arc<BloomFilter>>` for index data
    /// (field-id filter) of db-files.
    pub async fn ts_families_version_edit(&self) -> TskvResult<Vec<SummaryRequest>> {
        let mut requests = vec![];
        for db in self.dbs.values() {
            let db_reader = db.read().await;
            let ts_families = db_reader.ts_families();
            for (_, tsf) in ts_families.iter() {
                let tf_family = tsf.read().await;
                let ve = tf_family.build_version_edit();
                let file_metas = tf_family.column_files_bloom_filter().await?;

                let request = SummaryRequest {
                    version_edit: ve,
                    ts_family: tsf.clone(),
                    mem_caches: None,
                    file_metas: Some(file_metas),
                };

                requests.push(request);
            }
        }

        Ok(requests)
    }

    pub async fn get_tsfamily_seq_no_map(&self) -> HashMap<VnodeId, u64> {
        let mut r = HashMap::with_capacity(self.dbs.len());
        for db in self.dbs.values() {
            let db = db.read().await;
            for tsf in db.ts_families().values() {
                let tsf = tsf.read().await;
                r.insert(tsf.tf_id(), tsf.super_version().version.last_seq());
            }
        }
        r
    }
}

#[cfg(test)]
impl VersionSet {
    pub async fn tsf_num(&self) -> usize {
        let mut size = 0;
        for db in self.dbs.values() {
            size += db.read().await.tsf_num();
        }

        size
    }

    pub async fn get_database_tsfs(
        &self,
        tenant: &str,
        database: &str,
    ) -> Option<Vec<Arc<RwLock<TseriesFamily>>>> {
        let owner = make_owner(tenant, database);
        if let Some(db) = self.dbs.get(&owner) {
            return Some(db.read().await.ts_families().values().cloned().collect());
        }

        None
    }
}
