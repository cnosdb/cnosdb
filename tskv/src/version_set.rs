use std::collections::HashMap;
use std::sync::Arc;

use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::schema::{make_owner, split_owner, DatabaseSchema};
use snafu::ResultExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::context::GlobalSequenceContext;
use crate::database::Database;
use crate::error::{MetaSnafu, Result};
use crate::summary::VersionEdit;
use crate::tseries_family::{TseriesFamily, Version};
use crate::{ColumnFileId, Options, TseriesFamilyId};

#[derive(Debug)]
pub struct VersionSet {
    opt: Arc<Options>,
    /// Maps DBName -> DB
    dbs: HashMap<String, Arc<RwLock<Database>>>,
    runtime: Arc<Runtime>,
    memory_pool: MemoryPoolRef,
    metrics_register: Arc<MetricsRegister>,
}

impl VersionSet {
    pub fn empty(
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Self {
        Self {
            opt,
            dbs: HashMap::new(),
            runtime,
            memory_pool,
            metrics_register,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        meta: MetaRef,
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        ver_set: HashMap<TseriesFamilyId, Arc<Version>>,
        flush_task_sender: Sender<FlushReq>,
        compact_task_sender: Sender<CompactTask>,
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<Self> {
        let mut dbs = HashMap::new();
        for ver in ver_set.into_values() {
            let owner = ver.database().to_string();
            let (tenant, database) = split_owner(&owner);

            let schema = match meta.tenant_manager().tenant_meta(tenant).await {
                None => DatabaseSchema::new(tenant, database),
                Some(client) => match client.get_db_schema(database).context(MetaSnafu)? {
                    None => DatabaseSchema::new(tenant, database),
                    Some(schema) => schema,
                },
            };

            let db: &mut Arc<RwLock<Database>> = dbs.entry(owner).or_insert(Arc::new(RwLock::new(
                Database::new(
                    schema,
                    opt.clone(),
                    runtime.clone(),
                    meta.clone(),
                    memory_pool.clone(),
                    metrics_register.clone(),
                )
                .await?,
            )));

            let tf_id = ver.tf_id();
            db.write().await.open_tsfamily(
                ver,
                flush_task_sender.clone(),
                compact_task_sender.clone(),
            );
            db.write().await.get_ts_index_or_add(tf_id).await?;
        }

        Ok(Self {
            dbs,
            opt,
            runtime,
            memory_pool,
            metrics_register,
        })
    }

    pub fn options(&self) -> Arc<Options> {
        self.opt.clone()
    }

    pub async fn create_db(
        &mut self,
        schema: DatabaseSchema,
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
    ) -> Result<Arc<RwLock<Database>>> {
        let sub_register = self.metrics_register.sub_register([
            ("tenant", schema.tenant_name()),
            ("database", schema.database_name()),
        ]);
        let db = self
            .dbs
            .entry(schema.owner())
            .or_insert(Arc::new(RwLock::new(
                Database::new(
                    schema,
                    self.opt.clone(),
                    self.runtime.clone(),
                    meta.clone(),
                    memory_pool,
                    sub_register,
                )
                .await?,
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

    pub async fn get_db_schema(
        &self,
        tenant: &str,
        database: &str,
    ) -> Result<Option<DatabaseSchema>> {
        let owner = make_owner(tenant, database);
        let db = self.dbs.get(&owner);
        match db {
            None => Ok(None),
            Some(db) => Ok(Some(db.read().await.get_schema()?)),
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

    pub async fn get_tsfamily_by_tf_id(&self, tf_id: u32) -> Option<Arc<RwLock<TseriesFamily>>> {
        for db in self.dbs.values() {
            if let Some(v) = db.read().await.get_tsfamily(tf_id) {
                return Some(v);
            }
        }

        None
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

    /// Snashots last version before `last_seq` of system state.
    ///
    /// Generated data is `VersionEdit`s for all vnodes and db-files,
    /// and `HashMap<ColumnFileId, Arc<BloomFilter>>` for index data
    /// (field-id filter) of db-files.
    pub async fn snapshot(&self) -> (Vec<VersionEdit>, HashMap<ColumnFileId, Arc<BloomFilter>>) {
        let mut version_edits = vec![];
        let mut file_metas: HashMap<ColumnFileId, Arc<BloomFilter>> = HashMap::new();
        for db in self.dbs.values() {
            db.read()
                .await
                .snapshot(None, &mut version_edits, &mut file_metas)
                .await;
        }
        (version_edits, file_metas)
    }

    /// Try to build and send `FlushReq`s to flush job for all ts_families.
    pub async fn send_flush_req(&self) {
        for db in self.dbs.values() {
            for tsf in db.read().await.ts_families().values() {
                let tsf_inner = tsf.clone();
                self.runtime.spawn(async move {
                    let mut tsf = tsf_inner.write().await;
                    tsf.switch_to_immutable();
                    tsf.send_flush_req(true).await;
                });
            }
        }
    }

    /// **Please call this function after system recovered.**
    ///
    /// Get GlobalSequenceContext to store current minimum sequence number of all TseriesFamilies,
    /// one use is fetching wal files which could be deleted.
    pub async fn get_global_sequence_context(&self) -> GlobalSequenceContext {
        let mut tsf_seq_map: HashMap<TseriesFamilyId, u64> = HashMap::new();
        for (_, database) in self.dbs.iter() {
            for (tsf_id, tsf) in database.read().await.ts_families().iter() {
                tsf_seq_map.insert(*tsf_id, tsf.read().await.seq_no());
            }
        }

        GlobalSequenceContext::new(tsf_seq_map)
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
