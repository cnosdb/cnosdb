use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cache::ShardedAsyncCache;
use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::Timestamp;
use parking_lot::RwLock as SyncRwLock;
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::RwLock;
use utils::BloomFilter;

use crate::compaction::CompactTask;
use crate::context::GlobalContext;
use crate::error::{Error, Result};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::kv_option::{Options, StorageOptions, DELTA_PATH, TSM_PATH};
use crate::memcache::MemCache;
use crate::record_file::{Reader, RecordDataType, RecordDataVersion, Writer};
use crate::tseries_family::{ColumnFile, LevelInfo, TseriesFamily, Version};
use crate::tsm2::reader::TSM2Reader;
use crate::version_set::VersionSet;
use crate::{byte_utils, file_utils, ColumnFileId, LevelId, TseriesFamilyId};

const MAX_BATCH_SIZE: usize = 64;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct CompactMeta {
    pub file_id: ColumnFileId,
    pub file_size: u64,
    pub tsf_id: TseriesFamilyId,
    pub level: LevelId,
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
    pub high_seq: u64,
    pub low_seq: u64,
    pub is_delta: bool,
}

impl Default for CompactMeta {
    fn default() -> Self {
        Self {
            file_id: 0,
            file_size: 0,
            tsf_id: 0,
            level: 0,
            min_ts: Timestamp::MAX,
            max_ts: Timestamp::MIN,
            high_seq: u64::MIN,
            low_seq: u64::MIN,
            is_delta: false,
        }
    }
}

/// There are serial fields set with default value:
/// - tsf_id
/// - high_seq
/// - low_seq
impl From<&ColumnFile> for CompactMeta {
    fn from(file: &ColumnFile) -> Self {
        Self {
            file_id: file.file_id(),
            file_size: file.size(),
            level: file.level(),
            min_ts: file.time_range().min_ts,
            max_ts: file.time_range().max_ts,
            is_delta: file.is_delta(),
            ..Default::default()
        }
    }
}

impl CompactMeta {
    pub fn file_path(
        &self,
        storage_opt: &StorageOptions,
        database: &str,
        ts_family_id: TseriesFamilyId,
    ) -> PathBuf {
        if self.is_delta {
            let base_dir = storage_opt.delta_dir(database, ts_family_id);
            file_utils::make_delta_file(base_dir, self.file_id)
        } else {
            let base_dir = storage_opt.tsm_dir(database, ts_family_id);
            file_utils::make_tsm_file(base_dir, self.file_id)
        }
    }

    pub async fn rename_file(
        &mut self,
        storage_opt: &StorageOptions,
        database: &str,
        ts_family_id: TseriesFamilyId,
        file_id: ColumnFileId,
    ) -> Result<PathBuf> {
        let old_name = if self.is_delta {
            let base_dir = storage_opt
                .move_dir(database, ts_family_id)
                .join(DELTA_PATH);
            file_utils::make_delta_file(base_dir, self.file_id)
        } else {
            let base_dir = storage_opt.move_dir(database, ts_family_id).join(TSM_PATH);
            file_utils::make_tsm_file(base_dir, self.file_id)
        };

        let new_name = if self.is_delta {
            let base_dir = storage_opt.delta_dir(database, ts_family_id);
            file_utils::make_delta_file(base_dir, file_id)
        } else {
            let base_dir = storage_opt.tsm_dir(database, ts_family_id);
            file_utils::make_tsm_file(base_dir, file_id)
        };
        trace::info!("rename file from {:?} to {:?}", &old_name, &new_name);
        file_utils::rename(old_name, &new_name).await?;
        self.file_id = file_id;
        Ok(new_name)
    }
}

pub struct CompactMetaBuilder {
    pub ts_family_id: TseriesFamilyId,
}

impl CompactMetaBuilder {
    pub fn new(ts_family_id: TseriesFamilyId) -> Self {
        Self { ts_family_id }
    }

    pub fn build(
        &self,
        file_id: u64,
        file_size: u64,
        level: LevelId,
        min_ts: Timestamp,
        max_ts: Timestamp,
    ) -> CompactMeta {
        CompactMeta {
            file_id,
            file_size,
            tsf_id: self.ts_family_id,
            level,
            min_ts,
            max_ts,
            is_delta: level == 0,
            ..Default::default()
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct VersionEdit {
    pub has_seq_no: bool,
    pub seq_no: u64,
    pub has_file_id: bool,
    pub file_id: u64,
    pub max_level_ts: Timestamp,
    pub add_files: Vec<CompactMeta>,
    pub del_files: Vec<CompactMeta>,

    pub del_tsf: bool,
    pub add_tsf: bool,
    pub tsf_id: TseriesFamilyId,
    pub tsf_name: String,
}

impl Default for VersionEdit {
    fn default() -> Self {
        Self {
            has_seq_no: false,
            seq_no: 0,
            has_file_id: false,
            file_id: 0,
            max_level_ts: i64::MIN,
            add_files: vec![],
            del_files: vec![],
            del_tsf: false,
            add_tsf: false,
            tsf_id: 0,
            tsf_name: String::from(""),
        }
    }
}

impl VersionEdit {
    pub fn new(vnode_id: TseriesFamilyId) -> Self {
        Self {
            tsf_id: vnode_id,
            ..Default::default()
        }
    }

    pub fn new_add_vnode(vnode_id: u32, owner: String, seq_no: u64) -> Self {
        Self {
            tsf_id: vnode_id,
            tsf_name: owner,
            add_tsf: true,
            has_seq_no: true,
            seq_no,
            ..Default::default()
        }
    }

    pub fn new_del_vnode(vnode_id: u32) -> Self {
        Self {
            tsf_id: vnode_id,
            del_tsf: true,
            ..Default::default()
        }
    }

    pub fn update_vnode_id(&mut self, new_id: u32) {
        self.tsf_id = new_id;

        for f in self.add_files.iter_mut() {
            f.tsf_id = new_id;
        }

        for f in self.del_files.iter_mut() {
            f.tsf_id = new_id;
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::RecordFileEncode { source: (e) })
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        bincode::deserialize(buf).map_err(|e| Error::RecordFileDecode { source: (e) })
    }

    pub fn encode_vec(data: &[Self]) -> Result<Vec<u8>> {
        let mut buf: Vec<u8> = Vec::with_capacity(data.len() * 32);
        for ve in data {
            let ve_buf =
                bincode::serialize(ve).map_err(|e| Error::RecordFileEncode { source: (e) })?;
            let pos = buf.len();
            buf.resize(pos + 4 + ve_buf.len(), 0_u8);
            buf[pos..pos + 4].copy_from_slice((ve_buf.len() as u32).to_be_bytes().as_slice());
            buf[pos + 4..].copy_from_slice(&ve_buf);
        }

        Ok(buf)
    }

    pub fn decode_vec(buf: &[u8]) -> Result<Vec<Self>> {
        let mut list: Vec<Self> = Vec::with_capacity(buf.len() / 32 + 1);
        let mut pos = 0_usize;
        while pos < buf.len() {
            if buf.len() - pos < 4 {
                break;
            }
            let len = byte_utils::decode_be_u32(&buf[pos..pos + 4]);
            pos += 4;
            if buf.len() - pos < len as usize {
                break;
            }
            let ve = Self::decode(&buf[pos..pos + len as usize])?;
            pos += len as usize;
            list.push(ve);
        }

        Ok(list)
    }

    pub fn add_file(&mut self, compact_meta: CompactMeta, max_level_ts: i64) {
        if compact_meta.high_seq != 0 {
            // ComapctMeta.seq_no only makes sense when flush.
            // In other processes, we set high_seq 0 .
            self.has_seq_no = true;
            self.seq_no = compact_meta.high_seq;
        }
        self.has_file_id = true;
        self.file_id = self.file_id.max(compact_meta.file_id);
        self.max_level_ts = max(self.max_level_ts, max_level_ts);
        self.add_files.push(compact_meta);
    }

    pub fn del_file(&mut self, level: LevelId, file_id: u64, is_delta: bool) {
        self.del_files.push(CompactMeta {
            file_id,
            level,
            is_delta,
            ..Default::default()
        });
    }
}

impl Display for VersionEdit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seq_no: {}, file_id: {}, add_files: {}, del_files: {}, del_tsf: {}, add_tsf: {}, tsf_id: {}, tsf_name: {}, has_seq_no: {}, has_file_id: {}, max_level_ts: {}",
               self.seq_no, self.file_id, self.add_files.len(), self.del_files.len(), self.del_tsf, self.add_tsf, self.tsf_id, self.tsf_name, self.has_seq_no, self.has_file_id, self.max_level_ts)
    }
}

pub struct Summary {
    meta: MetaRef,
    file_no: u64,
    version_set: Arc<RwLock<VersionSet>>,
    ctx: Arc<GlobalContext>,
    writer: Writer,
    opt: Arc<Options>,
    runtime: Arc<Runtime>,
    metrics_register: Arc<MetricsRegister>,
}

impl Summary {
    // create a new summary file
    pub async fn new(
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<Self> {
        let db = VersionEdit::default();
        let path = file_utils::make_summary_file(opt.storage.summary_dir(), 0);
        let mut w = Writer::open(path, RecordDataType::Summary).await.unwrap();
        let buf = db.encode()?;
        let _ = w
            .write_record(
                RecordDataVersion::V1.into(),
                RecordDataType::Summary.into(),
                &[&buf],
            )
            .await?;
        w.sync().await?;

        Ok(Self {
            meta: meta.clone(),
            file_no: 0,
            version_set: Arc::new(RwLock::new(VersionSet::empty(
                meta.clone(),
                opt.clone(),
                runtime.clone(),
                memory_pool,
                metrics_register.clone(),
            ))),
            ctx: Arc::new(GlobalContext::default()),
            writer: w,
            opt,
            runtime,
            metrics_register,
        })
    }

    /// Recover from summary file
    ///
    /// If `load_file_filter` is `true`, field_filter will be loaded from file,
    /// otherwise default `BloomFilter::default()`
    #[allow(clippy::too_many_arguments)]
    pub async fn recover(
        meta: MetaRef,
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        compact_task_sender: Sender<CompactTask>,
        load_field_filter: bool,
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<Self> {
        let summary_path = opt.storage.summary_dir();
        let path = file_utils::make_summary_file(&summary_path, 0);
        let writer = Writer::open(path, RecordDataType::Summary).await.unwrap();
        let ctx = Arc::new(GlobalContext::default());
        let rd = Box::new(
            Reader::open(&file_utils::make_summary_file(&summary_path, 0))
                .await
                .unwrap(),
        );
        let vs = Self::recover_version(
            meta.clone(),
            rd,
            &ctx,
            opt.clone(),
            runtime.clone(),
            memory_pool,
            compact_task_sender,
            load_field_filter,
            metrics_register.clone(),
        )
        .await?;

        Ok(Self {
            meta: meta.clone(),
            file_no: 0,
            version_set: Arc::new(RwLock::new(vs)),
            ctx,
            writer,
            opt,
            runtime,
            metrics_register,
        })
    }

    /// Recover from summary file
    ///
    /// If `load_file_filter` is `true`, field_filter will be loaded from file,
    /// otherwise default `BloomFilter::default()`
    #[allow(clippy::too_many_arguments)]
    pub async fn recover_version(
        meta: MetaRef,
        mut reader: Box<Reader>,
        ctx: &GlobalContext,
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        memory_pool: MemoryPoolRef,
        compact_task_sender: Sender<CompactTask>,
        load_field_filter: bool,
        metrics_register: Arc<MetricsRegister>,
    ) -> Result<VersionSet> {
        let mut tsf_edits_map: HashMap<TseriesFamilyId, Vec<VersionEdit>> = HashMap::new();
        let mut database_map: HashMap<String, Arc<String>> = HashMap::new();
        let mut tsf_database_map: HashMap<TseriesFamilyId, Arc<String>> = HashMap::new();

        loop {
            let res = reader.read_record().await;
            match res {
                Ok(result) => {
                    let ed = VersionEdit::decode(&result.data)?;
                    if ed.add_tsf {
                        let db_ref = database_map
                            .entry(ed.tsf_name.clone())
                            .or_insert_with(|| Arc::new(ed.tsf_name.clone()));
                        tsf_database_map.insert(ed.tsf_id, db_ref.clone());
                        if ed.has_file_id {
                            tsf_edits_map.insert(ed.tsf_id, vec![ed]);
                        } else {
                            tsf_edits_map.insert(ed.tsf_id, vec![]);
                        }
                    } else if ed.del_tsf {
                        tsf_edits_map.remove(&ed.tsf_id);
                        tsf_database_map.remove(&ed.tsf_id);
                    } else if let Some(data) = tsf_edits_map.get_mut(&ed.tsf_id) {
                        data.push(ed);
                    }
                }
                Err(Error::Eof) => break,
                Err(Error::RecordFileHashCheckFailed { .. }) => continue,
                Err(e) => {
                    return Err(e);
                }
            }
        }

        let mut versions = HashMap::new();
        let mut max_seq_no_all = 0_u64;
        let mut has_file_id = false;
        let mut max_file_id_all = 0_u64;
        for (tsf_id, edits) in tsf_edits_map {
            let database = tsf_database_map.remove(&tsf_id).unwrap();

            let mut files: HashMap<u64, CompactMeta> = HashMap::new();
            let mut max_seq_no = 0;
            let mut max_level_ts = i64::MIN;
            for e in edits {
                if e.has_seq_no {
                    max_seq_no = std::cmp::max(max_seq_no, e.seq_no);
                    max_seq_no_all = std::cmp::max(max_seq_no_all, e.seq_no);
                }
                if e.has_file_id {
                    has_file_id = true;
                    max_file_id_all = std::cmp::max(max_file_id_all, e.file_id);
                }
                max_level_ts = std::cmp::max(max_level_ts, e.max_level_ts);
                for m in e.del_files {
                    files.remove(&m.file_id);
                }
                for m in e.add_files {
                    files.insert(m.file_id, m);
                }
            }

            // Recover levels_info according to `CompactMeta`s;
            let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(
                opt.storage.max_cached_readers,
            ));
            let weak_tsm_reader_cache = Arc::downgrade(&tsm_reader_cache);
            let mut levels = LevelInfo::init_levels(database.clone(), tsf_id, opt.storage.clone());
            for meta in files.into_values() {
                let field_filter = if load_field_filter {
                    let tsm_path = meta.file_path(opt.storage.as_ref(), &database, tsf_id);
                    let tsm_reader = TSM2Reader::open(tsm_path).await?;
                    let bloom_filter = tsm_reader.footer().series.bloom_filter();
                    Arc::new(bloom_filter.clone())
                } else {
                    Arc::new(BloomFilter::default())
                };
                levels[meta.level as usize].push_compact_meta(
                    &meta,
                    field_filter,
                    weak_tsm_reader_cache.clone(),
                );
            }
            let ver = Version::new(
                tsf_id,
                database,
                opt.storage.clone(),
                max_seq_no,
                levels,
                max_level_ts,
                tsm_reader_cache,
            );
            versions.insert(tsf_id, Arc::new(ver));
        }

        if has_file_id {
            ctx.set_file_id(max_file_id_all + 1);
        }
        let vs = VersionSet::new(
            meta,
            opt,
            runtime,
            memory_pool,
            versions,
            compact_task_sender,
            metrics_register,
        )
        .await?;
        Ok(vs)
    }

    /// Write VersionEdits into summary file, generate and then apply new Versions for TseriesFamilies.
    pub async fn apply_version_edit(&mut self, request: &SummaryRequest) -> Result<()> {
        // Write VersionEdits into summary file and join VersionEdits by Database/TseriesFamilyId.
        let tsf_id = request.ts_family.read().await.tf_id();

        trace::info!("Write version edit to summary for ts_family {}.", tsf_id);
        let buf = request.version_edit.encode()?;
        let _ = self
            .writer
            .write_record(
                RecordDataVersion::V1.into(),
                RecordDataType::Summary.into(),
                &[&buf],
            )
            .await?;
        self.writer.sync().await?;

        let mut min_seq = None;
        if request.version_edit.has_seq_no {
            min_seq = Some(request.version_edit.seq_no);
        }

        // Generate a new Version and then apply it.
        trace::info!("Applying new version for ts_family {}.", tsf_id);
        let mut file_metas = HashMap::new();
        if let Some(tmp) = &request.file_metas {
            file_metas = tmp.clone();
        }

        let new_version = request
            .ts_family
            .read()
            .await
            .version()
            .copy_apply_version_edits(vec![request.version_edit.clone()], &mut file_metas, min_seq);

        request
            .ts_family
            .write()
            .await
            .new_version(new_version, request.mem_caches.as_ref());
        trace::info!("Applied new version for ts_family {}.", tsf_id);

        Ok(())
    }

    pub async fn roll_summary_file(&mut self) -> Result<()> {
        if self.writer.file_size() < self.opt.storage.max_summary_size {
            return Ok(());
        }

        let new_path = file_utils::make_summary_file_tmp(self.opt.storage.summary_dir());
        LocalFileSystem::remove_if_exists(&new_path)
            .map_err(|e| Error::FileSystemError { source: e })?;

        let requests = self.version_set.read().await.snapshot_version_edit().await;
        self.writer = Writer::open(&new_path, RecordDataType::Summary).await?;

        for request in requests.iter() {
            self.apply_version_edit(request).await?;
        }

        let old_path = &self.writer.path().clone();
        trace::info!("Remove summary file {:?} -> {:?}", new_path, old_path,);
        std::fs::rename(new_path, old_path)?;

        self.writer = Writer::open(old_path, RecordDataType::Summary)
            .await
            .unwrap();

        Ok(())
    }

    pub fn version_set(&self) -> Arc<RwLock<VersionSet>> {
        self.version_set.clone()
    }

    pub fn global_context(&self) -> Arc<GlobalContext> {
        self.ctx.clone()
    }
}

pub async fn print_summary_statistics(path: impl AsRef<Path>) {
    let mut reader = Reader::open(&path).await.unwrap();
    println!("============================================================");
    let mut i = 0_usize;
    loop {
        match reader.read_record().await {
            Ok(record) => {
                let ve = VersionEdit::decode(&record.data).unwrap();
                println!("VersionEdit #{}, vnode_id: {}", i, ve.tsf_id);
                println!("------------------------------------------------------------");
                i += 1;
                if ve.add_tsf {
                    println!("  Add ts_family: {}", ve.tsf_id);
                    println!("------------------------------------------------------------");
                }
                if ve.del_tsf {
                    println!("  Delete ts_family: {}", ve.tsf_id);
                    println!("------------------------------------------------------------");
                }
                if ve.has_seq_no {
                    println!("  Presist sequence: {}", ve.seq_no);
                    println!("------------------------------------------------------------");
                }
                if ve.has_file_id {
                    if ve.add_files.is_empty() && ve.del_files.is_empty() {
                        println!("  Add file: None. Delete file: None.");
                    }
                    if !ve.add_files.is_empty() {
                        let mut buffer = String::new();
                        ve.add_files.iter().for_each(|f| {
                            buffer.push_str(
                                format!("{} (level: {}, {} B), ", f.file_id, f.level, f.file_size)
                                    .as_str(),
                            )
                        });
                        if !buffer.is_empty() {
                            buffer.truncate(buffer.len() - 2);
                        }
                        println!("  Add file:[ {} ]", buffer);
                    }
                    if !ve.del_files.is_empty() {
                        let mut buffer = String::new();
                        ve.del_files.iter().for_each(|f| {
                            buffer
                                .push_str(format!("{} (level: {}), ", f.file_id, f.level).as_str())
                        });
                        if !buffer.is_empty() {
                            buffer.truncate(buffer.len() - 2);
                        }
                        println!("  Delete file:[ {} ]", buffer);
                    }
                }
            }
            Err(Error::Eof) => break,
            Err(Error::RecordFileHashCheckFailed { .. }) => continue,
            Err(err) => panic!("Errors when read summary file: {}", err),
        }
        println!("============================================================");
    }
}

#[derive(Debug)]
pub struct SummaryRequest {
    pub version_edit: VersionEdit,
    pub ts_family: Arc<RwLock<TseriesFamily>>,
    pub mem_caches: Option<Vec<Arc<SyncRwLock<MemCache>>>>,
    pub file_metas: Option<HashMap<ColumnFileId, Arc<BloomFilter>>>,
}
#[derive(Debug)]
pub struct SummaryTask {
    pub request: SummaryRequest,
    pub call_back: OneShotSender<Result<()>>,
}

impl SummaryTask {
    pub fn new(
        ts_family: Arc<RwLock<TseriesFamily>>,
        version_edit: VersionEdit,
        file_metas: Option<HashMap<ColumnFileId, Arc<BloomFilter>>>,
        mem_caches: Option<Vec<Arc<SyncRwLock<MemCache>>>>,
        call_back: OneShotSender<Result<()>>,
    ) -> Self {
        Self {
            call_back,
            request: SummaryRequest {
                version_edit,
                ts_family,
                mem_caches,
                file_metas,
            },
        }
    }
}

#[derive(Clone)]
pub struct SummaryScheduler {
    sender: Sender<SummaryTask>,
}

impl SummaryScheduler {
    pub fn new(sender: Sender<SummaryTask>) -> Self {
        Self { sender }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::Arc;

    use config::Config;
    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_admin::AdminMeta;
    use metrics::metric_register::MetricsRegister;
    use models::schema::TenantOptions;
    use tokio::runtime::Runtime;
    use utils::BloomFilter;

    use crate::kv_option::{self, Options};
    use crate::summary::{CompactMeta, Summary, SummaryTask, VersionEdit};
    use crate::{Engine, TsKv, TseriesFamilyId};

    #[test]
    fn test_version_edit() {
        let mut ve = VersionEdit::default();

        let add_file_100 = CompactMeta {
            file_id: 100,
            ..Default::default()
        };
        ve.add_file(add_file_100, 100_000_000);

        let del_file_101 = CompactMeta {
            file_id: 101,
            ..Default::default()
        };
        ve.del_files.push(del_file_101);

        let ve_buf = ve.encode().unwrap();
        let ve2 = VersionEdit::decode(&ve_buf).unwrap();
        assert_eq!(ve2, ve);

        let ves = vec![ve, ve2];
        let ves_buf = VersionEdit::encode_vec(&ves).unwrap();
        let ves_2 = VersionEdit::decode_vec(&ves_buf).unwrap();
        assert_eq!(ves, ves_2);
    }

    struct SummaryTestHelper {
        tskv: TsKv,
        config: Config,
        runtime: Arc<Runtime>,
        meta_manager: Arc<AdminMeta>,
        test_case_name: String,
        memory_pool: Arc<GreedyMemoryPool>,
    }

    impl SummaryTestHelper {
        fn new(config: Config, test_case_name: String) -> Self {
            let runtime = Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(4)
                    .build()
                    .unwrap(),
            );

            let options = Options::from(&config);
            let runtime_clone = runtime.clone();
            let memory_pool = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));

            runtime_clone.block_on(async {
                let meta_manager = AdminMeta::new(config.clone()).await;
                meta_manager.add_data_node().await.unwrap();
                let _ = meta_manager
                    .create_tenant("cnosdb".to_string(), TenantOptions::default())
                    .await;
                let tskv = TsKv::open(
                    meta_manager.clone(),
                    options,
                    runtime.clone(),
                    memory_pool.clone(),
                    Arc::new(MetricsRegister::default()),
                )
                .await
                .unwrap();

                Self {
                    tskv,
                    config,
                    runtime,
                    meta_manager,
                    memory_pool,
                    test_case_name,
                }
            })
        }

        fn with_default_config(base_dir: String, test_case_name: String) -> Self {
            let mut config = config::get_config_for_test();
            config.storage.path = base_dir;
            Self::new(config, test_case_name)
        }
    }

    #[test]
    fn test_summary_recover() {
        let base_dir = "/tmp/test/summary/test_summary_recover";
        let _ = std::fs::remove_dir_all(base_dir);
        std::fs::create_dir_all(base_dir).unwrap();
        let test_helper = SummaryTestHelper::with_default_config(
            base_dir.to_string(),
            "test_summary_recover".to_string(),
        );

        let runtime = test_helper.runtime.clone();
        let options = kv_option::Options::from(&test_helper.config);

        test_helper.runtime.block_on(async move {
            test_helper
                .tskv
                .open_tsfamily("cnosdb", "hello", 100)
                .await
                .expect("add tsfamily successfully");

            let summary = Summary::recover(
                test_helper.meta_manager.clone(),
                Arc::new(options),
                runtime,
                test_helper.memory_pool.clone(),
                test_helper.tskv.context().compact_task_sender.clone(),
                false,
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();

            let version_set = summary.version_set.read().await;
            let all_db_names: Vec<String> = version_set.get_all_db().keys().cloned().collect();
            assert_eq!(all_db_names, vec!["cnosdb.hello".to_string()]);
            let db = version_set.get_all_db().get("cnosdb.hello").unwrap();
            let vnode_ids: Vec<TseriesFamilyId> =
                db.read().await.ts_families().keys().cloned().collect();
            assert_eq!(vnode_ids, vec![100]);
        });
    }

    #[test]
    fn test_tsf_num_recover() {
        let base_dir = "/tmp/test/summary/test_tsf_num_recover";
        let _ = std::fs::remove_dir_all(base_dir);
        std::fs::create_dir_all(base_dir).unwrap();
        let test_helper = SummaryTestHelper::with_default_config(
            base_dir.to_string(),
            "test_tsf_num_recover".to_string(),
        );

        let runtime_clone = test_helper.runtime.clone();
        let options = Arc::new(kv_option::Options::from(&test_helper.config));
        let compact_task_sender = test_helper.tskv.context().compact_task_sender.clone();

        runtime_clone.block_on(async move {
            test_helper
                .tskv
                .open_tsfamily("cnosdb", "test_tsf_num_recover", 100)
                .await
                .expect("add tsfamily successfully");

            let summary = Summary::recover(
                test_helper.meta_manager.clone(),
                options.clone(),
                test_helper.runtime.clone(),
                test_helper.memory_pool.clone(),
                compact_task_sender.clone(),
                false,
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();

            assert_eq!(summary.version_set.read().await.tsf_num().await, 1);

            test_helper
                .tskv
                .remove_tsfamily("cnosdb", "test_tsf_num_recover", 100)
                .await
                .unwrap();

            let summary = Summary::recover(
                test_helper.meta_manager.clone(),
                options.clone(),
                test_helper.runtime.clone(),
                test_helper.memory_pool.clone(),
                compact_task_sender,
                false,
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();

            assert_eq!(summary.version_set.read().await.tsf_num().await, 0);
        });
    }

    #[test]
    fn test_recover_summary_with_roll_0() {
        let base_dir = "/tmp/test/summary/test_recover_summary_with_roll_0";
        let _ = std::fs::remove_dir_all(base_dir);
        std::fs::create_dir_all(base_dir).unwrap();
        let test_helper = SummaryTestHelper::with_default_config(
            base_dir.to_string(),
            "test_recover_summary_with_roll_0".to_string(),
        );

        let tenant = "cnosdb";
        let database = "test_recover_summary_with_roll_0";
        let options = Arc::new(kv_option::Options::from(&test_helper.config));
        let compact_task_sender = test_helper.tskv.context().compact_task_sender.clone();

        // Create database, add some vnodes then delete some vnodes.
        test_helper.runtime.block_on(async {
            for i in 0..20 {
                test_helper
                    .tskv
                    .open_tsfamily(tenant, database, i)
                    .await
                    .expect("add tsfamily successfully");
            }

            for i in 0..10 {
                test_helper
                    .tskv
                    .remove_tsfamily(tenant, database, i)
                    .await
                    .expect("add tsfamily successfully");
            }

            // Recover and compare.
            let summary = Summary::recover(
                test_helper.meta_manager.clone(),
                options,
                test_helper.runtime.clone(),
                test_helper.memory_pool.clone(),
                compact_task_sender,
                false,
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();
            assert_eq!(summary.version_set.read().await.tsf_num().await, 10);
        });
    }

    #[test]
    fn test_recover_summary_with_roll_1() {
        let base_dir = "/tmp/test/summary/test_recover_summary_with_roll_1";
        let _ = std::fs::remove_dir_all(base_dir);
        std::fs::create_dir_all(base_dir).unwrap();
        let test_helper = SummaryTestHelper::with_default_config(
            base_dir.to_string(),
            "test_recover_summary_with_roll_1".to_string(),
        );

        const VNODE_ID: u32 = 10;
        let tenant = "cnosdb";
        let database = "test_recover_summary_with_roll_1";
        let options = Arc::new(kv_option::Options::from(&test_helper.config));
        let compact_task_sender = test_helper.tskv.context().compact_task_sender.clone();
        let summary_task_sender = test_helper.tskv.context().summary_task_sender.clone();

        // Create database, add some vnodes and delete some vnodes..
        test_helper.runtime.block_on(async {
            let vnode = test_helper
                .tskv
                .open_tsfamily(tenant, database, VNODE_ID)
                .await
                .expect("add tsfamily successfully");

            // Go to the next version.
            let mut version = vnode
                .ts_family
                .read()
                .await
                .version()
                .copy_apply_version_edits(vec![], &mut HashMap::new(), None);
            let tsm_reader_cache = Arc::downgrade(version.tsm2_reader_cache());

            let mut edit = VersionEdit::new(VNODE_ID);
            let meta = CompactMeta {
                file_id: 15,
                is_delta: false,
                file_size: 100,
                level: 1,
                min_ts: 1,
                max_ts: 1,
                tsf_id: VNODE_ID,
                high_seq: 1,
                ..Default::default()
            };
            version.levels_info_mut()[1].push_compact_meta(
                &meta,
                Arc::new(BloomFilter::default()),
                tsm_reader_cache,
            );
            vnode.ts_family.write().await.new_version(version, None);
            edit.add_file(meta, 1);

            let (sender, receiver) = tokio::sync::oneshot::channel();
            let task = SummaryTask::new(vnode.ts_family.clone(), edit, None, None, sender);
            summary_task_sender.send(task).await.unwrap();
            let _ = receiver.await;

            // Recover and compare.
            let summary = Summary::recover(
                test_helper.meta_manager.clone(),
                options.clone(),
                test_helper.runtime.clone(),
                test_helper.memory_pool.clone(),
                compact_task_sender,
                false,
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();

            let vs = summary.version_set.read().await;
            let tsf = vs.get_tsfamily_by_tf_id(VNODE_ID).await.unwrap();
            assert_eq!(tsf.read().await.version().last_seq(), 1);
            assert_eq!(tsf.read().await.version().levels_info()[1].tsf_id, VNODE_ID);
            assert!(!tsf.read().await.version().levels_info()[1].files[0].is_delta());
            assert_eq!(
                tsf.read().await.version().levels_info()[1].files[0].file_id(),
                15
            );
            assert_eq!(
                tsf.read().await.version().levels_info()[1].files[0].size(),
                100
            );
            assert_eq!(summary.ctx.file_id(), 16);
        });
    }
}
