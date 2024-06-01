use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use cache::ShardedAsyncCache;
use memory_pool::MemoryPoolRef;
use meta::model::MetaRef;
use metrics::metric_register::MetricsRegister;
use models::utils::now_timestamp_millis;
use models::Timestamp;
use parking_lot::RwLock as SyncRwLock;
use serde::{Deserialize, Serialize};
use snafu::{IntoError, ResultExt};
use tokio::runtime::Runtime;
use tokio::sync::oneshot::Sender as OneShotSender;
use tokio::sync::RwLock;
use utils::BloomFilter;

use crate::context::GlobalContext;
use crate::error::{
    IOSnafu, InvalidUtf8Snafu, RecordFileDecodeSnafu, RecordFileEncodeSnafu, TskvError, TskvResult,
};
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::kv_option::{Options, StorageOptions, DELTA_PATH, TSM_PATH};
use crate::memcache::MemCache;
use crate::record_file::{Reader, RecordDataType, Writer};
use crate::tseries_family::{ColumnFile, LevelInfo, TseriesFamily, Version};
use crate::version_set::VersionSet;
use crate::{file_utils, ColumnFileId, LevelId, TseriesFamilyId};

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct CompactMeta {
    pub file_id: ColumnFileId,
    pub file_size: u64,
    pub tsf_id: TseriesFamilyId,
    pub level: LevelId,
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
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

    pub fn relative_path(&self) -> PathBuf {
        if self.is_delta {
            let base_dir = PathBuf::from(DELTA_PATH);
            file_utils::make_delta_file(base_dir, self.file_id)
        } else {
            let base_dir = PathBuf::from(TSM_PATH);
            file_utils::make_tsm_file(base_dir, self.file_id)
        }
    }

    pub async fn rename_file(
        &self,
        old_dir: &Path,
        new_dir: &Path,
        new_id: ColumnFileId,
    ) -> TskvResult<PathBuf> {
        let old_id = self.file_id;
        let (old_name, new_name) = if self.is_delta {
            (
                file_utils::make_delta_file(old_dir.join(DELTA_PATH), old_id),
                file_utils::make_delta_file(new_dir.join(DELTA_PATH), new_id),
            )
        } else {
            (
                file_utils::make_tsm_file(old_dir.join(TSM_PATH), old_id),
                file_utils::make_tsm_file(new_dir.join(TSM_PATH), new_id),
            )
        };

        trace::info!("rename file from {:?} to {:?}", &old_name, &new_name);
        file_utils::rename(&old_name, &new_name).await?;

        Ok(new_name)
    }

    pub fn decode_vec(data: &mut Bytes, len: u64) -> TskvResult<Vec<Self>> {
        let mut files = Vec::with_capacity(len as usize);
        for _ in 0..len {
            let file_id = data.get_u64_le();
            let file_size = data.get_u64_le();
            let tsf_id = data.get_u32_le();
            let level = data.get_u32_le();
            let min_ts = data.get_i64_le();
            let max_ts = data.get_i64_le();
            let is_delta = data.get_u8() == 1;
            files.push(CompactMeta {
                file_id,
                file_size,
                tsf_id,
                level,
                min_ts,
                max_ts,
                is_delta,
            });
        }
        Ok(files)
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionEditVersion {
    V1,
    V2,
    Unknown(u8),
}

impl From<u8> for VersionEditVersion {
    fn from(value: u8) -> Self {
        match value {
            1 => VersionEditVersion::V1,
            2 => VersionEditVersion::V2,
            other => VersionEditVersion::Unknown(other),
        }
    }
}

impl From<VersionEditVersion> for u8 {
    fn from(v: VersionEditVersion) -> u8 {
        v.into_u8()
    }
}

impl VersionEditVersion {
    pub fn into_u8(self) -> u8 {
        match self {
            VersionEditVersion::V1 => 1,
            VersionEditVersion::V2 => 2,
            VersionEditVersion::Unknown(v) => v,
        }
    }

    /// Check if the version is greater or equal to the other.
    pub fn meet_minimum_version(&self, other: Self) -> bool {
        self.into_u8() >= other.into_u8()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct VersionEdit {
    pub seq_no: u64,
    pub file_id: u64,
    pub max_level_ts: Timestamp,
    pub add_files: Vec<CompactMeta>,
    pub del_files: Vec<CompactMeta>,

    pub act_tsf: VnodeAction,
    pub tsf_id: TseriesFamilyId,
    pub tsf_name: String,

    // From: VersionEditVersion::V2.
    pub timestamp_ms: Timestamp,
}

impl Default for VersionEdit {
    fn default() -> Self {
        Self {
            seq_no: 0,
            file_id: 0,
            max_level_ts: i64::MIN,
            add_files: vec![],
            del_files: vec![],
            act_tsf: VnodeAction::Update,
            tsf_id: 0,
            tsf_name: String::from(""),
            timestamp_ms: now_timestamp_millis(),
        }
    }
}

impl VersionEdit {
    pub fn new_update_vnode(vnode_id: u32, owner: String, seq_no: u64) -> Self {
        Self {
            seq_no,
            act_tsf: VnodeAction::Update,
            tsf_name: owner,
            tsf_id: vnode_id,
            ..Default::default()
        }
    }

    pub fn new_add_vnode(vnode_id: u32, owner: String, seq_no: u64) -> Self {
        Self {
            seq_no,
            act_tsf: VnodeAction::Add,
            tsf_id: vnode_id,
            tsf_name: owner,

            ..Default::default()
        }
    }

    pub fn new_del_vnode(vnode_id: u32, owner: String, seq_no: u64) -> Self {
        Self {
            seq_no,
            act_tsf: VnodeAction::Delete,
            tsf_id: vnode_id,
            tsf_name: owner,

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

    pub fn encode(&self) -> TskvResult<Vec<u8>> {
        bincode::serialize(self).context(RecordFileEncodeSnafu)
    }

    pub fn decode(data: Vec<u8>, version: VersionEditVersion) -> TskvResult<Self> {
        match version {
            VersionEditVersion::V1 => Self::decode_v1(data),
            VersionEditVersion::V2 => bincode::deserialize(&data).context(RecordFileDecodeSnafu),
            VersionEditVersion::Unknown(v) => Err(RecordFileDecodeSnafu.into_error(Box::new(
                bincode::ErrorKind::Custom(format!("Unknown version edit version: {v}")),
            ))),
        }
    }

    fn decode_v1(data: Vec<u8>) -> TskvResult<Self> {
        let mut r = Bytes::from(data);
        let seq_no = r.get_u64_le();
        let file_id = r.get_u64_le();
        let max_level_ts = r.get_i64_le();
        let add_files_len = r.get_u64_le();
        let add_files = CompactMeta::decode_vec(&mut r, add_files_len)?;
        let del_files_len = r.get_u64_le();
        let del_files = CompactMeta::decode_vec(&mut r, del_files_len)?;
        let act_tsf_code = r.get_u32_le();
        let act_tsf = match act_tsf_code {
            0 => VnodeAction::Add,
            1 => VnodeAction::Delete,
            2 => VnodeAction::Update,
            c => {
                return Err(RecordFileDecodeSnafu.into_error(Box::new(
                    bincode::ErrorKind::Custom(format!("Unexpected enum code of ActTsv: {c}")),
                )));
            }
        };
        let tsf_id = r.get_u32_le();
        let tsf_name_len = r.get_u64_le();
        let tsf_name = if let Some(tsf_name_bytes) = r.get(..tsf_name_len as usize) {
            let s = std::str::from_utf8(tsf_name_bytes).context(InvalidUtf8Snafu {
                message: "Unexpected tsf_name of VersionEdit".to_string(),
            })?;
            s.to_string()
        } else {
            String::new()
        };
        let decoded_data = Self {
            seq_no,
            file_id,
            max_level_ts,
            add_files,
            del_files,
            act_tsf,
            tsf_id,
            tsf_name,
            timestamp_ms: 0,
        };

        Ok(decoded_data)
    }

    pub fn add_file(&mut self, compact_meta: CompactMeta, max_level_ts: i64) {
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
        write!(f, "seq_no: {}, file_id: {}, add_files: {}, del_files: {}, act_tsf: {}, tsf_id: {}, tsf_name: {}, max_level_ts: {}",
               self.seq_no, self.file_id, self.add_files.len(), self.del_files.len(), self.act_tsf, self.tsf_id, self.tsf_name, self.max_level_ts)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub enum VnodeAction {
    Add,
    Delete,
    Update,
}

impl Display for VnodeAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VnodeAction::Add => write!(f, "Add"),
            VnodeAction::Delete => write!(f, "Delete"),
            VnodeAction::Update => write!(f, "Update"),
        }
    }
}

const SUMMARY_BUFFER_SIZE: usize = 1024 * 1024;

pub struct Summary {
    _meta: MetaRef,
    _file_no: u64,
    version_set: Arc<RwLock<VersionSet>>,
    ctx: Arc<GlobalContext>,
    writer: Writer,
    opt: Arc<Options>,
    _runtime: Arc<Runtime>,
    _metrics_register: Arc<MetricsRegister>,
}

impl Summary {
    // create a new summary file
    pub async fn new(
        opt: Arc<Options>,
        runtime: Arc<Runtime>,
        meta: MetaRef,
        memory_pool: MemoryPoolRef,
        metrics_register: Arc<MetricsRegister>,
    ) -> TskvResult<Self> {
        let db = VersionEdit::default();
        let path = file_utils::make_summary_file(opt.storage.summary_dir(), 0);
        let mut w = Writer::open(path, RecordDataType::Summary, SUMMARY_BUFFER_SIZE)
            .await
            .unwrap();
        let buf = db.encode()?;
        let _ = w
            .write_record(
                VersionEditVersion::V2.into(),
                RecordDataType::Summary.into(),
                &[&buf],
            )
            .await?;
        w.sync().await?;

        Ok(Self {
            _meta: meta.clone(),
            _file_no: 0,
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
            _runtime: runtime,
            _metrics_register: metrics_register,
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
        metrics_register: Arc<MetricsRegister>,
    ) -> TskvResult<Self> {
        let summary_path = opt.storage.summary_dir();
        let path = file_utils::make_summary_file(&summary_path, 0);
        let writer = Writer::open(path, RecordDataType::Summary, SUMMARY_BUFFER_SIZE)
            .await
            .unwrap();
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
            metrics_register.clone(),
        )
        .await?;

        Ok(Self {
            _meta: meta.clone(),
            _file_no: 0,
            version_set: Arc::new(RwLock::new(vs)),
            ctx,
            writer,
            opt,
            _runtime: runtime,
            _metrics_register: metrics_register,
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
        metrics_register: Arc<MetricsRegister>,
    ) -> TskvResult<VersionSet> {
        let mut tsf_edits_map: HashMap<TseriesFamilyId, Vec<VersionEdit>> = HashMap::new();
        let mut database_map: HashMap<String, Arc<String>> = HashMap::new();
        let mut tsf_database_map: HashMap<TseriesFamilyId, Arc<String>> = HashMap::new();

        loop {
            let record = match reader.read_record().await {
                Ok(r) => r,
                Err(TskvError::Eof) => break,
                Err(TskvError::RecordFileHashCheckFailed { .. }) => continue,
                Err(e) => {
                    return Err(e);
                }
            };
            let version = VersionEditVersion::from(record.data_version);
            let edit = VersionEdit::decode(record.data, version)?;
            match edit.act_tsf {
                VnodeAction::Add => {
                    let db_ref = database_map
                        .entry(edit.tsf_name.clone())
                        .or_insert_with(|| Arc::new(edit.tsf_name.clone()));
                    tsf_database_map.insert(edit.tsf_id, db_ref.clone());
                    tsf_edits_map.insert(edit.tsf_id, vec![edit]);
                }
                VnodeAction::Delete => {
                    tsf_edits_map.remove(&edit.tsf_id);
                    tsf_database_map.remove(&edit.tsf_id);
                }
                VnodeAction::Update => {
                    if let Some(data) = tsf_edits_map.get_mut(&edit.tsf_id) {
                        data.push(edit);
                    }
                }
            }
        }

        let mut versions = HashMap::new();
        let mut max_file_id = 0_u64;
        for (tsf_id, edits) in tsf_edits_map {
            let database = tsf_database_map.remove(&tsf_id).unwrap();

            let mut files: HashMap<u64, CompactMeta> = HashMap::new();
            let mut max_seq_no = 0;
            let mut max_level_ts = i64::MIN;
            for e in edits {
                max_seq_no = std::cmp::max(max_seq_no, e.seq_no);
                max_level_ts = std::cmp::max(max_level_ts, e.max_level_ts);
                max_file_id = std::cmp::max(max_file_id, e.file_id);
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
                levels[meta.level as usize].push_compact_meta(
                    &meta,
                    RwLock::new(None),
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

        ctx.set_file_id(max_file_id + 1);
        let vs =
            VersionSet::new(meta, opt, runtime, memory_pool, versions, metrics_register).await?;
        Ok(vs)
    }

    /// Write VersionEdits into summary file, generate and then apply new Versions for TseriesFamilies.
    pub async fn apply_version_edit(&mut self, request: &SummaryRequest) -> TskvResult<()> {
        // Write VersionEdits into summary file and join VersionEdits by Database/TseriesFamilyId.
        let tsf_id = request.ts_family.read().await.tf_id();

        trace::info!("Write version edit to summary for ts_family {}.", tsf_id);
        let buf = request.version_edit.encode()?;
        let _ = self
            .writer
            .write_record(
                VersionEditVersion::V2.into(),
                RecordDataType::Summary.into(),
                &[&buf],
            )
            .await?;
        self.writer.sync().await?;

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
            .copy_apply_version_edits(request.version_edit.clone(), &mut file_metas);

        let new_version = Arc::new(new_version);
        request
            .ts_family
            .write()
            .await
            .new_version(new_version, request.mem_caches.as_ref());
        trace::info!("Applied new version for ts_family {}.", tsf_id);

        Ok(())
    }

    pub async fn roll_summary_file(&mut self) -> TskvResult<()> {
        if self.writer.file_size() < self.opt.storage.max_summary_size {
            return Ok(());
        }

        let new_path = file_utils::make_summary_file_tmp(self.opt.storage.summary_dir());
        LocalFileSystem::remove_if_exists(&new_path)
            .map_err(|e| TskvError::FileSystemError { source: e })?;

        let requests = self
            .version_set
            .read()
            .await
            .ts_families_version_edit()
            .await?;
        self.writer = Writer::open(&new_path, RecordDataType::Summary, SUMMARY_BUFFER_SIZE).await?;

        for request in requests.iter() {
            self.apply_version_edit(request).await?;
        }

        let old_path = &self.writer.path().clone();
        trace::info!("Remove summary file {:?} -> {:?}", new_path, old_path,);
        std::fs::rename(new_path, old_path).context(IOSnafu)?;

        self.writer = Writer::open(old_path, RecordDataType::Summary, SUMMARY_BUFFER_SIZE)
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
                let ve = VersionEdit::decode(record.data, record.data_version.into()).unwrap();
                println!("VersionEdit #{}, vnode_id: {}", i, ve.tsf_id);
                println!("------------------------------------------------------------");
                i += 1;
                if ve.act_tsf == VnodeAction::Add {
                    println!("  Add ts_family: {}", ve.tsf_id);
                    println!("------------------------------------------------------------");
                }
                if ve.act_tsf == VnodeAction::Delete {
                    println!("  Delete ts_family: {}", ve.tsf_id);
                    println!("------------------------------------------------------------");
                }

                println!("  Persist sequence: {}", ve.seq_no);
                println!("------------------------------------------------------------");

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
                        buffer.push_str(format!("{} (level: {}), ", f.file_id, f.level).as_str())
                    });
                    if !buffer.is_empty() {
                        buffer.truncate(buffer.len() - 2);
                    }
                    println!("  Delete file:[ {} ]", buffer);
                }
            }
            Err(TskvError::Eof) => break,
            Err(TskvError::RecordFileHashCheckFailed { .. }) => continue,
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
    pub call_back: OneShotSender<TskvResult<()>>,
}

impl SummaryTask {
    pub fn new(
        ts_family: Arc<RwLock<TseriesFamily>>,
        version_edit: VersionEdit,
        file_metas: Option<HashMap<ColumnFileId, Arc<BloomFilter>>>,
        mem_caches: Option<Vec<Arc<SyncRwLock<MemCache>>>>,
        call_back: OneShotSender<TskvResult<()>>,
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
#[cfg(test)]
mod test {
    use std::sync::Arc;

    use config::Config;
    use memory_pool::GreedyMemoryPool;
    use meta::model::meta_admin::AdminMeta;
    use metrics::metric_register::MetricsRegister;
    use models::schema::{make_owner, TenantOptions};
    use models::Timestamp;
    use serde::{Deserialize, Serialize};
    use sysinfo::{ProcessRefreshKind, RefreshKind, System};
    use tokio::runtime::Runtime;
    use tokio::sync::RwLock;
    use utils::BloomFilter;

    use crate::kv_option::{self, Options};
    use crate::summary::{
        CompactMeta, Summary, SummaryTask, VersionEdit, VersionEditVersion, VnodeAction,
    };
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
        let ve2 = VersionEdit::decode(ve_buf, VersionEditVersion::V2).unwrap();
        assert_eq!(ve2, ve);
    }

    #[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
    pub struct VersionEditV1 {
        pub seq_no: u64,
        pub file_id: u64,
        pub max_level_ts: Timestamp,
        pub add_files: Vec<CompactMeta>,
        pub del_files: Vec<CompactMeta>,

        pub act_tsf: VnodeAction,
        pub tsf_id: TseriesFamilyId,
        pub tsf_name: String,
    }

    #[test]
    #[rustfmt::skip]
    fn test_version_edit_upgrade() {
        let v1 = VersionEditV1 {
            seq_no: 0, file_id: 1, max_level_ts: 2, act_tsf: VnodeAction::Update, tsf_id: 9, tsf_name: "t1.d2".to_string(),
            add_files: vec![], del_files: vec![CompactMeta { file_id: 3, file_size: 4, tsf_id: 5, level: 6, min_ts: 7, max_ts: 8, is_delta: false }],
        };
        let v2_from_v1 = VersionEdit {
            seq_no: 0, file_id: 1, max_level_ts: 2, act_tsf: VnodeAction::Update, tsf_id: 9, tsf_name: "t1.d2".to_string(), timestamp_ms: 0,
            add_files: vec![], del_files: vec![CompactMeta { file_id: 3, file_size: 4, tsf_id: 5, level: 6, min_ts: 7, max_ts: 8, is_delta: false }],
        };
        let v2 = VersionEdit {
            seq_no: 10, file_id: 11, max_level_ts: 12, act_tsf: VnodeAction::Update, tsf_id: 19, tsf_name: "tn3.db4".to_string(), timestamp_ms: 20,
            add_files: vec![], del_files: vec![CompactMeta { file_id: 13, file_size: 14, tsf_id: 15, level: 16, min_ts: 17, max_ts: 18, is_delta: false }],
        };
        let v1_bytes = bincode::serialize(&v1).unwrap();
        let v2_bytes = bincode::serialize(&v2).unwrap();
        let v2_dec_from_v1 = VersionEdit::decode(v1_bytes, VersionEditVersion::V1).unwrap();
        assert_eq!(v2_from_v1, v2_dec_from_v1);
        let v2_dec_from_v2 = VersionEdit::decode(v2_bytes, VersionEditVersion::V2).unwrap();
        assert_eq!(v2, v2_dec_from_v2);
    }

    struct SummaryTestHelper {
        tskv: TsKv,
        config: Config,
        runtime: Arc<Runtime>,
        meta_manager: Arc<AdminMeta>,
        _test_case_name: String,
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
                    _test_case_name: test_case_name,
                }
            })
        }

        fn with_default_config(base_dir: String, test_case_name: String) -> Self {
            let mut config = config::get_config_for_test();
            config.storage.path = base_dir;
            Self::new(config, test_case_name)
        }
    }

    pub fn kill_process(process_name: &str) {
        println!("- Killing processes {process_name}...");
        let system = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new()),
        );
        for (pid, process) in system.processes() {
            if process.name() == process_name {
                match process.kill_with(sysinfo::Signal::Kill) {
                    Some(true) => println!("- Killed process {pid} ('{}')", process.name()),
                    Some(false) => {
                        println!("- Failed killing process {pid} ('{}')", process.name())
                    }
                    None => println!("- Kill with signal 'Kill' isn't supported on this platform"),
                }
            }
        }
    }

    #[tokio::test]
    async fn test_summary_recover_all() {
        let temp_dir = tempfile::Builder::new().prefix("meta").tempdir().unwrap();
        let path = temp_dir.path().to_string_lossy().to_string();
        let cluster_name = "cluster_001".to_string();
        let addr = "127.0.0.1:8901".to_string();
        let size = 1024 * 1024 * 1024;
        meta::service::single::start_singe_meta_server(path, cluster_name, addr, size).await;
        let join_handle = tokio::spawn(async {
            let _ = tokio::task::spawn_blocking(|| {
                test_summary_recover();
                test_tsf_num_recover();
                test_recover_summary_with_roll_0();
                test_recover_summary_with_roll_1();
            })
            .await;
        });
        join_handle.await.unwrap();
        kill_process("cnosdb-meta");
        temp_dir.close().unwrap();
    }

    fn test_summary_recover() {
        println!("async run test_summary_recover.");
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

    fn test_tsf_num_recover() {
        println!("async run test_tsf_num_recover.");
        let base_dir = "/tmp/test/summary/test_tsf_num_recover";
        let _ = std::fs::remove_dir_all(base_dir);
        std::fs::create_dir_all(base_dir).unwrap();
        let test_helper = SummaryTestHelper::with_default_config(
            base_dir.to_string(),
            "test_tsf_num_recover".to_string(),
        );

        let runtime_clone = test_helper.runtime.clone();
        let options = Arc::new(kv_option::Options::from(&test_helper.config));

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
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();

            assert_eq!(summary.version_set.read().await.tsf_num().await, 0);
        });
    }

    fn test_recover_summary_with_roll_0() {
        println!("async run test_recover_summary_with_roll_0.");
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
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();
            assert_eq!(summary.version_set.read().await.tsf_num().await, 10);
        });
    }

    fn test_recover_summary_with_roll_1() {
        println!("async run test_recover_summary_with_roll_1.");
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
        let summary_task_sender = test_helper.tskv.context().summary_task_sender.clone();

        // Create database, add some vnodes and delete some vnodes..
        test_helper.runtime.block_on(async {
            let vnode = test_helper
                .tskv
                .open_tsfamily(tenant, database, VNODE_ID)
                .await
                .expect("add tsfamily successfully");

            // Go to the next version.
            let version = vnode.ts_family().read().await.version();
            let tsm_reader_cache = Arc::downgrade(version.tsm_reader_cache());

            let owner = make_owner(tenant, database);
            let mut edit = VersionEdit::new_update_vnode(VNODE_ID, owner, 100);
            let meta = CompactMeta {
                file_id: 15,
                is_delta: false,
                file_size: 100,
                level: 1,
                min_ts: 1,
                max_ts: 1,
                tsf_id: VNODE_ID,
            };

            let mut version = version.inner();
            version.levels_info_mut()[1].push_compact_meta(
                &meta,
                RwLock::new(Some(Arc::new(BloomFilter::default()))),
                tsm_reader_cache,
            );

            let version = Arc::new(version);
            vnode.ts_family().write().await.new_version(version, None);
            edit.add_file(meta, 1);

            let (sender, receiver) = tokio::sync::oneshot::channel();
            let task = SummaryTask::new(vnode.ts_family(), edit, None, None, sender);
            summary_task_sender.send(task).await.unwrap();
            let _ = receiver.await;

            // Recover and compare.
            let summary = Summary::recover(
                test_helper.meta_manager.clone(),
                options.clone(),
                test_helper.runtime.clone(),
                test_helper.memory_pool.clone(),
                Arc::new(MetricsRegister::default()),
            )
            .await
            .unwrap();

            let vs = summary.version_set.read().await;
            let tsf = vs.get_tsfamily_by_tf_id(VNODE_ID).await.unwrap();
            assert_eq!(tsf.read().await.version().last_seq(), 100);
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
