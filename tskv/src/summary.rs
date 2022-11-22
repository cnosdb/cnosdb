use std::fmt::Display;
use std::fs::{remove_file, rename};
use std::path::Path;
use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use futures::TryFutureExt;
use libc::write;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::watch::Receiver;
use tokio::sync::{mpsc::UnboundedSender, oneshot::Sender};

use config::get_config;
use models::Timestamp;
use trace::{debug, error, info};

use crate::compaction::FlushReq;
use crate::file_system::file_manager::try_exists;
use crate::{
    context::GlobalContext,
    error::{Error, Result},
    file_utils,
    kv_option::{Options, StorageOptions},
    record_file::{Reader, RecordDataType, RecordDataVersion, Writer},
    tseries_family::{ColumnFile, LevelInfo, Version},
    version_set::VersionSet,
    LevelId, TseriesFamilyId,
};

const MAX_BATCH_SIZE: usize = 64;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct CompactMeta {
    pub file_id: u64,
    pub file_size: u64,
    pub tsf_id: TseriesFamilyId,
    pub level: LevelId,
    pub min_ts: Timestamp,
    pub max_ts: Timestamp,
    pub high_seq: u64,
    pub low_seq: u64,
    pub is_delta: bool,
}

impl CompactMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_id: u64,
        file_size: u64,
        tsf_id: TseriesFamilyId,
        level: LevelId,
        min_ts: Timestamp,
        max_ts: Timestamp,
        is_delta: bool,
    ) -> Self {
        Self {
            file_id,
            file_size,
            tsf_id,
            level,
            min_ts,
            max_ts,
            is_delta,
            ..Default::default()
        }
    }
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
        VersionEdit::new()
    }
}

impl VersionEdit {
    pub fn new() -> Self {
        Self {
            seq_no: 0,
            file_id: 0,
            add_files: vec![],
            del_files: vec![],
            del_tsf: false,
            add_tsf: false,
            tsf_id: 0,
            tsf_name: String::from(""),
            has_seq_no: false,
            has_file_id: false,
            max_level_ts: i64::MIN,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Encode { source: (e) })
    }
    pub fn decode(buf: &[u8]) -> Result<Self> {
        bincode::deserialize(buf).map_err(|e| Error::Decode { source: (e) })
    }

    pub fn add_file(&mut self, compact_meta: CompactMeta, max_level_ts: i64) {
        self.has_seq_no = true;
        self.seq_no = compact_meta.high_seq;
        self.has_file_id = true;
        self.file_id = compact_meta.file_id;
        self.max_level_ts = max_level_ts;
        self.tsf_id = compact_meta.tsf_id;
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

    pub fn add_tsfamily(&mut self, tsf_id: u32, tsf_name: String) {
        self.add_tsf = true;
        self.tsf_name = tsf_name;
        self.tsf_id = tsf_id;
    }

    pub fn del_tsfamily(&mut self, tsf_id: u32) {
        self.del_tsf = true;
        self.tsf_id = tsf_id;
    }

    pub fn set_log_seq(&mut self, file_id: u64) {
        self.file_id = file_id;
    }
    pub fn set_tsf_id(&mut self, tsf_id: u32) {
        self.tsf_id = tsf_id;
    }
}

impl Display for VersionEdit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "seq_no: {}, file_id: {}, add_files: {}, del_files: {}, del_tsf: {}, add_tsf: {}, tsf_id: {}, tsf_name: {}, has_seq_no: {}, has_file_id: {}, max_level_ts: {}",
               self.seq_no, self.file_id, self.add_files.len(), self.del_files.len(), self.del_tsf, self.add_tsf, self.tsf_id, self.tsf_name, self.has_seq_no, self.has_file_id, self.max_level_ts)
    }
}

pub struct Summary {
    file_no: u64,
    version_set: Arc<RwLock<VersionSet>>,
    ctx: Arc<GlobalContext>,
    writer: Writer,
    opt: Arc<Options>,
}

impl Summary {
    // create a new summary file
    pub async fn new(
        opt: Arc<Options>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) -> Result<Self> {
        let db = VersionEdit::new();
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
            file_no: 0,
            version_set: Arc::new(RwLock::new(VersionSet::new(
                opt.clone(),
                HashMap::new(),
                flush_task_sender,
            ))),
            ctx: Arc::new(GlobalContext::default()),
            writer: w,
            opt,
        })
    }

    pub async fn recover(
        opt: Arc<Options>,
        flush_task_sender: UnboundedSender<FlushReq>,
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
        let vs = Self::recover_version(rd, &ctx, opt.clone(), flush_task_sender).await?;

        Ok(Self {
            file_no: 0,
            version_set: Arc::new(RwLock::new(vs)),
            ctx,
            writer,
            opt,
        })
    }

    // recover from summary file
    pub async fn recover_version(
        mut reader: Box<Reader>,
        ctx: &GlobalContext,
        opt: Arc<Options>,
        flush_task_sender: UnboundedSender<FlushReq>,
    ) -> Result<VersionSet> {
        let mut edits: HashMap<TseriesFamilyId, Vec<VersionEdit>> = HashMap::default();
        let mut databases: HashMap<TseriesFamilyId, String> = HashMap::default();

        loop {
            let res = reader.read_record().await;
            match res {
                Ok(result) => {
                    let ed = VersionEdit::decode(&result.data)?;
                    if ed.add_tsf {
                        edits.insert(ed.tsf_id, vec![]);
                        databases.insert(ed.tsf_id, ed.tsf_name.clone());
                    } else if ed.del_tsf {
                        edits.remove(&ed.tsf_id);
                        databases.remove(&ed.tsf_id);
                    } else if let Some(data) = edits.get_mut(&ed.tsf_id) {
                        data.push(ed);
                    }
                }
                Err(Error::Eof) => break,
                Err(e) => {
                    println!("Error reading record: {:?}", e);
                    break;
                }
            }
        }

        let mut versions = HashMap::new();
        let mut has_seq_no = false;
        let mut seq_no = 0_u64;
        let mut has_file_id = false;
        let mut file_id = 0_u64;
        for (id, eds) in edits {
            let database = databases.get(&id).unwrap().to_owned();
            // let cf_opts = cf_options.remove(cf_name).unwrap_or_default();

            let mut files: HashMap<u64, CompactMeta> = HashMap::new();
            let mut max_log = 0;
            let mut max_level_ts = i64::MIN;
            for e in eds {
                if e.has_seq_no {
                    has_seq_no = true;
                    seq_no = e.seq_no;
                }
                if e.has_file_id {
                    has_file_id = true;
                    file_id = e.file_id;
                }
                max_log = std::cmp::max(max_log, e.seq_no);
                max_level_ts = std::cmp::max(max_level_ts, e.max_level_ts);
                for m in e.del_files {
                    files.remove(&m.file_id);
                }
                for m in e.add_files {
                    files.insert(m.file_id, m);
                }
            }
            let mut levels = LevelInfo::init_levels(database.clone(), opt.storage.clone());
            // according files map to recover levels_info;
            for (fd, meta) in files {
                levels[meta.level as usize].push_compact_meta(&meta);
            }
            let ver = Version::new(
                id,
                database,
                opt.storage.clone(),
                max_log,
                levels,
                max_level_ts,
            );
            versions.insert(id, Arc::new(ver));
        }

        if has_seq_no {
            ctx.set_last_seq(seq_no + 1);
        }
        if has_file_id {
            ctx.set_file_id(file_id + 1);
        }

        let vs = VersionSet::new(opt.clone(), versions, flush_task_sender);
        Ok(vs)
    }

    /// Applies version edit to summary file, and generates new version for TseriesFamily.
    pub async fn apply_version_edit(&mut self, eds: Vec<VersionEdit>) -> Result<()> {
        self.write_summary(eds).await?;
        self.roll_summary_file().await?;
        Ok(())
    }

    async fn write_summary(&mut self, eds: Vec<VersionEdit>) -> Result<()> {
        let mut tsf_version_edits: HashMap<TseriesFamilyId, Vec<VersionEdit>> = HashMap::new();
        let mut tsf_min_seq: HashMap<TseriesFamilyId, u64> = HashMap::new();
        for edit in eds.into_iter() {
            let buf = edit.encode()?;
            let _ = self
                .writer
                .write_record(
                    RecordDataVersion::V1.into(),
                    RecordDataType::Summary.into(),
                    &[&buf],
                )
                .await?;
            self.writer.sync().await?;

            tsf_version_edits
                .entry(edit.tsf_id)
                .or_insert(Vec::new())
                .push(edit.clone());
            if edit.has_seq_no {
                tsf_min_seq.insert(edit.tsf_id, edit.seq_no);
            }
        }
        let version_set = self.version_set.write();
        for (tsf_id, version_edits) in tsf_version_edits {
            let min_seq = tsf_min_seq.get(&tsf_id);
            if let Some(tsf) = version_set.get_tsfamily_by_tf_id(tsf_id) {
                let new_version = tsf
                    .read()
                    .version()
                    .copy_apply_version_edits(version_edits, min_seq.copied());
                tsf.write().new_version(new_version);
            }
        }

        Ok(())
    }

    async fn roll_summary_file(&mut self) -> Result<()> {
        if self.writer.file_size() >= self.opt.storage.max_summary_size {
            let mut edits = vec![];
            let mut files = vec![];
            {
                let vs = self.version_set.read();
                let dbs = vs.get_all_db();
                for (name, db) in dbs {
                    let (mut tsf, mut tmp_files) = db.read().version_edit(self.ctx.last_seq());
                    edits.append(&mut tsf);
                    files.append(&mut tmp_files);
                }

                edits.append(&mut files);
            }

            let new_path = &file_utils::make_summary_file_tmp(self.opt.storage.summary_dir());
            let old_path = &self.writer.path().clone();
            if try_exists(new_path) {
                match remove_file(new_path) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("failed remove file {:?}, in case {:?}", new_path, e);
                    }
                };
            }
            self.writer = Writer::open(new_path, RecordDataType::Summary)
                .await
                .unwrap();
            self.write_summary(edits).await?;
            match rename(new_path, old_path) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "failed remove old file {:?}, and create new file {:?},in case {:?}",
                        old_path, new_path, e
                    );
                }
            };
            self.writer = Writer::open(old_path, RecordDataType::Summary)
                .await
                .unwrap();
        }
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
                println!("VersionEdit #{}", i);
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
            Err(err) => match err {
                Error::Eof => break,
                _ => panic!("Errors when read summary file: {}", err),
            },
        }
        println!("============================================================");
    }
}

pub struct SummaryProcessor {
    summary: Box<Summary>,
    cbs: Vec<Sender<Result<()>>>,
    edits: Vec<VersionEdit>,
}

impl SummaryProcessor {
    pub fn new(summary: Box<Summary>) -> Self {
        Self {
            summary,
            cbs: vec![],
            edits: vec![],
        }
    }

    pub fn batch(&mut self, mut task: SummaryTask) -> bool {
        let mut need_apply = self.edits.len() > MAX_BATCH_SIZE;
        if task.edits.len() == 1 && (task.edits[0].del_tsf || task.edits[0].add_tsf) {
            need_apply = true;
        }
        self.edits.append(&mut task.edits);
        self.cbs.push(task.cb);
        need_apply
    }

    pub async fn apply(&mut self) {
        let edits = std::mem::take(&mut self.edits);
        match self.summary.apply_version_edit(edits).await {
            Ok(()) => {
                for cb in self.cbs.drain(..) {
                    let _ = cb.send(Ok(()));
                }
            }
            Err(e) => {
                for cb in self.cbs.drain(..) {
                    let _ = cb.send(Err(Error::ErrApplyEdit));
                }
            }
        }
    }

    pub fn summary(&self) -> &Summary {
        &self.summary
    }
}

#[derive(Debug)]
pub struct SummaryTask {
    pub edits: Vec<VersionEdit>,
    pub cb: Sender<Result<()>>,
}

#[derive(Clone)]
pub struct SummaryScheduler {
    sender: UnboundedSender<SummaryTask>,
}

impl SummaryScheduler {
    pub fn new(sender: UnboundedSender<SummaryTask>) -> Self {
        Self { sender }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;

    use snafu::ResultExt;
    use tokio::sync::mpsc;
    use tokio::sync::mpsc::UnboundedSender;
    use trace::debug;

    use config::get_config;
    use models::schema::DatabaseSchema;

    use crate::file_system::file_manager;
    use crate::tseries_family::LevelInfo;
    use crate::{
        error,
        kv_option::{Options, StorageOptions},
        summary::{CompactMeta, Summary, VersionEdit},
    };

    #[tokio::test]
    async fn test_summary() {
        let base_dir = "/tmp/test/summary/1".to_string();
        let mut config = get_config("../config/config.toml");
        config.storage.path = base_dir.clone();
        let opt = Arc::new(Options::from(&config));

        let _ = fs::remove_dir_all(&base_dir);
        println!("Running test: test_summary_recover");
        test_summary_recover(opt.clone()).await;

        let _ = fs::remove_dir_all(&base_dir);
        println!("Running test: test_tsf_num_recover");
        test_tsf_num_recover(opt.clone()).await;

        println!("Running test: test_version_edit");
        test_version_edit();

        let _ = fs::remove_dir_all(&base_dir);
        println!("Running test: test_recover_summary_with_roll_0");
        test_recover_summary_with_roll_0(opt.clone()).await;

        let _ = fs::remove_dir_all(&base_dir);
        println!("Running test: test_recover_summary_with_roll_1");
        test_recover_summary_with_roll_1(opt).await;
    }

    async fn test_summary_recover(opt: Arc<Options>) {
        let (flush_task_sender, _) = mpsc::unbounded_channel();
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir).unwrap();
        }
        let mut summary = Summary::new(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();
        let mut edit = VersionEdit::new();
        edit.add_tsfamily(100, "hello".to_string());
        summary.apply_version_edit(vec![edit]).await.unwrap();
        let summary = Summary::recover(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();
    }

    async fn test_tsf_num_recover(opt: Arc<Options>) {
        let (flush_task_sender, _) = mpsc::unbounded_channel();
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir).unwrap();
        }
        let mut summary = Summary::new(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();

        let mut edit = VersionEdit::new();
        edit.add_tsfamily(100, "hello".to_string());
        summary.apply_version_edit(vec![edit]).await.unwrap();
        let mut summary = Summary::recover(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();
        assert_eq!(summary.version_set.read().tsf_num(), 1);

        let mut edit = VersionEdit::new();
        edit.del_tsfamily(100);
        summary.apply_version_edit(vec![edit]).await.unwrap();
        let summary = Summary::recover(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();
        assert_eq!(summary.version_set.read().tsf_num(), 0);
    }

    fn test_version_edit() {
        let compact = CompactMeta {
            file_id: 100,
            ..Default::default()
        };
        let add_list = vec![compact];
        let compact2 = CompactMeta {
            file_id: 101,
            ..Default::default()
        };
        let del_list = vec![compact2];
        let ve = VersionEdit::new();
        let buf = ve.encode().unwrap();
        let ve2 = VersionEdit::decode(&buf).unwrap();
        assert_eq!(ve2, ve);
    }

    // tips : we can use a small max_summary_size
    async fn test_recover_summary_with_roll_0(opt: Arc<Options>) {
        let (flush_task_sender, _) = mpsc::unbounded_channel();
        let database = "test".to_string();
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir).unwrap();
        }
        let mut summary = Summary::new(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();

        let (summary_task_sender, _) = mpsc::unbounded_channel();
        let (flush_task_sender, _) = mpsc::unbounded_channel();
        let mut edits = vec![];
        let db = summary
            .version_set
            .write()
            .create_db(DatabaseSchema::new(&database));
        for i in 0..40 {
            db.write().add_tsfamily(
                i,
                0,
                0,
                summary_task_sender.clone(),
                flush_task_sender.clone(),
            );
            let mut edit = VersionEdit::new();
            edit.add_tsfamily(i, database.clone());
            edits.push(edit.clone());
        }

        for _ in 0..100 {
            for i in 0..20 {
                db.write().del_tsfamily(i, summary_task_sender.clone());
                let mut edit = VersionEdit::new();
                edit.del_tsfamily(i);
                edits.push(edit.clone());
            }
        }
        summary.apply_version_edit(edits).await.unwrap();

        let summary = Summary::recover(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();

        assert_eq!(summary.version_set.read().tsf_num(), 20);
    }

    async fn test_recover_summary_with_roll_1(opt: Arc<Options>) {
        let (flush_task_sender, _) = mpsc::unbounded_channel();
        let database = "test".to_string();
        let summary_dir = opt.storage.summary_dir();
        if !file_manager::try_exists(&summary_dir) {
            std::fs::create_dir_all(&summary_dir).unwrap();
        }
        let mut summary = Summary::new(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();

        let (summary_task_sender, _) = mpsc::unbounded_channel();
        let (flush_task_sender, _) = mpsc::unbounded_channel();
        let db = summary
            .version_set
            .write()
            .create_db(DatabaseSchema::new(&database));
        db.write().add_tsfamily(
            10,
            0,
            0,
            summary_task_sender.clone(),
            flush_task_sender.clone(),
        );

        let mut edits = vec![];
        let mut edit = VersionEdit::new();
        edit.add_tsfamily(10, "hello".to_string());
        edits.push(edit);

        for _ in 0..100 {
            db.write().del_tsfamily(0, summary_task_sender.clone());
            let mut edit = VersionEdit::new();
            edit.del_tsfamily(0);
            edits.push(edit);
        }

        {
            let vs = summary.version_set.write();
            let tsf = vs.get_tsfamily_by_tf_id(10).unwrap();
            let mut version = tsf
                .read()
                .version()
                .copy_apply_version_edits(edits.clone(), None);

            summary.ctx.set_last_seq(1);
            let mut edit = VersionEdit::new();
            let meta = CompactMeta {
                file_id: 15,
                is_delta: false,
                file_size: 100,
                level: 1,
                min_ts: 1,
                max_ts: 1,
                tsf_id: 10,
                high_seq: 1,
                ..Default::default()
            };
            version.levels_info[1].push_compact_meta(&meta);
            tsf.write().new_version(version);
            edit.add_file(meta, 1);
            edits.push(edit);
        }

        summary.apply_version_edit(edits).await.unwrap();

        let summary = Summary::recover(opt.clone(), flush_task_sender.clone())
            .await
            .unwrap();

        let vs = summary.version_set.read();
        let tsf = vs.get_tsfamily_by_tf_id(10).unwrap();
        assert_eq!(tsf.read().version().last_seq, 1);
        assert_eq!(tsf.read().version().levels_info[1].tsf_id, 10);
        assert!(!tsf.read().version().levels_info[1].files[0].is_delta());
        assert_eq!(tsf.read().version().levels_info[1].files[0].file_id(), 15);
        assert_eq!(tsf.read().version().levels_info[1].files[0].size(), 100);
        assert_eq!(summary.ctx.file_id(), 16);
    }
}
