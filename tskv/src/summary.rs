use std::fs::{remove_file, rename};
use std::path::Path;
use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use futures::TryFutureExt;
use libc::write;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedSender, oneshot::Sender};

use crate::{
    context::GlobalContext,
    error::{Error, Result},
    file_utils,
    kv_option::{DBOptions, TseriesFamDesc, TseriesFamOpt},
    record_file::{Reader, Writer},
    tseries_family::{LevelInfo, Version},
    version_set::VersionSet,
    LevelId,
};

const MAX_BATCH_SIZE: usize = 64;
#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct CompactMeta {
    pub file_id: u64,
    pub file_size: u64,
    pub tsf_id: u32,
    pub ts_min: i64,
    pub ts_max: i64,
    pub level: u32,
    pub high_seq: u64,
    pub low_seq: u64,
    pub is_delta: bool,
}
impl CompactMeta {
    pub fn new() -> Self {
        Self {
            file_id: 0,
            file_size: 0,
            tsf_id: 0,
            ts_min: i64::MAX,
            ts_max: i64::MIN,
            level: 0,
            high_seq: u64::MIN,
            low_seq: u64::MIN,
            is_delta: false,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct VersionEdit {
    pub level: u32,

    pub has_seq_no: bool,
    pub seq_no: u64,
    pub has_file_id: bool,
    pub file_id: u64,
    pub add_files: Vec<CompactMeta>,
    pub del_files: Vec<CompactMeta>,

    pub del_tsf: bool,
    pub add_tsf: bool,
    pub tsf_id: u32,
    pub tsf_name: String,

    pub max_level_ts: i64,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self {
            level: 0,
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
    pub fn add_file(
        &mut self,
        level: u32,
        tsf_id: u32,
        file_id: u64,
        seq_no: u64,
        max_level_ts: i64,
        meta: CompactMeta,
    ) {
        self.has_file_id = true;
        self.file_id = file_id;
        self.has_seq_no = true;
        self.seq_no = seq_no;
        self.level = level;
        self.tsf_id = tsf_id;
        self.max_level_ts = max_level_ts;
        self.add_files.push(meta);
    }

    pub fn add_tsfamily(&mut self, tsf_id: u32, tsf_name: String) {
        self.add_tsf = true;
        self.tsf_name = tsf_name;
        self.tsf_id = tsf_id;
    }

    pub fn del_tsfamily(&mut self, tsf_if: u32) {
        self.del_tsf = true;
        self.tsf_id = tsf_if;
    }

    pub fn del_file(&mut self, level: LevelId, file_id: u64, is_delta: bool) {
        let mut cm = CompactMeta::new();
        cm.file_id = file_id;
        cm.level = level;
        cm.is_delta = is_delta;
        self.del_files.push(cm);
    }

    pub fn set_log_seq(&mut self, file_id: u64) {
        self.file_id = file_id;
    }
    pub fn set_tsf_id(&mut self, tsf_id: u32) {
        self.tsf_id = tsf_id;
    }
}

use crate::file_manager::try_exists;
use config::GLOBAL_CONFIG;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use trace::{debug, info};
use tracing::log::error;

#[derive(Debug, Eq, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum EditType {
    SummaryEdit, // 0
}

pub struct Summary {
    file_no: u64,
    version_set: Arc<RwLock<VersionSet>>,
    ctx: Arc<GlobalContext>,
    writer: Writer,
}

impl Summary {
    // create a new summary file
    pub async fn new(db_opt: &DBOptions) -> Result<Self> {
        let db = VersionEdit::new();
        let mut w = Writer::new(&file_utils::make_summary_file(&db_opt.db_path, 0)).unwrap();
        let buf = db.encode()?;
        let _ = w
            .write_record(1, EditType::SummaryEdit.into(), &buf)
            .map_err(|e| Error::LogRecordErr { source: (e) })
            .await?;
        w.hard_sync()
            .map_err(|e| Error::LogRecordErr { source: e })
            .await?;
        Self::recover(db_opt).await
    }

    pub async fn recover(db_opt: &DBOptions) -> Result<Self> {
        let writer = Writer::new(&file_utils::make_summary_file(&db_opt.db_path, 0)).unwrap();
        let ctx = Arc::new(GlobalContext::default());
        let rd = Box::new(Reader::new(&file_utils::make_summary_file(&db_opt.db_path, 0)).unwrap());
        let vs = Self::recover_version(rd, &ctx).await?;

        Ok(Self {
            file_no: 0,
            version_set: Arc::new(RwLock::new(vs)),
            ctx,
            writer,
        })
    }

    // recover from summary file
    pub async fn recover_version(mut rd: Box<Reader>, ctx: &GlobalContext) -> Result<VersionSet> {
        let mut tf_cfg = vec![];
        let mut edits: HashMap<u32, Vec<VersionEdit>> = HashMap::default();
        let mut tf_names: HashMap<u32, String> = HashMap::default();
        for i in 0..GLOBAL_CONFIG.tsfamily_num {
            edits.insert(i, vec![]);
            let name = format!("default{}", i);
            tf_names.insert(i, name.clone());
            tf_cfg.push(Arc::new(TseriesFamDesc {
                name,
                opt: Arc::new(TseriesFamOpt::default()),
            }));
        }
        ctx.set_max_tsf_idy(GLOBAL_CONFIG.tsfamily_num - 1);
        loop {
            let res = rd
                .read_record()
                .await
                .map_err(|e| Error::LogRecordErr { source: (e) });
            match res {
                Ok(result) => {
                    let ed = VersionEdit::decode(&result.data)?;
                    if ed.add_tsf {
                        ctx.set_max_tsf_idy(ed.tsf_id);
                        edits.insert(ed.tsf_id, vec![]);
                        tf_names.insert(ed.tsf_id, ed.tsf_name.clone());
                        tf_cfg.push(Arc::new(TseriesFamDesc {
                            name: ed.tsf_name,
                            opt: Default::default(),
                        }));
                    } else if ed.del_tsf {
                        edits.remove(&ed.tsf_id);
                        tf_names.remove(&ed.tsf_id);
                    } else if let Some(data) = edits.get_mut(&ed.tsf_id) {
                        data.push(ed);
                    }
                }
                Err(_) => break,
            }
        }

        let mut versions = HashMap::new();
        for (id, eds) in edits {
            let tsf_name = tf_names.get(&id).unwrap().to_owned();
            // let cf_opts = cf_options.remove(cf_name).unwrap_or_default();

            let mut files: HashMap<u64, CompactMeta> = HashMap::new();
            let mut max_log = 0;
            let mut max_level_ts = i64::MIN;
            for e in eds {
                if e.has_seq_no {
                    ctx.set_last_seq(e.seq_no);
                }
                if e.has_file_id {
                    ctx.set_file_id(e.file_id);
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
            let mut levels = HashMap::new();
            let test: CompactMeta = CompactMeta::default();
            // according files map to recover levels_info;
            levels.entry(0).or_insert_with(|| LevelInfo::init(0));
            for (fd, meta) in files {
                let info = levels
                    .entry(meta.level)
                    .or_insert_with(|| LevelInfo::init(meta.level));
                info.apply(&meta);
            }
            let mut lvls: Vec<LevelInfo> = levels.into_values().collect();
            lvls.sort_by(|a, b| a.level.partial_cmp(&b.level).unwrap());
            let ver = Version::new(id, max_log, tsf_name, lvls, max_level_ts);
            versions.insert(id, Arc::new(RwLock::new(ver)));
        }
        let vs = VersionSet::new(&tf_cfg, versions);
        Ok(vs.await)
    }
    // apply version edit to summary file
    // and write to memory struct
    pub async fn apply_version_edit(&mut self, eds: &[VersionEdit]) -> Result<()> {
        self.write_summary(eds).await?;
        self.roll_summary_file().await?;
        Ok(())
    }

    async fn write_summary(&mut self, eds: &[VersionEdit]) -> Result<()> {
        for edit in eds {
            let buf = edit.encode()?;
            let _ = self
                .writer
                .write_record(1, EditType::SummaryEdit.into(), &buf)
                .map_err(|e| Error::LogRecordErr { source: (e) })
                .await?;
            self.writer
                .hard_sync()
                .map_err(|e| Error::LogRecordErr { source: e })
                .await?;
        }
        Ok(())
    }

    async fn roll_summary_file(&mut self) -> Result<()> {
        if self.writer.file_size() >= GLOBAL_CONFIG.max_summary_size {
            let mut edits = vec![];
            {
                let vs = self.version_set.read();
                for i in vs.ts_families_names().iter() {
                    let mut edit = VersionEdit::new();
                    edit.add_tsfamily(*i.1, i.0.clone());
                    edits.push(edit);
                }
                for i in vs.ts_families().iter() {
                    let mut edit = VersionEdit::new();
                    let version = i.1.version().read();
                    let max_level_ts = version.max_level_ts;
                    for files in version.levels_info.iter() {
                        for file in files.files.iter() {
                            let mut meta = CompactMeta::new();
                            meta.file_id = file.file_id();
                            meta.is_delta = file.is_delta();
                            meta.file_size = file.size();
                            meta.level = files.level;
                            meta.ts_min = file.range().min_ts;
                            meta.ts_max = file.range().max_ts;
                            meta.tsf_id = files.tsf_id;
                            meta.high_seq = self.ctx.last_seq();
                            edit.add_file(
                                files.level,
                                files.tsf_id,
                                file.file_id(),
                                self.ctx.last_seq(),
                                max_level_ts,
                                meta,
                            );
                        }
                    }
                    edits.push(edit);
                }
            }
            let new_path = &file_utils::make_summary_file_tmp(GLOBAL_CONFIG.db_path.clone());
            let old_path = &self.writer.path().clone();
            if try_exists(new_path) {
                match remove_file(new_path) {
                    Ok(_) => (),
                    Err(e) => {
                        error!("failed remove file {:?}, in case {:?}", new_path, e);
                    }
                };
            }
            self.writer = Writer::new(new_path).unwrap();
            self.write_summary(&edits).await?;
            match rename(new_path, old_path) {
                Ok(_) => (),
                Err(e) => {
                    error!(
                        "failed remove old file {:?}, and create new file {:?},in case {:?}",
                        old_path, new_path, e
                    );
                }
            };
            self.writer = Writer::new(old_path).unwrap();
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
        match self.summary.apply_version_edit(&edits).await {
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

    pub fn summary(&self) -> &Box<Summary> {
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
    use std::sync::Arc;

    use config::GLOBAL_CONFIG;
    use snafu::ResultExt;
    use tokio::sync::mpsc;
    use tracing::debug;

    use crate::tseries_family::LevelInfo;
    use crate::{
        error, file_manager,
        kv_option::{DBOptions, TseriesFamOpt},
        summary::{CompactMeta, EditType, Summary, VersionEdit},
    };

    #[tokio::test]
    async fn test_summary_recover() {
        let opt = DBOptions {
            db_path: "/tmp/summary/0".to_string(),
            ..Default::default()
        };
        if !file_manager::try_exists(&opt.db_path) {
            std::fs::create_dir_all(&opt.db_path)
                .context(error::IOSnafu)
                .unwrap();
        }
        let mut summary = Summary::new(&opt).await.unwrap();
        let mut edit = VersionEdit::new();
        edit.add_tsfamily(100, "hello".to_string());
        summary.apply_version_edit(&[edit]).await.unwrap();
        let summary = Summary::recover(&opt).await.unwrap();
        assert_eq!(summary.ctx.max_tsf_id(), 100);
    }

    #[tokio::test]
    async fn test_tsf_num_recover() {
        let opt = DBOptions {
            db_path: "/tmp/summary/1".to_string(),
            ..Default::default()
        };
        if !file_manager::try_exists(&opt.db_path) {
            std::fs::create_dir_all(&opt.db_path)
                .context(error::IOSnafu)
                .unwrap();
        }
        let mut summary = Summary::new(&opt).await.unwrap();
        assert_eq!(
            summary.version_set.read().tsf_num(),
            GLOBAL_CONFIG.tsfamily_num as usize
        );
        let mut edit = VersionEdit::new();
        edit.add_tsfamily(100, "hello".to_string());
        summary.apply_version_edit(&[edit]).await.unwrap();
        let mut summary = Summary::recover(&opt).await.unwrap();
        assert_eq!(
            summary.version_set.read().tsf_num(),
            (GLOBAL_CONFIG.tsfamily_num + 1) as usize
        );
        let mut edit = VersionEdit::new();
        edit.del_tsfamily(100);
        summary.apply_version_edit(&[edit]).await.unwrap();
        let summary = Summary::recover(&opt).await.unwrap();
        assert_eq!(
            summary.version_set.read().tsf_num(),
            GLOBAL_CONFIG.tsfamily_num as usize
        );
    }

    #[test]
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
    #[tokio::test]
    async fn test_recover_summary_with_roll_0() {
        let opt = DBOptions {
            db_path: "/tmp/summary/2".to_string(),
            ..Default::default()
        };
        if !file_manager::try_exists(&opt.db_path) {
            std::fs::create_dir_all(&opt.db_path)
                .context(error::IOSnafu)
                .unwrap();
        }
        let mut summary = Summary::new(&opt).await.unwrap();

        let (summary_task_sender, _summary_task_receiver) = mpsc::unbounded_channel();

        let mut edits = vec![];
        for i in 0..40 {
            summary.version_set.write().add_tsfamily(
                i,
                format!("hello{}", i),
                0,
                0,
                Arc::new(TseriesFamOpt::default()),
                summary_task_sender.clone(),
            );
            let mut edit = VersionEdit::new();
            edit.add_tsfamily(i, "hello".to_string());
            edits.push(edit.clone());
        }
        for _ in 0..100 {
            for i in 1..21 {
                summary.version_set.write().del_tsfamily(
                    i,
                    format!("hello{}", i),
                    summary_task_sender.clone(),
                );
                let mut edit = VersionEdit::new();
                edit.del_tsfamily(i);
                edits.push(edit.clone());
            }
        }
        summary.apply_version_edit(&edits).await.unwrap();
        let summary = Summary::recover(&opt).await.unwrap();

        assert_eq!(summary.version_set.read().tsf_num(), 20);
    }

    #[tokio::test]
    async fn test_recover_summary_with_roll_1() {
        let opt = DBOptions {
            db_path: "/tmp/summary/3".to_string(),
            ..Default::default()
        };
        if !file_manager::try_exists(&opt.db_path) {
            std::fs::create_dir_all(&opt.db_path)
                .context(error::IOSnafu)
                .unwrap();
        }
        let mut summary = Summary::new(&opt).await.unwrap();

        let (summary_task_sender, _summary_task_receiver) = mpsc::unbounded_channel();

        let mut edits = vec![];
        {
            summary.version_set.write().add_tsfamily(
                10,
                "hello".to_string(),
                0,
                0,
                Arc::new(TseriesFamOpt::default()),
                summary_task_sender.clone(),
            );
            let mut edit = VersionEdit::new();
            edit.add_tsfamily(10, "hello".to_string());
            edits.push(edit.clone());

            for _ in 0..100 {
                summary.version_set.write().del_tsfamily(
                    0,
                    "default0".to_string(),
                    summary_task_sender.clone(),
                );
                let mut edit = VersionEdit::new();
                edit.del_tsfamily(0);
                edits.push(edit.clone());
            }

            let vs = summary.version_set.read();
            let tsf = vs.get_tsfamily_by_tf_id(10).unwrap();

            tsf.version().write().levels_info.push(LevelInfo::init(0));
            tsf.version().write().levels_info.push(LevelInfo::init(1));

            summary.ctx.set_last_seq(1);
            let mut edit = VersionEdit::new();
            let mut meta = CompactMeta::new();
            meta.file_id = 15;
            meta.is_delta = false;
            meta.file_size = 100;
            meta.level = 1;
            meta.ts_min = 1;
            meta.ts_max = 1;
            meta.tsf_id = 10;
            meta.high_seq = 1;
            tsf.version().write().levels_info[1].apply(&meta);

            edit.add_file(1, 10, 15, 1, 1, meta);
            edits.push(edit.clone());
        }
        summary.apply_version_edit(&edits).await.unwrap();
        let summary = Summary::recover(&opt).await.unwrap();

        let vs = summary.version_set.read();
        let tsf = vs.get_tsfamily_by_tf_id(10).unwrap();
        assert_eq!(tsf.version().read().last_seq, 1);
        assert_eq!(tsf.version().read().levels_info[1].tsf_id, 10);
        assert_eq!(
            tsf.version().read().levels_info[1].files[0].is_delta(),
            false
        );
        assert_eq!(tsf.version().read().levels_info[1].files[0].file_id(), 15);
        assert_eq!(tsf.version().read().levels_info[1].files[0].size(), 100);
        assert_eq!(summary.ctx.file_id(), 15);
        assert_eq!(summary.ctx.max_tsf_id(), 10);
    }
}
