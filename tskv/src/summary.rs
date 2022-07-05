use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use futures::TryFutureExt;
use libc::write;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedSender, oneshot::Sender, RwLock};

use crate::{
    context::GlobalContext,
    error::{Error, Result},
    file_utils,
    kv_option::{DBOptions, TseriesFamDesc, TseriesFamOpt},
    kvcore::KvContext,
    record_file::{Reader, Writer},
    tseries_family::{LevelInfo, Version},
    version_set::VersionSet,
};

const MAX_BATCH_SIZE: usize = 64;
#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct CompactMeta {
    pub file_id: u64, // file id
    pub file_size: u64,
    pub ts_min: i64,
    pub ts_max: i64,
    pub level: u32,
    pub high_seq: u64,
    pub low_seq: u64,
    pub is_delta: bool,
}
impl CompactMeta {
    pub fn new(file_id: u64, level: u32) -> Self {
        Self { file_id,
               file_size: 0,
               ts_min: i64::MAX,
               ts_max: i64::MIN,
               level,
               high_seq: u64::MIN,
               low_seq: u64::MIN,
               is_delta: false }
    }
    pub fn file_id(&self) -> u64 {
        self.file_id
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
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
        Self { level: 0,
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
               max_level_ts: i64::MIN }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Encode { source: (e) })
    }
    pub fn decode(buf: &[u8]) -> Result<Self> {
        bincode::deserialize(buf).map_err(|e| Error::Decode { source: (e) })
    }
    pub fn add_file(&mut self,
                    level: u32,
                    tsf_id: u32,
                    log_seq: u64,
                    seq_no: u64,
                    max_level_ts: i64,
                    meta: CompactMeta) {
        self.has_file_id = true;
        self.file_id = log_seq;
        self.has_seq_no = true;
        self.seq_no = seq_no;
        self.level = level;
        self.max_level_ts = max_level_ts;
        self.add_files.push(meta);
    }
    // todo:
    pub fn del_file(&mut self) {}

    pub fn set_log_seq(&mut self, file_id: u64) {
        self.file_id = file_id;
    }
    pub fn set_tsf_id(&mut self, tsf_id: u32) {
        self.tsf_id = tsf_id;
    }
}

use logger::{debug, info};
use num_enum::{IntoPrimitive, TryFromPrimitive};

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
    pub async fn new(tf_desc: &[TseriesFamDesc], db_opt: &DBOptions) -> Result<Self> {
        let db = VersionEdit::new();
        let mut w = Writer::new(&file_utils::make_summary_file(&db_opt.db_path, 0));
        let buf = db.encode()?;
        let _ = w.write_record(1, EditType::SummaryEdit.into(), &buf)
                 .map_err(|e| Error::LogRecordErr { source: (e) })
                 .await?;
        w.hard_sync().map_err(|e| Error::LogRecordErr { source: e }).await?;
        Self::recover(tf_desc, db_opt).await
    }

    pub async fn recover(tf_desc: &[TseriesFamDesc], db_opt: &DBOptions) -> Result<Self> {
        let writer = Writer::new(&file_utils::make_summary_file(&db_opt.db_path, 0));
        let ctx = Arc::new(GlobalContext::default());
        let rd = Box::new(Reader::new(&file_utils::make_summary_file(&db_opt.db_path, 0)));
        let vs = Self::recover_version(tf_desc, rd, &ctx).await?;

        Ok(Self { file_no: 0, version_set: Arc::new(RwLock::new(vs)), ctx, writer })
    }

    // recover from summary file
    pub async fn recover_version(tf_cfg: &[TseriesFamDesc],
                                 mut rd: Box<Reader>,
                                 ctx: &GlobalContext)
                                 -> Result<VersionSet> {
        let mut edits: HashMap<u32, Vec<VersionEdit>> = HashMap::default();
        edits.insert(0, vec![]);
        let mut tf_names: HashMap<u32, String> = HashMap::default();
        tf_names.insert(0, "default".to_string());
        loop {
            let res = rd.read_record().await.map_err(|e| Error::LogRecordErr { source: (e) });
            match res {
                Ok(result) => {
                    let ed = VersionEdit::decode(&result.data)?;
                    if ed.add_tsf {
                        edits.insert(ed.tsf_id, vec![]);
                        tf_names.insert(ed.tsf_id, ed.tsf_name);
                    } else if ed.del_tsf {
                        edits.remove(&ed.tsf_id);
                        tf_names.remove(&ed.tsf_id);
                    } else if let Some(data) = edits.get_mut(&ed.tsf_id) {
                        data.push(ed);
                    }
                },
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
                max_log = std::cmp::max(max_log, e.file_id);
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
            for (fd, meta) in files {
                let info = levels.entry(meta.level).or_insert_with(|| LevelInfo::init(meta.level));
                info.apply(&meta);
            }
            let mut lvls: Vec<LevelInfo> = levels.into_values().collect();
            lvls.reverse();
            let ver = Version::new(id, max_log, tsf_name, lvls, max_level_ts);
            versions.insert(id, Arc::new(RwLock::new(ver)));
        }
        let vs = VersionSet::new(tf_cfg, versions);
        Ok(vs.await)
    }
    // apply version edit to summary file
    // and write to memory struct
    pub async fn apply_version_edit(&mut self, eds: &[VersionEdit]) -> Result<()> {
        for edit in eds {
            let buf = edit.encode()?;
            let _ = self.writer
                        .write_record(1, EditType::SummaryEdit.into(), &buf)
                        .map_err(|e| Error::LogRecordErr { source: (e) })
                        .await?;
            self.writer.hard_sync().map_err(|e| Error::LogRecordErr { source: e }).await?;
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

pub struct SummaryProcesser {
    summary: Box<Summary>,
    cbs: Vec<Sender<Result<()>>>,
    edits: Vec<VersionEdit>,
}

impl SummaryProcesser {
    pub fn new(summary: Box<Summary>) -> Self {
        Self { summary, cbs: vec![], edits: vec![] }
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
            },
            Err(e) => {
                for cb in self.cbs.drain(..) {
                    let _ = cb.send(Err(Error::ErrApplyEdit));
                }
            },
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

#[test]
fn test_version_edit() {
    let compact = CompactMeta { file_id: 100, ..Default::default() };
    let add_list = vec![compact];
    let compact2 = CompactMeta { file_id: 101, ..Default::default() };
    let del_list = vec![compact2];
    let ve = VersionEdit::new();
    let buf = ve.encode().unwrap();
    let ve2 = VersionEdit::decode(&buf).unwrap();
    assert_eq!(ve2, ve);
}

fn test_enum_convert() {
    let t = EditType::SummaryEdit;
    let i: u8 = t.into();
    let y: u8 = EditType::SummaryEdit.into();
    dbg!(y, i);
    let zero: EditType = 1u8.try_into().unwrap();
    assert_eq!(zero, EditType::SummaryEdit);
}
