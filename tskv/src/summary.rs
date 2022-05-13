use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use futures::TryFutureExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc::UnboundedSender, oneshot::Sender, RwLock};

use crate::{
    context::GlobalContext,
    error::{Error, Result},
    file_utils,
    kv_option::{DBOptions, TseriesFamDesc, TseriesFamOpt},
    record_file::{Reader, Writer},
    KvContext, LevelInfo, Version, VersionSet,
};

const MAX_BATCH_SIZE: usize = 64;
#[derive(Serialize, Deserialize, PartialEq, Debug, Default, Clone)]
pub struct CompactMeta {
    pub file_id: u64, // file id
    pub ts_min: u64,
    pub ts_max: u64,
    pub level: u32,
    pub high_seq: u64,
    pub low_seq: u64,
}
impl CompactMeta {
    pub fn new(file_id: u64, level: u32) -> Self {
        Self { file_id,
               ts_min: u64::MAX,
               ts_max: u64::MIN,
               level,
               high_seq: u64::MIN,
               low_seq: u64::MIN }
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
    pub has_log_seq: bool,
    pub log_seq: u64,
    pub add_files: Vec<CompactMeta>,
    pub del_files: Vec<CompactMeta>,

    pub del_tsf: bool,
    pub add_tsf: bool,
    pub tsf_id: u32,
    pub tsf_name: String,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self { level: 0,
               seq_no: 0,
               log_seq: 0,
               add_files: vec![],
               del_files: vec![],
               del_tsf: false,
               add_tsf: false,
               tsf_id: 0,
               tsf_name: String::from(""),
               has_seq_no: false,
               has_log_seq: false }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Encode { source: (e) })
    }
    pub fn decode(buf: &Vec<u8>) -> Result<Self> {
        bincode::deserialize(buf).map_err(|e| Error::Decode { source: (e) })
    }
    pub fn add_file(&mut self, tsf_id: u32, log_seq: u64, seq_no: u64, meta: CompactMeta) {
        self.has_log_seq = true;
        self.log_seq = log_seq;
        self.has_seq_no = true;
        self.seq_no = seq_no;
        self.add_files.push(meta);
    }
    // todo:
    pub fn del_file(&mut self) {}

    pub fn set_log_seq(&mut self, log_seq: u64) {
        self.log_seq = log_seq;
    }
    pub fn set_tsf_id(&mut self, tsf_id: u32) {
        self.tsf_id = tsf_id;
    }
}

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
        let ctx = Arc::new(GlobalContext::default());
        let rd = Box::new(Reader::new(&file_utils::make_summary_file(&db_opt.db_path, 0)));
        let vs = Self::recover_version(tf_desc, rd, &ctx).await?;

        Ok(Self { file_no: 0, version_set: Arc::new(RwLock::new(vs)), ctx })
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
            for e in eds {
                if e.has_seq_no {
                    ctx.set_last_seq(e.seq_no);
                }
                if e.has_log_seq {
                    ctx.set_log_seq(e.log_seq);
                }
                max_log = std::cmp::max(max_log, e.log_seq);
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
                let info = levels.entry(meta.level).or_insert(LevelInfo::init(meta.level));
                info.apply(&meta);
            }
            let lvls = levels.into_values().collect();
            let ver = Version::new(id, max_log, tsf_name, lvls);
            versions.insert(id, Arc::new(ver));
        }
        let vs = VersionSet::new(tf_cfg, versions);
        Ok(vs)
    }
    // apply version edit to summary file
    // and write to memory struct
    pub async fn apply_version_edit(&self, eds: &[VersionEdit]) -> Result<()> {
        Ok(())
    }

    pub fn version_set(&self) -> Arc<RwLock<VersionSet>> {
        self.version_set.clone()
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
}
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
    let mut compact = CompactMeta::default();
    compact.file_id = 100;
    let add_list = vec![compact];
    let mut compact2 = CompactMeta::default();
    compact2.file_id = 101;
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
