use std::{
    cmp::max,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use models::FieldId;
use parking_lot::{Mutex, RwLock};
use regex::internal::Input;
use snafu::{NoneError, ResultExt};
use tokio::sync::{mpsc::UnboundedSender, oneshot, oneshot::Sender};
use trace::{debug, error, info, warn};

use crate::{
    compaction::FlushReq,
    context::GlobalContext,
    direct_io::FileSync,
    error::{self, Error, Result},
    file_manager,
    file_utils::{make_delta_file_name, make_tsm_file_name},
    kv_option::TseriesFamOpt,
    memcache::{MemCache, MemEntry},
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::LevelInfo,
    tsm::{DataBlock, TsmWriter},
    version_set::VersionSet,
    TseriesFamilyId,
};

pub struct FlushTask {
    mems: Vec<Arc<RwLock<MemCache>>>,
    meta: CompactMeta,
    tsf_id: TseriesFamilyId,
    path_tsm: String,
    path_delta: String,
}

impl FlushTask {
    pub fn new(mems: Vec<Arc<RwLock<MemCache>>>,
               tsf_id: TseriesFamilyId,
               path_tsm: String,
               path_delta: String)
               -> Self {
        let meta = CompactMeta::new();
        Self { mems, meta, tsf_id, path_tsm, path_delta }
    }
    pub async fn run(&mut self,
                     version_set: Arc<RwLock<VersionSet>>,
                     kernel: Arc<GlobalContext>,
                     edits: &mut Vec<VersionEdit>,
                     cf_opt: Arc<TseriesFamOpt>)
                     -> Result<()> {
        let (mut ts_min, mut ts_max) = (i64::MAX, i64::MIN);
        let (mut high_seq, mut low_seq) = (0, u64::MAX);
        let mut field_map = HashMap::new();
        let mut field_map_delta = HashMap::new();
        let mut field_size = HashMap::new();
        let mut field_size_delta = HashMap::new();
        let mut mem_guard = vec![];
        for i in self.mems.iter() {
            mem_guard.push(i.write());
        }
        for mem in mem_guard.iter() {
            let data = &mem.data_cache;
            // get req seq_no range
            if mem.seq_no > high_seq {
                high_seq = mem.seq_no;
            }
            if mem.seq_no < low_seq {
                low_seq = mem.seq_no;
            }
            for (field_id, entry) in data {
                if mem.is_delta {
                    let sum = field_size_delta.entry(field_id).or_insert(0_usize);
                    *sum += entry.cells.len();
                    let item = field_map_delta.entry(field_id).or_insert(vec![]);
                    item.push(entry);
                } else {
                    let sum = field_size.entry(field_id).or_insert(0_usize);
                    *sum += entry.cells.len();
                    let item = field_map.entry(field_id).or_insert(vec![]);
                    item.push(entry);
                }
            }
        }
        let block_set_delta =
            build_block_set(field_size_delta, field_map_delta, &mut ts_max, &mut ts_min);
        // build tsm file
        if !block_set_delta.is_empty() {
            self.meta.file_id = kernel.file_id();
            kernel.file_id_next();
            build_tsm_file_workflow(&mut self.meta,
                                    block_set_delta,
                                    self.tsf_id,
                                    &self.path_delta,
                                    low_seq,
                                    high_seq,
                                    ts_max,
                                    ts_min,
                                    0,
                                    true,
                                    edits,
                                    version_set.clone(),
                                    cf_opt.clone()).await
                                                   .expect("failed to build delta file");
        }
        (ts_min, ts_max) = (i64::MAX, i64::MIN);
        let block_set = build_block_set(field_size, field_map, &mut ts_max, &mut ts_min);
        if !block_set.is_empty() {
            self.meta.file_id = kernel.file_id();
            kernel.file_id_next();
            build_tsm_file_workflow(&mut self.meta,
                                    block_set,
                                    self.tsf_id,
                                    &self.path_tsm,
                                    low_seq,
                                    high_seq,
                                    ts_max,
                                    ts_min,
                                    1,
                                    false,
                                    edits,
                                    version_set.clone(),
                                    cf_opt).await
                                           .expect("Failed to build tsm file");
        }
        for i in mem_guard.iter_mut() {
            i.flushed = true;
        }
        Ok(())
    }
}

async fn build_tsm_file_workflow(meta: &mut CompactMeta,
                                 block_set: HashMap<FieldId, DataBlock>,
                                 tsf_id: u32,
                                 path: &str,
                                 low_seq: u64,
                                 high_seq: u64,
                                 ts_max: i64,
                                 ts_min: i64,
                                 level: usize,
                                 is_delta: bool,
                                 edits: &mut Vec<VersionEdit>,
                                 version_set: Arc<RwLock<VersionSet>>,
                                 cf_opt: Arc<TseriesFamOpt>)
                                 -> Result<()> {
    let (fname, fseq) = if is_delta {
        (make_delta_file_name(path, meta.file_id), meta.file_id)
    } else {
        (make_tsm_file_name(path, meta.file_id), meta.file_id)
    };
    let file_size = build_tsm_file(fname, fseq, is_delta, block_set)?;
    // update meta
    meta.tsf_id = tsf_id;
    meta.low_seq = low_seq;
    meta.high_seq = high_seq;
    meta.ts_max = ts_max;
    meta.ts_min = ts_min;
    meta.level = level as u32;
    meta.file_size = file_size;
    meta.is_delta = is_delta;
    let max_level_ts;
    {
        let version_s = version_set.read();
        let tsf_immut = match version_s.get_tsfamily_by_tf_id(tsf_id) {
            None => {
                error!("failed get tsfamily by tsf id {}", tsf_id);
                return Err(Error::InvalidTsfid { tf_id: tsf_id });
            },
            Some(v) => v,
        };
        let mut version = tsf_immut.version().write();
        max_level_ts = version.max_level_ts;
        while version.levels_info.len() <= level {
            let i: u32 = version.levels_info.len() as u32;
            version.levels_info.push(LevelInfo::init(i));
            version.levels_info[i as usize].tsf_opt = cf_opt.clone()
        }
        version.levels_info[level].apply(meta);
    }
    let mut edit = VersionEdit::new();
    edit.add_file(meta.level, tsf_id, meta.file_id, high_seq, max_level_ts, meta.clone());
    edits.push(edit);
    Ok(())
}

fn build_block_set(field_size: HashMap<&FieldId, usize>,
                   field_map: HashMap<&FieldId, Vec<&MemEntry>>,
                   ts_max: &mut i64,
                   ts_min: &mut i64)
                   -> HashMap<FieldId, DataBlock> {
    let mut block_set = HashMap::new();
    for (fid, entrys) in field_map {
        let size = match field_size.get(&fid) {
            None => {
                error!("failed to get field size");
                continue;
            },
            Some(v) => v,
        };

        let entry = match entrys.first() {
            None => {
                error!("failed to get mem entry");
                continue;
            },
            Some(v) => v,
        };
        let mut block = DataBlock::new(*size, entry.field_type);
        for entry in entrys.iter() {
            // get tsm ts range
            if entry.ts_max > *ts_max {
                *ts_max = entry.ts_max;
            }
            if entry.ts_min < *ts_min {
                *ts_min = entry.ts_min;
            }
            block.batch_insert(&entry.cells);
        }
        block_set.insert(*fid, block);
    }
    block_set
}

fn build_tsm_file(tsm_path: impl AsRef<Path>,
                  tsm_sequence: u64,
                  is_delta: bool,
                  block_set: HashMap<FieldId, DataBlock>)
                  -> Result<u64> {
    let file = file_manager::get_file_manager().create_file(tsm_path)?;
    let mut writer = TsmWriter::open(file.into_cursor(), tsm_sequence, is_delta, 0)?;
    for (fid, blk) in block_set.iter() {
        writer.write_block(*fid, blk).context(error::WriteTsmSnafu)?;
    }
    writer.write_index().context(error::WriteTsmSnafu)?;
    writer.flush().context(error::WriteTsmSnafu)?;
    Ok(writer.size())
}

pub async fn run_flush_memtable_job(reqs: Arc<Mutex<Vec<FlushReq>>>,
                                    kernel: Arc<GlobalContext>,
                                    tsf_config: HashMap<u32, Arc<TseriesFamOpt>>,
                                    version_set: Arc<RwLock<VersionSet>>,
                                    summary_task_sender: UnboundedSender<SummaryTask>)
                                    -> Result<()> {
    let mut mems = vec![];
    {
        let mut reqs = reqs.lock();
        info!("get flush request len {}", reqs.len());
        if reqs.len() == 0 {
            return Ok(());
        }
        for req in reqs.iter() {
            for (tf, mem) in &req.mems {
                while *tf >= mems.len() as u32 {
                    mems.push(vec![]);
                }
                mems[(*tf) as usize].push(mem.clone());
            }
        }
        reqs.clear();
    }
    let mut edits: Vec<VersionEdit> = vec![];
    for (i, memtables) in mems.iter().enumerate() {
        if !memtables.is_empty() {
            // todo: build path by vnode data
            let idx = i as u32;
            let cf_opt =
                tsf_config.get(&idx).cloned().unwrap_or_else(|| Arc::new(TseriesFamOpt::default()));

            let path_tsm = cf_opt.tsm_dir.clone() + &i.to_string();
            let path_delta = cf_opt.delta_dir.clone() + &i.to_string();
            let mut job = FlushTask::new(memtables.clone(), i as u32, path_tsm, path_delta);
            job.run(version_set.clone(), kernel.clone(), &mut edits, cf_opt.clone()).await?;
        }
    }
    let (task_state_sender, task_state_receiver) = oneshot::channel();
    let task = SummaryTask { edits, cb: task_state_sender };
    if let Err(_) = summary_task_sender.send(task) {
        error!("failed to send Summary task,the edits not be loaded!")
    }
    Ok(())
}
