use std::{
    cmp::max,
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use models::{FieldId, Timestamp};
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
    tseries_family::{LevelInfo, Version},
    tsm::{self, DataBlock, TsmWriter},
    version_set::VersionSet,
    TseriesFamilyId,
};

pub struct FlushTask {
    mems: Vec<Arc<RwLock<MemCache>>>,
    meta: CompactMeta,
    tsf_id: TseriesFamilyId,
    path_tsm: PathBuf,
    path_delta: PathBuf,
}

impl FlushTask {
    pub fn new(
        mems: Vec<Arc<RwLock<MemCache>>>,
        tsf_id: TseriesFamilyId,
        path_tsm: PathBuf,
        path_delta: PathBuf,
    ) -> Self {
        Self {
            mems,
            meta: CompactMeta::default(),
            tsf_id,
            path_tsm,
            path_delta,
        }
    }

    pub async fn run(
        &mut self,
        version: Arc<Version>,
        kernel: Arc<GlobalContext>,
        edits: &mut Vec<VersionEdit>,
        cf_opt: Arc<TseriesFamOpt>,
    ) -> Result<()> {
        let (mut min_ts, mut max_ts) = (i64::MAX, i64::MIN);
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
            build_block_set(field_size_delta, field_map_delta, &mut min_ts, &mut max_ts);
        // build tsm file
        if !block_set_delta.is_empty() {
            kernel.file_id_next();
            self.meta.file_id = kernel.file_id();
            let tmp_meta = CompactMeta {
                file_id: 0,
                file_size: 0,
                tsf_id: self.tsf_id,
                min_ts,
                max_ts,
                level: 0,
                high_seq,
                low_seq,
                is_delta: true,
            };
            update_meta(
                block_set_delta,
                tsm::MAX_BLOCK_VALUES,
                &self.path_delta,
                &mut self.meta,
                &tmp_meta,
            )?;
            append_meta_to_version_edits(&self.meta, edits, version.max_level_ts);
        }
        (min_ts, max_ts) = (i64::MAX, i64::MIN);
        let block_set = build_block_set(field_size, field_map, &mut min_ts, &mut max_ts);
        if !block_set.is_empty() {
            kernel.file_id_next();
            self.meta.file_id = kernel.file_id();
            let tmp_meta = CompactMeta {
                file_id: 0,
                file_size: 0,
                tsf_id: self.tsf_id,
                min_ts,
                max_ts,
                level: 1,
                high_seq,
                low_seq,
                is_delta: false,
            };
            update_meta(
                block_set,
                tsm::MAX_BLOCK_VALUES,
                &self.path_tsm,
                &mut self.meta,
                &tmp_meta,
            )?;
            append_meta_to_version_edits(&self.meta, edits, version.max_level_ts);
        }
        for i in mem_guard.iter_mut() {
            i.flushed = true;
        }
        Ok(())
    }
}

fn update_meta(
    block_set: HashMap<FieldId, DataBlock>,
    max_block_size: u32,
    path: &PathBuf,
    meta: &mut CompactMeta,
    tmp_meta: &CompactMeta,
) -> Result<()> {
    let (fname, fseq) = if tmp_meta.is_delta {
        (make_delta_file_name(path, meta.file_id), meta.file_id)
    } else {
        (make_tsm_file_name(path, meta.file_id), meta.file_id)
    };
    // update meta
    meta.tsf_id = tmp_meta.tsf_id;
    meta.low_seq = tmp_meta.low_seq;
    meta.high_seq = tmp_meta.high_seq;
    meta.max_ts = tmp_meta.max_ts;
    meta.min_ts = tmp_meta.min_ts;
    meta.level = tmp_meta.level;
    meta.file_size = build_tsm_file(fname, fseq, tmp_meta.is_delta, block_set, max_block_size)?;
    meta.is_delta = tmp_meta.is_delta;
    Ok(())
}

fn append_meta_to_version_edits(
    meta: &CompactMeta,
    edits: &mut Vec<VersionEdit>,
    max_level_ts: Timestamp,
) {
    let mut edit = VersionEdit::new();
    edit.add_file(meta.clone(), max_level_ts);
    edits.push(edit);
}

fn build_block_set(
    field_size: HashMap<&FieldId, usize>,
    field_map: HashMap<&FieldId, Vec<&MemEntry>>,
    ts_min: &mut i64,
    ts_max: &mut i64,
) -> HashMap<FieldId, DataBlock> {
    let mut block_set = HashMap::new();
    for (fid, entries) in field_map {
        let size = match field_size.get(&fid) {
            None => {
                error!("failed to get field size");
                continue;
            }
            Some(v) => v,
        };

        let entry = match entries.first() {
            None => {
                error!("failed to get mem entry");
                continue;
            }
            Some(v) => v,
        };
        let mut block = DataBlock::new(*size, entry.field_type);
        for entry in entries.iter() {
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

fn build_tsm_file(
    tsm_path: impl AsRef<Path>,
    tsm_sequence: u64,
    is_delta: bool,
    block_set: HashMap<FieldId, DataBlock>,
    max_block_size: u32,
) -> Result<u64> {
    let file = file_manager::get_file_manager().create_file(tsm_path)?;
    let mut writer = TsmWriter::open(file.into_cursor(), tsm_sequence, is_delta, 0)?;
    for (fid, blk) in block_set.into_iter() {
        let merged_blks = DataBlock::merge_blocks(vec![blk], max_block_size);
        for merged_blk in merged_blks.iter() {
            writer
                .write_block(fid, merged_blk)
                .context(error::WriteTsmSnafu)?;
        }
    }
    writer.write_index().context(error::WriteTsmSnafu)?;
    writer.flush().context(error::WriteTsmSnafu)?;
    Ok(writer.size())
}

pub async fn run_flush_memtable_job(
    reqs: Arc<Mutex<Vec<FlushReq>>>,
    kernel: Arc<GlobalContext>,
    tsf_config: HashMap<u32, Arc<TseriesFamOpt>>,
    version_set: Arc<RwLock<VersionSet>>,
    summary_task_sender: UnboundedSender<SummaryTask>,
    compact_task_sender: UnboundedSender<TseriesFamilyId>,
) -> Result<()> {
    let mut mems: HashMap<TseriesFamilyId, Vec<Arc<RwLock<MemCache>>>> = HashMap::new();
    {
        let mut reqs = reqs.lock();
        info!("get flush request len {}", reqs.len());
        if reqs.len() == 0 {
            return Ok(());
        }
        for req in reqs.iter() {
            for (tf, mem) in &req.mems {
                let mem_vec = mems.entry(*tf).or_insert(Vec::new());
                mem_vec.push(mem.clone());
            }
        }
        reqs.clear();
    }
    let mut edits: Vec<VersionEdit> = vec![];
    for (tsf_id, memtables) in mems.iter() {
        if let Some(tsf) = version_set.read().get_tsfamily_by_tf_id(*tsf_id) {
            if !memtables.is_empty() {
                // todo: build path by vnode data
                let cf_opt = tsf.read().options();
                let path_tsm = cf_opt.tsm_dir(*tsf_id);
                let path_delta = cf_opt.delta_dir(*tsf_id);
                let mut job = FlushTask::new(memtables.clone(), *tsf_id, path_tsm, path_delta);
                job.run(
                    tsf.read().version(),
                    kernel.clone(),
                    &mut edits,
                    cf_opt.clone(),
                )
                .await?;
                match compact_task_sender.send(*tsf_id) {
                    Err(e) => error!("{}", e),
                    _ => {}
                }
            }
        }
    }
    let (task_state_sender, task_state_receiver) = oneshot::channel();
    let task = SummaryTask {
        edits,
        cb: task_state_sender,
    };
    if summary_task_sender.send(task).is_err() {
        error!("failed to send Summary task,the edits not be loaded!")
    }
    Ok(())
}
