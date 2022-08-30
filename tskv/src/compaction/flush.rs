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
    tsf_id: TseriesFamilyId,
    kernel: Arc<GlobalContext>,
    path_tsm: PathBuf,
    path_delta: PathBuf,
}

impl FlushTask {
    pub fn new(
        mems: Vec<Arc<RwLock<MemCache>>>,
        tsf_id: TseriesFamilyId,
        kernel: Arc<GlobalContext>,
        path_tsm: PathBuf,
        path_delta: PathBuf,
    ) -> Self {
        Self {
            mems,
            tsf_id,
            kernel,
            path_tsm,
            path_delta,
        }
    }

    pub async fn run(&mut self, version: Arc<Version>, edits: &mut Vec<VersionEdit>) -> Result<()> {
        let mut mem_guard = vec![];
        for i in self.mems.iter() {
            mem_guard.push(i.write());
        }

        let (mut high_seq, mut low_seq) = (0, u64::MAX);
        let mut field_map = HashMap::new();
        let mut field_map_delta = HashMap::new();
        let mut field_size = HashMap::new();
        let mut field_size_delta = HashMap::new();
        for mem in mem_guard.iter() {
            // get req seq_no range
            if mem.seq_no() > high_seq {
                high_seq = mem.seq_no();
            }
            if mem.seq_no() < low_seq {
                low_seq = mem.seq_no();
            }

            if mem.is_delta {
                mem.copy_data(&mut field_map_delta, &mut field_size_delta);
            } else {
                mem.copy_data(&mut field_map, &mut field_size);
            }
        }

        let (mut min_ts, mut max_ts) = (i64::MAX, i64::MIN);
        let block_set_delta =
            build_block_set(field_size_delta, field_map_delta, &mut min_ts, &mut max_ts);
        if !block_set_delta.is_empty() {
            let mut meta = self.write_block_set(block_set_delta, tsm::MAX_BLOCK_VALUES, true)?;
            meta.low_seq = low_seq;
            meta.high_seq = high_seq;
            let max_level_ts = meta.max_ts.max(version.max_level_ts);
            let mut edit = VersionEdit::new();
            edit.add_file(meta, max_level_ts);
            edits.push(edit);
        }

        // Write tsm files.
        (min_ts, max_ts) = (i64::MAX, i64::MIN);
        let block_set = build_block_set(field_size, field_map, &mut min_ts, &mut max_ts);
        if !block_set.is_empty() {
            let mut meta = self.write_block_set(block_set, tsm::MAX_BLOCK_VALUES, false)?;
            meta.low_seq = low_seq;
            meta.high_seq = high_seq;
            let max_level_ts = meta.max_ts.max(version.max_level_ts);
            let mut edit = VersionEdit::new();
            edit.add_file(meta, max_level_ts);
            edits.push(edit);
        }

        for i in mem_guard.iter_mut() {
            i.flushed = true;
        }
        Ok(())
    }

    fn write_block_set(
        &self,
        block_set: HashMap<FieldId, DataBlock>,
        max_block_size: u32,
        is_delta: bool,
    ) -> Result<CompactMeta> {
        let out_level = if is_delta { 0 } else { 1 };
        let mut tsm_writer = if is_delta {
            tsm::new_tsm_writer(
                self.path_delta.clone(),
                self.kernel.file_id_next(),
                is_delta,
                0,
            )?
        } else {
            tsm::new_tsm_writer(
                self.path_tsm.clone(),
                self.kernel.file_id_next(),
                is_delta,
                0,
            )?
        };

        info!("Flush: File {} been created.", tsm_writer.sequence());

        for (fid, blk) in block_set.into_iter() {
            let merged_blks = DataBlock::merge_blocks(vec![blk], max_block_size);
            for merged_blk in merged_blks.iter() {
                tsm_writer
                    .write_block(fid, merged_blk)
                    .context(error::WriteTsmSnafu)?;
            }
        }
        tsm_writer.write_index().context(error::WriteTsmSnafu)?;
        tsm_writer.flush().context(error::WriteTsmSnafu)?;

        info!(
            "Flush: File: {} write finished ({} B).",
            tsm_writer.sequence(),
            tsm_writer.size()
        );

        let compact_meta = CompactMeta::new(
            tsm_writer.sequence(),
            tsm_writer.size(),
            self.tsf_id,
            out_level,
            tsm_writer.min_ts(),
            tsm_writer.max_ts(),
            is_delta,
        );

        Ok(compact_meta)
    }
}

fn build_block_set(
    field_size: HashMap<FieldId, usize>,
    field_map: HashMap<FieldId, Vec<Arc<RwLock<MemEntry>>>>,
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

        let mut block = DataBlock::new(*size, entry.read().field_type);

        for entry in entries.iter() {
            let entry = entry.read();
            // get tsm ts range
            if entry.ts_max > *ts_max {
                *ts_max = entry.ts_max;
            }
            if entry.ts_min < *ts_min {
                *ts_min = entry.ts_min;
            }
            block.batch_insert(&entry.cells);
        }
        block_set.insert(fid, block);
    }
    block_set
}

pub async fn run_flush_memtable_job(
    reqs: Arc<Mutex<Vec<FlushReq>>>,
    kernel: Arc<GlobalContext>,
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
            if memtables.is_empty() {
                continue;
            }

            // todo: build path by vnode data
            let ts_family_opt = tsf.read().options();
            let path_tsm = ts_family_opt.tsm_dir(*tsf_id);
            let path_delta = ts_family_opt.delta_dir(*tsf_id);

            let mut job = FlushTask::new(
                memtables.clone(),
                *tsf_id,
                kernel.clone(),
                path_tsm,
                path_delta,
            );
            job.run(tsf.read().version(), &mut edits).await?;

            match compact_task_sender.send(*tsf_id) {
                Err(e) => error!("{}", e),
                _ => {}
            }
        }
    }

    info!("Flush: Flush finished, version edits: {:?}", edits);

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

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use models::{FieldId, ValueType};
    use parking_lot::{Mutex, RwLock};
    use tokio::sync::mpsc;

    use crate::{
        compaction::FlushReq,
        context::GlobalContext,
        kv_option::TseriesFamOpt,
        memcache::{MemCache, MemRaw},
        tseries_family::FLUSH_REQ,
        version_set::VersionSet,
    };

    use super::run_flush_memtable_job;

    fn make_value(value_type: ValueType) -> Vec<u8> {
        match value_type {
            ValueType::Float => 1.0_f64.to_be_bytes().as_slice().to_vec(),
            ValueType::Integer => 1_i64.to_be_bytes().as_slice().to_vec(),
            ValueType::Unsigned => 1_u64.to_be_bytes().as_slice().to_vec(),
            ValueType::Boolean => [0_u8].to_vec(),
            ValueType::String => "HelloWorld".as_bytes().to_vec(),
            ValueType::Unknown => panic!("Unsuported value type: Unknown"),
        }
    }

    /// MemRaw descriptor: [ (seq_start, seq_end), timestamp_start, [ (FieldId, ValueType, Value) ] ]
    fn make_mem_cache(desc: ((u64, u64), i64, Vec<(FieldId, ValueType, Vec<u8>)>)) -> MemCache {
        let cache = MemCache::new(1, 1048576, 0, false);
        let mut timestamp = desc.1;
        for seq in desc.0 .0..desc.0 .1 {
            for (fid, ftyp, fval) in desc.2.iter() {
                let value = make_value(*ftyp);
                let mut raw = MemRaw {
                    seq,
                    ts: timestamp,
                    field_id: *fid,
                    field_type: *ftyp,
                    val: &fval,
                };
                cache.insert_raw(&mut raw).unwrap();
            }
            timestamp += 1;
        }
        cache
    }
}
