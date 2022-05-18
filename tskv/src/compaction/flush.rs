use std::{collections::HashMap, sync::Arc};

use crate::{
    compaction::FlushRequest,
    context::GlobalContext,
    error::{Error, Result},
    file_utils::make_tsm_file_name,
    kv_option::TseriesFamOpt,
    memcache::MemCache,
    summary::{CompactMeta, VersionEdit},
};

// 构建flush task 将memcache中的数据 flush到 tsm文件中
pub struct FlushTask {
    edit: VersionEdit,
    mems: Vec<Arc<MemCache>>,
    meta: CompactMeta,
    tsf_id: u32,
    path: String,
}

impl FlushTask {
    pub fn new(mems: Vec<Arc<MemCache>>, tsf_id: u32, log_seq: u64, path: String) -> Self {
        let mut edit = VersionEdit::new();
        edit.set_log_seq(log_seq);
        edit.set_tsf_id(tsf_id);
        let meta = CompactMeta::new(log_seq, 0); //flush to level 0
        Self { edit, mems, meta, tsf_id, path }
    }
    // todo: build tsm file
    pub async fn run(&mut self) -> Result<CompactMeta> {
        let fname = make_tsm_file_name(&self.path, self.meta.file_id());
        Ok(self.meta.clone())
    }
}

pub async fn run_flush_memtable_job(reqs: Vec<FlushRequest>,
                                    kernel: Arc<GlobalContext>,
                                    tsf_config: HashMap<u32, Arc<TseriesFamOpt>>,
                                    snapshots: Vec<u64>)
                                    -> Result<()> {
    let mut mems = vec![];
    for req in &reqs {
        for (tf, mem) in &req.mems {
            while *tf >= mems.len() as u32 {
                mems.push(vec![]);
            }
            mems[(*tf) as usize].push(mem.clone());
        }
    }
    let mut edits = vec![];
    for (i, memtables) in mems.iter().enumerate() {
        if !memtables.is_empty() {
            // todo: build path by vnode data
            let path = "db/tsf/".to_string() + &i.to_string();
            let log_seq = kernel.log_seq_next();
            let idx = i as u32;
            let cf_opt =
                tsf_config.get(&idx).cloned().unwrap_or_else(|| Arc::new(TseriesFamOpt::default()));
            let mut job = FlushTask::new(memtables.clone(), i as u32, log_seq, path);
            let meta = job.run().await?;
            // let mut meta = CompactMeta::default();
            let mut edit = VersionEdit::new();
            edit.add_file(i as u32, log_seq, kernel.last_seq(), meta);
            edits.push(edit);
        }
    }
    Ok(())
}
