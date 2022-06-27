use std::{cmp::max, collections::HashMap, path::PathBuf, sync::Arc};

use regex::internal::Input;

use crate::{
    compaction::FlushReq,
    context::GlobalContext,
    direct_io::FileSync,
    error::{Error, Result},
    file_manager,
    file_utils::make_tsm_file_name,
    kv_option::TseriesFamOpt,
    memcache::MemCache,
    summary::{CompactMeta, VersionEdit},
    tsm::{DataBlock, FooterBuilder, TsmBlockWriter, TsmIndexBuilder},
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
        let (mut ts_min, mut ts_max) = (i64::MAX, i64::MIN);
        let (mut high_seq, mut low_seq) = (0, u64::MAX);
        let mut field_map = HashMap::new();
        let mut field_size = HashMap::new();
        for mem in &self.mems {
            let data = &mem.data_cache;
            // get req seq_no range
            if mem.seq_no > high_seq {
                high_seq = mem.seq_no;
            }
            if mem.seq_no < low_seq {
                low_seq = mem.seq_no;
            }
            for (field_id, entry) in data {
                let sum = field_size.entry(field_id).or_insert(0_usize);
                *sum = *sum + entry.cells.len();
                let item = field_map.entry(field_id).or_insert(vec![entry]);
                item.push(entry);
            }
        }
        let mut block_set = HashMap::new();
        for (fid, entrys) in field_map {
            let size = field_size.get(&fid).unwrap();
            let entry = entrys.first().unwrap();
            let mut block = DataBlock::new(*size, entry.field_type);
            for entry in entrys.iter() {
                // get tsm ts range
                if entry.ts_max > ts_max {
                    ts_max = entry.ts_max;
                }
                if entry.ts_min < ts_min {
                    ts_min = entry.ts_min;
                }
                block.batch_insert(&entry.cells);
            }
            block_set.insert(*fid, block);
        }
        // build tsm file
        let fname = make_tsm_file_name(&self.path, self.meta.file_id());
        let file_size = build_tsm_file(fname, block_set)?;
        // update meta
        self.meta.low_seq = low_seq;
        self.meta.high_seq = high_seq;
        self.meta.ts_min = ts_min;
        self.meta.file_size = file_size;
        Ok(self.meta.clone())
    }
}

fn build_tsm_file(fname: PathBuf, block_set: HashMap<u64, DataBlock>) -> Result<u64> {
    let file = file_manager::get_file_manager().create_file(fname).unwrap();
    let mut fs_cursor = file.into_cursor();
    let mut rd = TsmBlockWriter::new(&mut fs_cursor);

    let index = rd.build(block_set)?;
    let mut id = TsmIndexBuilder::new(&mut fs_cursor);
    let pos = id.build(index)?;
    let mut foot = FooterBuilder::new(&mut fs_cursor);
    foot.build(pos)?;
    fs_cursor.sync_all(FileSync::Hard)
             .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
    let len = fs_cursor.len();
    Ok(len)
}

pub async fn run_flush_memtable_job(reqs: Vec<FlushReq>,
                                    kernel: Arc<GlobalContext>,
                                    tsf_config: HashMap<u32, Arc<TseriesFamOpt>>)
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
            let idx = i as u32;
            let cf_opt =
                tsf_config.get(&idx).cloned().unwrap_or_else(|| Arc::new(TseriesFamOpt::default()));

            let path = cf_opt.tsm_dir.clone() + &i.to_string();
            let log_seq = kernel.log_seq_next();
            let mut job = FlushTask::new(memtables.clone(), i as u32, log_seq, path);
            let meta = job.run().await?;
            let mut edit = VersionEdit::new();
            edit.add_file(i as u32, log_seq, kernel.last_seq(), meta);
            edits.push(edit);
        }
    }
    Ok(())
}
