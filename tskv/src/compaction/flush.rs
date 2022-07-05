use std::{cmp::max, collections::HashMap, path::PathBuf, sync::Arc};

use models::FieldId;
use parking_lot::Mutex;
use regex::internal::Input;
use tokio::sync::RwLock;

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
    tseries_family::LevelInfo,
    tsm::{DataBlock, TsmBlockWriter, TsmFooterWriter, TsmHeaderWriter, TsmIndexWriter},
    version_set::VersionSet,
};

// 构建flush task 将memcache中的数据 flush到 tsm文件中
pub struct FlushTask {
    edit: VersionEdit,
    mems: Vec<Arc<RwLock<MemCache>>>,
    meta: CompactMeta,
    tsf_id: u32,
    path: String,
}

impl FlushTask {
    pub fn new(mems: Vec<Arc<RwLock<MemCache>>>, tsf_id: u32, log_seq: u64, path: String) -> Self {
        let mut edit = VersionEdit::new();
        edit.set_log_seq(log_seq);
        edit.set_tsf_id(tsf_id);

        let meta = CompactMeta::new(log_seq, 0); //flush to level 0
        Self { edit, mems, meta, tsf_id, path }
    }
    // todo: build tsm file
    pub async fn run(&mut self, version_set: Arc<RwLock<VersionSet>>) -> Result<CompactMeta> {
        let (mut ts_min, mut ts_max) = (i64::MAX, i64::MIN);
        let (mut high_seq, mut low_seq) = (0, u64::MAX);
        let mut field_map = HashMap::new();
        let mut field_size = HashMap::new();
        let mut mem_guard = vec![];
        for i in self.mems.iter() {
            mem_guard.push(i.read().await);
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
                let sum = field_size.entry(field_id).or_insert(0_usize);
                *sum += entry.cells.len();
                let item = field_map.entry(field_id).or_insert(vec![]);
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
        let mut version_s = version_set.write().await;
        let mut version =
            version_s.get_tsfamily(self.tsf_id as u64).unwrap().version().write().await;
        if version.levels_info.is_empty() {
            version.levels_info.push(LevelInfo::init(0));
        }
        version.levels_info[0].apply(&self.meta);
        Ok(self.meta.clone())
    }
}

fn build_tsm_file(fname: PathBuf, block_set: HashMap<FieldId, DataBlock>) -> Result<u64> {
    let file = file_manager::get_file_manager().create_file(fname).unwrap();
    let mut fs_cursor = file.into_cursor();

    TsmHeaderWriter::write_to(&mut fs_cursor)?;
    let index = TsmBlockWriter::write_to(&mut fs_cursor, block_set)?;
    let index_pos = fs_cursor.pos();
    let bloom_filter = TsmIndexWriter::write_to(&mut fs_cursor, index)?;
    TsmFooterWriter::write_to(&mut fs_cursor, &bloom_filter, index_pos)?;
    fs_cursor.sync_all(FileSync::Hard)
             .map_err(|e| Error::WriteTsmErr { reason: e.to_string() })?;
    let len = fs_cursor.len();
    Ok(len)
}

pub async fn run_flush_memtable_job(reqs: Arc<Mutex<Vec<FlushReq>>>,
                                    kernel: Arc<GlobalContext>,
                                    tsf_config: HashMap<u32, Arc<TseriesFamOpt>>,
                                    version_set: Arc<RwLock<VersionSet>>)
                                    -> Result<()> {
    let mut mems = vec![];
    {
        let mut reqs = reqs.lock();
        println!("get flush request len {}", reqs.len());
        for req in reqs.iter() {
            for (tf, mem) in &req.mems {
                while *tf >= mems.len() as u32 {
                    mems.push(vec![]);
                }
                mems[(*tf) as usize].push(mem.clone());
            }
        }
        reqs.clear();
        println!("finish flush request");
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
            let meta = job.run(version_set.clone()).await?;
            let mut edit = VersionEdit::new();
            edit.add_file(i as u32, log_seq, kernel.last_seq(), meta);
            edits.push(edit);
        }
    }
    Ok(())
}
