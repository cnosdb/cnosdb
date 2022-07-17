use std::{
    collections::{HashMap, VecDeque},
    iter::Peekable,
    marker::PhantomData,
    sync::Arc,
};

use evmap::new;
use logger::{error, info};
use models::{FieldId, Timestamp, ValueType};
use snafu::ResultExt;

use crate::{
    compaction::CompactReq,
    context::GlobalContext,
    direct_io::File,
    error::{self, Result},
    file_manager::{self, get_file_manager},
    file_utils,
    kv_option::TseriesFamOpt,
    memcache::DataType,
    summary::{CompactMeta, VersionEdit},
    tseries_family::ColumnFile,
    tsm::{
        self, BlockMeta, BlockMetaIterator, ColumnReader, DataBlock, Index, IndexIterator,
        IndexMeta, IndexReader, TsmReader, TsmWriter,
    },
    Error, LevelId,
};

struct CompactIterator {
    tsm_readers: Vec<TsmReader>,

    tsm_index_iters: Vec<Peekable<IndexIterator>>,
    /// When a TSM file at index i is ended, finished_idxes[i] is set to true.
    finished_readers: Vec<bool>,
    /// How many finished_idxes is set to true
    finished_reader_cnt: usize,
    curr_fid: Option<FieldId>,
    last_fid: Option<FieldId>,
    mergine_blocks: VecDeque<DataBlock>,
}

impl CompactIterator {}

impl Iterator for CompactIterator {
    type Item = Result<(FieldId, DataBlock)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(blk) = self.mergine_blocks.pop_front() {
            return Some(Ok((self.last_fid.expect("been checked"), blk)));
        }
        loop {
            println!("----------------------------");
            // Get all of block_metas of this field id
            let mut turn_tsm_blks: Vec<Peekable<BlockMetaIterator>> = Vec::new();
            let mut turn_tsm_blks_cnt = 0_usize;
            // turn_tsm_blks[i] is in tsm_files[ tsm_blk_tsm_file_idx_map[i] ]
            // turn_tsm_blks[i] is in tsm_readers[ tsm_blk_tsm_file_idx_map[i] ]
            let mut tsm_blk_tsm_file_idx_map: Vec<usize> = Vec::new();
            let mut next_tsm_file_idx = 0_usize;
            let (mut turn_min_ts, mut turn_max_ts) = (Timestamp::MAX, Timestamp::MIN);

            // For each tsm-file, handle one field id
            for (i, idx) in self.tsm_index_iters.iter_mut().enumerate() {
                next_tsm_file_idx += 1;
                if self.finished_readers[i] {
                    println!("file no.{} has been finished.", i);
                    continue;
                }
                if let Some(idx_meta) = idx.peek() {
                    println!("got idx_meta: field_id: {}, field_type: {:?}, block_count: {}",
                             idx_meta.field_id(),
                             idx_meta.field_type(),
                             idx_meta.block_count());
                    // Get field id from first block for this turn
                    if let Some(fid) = self.curr_fid {
                        if fid != idx_meta.field_id() {
                            println!("skip idx_meta to {}", idx_meta.field_id());
                            continue;
                        }
                    } else {
                        // This is the first idx.
                        self.curr_fid = Some(idx_meta.field_id());
                        self.last_fid = Some(idx_meta.field_id());
                        println!("turn first field_id: {}", idx_meta.field_id());
                        (turn_min_ts, turn_max_ts) = idx_meta.timerange();
                    }

                    let blk_cnt = idx_meta.block_count();
                    turn_tsm_blks_cnt += idx_meta.block_count() as usize;

                    let (min_ts, max_ts) = idx_meta.timerange();
                    if overlaps_tuples((turn_min_ts, turn_max_ts), (min_ts, max_ts)) {
                        turn_min_ts = turn_min_ts.min(min_ts);
                        turn_max_ts = turn_max_ts.max(max_ts);
                        turn_tsm_blks.push(idx_meta.block_iterator().peekable());
                        tsm_blk_tsm_file_idx_map.push(next_tsm_file_idx - 1);
                    } else {
                        println!("skip idx_meta: field_id: {}, field_type: {:?}, block_count: {}",
                                 idx_meta.field_id(),
                                 idx_meta.field_type(),
                                 idx_meta.block_count());
                        continue;
                    }
                } else {
                    // This tsm-file has been finished
                    println!("file no.{} is finished.", i);
                    self.finished_readers[i] = true;
                    self.finished_reader_cnt += 1;
                }

                // To next field
                idx.next();
            }

            if turn_tsm_blks_cnt == 0 {
                if self.finished_reader_cnt >= self.finished_readers.len() {
                    break;
                }
                continue;
            }

            // Starts to merge blocks for this field id
            let (mut blk_min_ts, mut blk_max_ts) = (Timestamp::MIN, Timestamp::MAX);
            loop {
                let mut overlaped_blks: Vec<DataBlock> = Vec::new();
                for (i, blk_iter) in turn_tsm_blks.iter_mut().enumerate() {
                    if let Some(blk_meta) = blk_iter.peek() {
                        if i == 0 {
                            // Add first block
                            (blk_min_ts, blk_max_ts) = (blk_meta.min_ts(), blk_meta.max_ts());
                            let data_blk =
                                match self.tsm_readers[tsm_blk_tsm_file_idx_map[i]].get_data_block(&blk_meta)
                                                                        .context(error::ReadTsmSnafu) {
                                    Ok(blk) => blk,
                                    Err(e) => return Some(Err(e)),
                                };
                            println!("add data block first: {}", data_blk);
                            overlaped_blks.push(data_blk);
                            // Go to next
                            blk_iter.next();
                        } else {
                            // Add the next overlaping blocks
                            // TODO block is earlier than the timerange also needed to merge.
                            if overlaps_tuples((blk_min_ts, blk_max_ts),
                                               (blk_meta.min_ts(), blk_meta.max_ts()))
                            {
                                blk_min_ts = blk_min_ts.min(blk_meta.min_ts());
                                blk_max_ts = blk_max_ts.max(blk_meta.max_ts());
                                let data_blk =
                                    match self.tsm_readers[i].get_data_block(&blk_meta)
                                                             .context(error::ReadTsmSnafu)
                                    {
                                        Ok(blk) => blk,
                                        Err(e) => return Some(Err(e)),
                                    };
                                println!("add data block next: {}", data_blk);
                                overlaped_blks.push(data_blk);
                                // Go to next
                                blk_iter.next();
                            } else {
                                continue;
                            }
                        }
                    }
                }
                // All blocks handled, this turn finished.
                if overlaped_blks.len() == 0 {
                    break;
                }
                let data_blk = DataBlock::merge_blocks(overlaped_blks);
                println!("merge data block: {}", data_blk);
                self.mergine_blocks.push_back(data_blk);
            }

            self.curr_fid = None;
            if self.finished_reader_cnt >= self.finished_readers.len() {
                break;
            }
        }

        if let Some(blk) = self.mergine_blocks.pop_front() {
            return Some(Ok((self.last_fid.expect("been checked"), blk)));
        }
        None
    }
}

/// Returns if r1 (min_ts, max_ts) overlaps r2 (min_ts, max_ts)
pub fn overlaps_tuples(r1: (i64, i64), r2: (i64, i64)) -> bool {
    r1.0 <= r2.1 && r1.1 >= r2.0
}

pub fn run_compaction_job(request: CompactReq,
                          kernel: Arc<GlobalContext>)
                          -> Result<Vec<VersionEdit>> {
    let version = request.version;

    if version.levels_info().len() == 0 {
        return Ok(vec![]);
    }

    // Buffers all tsm-files and it's indexes for this compaction
    let max_data_block_size = 1000; // TODO this const value is in module tsm
    let mut tsf_opt: Option<Arc<TseriesFamOpt>> = None;
    let mut tsm_files: Vec<Arc<File>> = Vec::new();
    let mut tsm_readers = Vec::new();
    let mut tsm_index_iters = Vec::new();
    for lvl in version.levels_info().iter() {
        if lvl.level() != request.files.0 {
            continue;
        }
        tsf_opt = Some(lvl.tsf_opt.clone());
        for col_file in request.files.1.iter() {
            // Delta file is not compacted here
            if col_file.is_delta() {
                continue;
            }
            let file = Arc::new(col_file.file(lvl.tsf_opt.clone())?);
            tsm_files.push(file.clone());
            let tsm_reader = match col_file.tombstone_file(lvl.tsf_opt.clone()) {
                Ok(f) => TsmReader::open(file, Some(Arc::new(f)))?,
                Err(e) => match e {
                    Error::OpenFile { source } => TsmReader::open(file, None)?,
                    others => return Err(others),
                },
            };
            let idx_iter = tsm_reader.index_iterator().peekable();
            tsm_readers.push(tsm_reader);
            tsm_index_iters.push(idx_iter);
        }
        // This should be only one
        break;
    }
    if tsf_opt.is_none() {
        error!("Cannot get tseries_fam_opt");
        return Err(Error::Compact { reason: "TseriesFamOpt is none".to_string() });
    }
    if tsm_index_iters.len() == 0 {
        // Nothing to compact
        return Ok(vec![]);
    }

    let tsm_readers_cnt = tsm_readers.len();
    let mut iter = CompactIterator { tsm_readers,
                                     tsm_index_iters,
                                     finished_readers: vec![false; tsm_readers_cnt],
                                     finished_reader_cnt: 0_usize,
                                     curr_fid: None,
                                     last_fid: None,
                                     mergine_blocks: VecDeque::new() };
    let tsm_dir = tsf_opt.expect("been checked").tsm_dir.clone();
    let mut tsm_writer = tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0)?;
    let mut version_edits: Vec<VersionEdit> = Vec::new();
    while let Some(next_blk) = iter.next() {
        println!("got next block: {}", next_blk.is_err());
        if let Ok((fid, blk)) = next_blk {
            println!("field_id: {}, data_block: {}", fid, blk);
            match tsm_writer.write_block(fid, &blk) {
                Err(e) => match e {
                    crate::tsm::WriteTsmError::IO { source } => {
                        // error!("IO error when write tsm");
                        println!("IO error when write tsm");
                    },
                    crate::tsm::WriteTsmError::Encode { source } => {
                        // error!("Encoding error when write tsm");
                        println!("Encoding error when write tsm");
                    },
                    crate::tsm::WriteTsmError::MaxFileSizeExceed { source } => {
                        tsm_writer.write_index().context(error::WriteTsmSnafu)?;
                        tsm_writer.flush().context(error::WriteTsmSnafu)?;
                        let cm = new_compact_meta(tsm_writer.sequence(),
                                                  tsm_writer.size(),
                                                  request.out_level);
                        let mut ve = VersionEdit::new();
                        ve.add_file(request.out_level,
                                    request.tsf_id,
                                    tsm_writer.sequence(),
                                    0,
                                    version.max_level_ts,
                                    cm);
                        version_edits.push(ve);
                        tsm_writer =
                            tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0)?;
                    },
                },
                _ => {},
            }
        }
    }

    tsm_writer.write_index().context(error::WriteTsmSnafu)?;
    tsm_writer.flush().context(error::WriteTsmSnafu)?;
    let cm = new_compact_meta(tsm_writer.sequence(), tsm_writer.size(), request.out_level);
    let mut ve = VersionEdit::new();
    ve.add_file(request.out_level,
                request.tsf_id,
                tsm_writer.sequence(),
                0,
                version.max_level_ts,
                cm);
    version_edits.push(ve);

    Ok(version_edits)
}

fn new_compact_meta(file_id: u64, file_size: u64, level: LevelId) -> CompactMeta {
    let mut cm = CompactMeta::new();
    cm.file_id = file_id;
    cm.file_size = file_size;
    cm.ts_min = 0;
    cm.ts_max = 0;
    cm.level = level;
    cm.high_seq = 0;
    cm.low_seq = 0;
    cm.is_delta = false;
    cm
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        default,
        sync::{
            atomic::{AtomicBool, AtomicU32, AtomicU64},
            Arc,
        },
    };

    use utils::BloomFilter;

    use crate::{
        compaction::{run_compaction_job, CompactReq},
        context::GlobalContext,
        file_manager,
        kv_option::TseriesFamOpt,
        tseries_family::{ColumnFile, LevelInfo, TimeRange, Version},
        tsm::{self, DataBlock},
    };

    fn prepare_column_file() -> (u64, Vec<Arc<ColumnFile>>) {
        let dir = "/tmp/test/compaction";
        if !file_manager::try_exists(dir) {
            std::fs::create_dir_all(dir).unwrap();
        }
        let test_data =
            vec![HashMap::from([(1, DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }),
                                (2, DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }),
                                (3, DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] })]),
                 HashMap::from([(1, DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }),
                                (2, DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }),
                                (3, DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] })]),
                 HashMap::from([(1, DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }),
                                (2, DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }),
                                (3, DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] })]),];

        let mut cfs = Vec::new();
        for (i, data) in test_data.iter().enumerate() {
            let file_seq = i as u64 + 1;
            let mut writer = tsm::new_tsm_writer(&dir, file_seq, false, 0).unwrap();
            for (fid, blk) in data.iter() {
                writer.write_block(*fid, blk).unwrap();
            }
            writer.write_index().unwrap();
            writer.flush().unwrap();
            cfs.push(Arc::new(ColumnFile::new(file_seq,
                                              TimeRange::new(writer.min_ts(), writer.max_ts()),
                                              writer.size(),
                                              false)));
        }

        (4, cfs)
    }

    fn prepare_tseries_fam_opt() -> Arc<TseriesFamOpt> {
        Arc::new(TseriesFamOpt { base_file_size: 16777216,
                                 max_compact_size: 2147483648,
                                 tsm_dir: "/tmp/test/compaction".to_string(),
                                 ..Default::default() })
    }

    #[test]
    fn test_compaction_fast() {
        let (next_file_id, files) = prepare_column_file();
        let tsf_opt = prepare_tseries_fam_opt();
        let mut lv1_info = LevelInfo::init(1);
        lv1_info.tsf_opt = tsf_opt;
        let level_infos =
            vec![lv1_info, LevelInfo::init(2), LevelInfo::init(3), LevelInfo::init(4),];
        let version = Arc::new(Version::new(1, 1, "version_1".to_string(), level_infos, 1000));
        let compact_req = CompactReq { files: (1, files), version, tsf_id: 1, out_level: 2 };
        let kernel = Arc::new(GlobalContext::new());
        kernel.set_file_id(next_file_id);

        run_compaction_job(compact_req, kernel.clone()).unwrap();
    }

    #[test]
    fn test_compaction_slow() {
        let files =
            vec![Arc::new(ColumnFile::new(1, TimeRange::new(1, 10000), 9626716, false)),
                 Arc::new(ColumnFile::new(2, TimeRange::new(10001, 20000), 9628296, false)),
                 Arc::new(ColumnFile::new(3, TimeRange::new(20001, 30000), 9628799, false)),];
        let tsf_opt = Arc::new(TseriesFamOpt { base_file_size: 16777216,
                                               max_compact_size: 2147483648,
                                               tsm_dir: "/tmp/test/compaction".to_string(),
                                               ..Default::default() });
        let mut lv1_info = LevelInfo::init(1);
        lv1_info.tsf_opt = tsf_opt;
        let level_infos =
            vec![lv1_info, LevelInfo::init(2), LevelInfo::init(3), LevelInfo::init(4),];
        let version = Arc::new(Version::new(1, 1, "version_1".to_string(), level_infos, 1000));
        let compact_req = CompactReq { files: (1, files), version, tsf_id: 1, out_level: 2 };
        let kernel = Arc::new(GlobalContext::new());
        kernel.set_file_id(4);

        run_compaction_job(compact_req, kernel.clone()).unwrap();
    }
}
