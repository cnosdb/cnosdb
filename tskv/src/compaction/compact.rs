use std::{
    collections::{HashMap, VecDeque},
    iter::Peekable,
    marker::PhantomData,
    sync::Arc,
};

use evmap::new;
use models::{FieldId, Timestamp, ValueType};
use snafu::ResultExt;
use utils::overlaps_tuples;

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
        self, BlockMeta, BlockMetaIterator, ColumnReader, DataBlock, Index, IndexMeta, IndexReader,
        TsmReader, TsmWriter,
    },
    Error, LevelId,
};

pub fn run_compaction_job(request: CompactReq,
                          kernel: Arc<GlobalContext>)
                          -> Result<Vec<VersionEdit>> {
    // if request.files.0 < 3 {
    //     fast_compact();
    //     return Ok(());
    // }
    dbg!("Start compaction job");
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
            let tr = match col_file.tombstone_file(lvl.tsf_opt.clone()) {
                Ok(f) => TsmReader::open(file, Some(Arc::new(f)))?,
                Err(e) => {
                    println!("[ERROR] open tombstone file failed: {}", e);
                    TsmReader::open(file, None)?
                },
            };
            let idx_iter = tr.index_iterator().peekable();
            tsm_readers.push(tr);
            tsm_index_iters.push(idx_iter);
        }
        break;
    }
    dbg!(&tsf_opt);
    if tsf_opt.is_none() {
        println!("[ERROR] cannot get tseries_fam_opt");
        return Err(Error::Compact { reason: "TseriesFamOpt is none".to_string() });
    }
    dbg!(tsm_index_iters.len());
    if tsm_index_iters.len() == 0 {
        // Nothing to compact
        return Ok(vec![]);
    }
    // let mut buffers = Vec::new();
    // When a TSM file at index i is ended, finished_idxes[i] is set to true.
    let mut finished_idxes = vec![false; tsm_readers.len()];
    // How many finished_idxes is set to true
    let mut finished_idx_cnt = 1_usize;
    let mut curr_fid: Option<FieldId> = None;
    let mut last_fid: Option<FieldId> = None;
    let mut merged_blocks: VecDeque<DataBlock> = VecDeque::new();

    let tsm_dir = tsf_opt.expect("been checked").tsm_dir.clone();
    let mut tsm_writer = tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0)?;
    let mut version_edits: Vec<VersionEdit> = Vec::new();
    loop {
        if merged_blocks.len() > 0 {
            // Has one or more merged blocks, and block_size is bigger than a max block.
            // Just write
            let field_id = last_fid.expect("been checked");

            println!("    writeting data to _{:06}.tsm", tsm_writer.sequence());
            while let Some(data_blk) = merged_blocks.front() {
                // if data_blk.len() >= max_data_block_size {
                println!("    write-1: field_id: {} - data_block: {}", last_fid.unwrap(), data_blk);
                match tsm_writer.write_block(field_id, &data_blk) {
                    Err(e) => match e {
                        crate::tsm::WriteTsmError::IO { source } => {
                            println!("    [ERROR] io error when write tsm");
                        },
                        crate::tsm::WriteTsmError::Encode { source } => {
                            println!("    [ERROR] encode error when write tsm");
                        },
                        crate::tsm::WriteTsmError::MaxFileSizeExceed { source } => {
                            println!("    [ERROR] max file size exceed when write tsm");
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
                println!("    writer-1: file_size: {}", tsm_writer.size());
                merged_blocks.pop_front();
                // }
            }
        }

        // Get all of block_metas of this field id
        let mut turn_tsm_blks: Vec<Peekable<BlockMetaIterator>> = Vec::new();
        let mut turn_tsm_blks_cnt = 0_usize;
        // turn_tsm_blks[i] is in tsm_files[ tsm_blk_tsm_file_idx_map[i] ]
        // turn_tsm_blks[i] is in tsm_readers[ tsm_blk_tsm_file_idx_map[i] ]
        let mut tsm_blk_tsm_file_idx_map: Vec<usize> = Vec::new();
        let mut next_tsm_file_idx = 0_usize;
        let (mut turn_min_ts, mut turn_max_ts) = (Timestamp::MAX, Timestamp::MIN);

        // For each tsm-file, handle one field id
        for (i, idx) in tsm_index_iters.iter_mut().enumerate() {
            next_tsm_file_idx += 1;
            println!("    finish-{}? - {}, {}, {}",
                     finished_idx_cnt, &finished_idxes[0], &finished_idxes[1], &finished_idxes[2]);
            if finished_idxes[i] {
                println!("    index no.{} skipped.", i);
                continue;
            }
            if let Some(idx_meta) = idx.peek() {
                // Get field id from first block for this turn
                if let Some(fid) = curr_fid {
                    if fid != idx_meta.field_id() {
                        println!("    field {} skipped.", fid);
                        continue;
                    }
                } else {
                    // This is the first i dx.
                    curr_fid = Some(idx_meta.field_id());
                    last_fid = Some(idx_meta.field_id());
                    println!("    current field_id: {}", curr_fid.unwrap());
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
                    continue;
                }
            } else {
                // This tsm-file has been finished
                finished_idxes[i] = true;
                finished_idx_cnt += 1;
            }

            // To next field
            idx.next();
        }

        if turn_tsm_blks_cnt == 0 {
            println!("turn_tsm_blks_cnt is 0");
            continue;
        }

        // Merge these blocks of this field id
        // if turn_tsm_blks_cnt <= 1 {
        // write()
        // continue;
        // }

        // Starts to merge blocks for this field id
        let (mut blk_min_ts, mut blk_max_ts) = (Timestamp::MIN, Timestamp::MAX);
        loop {
            let mut merging_blks: Vec<DataBlock> = Vec::new();
            for (i, blk_iter) in turn_tsm_blks.iter_mut().enumerate() {
                if let Some(blk_meta) = blk_iter.peek() {
                    println!("reading block {}", blk_meta);
                    if i == 0 {
                        // Add first block
                        (blk_min_ts, blk_max_ts) = (blk_meta.min_ts(), blk_meta.max_ts());
                        let data_blk =
                            tsm_readers[tsm_blk_tsm_file_idx_map[i]].get_data_block(&blk_meta)
                                                                    .context(error::ReadTsmSnafu)?;
                        merging_blks.push(data_blk);
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
                            let data_blk = tsm_readers[i].get_data_block(&blk_meta)
                                                         .context(error::ReadTsmSnafu)?;
                            merging_blks.push(data_blk);
                            // Go to next
                            blk_iter.next();
                        } else {
                            println!("block skipped {}", blk_meta);
                            continue;
                        }
                    }
                }
            }
            // All blocks handled, this turn finished.
            if merging_blks.len() == 0 {
                break;
            }
            let data_blk = DataBlock::merge_blocks(merging_blks);
            merged_blocks.push_back(data_blk);
        }

        curr_fid = None;
        if finished_idx_cnt >= finished_idxes.len() {
            println!("finished_idx_cnt >= finished_idxes.len(");
            break;
        }
    }

    println!("Loop finished.");

    if merged_blocks.len() > 0 {
        let field_id = last_fid.expect("been checked");
        for data_blk in merged_blocks.iter() {
            println!("    write-2: field_id: {} - data_block: {}", last_fid.unwrap(), data_blk);
            match tsm_writer.write_block(field_id, &data_blk) {
                Err(e) => match e {
                    crate::tsm::WriteTsmError::IO { source } => {
                        println!("[ERROR] io error when write tsm");
                    },
                    crate::tsm::WriteTsmError::Encode { source } => {
                        println!("[ERROR] encode error when write tsm");
                    },
                    crate::tsm::WriteTsmError::MaxFileSizeExceed { source } => {
                        println!("[ERROR] max file size exceed when write tsm");
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
    println!("Finish compaction job, the last file size is {}", tsm_writer.size());

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
        kv_option::TseriesFamOpt,
        tseries_family::{ColumnFile, LevelInfo, TimeRange, Version},
    };

    fn prepare() {}

    #[test]
    fn test_compaction() {
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
