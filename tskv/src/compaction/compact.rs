use std::{
    collections::{HashMap, VecDeque},
    iter::Peekable,
    marker::PhantomData,
    sync::Arc,
};

use evmap::new;
use models::{FieldId, Timestamp, ValueType};

use crate::{
    compaction::CompactReq,
    context::GlobalContext,
    direct_io::File,
    error::Result,
    memcache::DataType,
    summary::VersionEdit,
    tseries_family::ColumnFile,
    tsm::{
        BlockMeta, BlockMetaIterator, ColumnReader, DataBlock, Index, IndexMeta, IndexReader,
        TsmReader,
    },
};

pub async fn run_compaction_job(request: CompactReq, kernel: Arc<GlobalContext>) -> Result<()> {
    // if request.files.0 < 3 {
    //     fast_compact();
    //     return Ok(());
    // }
    let version = request.version;

    if version.levels_info().len() == 0 {
        return Ok(());
    }

    // Buffers all tsm-files and it's indexes for this compaction
    let max_data_block_size = 1000; // TODO this const value is in module tsm
    let mut tsm_files: Vec<Arc<File>> = Vec::new();
    let mut tsm_tombstones: Vec<Arc<File>> = Vec::new();
    let mut tsm_readers = Vec::new();
    let mut tsm_index_iters = Vec::new();
    for lvl in version.levels_info().iter() {
        if lvl.level() != request.files.0 {
            continue;
        }
        for col_file in lvl.files.iter() {
            let file = Arc::new(col_file.file(lvl.tsf_opt.clone())?);
            tsm_files.push(file.clone());
            let tomb_file = Arc::new(col_file.tombstone_file(lvl.tsf_opt.clone())?);
            tsm_tombstones.push(tomb_file.clone());
            let tr = TsmReader::open(file, tomb_file)?;
            let idx_iter = tr.index_iterator().peekable();
            tsm_readers.push(tr);
            tsm_index_iters.push(idx_iter);
        }
    }
    // let mut buffers = Vec::new();
    let mut finished_idxes = vec![false; tsm_readers.len()];
    let mut finished_idx_cnt = 0_usize;
    let mut curr_fid: Option<FieldId> = None;
    let mut merged_blocks: VecDeque<DataBlock> = VecDeque::new();
    loop {
        // Has one or more merged blocks, and block_size is bigger than a max block.
        // Just write
        if let Some(data_blk) = merged_blocks.front() {
            if data_blk.len() >= max_data_block_size {
                // write(data_blk)
                println!("write: {:?}", data_blk.time_range(0, data_blk.len()));
                merged_blocks.pop_front();
            }
        }

        // get all of block_metas of this field id
        let mut turn_tsm_blks: Vec<BlockMetaIterator> = Vec::new();
        let mut turn_tsm_blks_cnt = 0_usize;
        let (mut turn_min_ts, mut turn_max_ts) = (Timestamp::MAX, Timestamp::MIN);

        // for each tsm-file, handle one field id
        for (i, idx) in tsm_index_iters.iter_mut().enumerate() {
            if finished_idxes[i] {
                continue;
            }
            if let Some(idx_meta) = idx.peek() {
                // get field id from first block for this turn
                if let Some(fid) = curr_fid {
                    if fid != idx_meta.field_id() {
                        continue;
                    }
                } else {
                    // This is the first idx.
                    curr_fid = Some(idx_meta.field_id());
                    let (min_ts, max_ts) = idx_meta.timerange();
                    turn_min_ts = turn_min_ts.max(min_ts);
                    turn_max_ts = turn_max_ts.min(max_ts);
                }

                let blk_cnt = idx_meta.block_count();
                turn_tsm_blks_cnt += idx_meta.block_count() as usize;

                let (min_ts, max_ts) = idx_meta.timerange();
                if overlaps((turn_min_ts, turn_max_ts), (min_ts, max_ts)) {
                    turn_min_ts = turn_min_ts.min(min_ts);
                    turn_max_ts = turn_max_ts.max(max_ts);
                    turn_tsm_blks.push(idx_meta.iter());
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
            continue;
        }

        // merge these blocks of this field id
        if turn_tsm_blks_cnt <= 1 {
            // 只有一个块，直接输出
            // write()
            continue;
        }

        // starts to merge blocks for this field id
        let mut tsm_blk_iters = Vec::new();
        for blk_meta_iter in turn_tsm_blks.into_iter() {
            tsm_blk_iters.push(blk_meta_iter.peekable());
        }
        let (mut blk_min_ts, mut blk_max_ts) = (Timestamp::MIN, Timestamp::MAX);
        loop {
            let mut merging_blks: Vec<DataBlock> = Vec::new();
            for (i, blk_iter) in tsm_blk_iters.iter_mut().enumerate() {
                if let Some(blk_meta) = blk_iter.peek() {
                    if i == 1 {
                        // Add first block
                        (blk_min_ts, blk_max_ts) = (blk_meta.min_ts(), blk_meta.max_ts());
                        let data_blk = tsm_readers[i].get_data_block(&blk_meta)?;
                        // TODO remove tombstombs
                        merging_blks.push(data_blk);
                        // Go to next
                        blk_iter.next();
                    } else {
                        // Add the next overlaping blocks
                        if overlaps((blk_min_ts, blk_max_ts),
                                    (blk_meta.min_ts(), blk_meta.max_ts()))
                        {
                            blk_min_ts = blk_min_ts.min(blk_meta.min_ts());
                            blk_max_ts = blk_max_ts.max(blk_meta.max_ts());
                            let data_blk = tsm_readers[i].get_data_block(&blk_meta)?;
                            // TODO remove tombstombs
                            merging_blks.push(data_blk);
                            // Go to next
                            blk_iter.next();
                        } else {
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
            break;
        }
    }

    if merged_blocks.len() > 0 {
        // write(data_blk)
        for data_blk in merged_blocks.iter() {
            println!("write: {:?}", data_blk.time_range(0, data_blk.len()));
        }
    }

    Ok(())
}

fn overlaps(r1: (Timestamp, Timestamp), r2: (Timestamp, Timestamp)) -> bool {
    r1.0 <= r2.1 && r1.1 >= r2.0
}
