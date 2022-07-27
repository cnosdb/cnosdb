use std::{
    collections::{BinaryHeap, HashMap, VecDeque},
    iter::Peekable,
    marker::PhantomData,
    path::PathBuf,
    sync::Arc,
};

use evmap::new;
use models::{FieldId, Timestamp, ValueType};
use snafu::ResultExt;
use trace::{error, info};

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

struct CompactingBlockMeta(usize, BlockMeta);

impl PartialEq for CompactingBlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0 && self.1 == other.1
    }
}

impl Eq for CompactingBlockMeta {}

impl PartialOrd for CompactingBlockMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.1.cmp(&other.1))
    }
}

impl Ord for CompactingBlockMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.1.cmp(&other.1)
    }
}

enum CompactingBlock {
    DataBlock { field_id: FieldId, data_block: DataBlock },
    Raw { meta: BlockMeta, raw: Vec<u8> },
}

impl CompactingBlock {
    fn to_data_blocks(source: Vec<Self>) -> Result<Vec<DataBlock>> {
        let mut res: Vec<DataBlock> = Vec::with_capacity(source.len());
        for cb in source.into_iter() {
            match cb {
                CompactingBlock::DataBlock { data_block, .. } => {
                    res.push(data_block);
                },
                CompactingBlock::Raw { meta, raw } => {
                    let data_block = tsm::decode_data_block(&raw, meta.field_type(), meta.size(),
                                         meta.val_off() - meta.offset())
                                         .context(error::ReadTsmSnafu)?;
                    res.push(data_block);
                },
            }
        }

        Ok(res)
    }
}

struct CompactIterator {
    tsm_readers: Vec<TsmReader>,

    tsm_index_iters: Vec<Peekable<IndexIterator>>,
    tmp_tsm_blks: Vec<BlockMetaIterator>,
    /// Index to mark `Peekable<BlockMetaIterator>` in witch `TsmReader`,
    /// tmp_tsm_blks[i] is in self.tsm_readers[ tmp_tsm_blk_tsm_reader_idx[i] ]
    tmp_tsm_blk_tsm_reader_idx: Vec<usize>,
    /// When a TSM file at index i is ended, finished_idxes[i] is set to true.
    finished_readers: Vec<bool>,
    /// How many finished_idxes is set to true
    finished_reader_cnt: usize,
    curr_fid: Option<FieldId>,
    last_fid: Option<FieldId>,

    merged_blocks: VecDeque<CompactingBlock>,

    max_datablock_values: u32,
}

/// To reduce construction code
impl Default for CompactIterator {
    fn default() -> Self {
        Self { tsm_readers: Default::default(),
               tsm_index_iters: Default::default(),
               tmp_tsm_blks: Default::default(),
               tmp_tsm_blk_tsm_reader_idx: Default::default(),
               finished_readers: Default::default(),
               finished_reader_cnt: Default::default(),
               curr_fid: Default::default(),
               last_fid: Default::default(),
               merged_blocks: Default::default(),
               max_datablock_values: Default::default() }
    }
}

impl CompactIterator {
    /// Update tmp_tsm_blks and tmp_tsm_blk_tsm_reader_idx for field id in next iteration.
    fn next_field_id(&mut self) {
        self.tmp_tsm_blks = Vec::with_capacity(self.tsm_index_iters.len());
        self.tmp_tsm_blk_tsm_reader_idx = Vec::with_capacity(self.tsm_index_iters.len());
        let mut next_tsm_file_idx = 0_usize;
        for (i, idx) in self.tsm_index_iters.iter_mut().enumerate() {
            next_tsm_file_idx += 1;
            if self.finished_readers[i] {
                info!("file no.{} has been finished, continue.", i);
                continue;
            }
            if let Some(idx_meta) = idx.peek() {
                // Get field id from first block for this iteration
                if let Some(fid) = self.curr_fid {
                    // This is the idx of the next field_id.
                    if fid != idx_meta.field_id() {
                        continue;
                    }
                } else {
                    // This is the first idx.
                    self.curr_fid = Some(idx_meta.field_id());
                    self.last_fid = Some(idx_meta.field_id());
                }

                let blk_cnt = idx_meta.block_count();

                self.tmp_tsm_blks.push(idx_meta.block_iterator());
                self.tmp_tsm_blk_tsm_reader_idx.push(next_tsm_file_idx - 1);
                info!("merging idx_meta: field_id: {}, field_type: {:?}, block_count: {}, timerange: {:?}",
                      idx_meta.field_id(),
                      idx_meta.field_type(),
                      idx_meta.block_count(),
                      idx_meta.timerange());
            } else {
                // This tsm-file has been finished
                info!("file no.{} is finished.", i);
                self.finished_readers[i] = true;
                self.finished_reader_cnt += 1;
            }

            // To next field
            idx.next();
        }
    }

    /// Collect merging `DataBlock`s.
    fn next_merging_blocks(&mut self) -> Result<()> {
        if self.tmp_tsm_blks.len() == 0 {
            return Ok(());
        }
        let mut sorted_blk_metas: BinaryHeap<CompactingBlockMeta> =
            BinaryHeap::with_capacity(self.tmp_tsm_blks.len());
        let field_id = self.curr_fid.expect("method next_field_id has been called");
        // Get all block_meta, and check overlaps.
        for (i, blk_iter) in self.tmp_tsm_blks.iter_mut().enumerate() {
            while let Some(blk_meta) = blk_iter.next() {
                sorted_blk_metas.push(CompactingBlockMeta(self.tmp_tsm_blk_tsm_reader_idx[i],
                                                          blk_meta));
            }
        }

        // If BlockMeta::count is less than max_datablock_values, we need to merge it with the
        // next block
        let mut merging_blks: Vec<CompactingBlock> = Vec::new();
        // let mut merging_blk: Option<DataBlock> = None;
        let mut merged_blk_timerange = (Timestamp::MAX, Timestamp::MIN);
        // If BlockMeta::count reaches max_datablock_values, we don't decode the block.
        let mut buf = vec![0_u8; 1024];
        let mut is_first = true;
        while let Some(cbm) = sorted_blk_metas.pop() {
            // 1. Store DataBlocks in merging_blocks, merged_blk_timerange set by the first
            //      BlockMeta
            // 2. For each BlockMeta:
            //   2.1. If it's timerange overlaps with merged_blk_timerange, read DataBlock and push
            //          to merging_blocks, and update merged_blk_timerange
            //   2.2. Else:
            //     2.2.1. If merging_blks's length is 1 and it's a Raw, put to self::merged_blocks.
            //     2.2.2. Else merge merging_blks into Vec<DataBlock>, push to back of
            //              self::merged_blocks, and clean merging_blks.
            //            The last one of the Vec<DataBlock> is special, if it's length is less than
            //              self::max_datablock_values, do not push it.
            //     2.2.3. If it's length reaches self::max_datablock_values, put Raw to
            //              merging_blks, otherwise put DataBlock (2.2.1).
            // 3. Read DataBLock for the remaining BlockMeta, push to self::merged_blks.

            // Exists merging DataBlock in past iteration
            if is_first {
                is_first = false;
                merged_blk_timerange = (cbm.1.min_ts(), cbm.1.max_ts());
                if cbm.1.size() as usize > buf.len() {
                    buf.resize(cbm.1.size() as usize, 0);
                }
                let size = self.tsm_readers[cbm.0].get_raw_data(&cbm.1, &mut buf)
                                                  .context(error::ReadTsmSnafu)?;
                merging_blks.push(CompactingBlock::Raw { meta: cbm.1, raw: buf[..size].to_vec() });
            } else if overlaps_tuples(merged_blk_timerange, (cbm.1.min_ts(), cbm.1.max_ts())) {
                // 2.1
                let data_block =
                    self.tsm_readers[cbm.0].get_data_block(&cbm.1).context(error::ReadTsmSnafu)?;

                merged_blk_timerange.0 = merged_blk_timerange.0.min(cbm.1.min_ts());
                merged_blk_timerange.1 = merged_blk_timerange.0.max(cbm.1.max_ts());
                merging_blks.push(CompactingBlock::DataBlock { field_id, data_block });
            } else {
                // 2.2
                if merging_blks.len() > 0 {
                    if merging_blks.len() == 1 {
                        // 2.2.1
                        if let Some(CompactingBlock::Raw { meta, raw }) = merging_blks.first() {
                            if meta.count() == self.max_datablock_values {
                                self.merged_blocks.push_back(merging_blks.remove(0));
                            }
                        }
                    } else {
                        // 2.2.2
                        let merging_data_blks = CompactingBlock::to_data_blocks(merging_blks)?;
                        merging_blks = Vec::new();
                        let merged_data_blks =
                            DataBlock::merge_blocks(merging_data_blks, self.max_datablock_values);

                        for (i, data_block) in merged_data_blks.into_iter().enumerate() {
                            if data_block.len() < self.max_datablock_values as usize {
                                merging_blks.push(CompactingBlock::DataBlock { field_id,
                                                                               data_block });
                                break;
                            }
                            self.merged_blocks
                                .push_back(CompactingBlock::DataBlock { field_id, data_block });
                        }
                    }

                    // This DataBlock doesn't need to merge
                    if cbm.1.count() == self.max_datablock_values {
                        // 2.2.3
                        if cbm.1.size() as usize > buf.len() {
                            buf.resize(cbm.1.size() as usize, 0);
                        }
                        let size = self.tsm_readers[cbm.0].get_raw_data(&cbm.1, &mut buf)
                                                          .context(error::ReadTsmSnafu)?;
                        merged_blk_timerange.0 = merged_blk_timerange.0.min(cbm.1.min_ts());
                        merged_blk_timerange.1 = merged_blk_timerange.0.max(cbm.1.max_ts());
                        merging_blks.push(CompactingBlock::Raw { meta: cbm.1,
                                                                 raw: buf[..size].to_vec() });
                    } else {
                        // cbm.1.count is less than max_datablock_values
                        let data_block = self.tsm_readers[cbm.0].get_data_block(&cbm.1)
                                                                .context(error::ReadTsmSnafu)?;
                        merging_blks.push(CompactingBlock::DataBlock { field_id, data_block });
                    }
                }
            }
        }

        if merging_blks.len() > 0 {
            let merging_data_blks = CompactingBlock::to_data_blocks(merging_blks)?;
            let merged_data_blks =
                DataBlock::merge_blocks(merging_data_blks, self.max_datablock_values);

            for (i, data_block) in merged_data_blks.into_iter().enumerate() {
                self.merged_blocks.push_back(CompactingBlock::DataBlock { field_id, data_block });
            }
        }

        Ok(())
    }
}

impl Iterator for CompactIterator {
    type Item = Result<CompactingBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(blk) = self.merged_blocks.pop_front() {
            return Some(Ok(blk));
        }
        loop {
            info!("------------------------------");

            // For each tsm-file, get next index reader for current iteration field id
            self.next_field_id();

            info!("selected blocks count: {} in iteration", self.tmp_tsm_blks.len());
            if self.tmp_tsm_blks.len() == 0 {
                info!("iteration field_id {:?} is finished", self.curr_fid);
                self.curr_fid = None;
                break;
            }

            // Get all of block_metas of this field id, and merge these blocks
            if let Err(e) = self.next_merging_blocks() {
                return Some(Err(e));
            }

            if self.finished_reader_cnt >= self.finished_readers.len() {
                break;
            }
        }

        if let Some(blk) = self.merged_blocks.pop_front() {
            return Some(Ok(blk));
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
    let mut tsf_id: Option<u32> = None;
    let mut tsm_files: Vec<PathBuf> = Vec::new();
    let mut tsm_readers = Vec::new();
    let mut tsm_index_iters = Vec::new();
    for lvl in version.levels_info().iter() {
        if lvl.level() != request.files.0 {
            continue;
        }
        tsf_opt = Some(lvl.tsf_opt.clone());
        tsf_id = Some(lvl.tsf_id);
        for col_file in request.files.1.iter() {
            // Delta file is not compacted here
            if col_file.is_delta() {
                continue;
            }
            let tsm_file = col_file.file_path(lvl.tsf_opt.clone(), lvl.tsf_id);
            tsm_files.push(tsm_file.clone());
            let tsm_reader = TsmReader::open(&tsm_file)?;
            let idx_iter = tsm_reader.index_iterator().peekable();
            tsm_readers.push(tsm_reader);
            tsm_index_iters.push(idx_iter);
        }
        // There is only one level to compact.
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
                                     max_datablock_values: max_data_block_size,
                                     ..Default::default() };
    let tsm_dir = tsf_opt.expect("been checked").tsm_dir(tsf_id.expect("been checked"));
    let mut tsm_writer = tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0)?;
    let mut version_edits: Vec<VersionEdit> = Vec::new();
    while let Some(next_blk) = iter.next() {
        if let Ok(blk) = next_blk {
            info!("===============================");
            let write_ret = match blk {
                CompactingBlock::DataBlock { field_id: fid, data_block: b } => {
                    tsm_writer.write_block(fid, &b)
                },
                CompactingBlock::Raw { meta, raw } => tsm_writer.write_raw(&meta, &raw),
            };
            match write_ret {
                Err(e) => match e {
                    crate::tsm::WriteTsmError::IO { source } => {
                        // TODO handle this
                        error!("IO error when write tsm");
                    },
                    crate::tsm::WriteTsmError::Encode { source } => {
                        // TODO handle this
                        error!("Encoding error when write tsm");
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
    for file in request.files.1 {
        ve.del_file(request.files.0, file.file_id(), file.is_delta());
    }
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
    use core::panic;
    use std::{
        collections::HashMap,
        default,
        path::Path,
        sync::{
            atomic::{AtomicBool, AtomicU32, AtomicU64},
            Arc,
        },
    };

    use models::{FieldId, Timestamp, ValueType};
    use utils::BloomFilter;

    use crate::{
        compaction::{run_compaction_job, CompactReq},
        context::GlobalContext,
        file_manager,
        kv_option::TseriesFamOpt,
        tseries_family::{ColumnFile, LevelInfo, TimeRange, Version},
        tsm::{self, DataBlock, TsmReader},
    };

    fn write_data_blocks_to_column_file(dir: impl AsRef<Path>,
                                        data: Vec<HashMap<FieldId, Vec<DataBlock>>>)
                                        -> (u64, Vec<Arc<ColumnFile>>) {
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut cfs = Vec::new();
        let mut file_seq = 0;
        for (i, d) in data.iter().enumerate() {
            file_seq = i as u64 + 1;
            let mut writer = tsm::new_tsm_writer(&dir, file_seq, false, 0).unwrap();
            for (fid, data_blks) in d.iter() {
                for blk in data_blks.iter() {
                    writer.write_block(*fid, blk).unwrap();
                }
            }
            writer.write_index().unwrap();
            writer.flush().unwrap();
            cfs.push(Arc::new(ColumnFile::new(file_seq,
                                              TimeRange::new(writer.min_ts(), writer.max_ts()),
                                              writer.size(),
                                              false)));
        }
        (file_seq + 1, cfs)
    }

    fn read_data_block_from_column_file(path: impl AsRef<Path>)
                                        -> HashMap<FieldId, Vec<DataBlock>> {
        let tsm_reader = TsmReader::open(path).unwrap();
        let mut data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx in tsm_reader.index_iterator() {
            let field_id = idx.field_id();
            for blk_meta in idx.block_iterator() {
                let blk = tsm_reader.get_data_block(&blk_meta).unwrap();
                data.entry(field_id).or_insert(vec![]).push(blk);
            }
        }
        data
    }

    /// Compare DataBlocks in path with the expected_Data using assert_eq.
    fn check_column_file(path: impl AsRef<Path>, expected_data: HashMap<FieldId, Vec<DataBlock>>) {
        let data = read_data_block_from_column_file(path);
        let data_field_ids = data.keys().copied().collect::<Vec<_>>().sort();
        let expected_data_field_ids = expected_data.keys().copied().collect::<Vec<_>>().sort();
        assert_eq!(data_field_ids, expected_data_field_ids);

        for (k, v) in expected_data.iter() {
            let data_blks = data.get(k).unwrap();
            assert_eq!(data.get(k).unwrap(), v);
        }
    }

    fn get_tsf_opt(tsm_dir: &str) -> Arc<TseriesFamOpt> {
        Arc::new(TseriesFamOpt { base_file_size: 16777216,
                                 max_compact_size: 2147483648,
                                 tsm_dir: tsm_dir.to_string(),
                                 ..Default::default() })
    }

    fn prepare_compact_req_and_kernel(tsf_opt: Arc<TseriesFamOpt>,
                                      next_file_id: u64,
                                      files: Vec<Arc<ColumnFile>>)
                                      -> (CompactReq, Arc<GlobalContext>) {
        let mut lv1_info = LevelInfo::init(1);
        lv1_info.tsf_opt = tsf_opt;
        let level_infos =
            vec![lv1_info, LevelInfo::init(2), LevelInfo::init(3), LevelInfo::init(4),];
        let version = Arc::new(Version::new(1, 1, "version_1".to_string(), level_infos, 1000));
        let compact_req = CompactReq { files: (1, files), version, tsf_id: 1, out_level: 2 };
        let kernel = Arc::new(GlobalContext::new());
        kernel.set_file_id(next_file_id);

        (compact_req, kernel)
    }

    #[test]
    fn test_compaction_fast() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
        ]);

        let dir = "/tmp/test/compaction";
        let tsf_opt = get_tsf_opt(&dir);
        let dir = tsf_opt.tsm_dir(0);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data);
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(tsf_opt.clone(), next_file_id, files);
        run_compaction_job(compact_req, kernel).unwrap();
        check_column_file(dir.join("_000004.tsm"), expected_data);
    }

    #[test]
    fn test_compaction_1() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
        ]);

        let dir = "/tmp/test/compaction/1";
        let tsf_opt = get_tsf_opt(&dir);
        let dir = tsf_opt.tsm_dir(0);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data);
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(tsf_opt.clone(), next_file_id, files);
        run_compaction_job(compact_req, kernel.clone()).unwrap();
        check_column_file(dir.join("_000004.tsm"), expected_data);
    }

    #[test]
    fn test_compaction_2() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 2, 3, 5] }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 2, 3, 5] }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3] }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6] }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7], val: vec![4, 5, 6, 8] }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9] }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9] }]),
        ]);

        let dir = "/tmp/test/compaction/2";
        let tsf_opt = get_tsf_opt(&dir);
        let dir = tsf_opt.tsm_dir(0);

        let (next_file_id, files) = write_data_blocks_to_column_file(&dir, data);
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(tsf_opt.clone(), next_file_id, files);
        run_compaction_job(compact_req, kernel.clone()).unwrap();
        check_column_file(dir.join("_000004.tsm"), expected_data);
    }

    /// Returns a generated `DataBlock` with default value and specified size, `DataBlock::ts`
    /// is from arg `min_ts`.
    ///
    /// The default value is different for each ValueType:
    /// - Unsigned: 1
    /// - Integer: 1
    /// - String: "1"
    /// - Float: 1.0
    /// - Boolean: true
    /// - Unknown: will create a panic
    fn generate_data_block(value_type: ValueType, min_ts: Timestamp, size: usize) -> DataBlock {
        match value_type {
            ValueType::Unsigned => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(1000);
                let mut val_vec: Vec<u64> = Vec::with_capacity(1000);
                for ts in min_ts..min_ts + size as Timestamp {
                    ts_vec.push(ts);
                    val_vec.push(1_u64);
                }
                DataBlock::U64 { ts: ts_vec, val: val_vec }
            },
            ValueType::Integer => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(1000);
                let mut val_vec: Vec<i64> = Vec::with_capacity(1000);
                for ts in min_ts..min_ts + size as Timestamp {
                    ts_vec.push(ts);
                    val_vec.push(1_i64);
                }
                DataBlock::I64 { ts: ts_vec, val: val_vec }
            },
            ValueType::String => {
                let word = b"1".to_vec();
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<Vec<u8>> = Vec::with_capacity(10000);
                for ts in min_ts..min_ts + size as Timestamp {
                    ts_vec.push(ts);
                    let idx = rand::random::<usize>() % 20;
                    val_vec.push(word.clone());
                }
                DataBlock::Str { ts: ts_vec, val: val_vec }
            },
            ValueType::Float => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<f64> = Vec::with_capacity(10000);
                for ts in min_ts..min_ts + size as Timestamp {
                    ts_vec.push(ts);
                    val_vec.push(1.0);
                }
                DataBlock::F64 { ts: ts_vec, val: val_vec }
            },
            ValueType::Boolean => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<bool> = Vec::with_capacity(10000);
                for ts in min_ts..min_ts + size as Timestamp {
                    ts_vec.push(ts);
                    val_vec.push(true);
                }
                DataBlock::Bool { ts: ts_vec, val: val_vec }
            },
            ValueType::Unknown => {
                panic!("value type is Unknown")
            },
        }
    }

    #[test]
    fn test_compaction_3() {
        #[rustfmt::skip]
        let data_desc = [
            // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, DataBlock_Size) ] )]
            (1_u64, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1_u64, 1_i64, 1000_usize),
                (ValueType::Unsigned, 1, 1001, 1000),
                (ValueType::Unsigned, 1, 2001, 500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000),
                (ValueType::Integer, 2, 1001, 500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000),
                (ValueType::Boolean, 3, 1001, 500),
            ]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 1000),
                (ValueType::Unsigned, 1, 3001, 1000),
                (ValueType::Unsigned, 1, 4001, 500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 1000),
                (ValueType::Integer, 2, 2001, 1000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 1000),
                (ValueType::Boolean, 3, 2001, 500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000),
                (ValueType::Float, 4, 1001, 500),
            ]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 1000),
                (ValueType::Unsigned, 1, 5001, 1000),
                (ValueType::Unsigned, 1, 6001, 500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 1000),
                (ValueType::Integer, 2, 4001, 1000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 1000),
                (ValueType::Boolean, 3, 3001, 500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 1000),
                (ValueType::Float, 4, 2001, 500),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, 1, 1000),
                    generate_data_block(ValueType::Unsigned, 1001, 1000),
                    generate_data_block(ValueType::Unsigned, 2001, 1000),
                    generate_data_block(ValueType::Unsigned, 3001, 1000),
                    generate_data_block(ValueType::Unsigned, 4001, 1000),
                    generate_data_block(ValueType::Unsigned, 5001, 1000),
                    generate_data_block(ValueType::Unsigned, 6001, 500),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, 1, 1000),
                    generate_data_block(ValueType::Integer, 1001, 1000),
                    generate_data_block(ValueType::Integer, 2001, 1000),
                    generate_data_block(ValueType::Integer, 3001, 1000),
                    generate_data_block(ValueType::Integer, 4001, 1000),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, 1, 1000),
                    generate_data_block(ValueType::Boolean, 1001, 1000),
                    generate_data_block(ValueType::Boolean, 2001, 1000),
                    generate_data_block(ValueType::Boolean, 3001, 500),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, 1, 1000),
                    generate_data_block(ValueType::Float, 1001, 1000),
                    generate_data_block(ValueType::Float, 2001, 500),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/3";
        let tsf_opt = get_tsf_opt(&dir);
        let dir = tsf_opt.tsm_dir(0);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut column_files = Vec::new();
        for (tsm_sequence, args) in data_desc.iter() {
            let mut tsm_writer = tsm::new_tsm_writer(&dir, *tsm_sequence, false, 0).unwrap();
            for arg in args.iter() {
                tsm_writer.write_block(arg.1, &generate_data_block(arg.0, arg.2, arg.3)).unwrap();
            }
            tsm_writer.write_index().unwrap();
            tsm_writer.flush().unwrap();
            column_files.push(Arc::new(ColumnFile::new(*tsm_sequence,
                                                       TimeRange::new(tsm_writer.min_ts(),
                                                                      tsm_writer.max_ts()),
                                                       tsm_writer.size(),
                                                       false)));
        }

        let next_file_id = 4_u64;

        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(tsf_opt.clone(), next_file_id, column_files);

        run_compaction_job(compact_req, kernel.clone()).unwrap();

        check_column_file(dir.join("_000004.tsm"), expected_data);
    }
}
