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
use trace::{debug, error, info, trace};

use crate::file_system::file_manager::{self, get_file_manager};
use crate::{
    compaction::CompactReq,
    context::GlobalContext,
    error::{self, Result},
    file_utils,
    kv_option::Options,
    memcache::DataType,
    summary::{CompactMeta, VersionEdit},
    tseries_family::{ColumnFile, TimeRange},
    tsm::{
        self, BlockMeta, BlockMetaIterator, ColumnReader, DataBlock, Index, IndexIterator,
        IndexMeta, IndexReader, TsmReader, TsmWriter,
    },
    Error, LevelId,
};

/// Temporary compacting data block meta
struct CompactingBlockMeta {
    readers_idx: usize,
    has_tombstone: bool,
    block_meta: BlockMeta,
}

impl PartialEq for CompactingBlockMeta {
    fn eq(&self, other: &Self) -> bool {
        self.readers_idx == other.readers_idx && self.block_meta == other.block_meta
    }
}

impl Eq for CompactingBlockMeta {}

impl PartialOrd for CompactingBlockMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.block_meta.cmp(&other.block_meta))
    }
}

impl Ord for CompactingBlockMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.block_meta.cmp(&other.block_meta)
    }
}

impl CompactingBlockMeta {
    pub fn new(readers_idx: usize, has_tombstone: bool, block_meta: BlockMeta) -> Self {
        Self {
            readers_idx,
            has_tombstone,
            block_meta,
        }
    }
}

/// Temporary compacting data block.
/// - priority: When merging two (timestamp, value) pair with the same
/// timestamp from two data blocks, pair from data block with lower
/// priority will be discarded.
enum CompactingBlock {
    DataBlock {
        priority: usize,
        field_id: FieldId,
        data_block: DataBlock,
    },
    Raw {
        priority: usize,
        meta: BlockMeta,
        raw: Vec<u8>,
    },
}

impl CompactingBlock {
    /// Sort the given `CompactingBlock`s by priority, transform all of them
    /// into CompactingBlock::DataBlock (for CompactingBlock::Raw)
    fn rebuild_data_blocks(mut source: Vec<Self>) -> Result<Vec<DataBlock>> {
        source.sort_by_key(|k| match k {
            CompactingBlock::DataBlock { priority, .. } => *priority,
            CompactingBlock::Raw { priority, .. } => *priority,
        });

        let mut res: Vec<DataBlock> = Vec::with_capacity(source.len());
        for cb in source.into_iter() {
            match cb {
                CompactingBlock::DataBlock { data_block, .. } => {
                    res.push(data_block);
                }
                CompactingBlock::Raw { meta, raw, .. } => {
                    let data_block = tsm::decode_data_block(
                        &raw,
                        meta.field_type(),
                        meta.val_off() - meta.offset(),
                    )
                    .context(error::ReadTsmSnafu)?;
                    res.push(data_block);
                }
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
        Self {
            tsm_readers: Default::default(),
            tsm_index_iters: Default::default(),
            tmp_tsm_blks: Default::default(),
            tmp_tsm_blk_tsm_reader_idx: Default::default(),
            finished_readers: Default::default(),
            finished_reader_cnt: Default::default(),
            curr_fid: Default::default(),
            last_fid: Default::default(),
            merged_blocks: Default::default(),
            max_datablock_values: Default::default(),
        }
    }
}

impl CompactIterator {
    /// Update tmp_tsm_blks and tmp_tsm_blk_tsm_reader_idx for field id in next iteration.
    fn next_field_id(&mut self) {
        self.tmp_tsm_blks = Vec::with_capacity(self.tsm_index_iters.len());
        self.tmp_tsm_blk_tsm_reader_idx = Vec::with_capacity(self.tsm_index_iters.len());
        for (next_tsm_file_idx, (i, idx)) in self.tsm_index_iters.iter_mut().enumerate().enumerate()
        {
            if self.finished_readers[i] {
                trace!("file no.{} has been finished, continue.", i);
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
                self.tmp_tsm_blk_tsm_reader_idx.push(next_tsm_file_idx);
                trace!("merging idx_meta: field_id: {}, field_type: {:?}, block_count: {}, time_range: {:?}",
                      idx_meta.field_id(),
                      idx_meta.field_type(),
                      idx_meta.block_count(),
                      idx_meta.time_range());
            } else {
                // This tsm-file has been finished
                trace!("file no.{} is finished.", i);
                self.finished_readers[i] = true;
                self.finished_reader_cnt += 1;
            }

            // To next field
            idx.next();
        }
    }

    /// Collect merging `DataBlock`s.
    async fn next_merging_blocks(&mut self) -> Result<()> {
        if self.tmp_tsm_blks.is_empty() {
            return Ok(());
        }
        let mut sorted_blk_metas: BinaryHeap<CompactingBlockMeta> =
            BinaryHeap::with_capacity(self.tmp_tsm_blks.len());
        let field_id = self.curr_fid.expect("method next_field_id has been called");
        // Get all block_meta, and check if it's tsm file has a related tombstone file.
        for (i, blk_iter) in self.tmp_tsm_blks.iter_mut().enumerate() {
            for blk_meta in blk_iter.by_ref() {
                let tsm_has_tombstone =
                    self.tsm_readers[self.tmp_tsm_blk_tsm_reader_idx[i]].has_tombstone();
                sorted_blk_metas.push(CompactingBlockMeta::new(
                    self.tmp_tsm_blk_tsm_reader_idx[i],
                    tsm_has_tombstone,
                    blk_meta,
                ));
            }
        }

        // If BlockMeta::count is less than max_datablock_values, we need to merge it with the
        // next block
        let mut merging_blks: Vec<CompactingBlock> = Vec::new();
        // let mut merging_blk: Option<DataBlock> = None;
        let mut merged_blk_time_range = (Timestamp::MAX, Timestamp::MIN);
        // If BlockMeta::count reaches max_datablock_values, we don't decode the block.
        let mut buf = vec![0_u8; 1024];
        let mut is_first = true;
        while let Some(cbm) = sorted_blk_metas.pop() {
            // 1. Store DataBlocks in merging_blocks, merged_blk_time_range set by the first
            //      BlockMeta
            // 2. For each BlockMeta:
            //   2.1. If it's time_range overlaps with merged_blk_time_range, read DataBlock and
            //          push to merging_blocks, and update merged_blk_time_range
            //   2.2. Else:
            //     2.2.1. If merging_blks's length is 1 and it's a Raw, put to self::merged_blocks.
            //     2.2.2. Else merge merging_blks into Vec<DataBlock>, push to back of
            //              self::merged_blocks, and clean merging_blks.
            //            The last one of the Vec<DataBlock> is special, if it's length is less than
            //              self::max_datablock_values, do not push it.
            //     2.2.3. If it's length reaches self::max_datablock_values, and there is no
            //              tombstones, put Raw to merging_blks, otherwise put DataBlock
            //              (used in 2.2.1).
            // 3. Read DataBLock for the remaining BlockMeta, push to self::merged_blks.

            // Exists merging DataBlock in past iteration
            if is_first {
                is_first = false;
                merged_blk_time_range = (cbm.block_meta.min_ts(), cbm.block_meta.max_ts());
                if cbm.block_meta.size() as usize > buf.len() {
                    buf.resize(cbm.block_meta.size() as usize, 0);
                }
                if cbm.has_tombstone {
                    let data_block = self.tsm_readers[cbm.readers_idx]
                        .get_data_block(&cbm.block_meta)
                        .await
                        .context(error::ReadTsmSnafu)?;
                    merging_blks.push(CompactingBlock::DataBlock {
                        priority: cbm.readers_idx + 1,
                        field_id,
                        data_block,
                    });
                } else {
                    let size = self.tsm_readers[cbm.readers_idx]
                        .get_raw_data(&cbm.block_meta, &mut buf)
                        .await
                        .context(error::ReadTsmSnafu)?;
                    merging_blks.push(CompactingBlock::Raw {
                        priority: cbm.readers_idx + 1,
                        meta: cbm.block_meta,
                        raw: buf[..size].to_vec(),
                    });
                }
            } else if overlaps_tuples(
                merged_blk_time_range,
                (cbm.block_meta.min_ts(), cbm.block_meta.max_ts()),
            ) {
                // 2.1
                let data_block = self.tsm_readers[cbm.readers_idx]
                    .get_data_block(&cbm.block_meta)
                    .await
                    .context(error::ReadTsmSnafu)?;
                merging_blks.push(CompactingBlock::DataBlock {
                    priority: cbm.readers_idx + 1,
                    field_id,
                    data_block,
                });

                merged_blk_time_range.0 = merged_blk_time_range.0.min(cbm.block_meta.min_ts());
                merged_blk_time_range.1 = merged_blk_time_range.0.max(cbm.block_meta.max_ts());
            } else {
                // 2.2
                if !merging_blks.is_empty() {
                    if merging_blks.len() == 1 {
                        // 2.2.1
                        if let Some(CompactingBlock::Raw { meta, raw, .. }) = merging_blks.first() {
                            if meta.count() == self.max_datablock_values {
                                self.merged_blocks.push_back(merging_blks.remove(0));
                            }
                        }
                    } else {
                        // 2.2.2
                        let merging_data_blks = CompactingBlock::rebuild_data_blocks(merging_blks)?;
                        merging_blks = Vec::new();
                        let merged_data_blks =
                            DataBlock::merge_blocks(merging_data_blks, self.max_datablock_values);

                        for (i, data_block) in merged_data_blks.into_iter().enumerate() {
                            if data_block.len() < self.max_datablock_values as usize {
                                merging_blks.push(CompactingBlock::DataBlock {
                                    priority: 0,
                                    field_id,
                                    data_block,
                                });
                                break;
                            }
                            self.merged_blocks.push_back(CompactingBlock::DataBlock {
                                priority: 0,
                                field_id,
                                data_block,
                            });
                        }
                    }

                    // This DataBlock doesn't need to merge
                    if cbm.block_meta.count() == self.max_datablock_values {
                        // 2.2.3
                        if cbm.block_meta.size() as usize > buf.len() {
                            buf.resize(cbm.block_meta.size() as usize, 0);
                        }
                        merged_blk_time_range.0 =
                            merged_blk_time_range.0.min(cbm.block_meta.min_ts());
                        merged_blk_time_range.1 =
                            merged_blk_time_range.0.max(cbm.block_meta.max_ts());
                        if cbm.has_tombstone {
                            let data_block = self.tsm_readers[cbm.readers_idx]
                                .get_data_block(&cbm.block_meta)
                                .await
                                .context(error::ReadTsmSnafu)?;
                            merging_blks.push(CompactingBlock::DataBlock {
                                priority: cbm.readers_idx + 1,
                                field_id,
                                data_block,
                            });
                        } else {
                            let size = self.tsm_readers[cbm.readers_idx]
                                .get_raw_data(&cbm.block_meta, &mut buf)
                                .await
                                .context(error::ReadTsmSnafu)?;
                            merging_blks.push(CompactingBlock::Raw {
                                priority: cbm.readers_idx + 1,
                                meta: cbm.block_meta,
                                raw: buf[..size].to_vec(),
                            });
                        }
                    } else {
                        // cbm.block_meta.count is less than max_datablock_values
                        let data_block = self.tsm_readers[cbm.readers_idx]
                            .get_data_block(&cbm.block_meta)
                            .await
                            .context(error::ReadTsmSnafu)?;
                        merging_blks.push(CompactingBlock::DataBlock {
                            priority: cbm.readers_idx + 1,
                            field_id,
                            data_block,
                        });
                    }
                }
            }
        }

        if !merging_blks.is_empty() {
            let merging_data_blks = CompactingBlock::rebuild_data_blocks(merging_blks)?;
            let merged_data_blks =
                DataBlock::merge_blocks(merging_data_blks, self.max_datablock_values);

            for (i, data_block) in merged_data_blks.into_iter().enumerate() {
                self.merged_blocks.push_back(CompactingBlock::DataBlock {
                    priority: 0,
                    field_id,
                    data_block,
                });
            }
        }

        Ok(())
    }
}

impl CompactIterator {
    pub async fn next(&mut self) -> Option<Result<CompactingBlock>> {
        if let Some(blk) = self.merged_blocks.pop_front() {
            return Some(Ok(blk));
        }
        loop {
            trace!("------------------------------");

            // For each tsm-file, get next index reader for current iteration field id
            self.next_field_id();

            trace!(
                "selected blocks count: {} in iteration",
                self.tmp_tsm_blks.len()
            );
            if self.tmp_tsm_blks.is_empty() {
                trace!("iteration field_id {:?} is finished", self.curr_fid);
                self.curr_fid = None;
                break;
            }

            // Get all of block_metas of this field id, and merge these blocks
            if let Err(e) = self.next_merging_blocks().await {
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

pub async fn run_compaction_job(
    request: CompactReq,
    kernel: Arc<GlobalContext>,
) -> Result<Option<VersionEdit>> {
    info!(
        "Compaction: Running compaction job on ts_family: {} and files: [ {} ]",
        request.ts_family_id,
        request
            .files
            .iter()
            .map(|f| {
                format!(
                    "{{ Level-{}, file_id: {}, time_range: {}-{} }}",
                    f.level(),
                    f.file_id(),
                    f.time_range().min_ts,
                    f.time_range().max_ts
                )
            })
            .collect::<Vec<String>>()
            .join(", ")
    );

    let version = request.version;

    // Buffers all tsm-files and it's indexes for this compaction
    let max_data_block_size = 1000; // TODO this const value is in module tsm
    let tsf_id = request.ts_family_id;
    let storage_opt = request.storage_opt;
    let mut tsm_files: Vec<PathBuf> = Vec::new();
    let mut tsm_readers = Vec::new();
    let mut tsm_index_iters = Vec::new();
    for col_file in request.files.iter() {
        let tsm_file = col_file.file_path();
        let tsm_reader = TsmReader::open(&tsm_file).await?;
        tsm_files.push(tsm_file);
        let idx_iter = tsm_reader.index_iterator().peekable();
        tsm_readers.push(tsm_reader);
        tsm_index_iters.push(idx_iter);
    }

    if tsm_index_iters.is_empty() {
        // Nothing to compact
        return Ok(None);
    }

    let tsm_readers_cnt = tsm_readers.len();
    let mut iter = CompactIterator {
        tsm_readers,
        tsm_index_iters,
        finished_readers: vec![false; tsm_readers_cnt],
        max_datablock_values: max_data_block_size,
        ..Default::default()
    };
    let tsm_dir = storage_opt.tsm_dir(&request.database, tsf_id);
    let mut tsm_writer = tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0).await?;
    info!("Compaction: File {} been created.", tsm_writer.sequence());
    let mut version_edit = VersionEdit::new();
    version_edit.tsf_id = tsf_id;

    loop {
        let block = iter.next().await;
        match block {
            None => break,
            Some(next) => {
                let blk = next?;
                trace!("===============================");
                let write_ret = match blk {
                    CompactingBlock::DataBlock {
                        field_id: fid,
                        data_block: b,
                        ..
                    } => {
                        // TODO: let enc = b.encodings();
                        tsm_writer.write_block(fid, &b).await
                    }
                    CompactingBlock::Raw { meta, raw, .. } => {
                        tsm_writer.write_raw(&meta, &raw).await
                    }
                };
                if let Err(e) = write_ret {
                    match e {
                        tsm::WriteTsmError::IO { source } => {
                            // TODO handle this
                            error!("IO error when write tsm");
                        }
                        tsm::WriteTsmError::Encode { source } => {
                            // TODO handle this
                            error!("Encoding error when write tsm");
                        }
                        tsm::WriteTsmError::MaxFileSizeExceed { source } => {
                            tsm_writer
                                .write_index()
                                .await
                                .context(error::WriteTsmSnafu)?;
                            tsm_writer.finish().await.context(error::WriteTsmSnafu)?;
                            info!(
                                "Compaction: File: {} write finished (level: {}, {} B).",
                                tsm_writer.sequence(),
                                request.out_level,
                                tsm_writer.size()
                            );
                            let cm = new_compact_meta(&tsm_writer, request.out_level);
                            version_edit.add_file(cm, version.max_level_ts);
                            tsm_writer =
                                tsm::new_tsm_writer(&tsm_dir, kernel.file_id_next(), false, 0)
                                    .await?;
                            info!("Compaction: File {} been created.", tsm_writer.sequence());
                        }
                        tsm::WriteTsmError::Finished { path } => {
                            error!("Tsm writer finished: {}", path.display());
                        }
                    }
                }
            }
        }
    }

    tsm_writer
        .write_index()
        .await
        .context(error::WriteTsmSnafu)?;
    tsm_writer.finish().await.context(error::WriteTsmSnafu)?;
    info!(
        "Compaction: File: {} write finished (level: {}, {} B).",
        tsm_writer.sequence(),
        request.out_level,
        tsm_writer.size()
    );
    let cm = new_compact_meta(&tsm_writer, request.out_level);
    version_edit.add_file(cm, version.max_level_ts);
    for file in request.files {
        version_edit.del_file(file.level(), file.file_id(), file.is_delta());
    }

    info!(
        "Compaction: Compact finished, version edits: {:?}",
        version_edit
    );

    Ok(Some(version_edit))
}

fn new_compact_meta(tsm_writer: &TsmWriter, level: LevelId) -> CompactMeta {
    CompactMeta {
        file_id: tsm_writer.sequence(),
        file_size: tsm_writer.size(),
        level,
        min_ts: tsm_writer.min_ts(),
        max_ts: tsm_writer.max_ts(),
        high_seq: 0,
        low_seq: 0,
        is_delta: false,
        ..Default::default()
    }
}

#[cfg(test)]
mod test {
    use core::panic;
    use minivec::MiniVec;
    use std::{
        collections::HashMap,
        default,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, AtomicU32, AtomicU64},
            Arc,
        },
    };

    use models::{FieldId, Timestamp, ValueType};
    use utils::BloomFilter;

    use crate::file_system::file_manager;
    use crate::{
        compaction::{run_compaction_job, CompactReq},
        context::GlobalContext,
        file_utils,
        kv_option::Options,
        summary::VersionEdit,
        tseries_family::{ColumnFile, LevelInfo, TimeRange, Version},
        tsm::{self, codec::DataBlockEncoding, DataBlock, Tombstone, TsmReader, TsmTombstone},
        TseriesFamilyId,
    };

    async fn write_data_blocks_to_column_file(
        dir: impl AsRef<Path>,
        data: Vec<HashMap<FieldId, Vec<DataBlock>>>,
        tsf_id: TseriesFamilyId,
        tsf_opt: Arc<Options>,
    ) -> (u64, Vec<Arc<ColumnFile>>) {
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut cfs = Vec::new();
        let mut file_seq = 0;
        for (i, d) in data.iter().enumerate() {
            file_seq = i as u64 + 1;
            let mut writer = tsm::new_tsm_writer(&dir, file_seq, false, 0).await.unwrap();
            for (fid, data_blks) in d.iter() {
                for blk in data_blks.iter() {
                    writer.write_block(*fid, blk).await.unwrap();
                }
            }
            writer.write_index().await.unwrap();
            writer.finish().await.unwrap();
            cfs.push(Arc::new(ColumnFile::new(
                file_seq,
                2,
                TimeRange::new(writer.min_ts(), writer.max_ts()),
                writer.size(),
                false,
                writer.path(),
            )));
        }
        (file_seq + 1, cfs)
    }

    async fn read_data_blocks_from_column_file(
        path: impl AsRef<Path>,
    ) -> HashMap<FieldId, Vec<DataBlock>> {
        let tsm_reader = TsmReader::open(path).await.unwrap();
        let mut data: HashMap<FieldId, Vec<DataBlock>> = HashMap::new();
        for idx in tsm_reader.index_iterator() {
            let field_id = idx.field_id();
            for blk_meta in idx.block_iterator() {
                let blk = tsm_reader.get_data_block(&blk_meta).await.unwrap();
                data.entry(field_id).or_insert(vec![]).push(blk);
            }
        }
        data
    }

    fn get_result_file_path(dir: impl AsRef<Path>, version_edit: VersionEdit) -> PathBuf {
        if version_edit.has_file_id && !version_edit.add_files.is_empty() {
            let file_id = version_edit.add_files.first().unwrap().file_id;
            return file_utils::make_tsm_file_name(dir, file_id);
        }

        panic!("VersionEdit doesn't contain any add_files.");
    }

    /// Compare DataBlocks in path with the expected_Data using assert_eq.
    async fn check_column_file(
        dir: impl AsRef<Path>,
        version_edit: VersionEdit,
        expected_data: HashMap<FieldId, Vec<DataBlock>>,
    ) {
        let path = get_result_file_path(dir, version_edit);
        let data = read_data_blocks_from_column_file(path).await;
        let mut data_field_ids = data.keys().copied().collect::<Vec<_>>();
        data_field_ids.sort_unstable();
        let mut expected_data_field_ids = expected_data.keys().copied().collect::<Vec<_>>();
        expected_data_field_ids.sort_unstable();
        assert_eq!(data_field_ids, expected_data_field_ids);

        for (k, v) in expected_data.iter() {
            let data_blks = data.get(k).unwrap();
            if v.len() != data_blks.len() {
                panic!("v.len() != data_blks.len()");
            }
            for (v_idx, v_blk) in v.iter().enumerate() {
                assert_eq!(data_blks.get(v_idx).unwrap(), v_blk);
            }
        }
    }

    fn create_options(base_dir: String) -> Arc<Options> {
        let dir = "../config/config.toml";
        let mut config = config::get_config(dir);
        config.storage.path = base_dir;
        let opt = Options::from(&config);
        Arc::new(opt)
    }

    fn prepare_compact_req_and_kernel(
        database: String,
        opt: Arc<Options>,
        next_file_id: u64,
        files: Vec<Arc<ColumnFile>>,
    ) -> (CompactReq, Arc<GlobalContext>) {
        let version = Arc::new(Version::new(
            1,
            "version_1".to_string(),
            opt.storage.clone(),
            1,
            LevelInfo::init_levels(database.clone(), opt.storage.clone()),
            1000,
        ));
        let compact_req = CompactReq {
            ts_family_id: 1,
            database,
            storage_opt: opt.storage.clone(),
            files,
            version,
            out_level: 2,
        };
        let kernel = Arc::new(GlobalContext::new());
        kernel.set_file_id(next_file_id);

        (compact_req, kernel)
    }

    #[tokio::test]
    async fn test_compaction_fast() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction";
        let database = "dba".to_string();
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) =
            write_data_blocks_to_column_file(&dir, data, 1, opt.clone()).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let version_edit = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_1() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction/1";
        let database = "dba".to_string();
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) =
            write_data_blocks_to_column_file(&dir, data, 1, opt.clone()).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let version_edit = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_2() {
        #[rustfmt::skip]
        let data = vec![
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 2, 3, 5], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4], val: vec![1, 2, 3, 5], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![1, 2, 3], val: vec![1, 2, 3], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![4, 5, 6], val: vec![4, 5, 6], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![4, 5, 6, 7], val: vec![4, 5, 6, 8], enc: DataBlockEncoding::default() }]),
            ]),
            HashMap::from([
                (1, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (2, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default() }]),
                (3, vec![DataBlock::I64 { ts: vec![7, 8, 9], val: vec![7, 8, 9], enc: DataBlockEncoding::default()}]),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data = HashMap::from([
            (1, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (2, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
            (3, vec![DataBlock::I64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], val: vec![1, 2, 3, 4, 5, 6, 7, 8, 9], enc: DataBlockEncoding::default() }]),
        ]);

        let dir = "/tmp/test/compaction/2";
        let database = "dba".to_string();
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);

        let (next_file_id, files) =
            write_data_blocks_to_column_file(&dir, data, 1, opt.clone()).await;
        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, files);
        let version_edit = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();
        check_column_file(dir, version_edit, expected_data).await;
    }

    /// Returns a generated `DataBlock` with default value and specified size, `DataBlock::ts`
    /// is all the time-ranges in data_descriptors.
    ///
    /// The default value is different for each ValueType:
    /// - Unsigned: 1
    /// - Integer: 1
    /// - String: "1"
    /// - Float: 1.0
    /// - Boolean: true
    /// - Unknown: will create a panic
    fn generate_data_block(value_type: ValueType, data_descriptors: Vec<(i64, i64)>) -> DataBlock {
        match value_type {
            ValueType::Unsigned => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(1000);
                let mut val_vec: Vec<u64> = Vec::with_capacity(1000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1_u64);
                    }
                }
                DataBlock::U64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Integer => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(1000);
                let mut val_vec: Vec<i64> = Vec::with_capacity(1000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1_i64);
                    }
                }
                DataBlock::I64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::String => {
                let word = MiniVec::from(&b"1"[..]);
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<MiniVec<u8>> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(word.clone());
                    }
                }
                DataBlock::Str {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Float => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<f64> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(1.0);
                    }
                }
                DataBlock::F64 {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Boolean => {
                let mut ts_vec: Vec<Timestamp> = Vec::with_capacity(10000);
                let mut val_vec: Vec<bool> = Vec::with_capacity(10000);
                for (min_ts, max_ts) in data_descriptors {
                    for ts in min_ts..max_ts + 1 {
                        ts_vec.push(ts);
                        val_vec.push(true);
                    }
                }
                DataBlock::Bool {
                    ts: ts_vec,
                    val: val_vec,
                    enc: DataBlockEncoding::default(),
                }
            }
            ValueType::Unknown => {
                panic!("value type is Unknown")
            }
        }
    }

    #[tokio::test]
    async fn test_compaction_3() {
        #[rustfmt::skip]
        let data_desc = [
            // [( tsm_sequence, vec![ (ValueType, FieldId, Timestamp_Begin, Timestamp_end) ] )]
            (1_u64, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1_u64, 1_i64, 1000_i64),
                (ValueType::Unsigned, 1, 1001, 2000),
                (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000),
                (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000),
                (ValueType::Boolean, 3, 1001, 1500),
            ]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000),
                (ValueType::Unsigned, 1, 3001, 4000),
                (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000),
                (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000),
                (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000),
                (ValueType::Float, 4, 1001, 1500),
            ]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 5000),
                (ValueType::Unsigned, 1, 5001, 6000),
                (ValueType::Unsigned, 1, 6001, 6500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 4000),
                (ValueType::Integer, 2, 4001, 5000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 3000),
                (ValueType::Boolean, 3, 3001, 3500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 2000),
                (ValueType::Float, 4, 2001, 2500),
            ]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(1, 1000)]),
                    generate_data_block(ValueType::Unsigned, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Unsigned, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Unsigned, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Unsigned, vec![(4001, 5000)]),
                    generate_data_block(ValueType::Unsigned, vec![(5001, 6000)]),
                    generate_data_block(ValueType::Unsigned, vec![(6001, 6500)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                    generate_data_block(ValueType::Integer, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Integer, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Integer, vec![(3001, 4000)]),
                    generate_data_block(ValueType::Integer, vec![(4001, 5000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                    generate_data_block(ValueType::Boolean, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Boolean, vec![(3001, 3500)]),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, vec![(1, 1000)]),
                    generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Float, vec![(2001, 2500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/3";
        let database = "dba".to_string();
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut column_files = Vec::new();
        for (tsm_sequence, args) in data_desc.iter() {
            let mut tsm_writer = tsm::new_tsm_writer(&dir, *tsm_sequence, false, 0)
                .await
                .unwrap();
            for arg in args.iter() {
                tsm_writer
                    .write_block(arg.1, &generate_data_block(arg.0, vec![(arg.2, arg.3)]))
                    .await
                    .unwrap();
            }
            tsm_writer.write_index().await.unwrap();
            tsm_writer.finish().await.unwrap();
            column_files.push(Arc::new(ColumnFile::new(
                *tsm_sequence,
                2,
                TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
                tsm_writer.size(),
                false,
                tsm_writer.path(),
            )));
        }

        let next_file_id = 4_u64;

        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, column_files);

        let version_edit = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data).await;
    }

    #[tokio::test]
    async fn test_compaction_4() {
        #[rustfmt::skip]
        let data_desc = [
            // [( tsm_sequence, vec![(ValueType, FieldId, Timestamp_Begin, Timestamp_end)], vec![Option<(FieldId, MinTimestamp, MaxTimestamp)>] )]
            (1_u64, vec![
                // 1, 1~2500
                (ValueType::Unsigned, 1_u64, 1_i64, 1000_i64),
                (ValueType::Unsigned, 1, 1001, 2000),
                (ValueType::Unsigned, 1, 2001, 2500),
                // 2, 1~1500
                (ValueType::Integer, 2, 1, 1000),
                (ValueType::Integer, 2, 1001, 1500),
                // 3, 1~1500
                (ValueType::Boolean, 3, 1, 1000),
                (ValueType::Boolean, 3, 1001, 1500),
            ], vec![
                Some((1_u64, 1_i64, 2_i64)),
                None, None,
                Some((2, 1001, 1002)),
                None, None,
                None, Some((3, 1499, 1500)), None
            ]),
            (2, vec![
                // 1, 2001~4500
                (ValueType::Unsigned, 1, 2001, 3000),
                (ValueType::Unsigned, 1, 3001, 4000),
                (ValueType::Unsigned, 1, 4001, 4500),
                // 2, 1001~3000
                (ValueType::Integer, 2, 1001, 2000),
                (ValueType::Integer, 2, 2001, 3000),
                // 3, 1001~2500
                (ValueType::Boolean, 3, 1001, 2000),
                (ValueType::Boolean, 3, 2001, 2500),
                // 4, 1~1500
                (ValueType::Float, 4, 1, 1000),
                (ValueType::Float, 4, 1001, 1500),
            ], vec![
                Some((1, 2001, 2100)),
                None, Some((1, 4500, 4501)),
                Some((2, 2501, 2502)),
                None, None,
                None, None, None
            ]),
            (3, vec![
                // 1, 4001~6500
                (ValueType::Unsigned, 1, 4001, 5000),
                (ValueType::Unsigned, 1, 5001, 6000),
                (ValueType::Unsigned, 1, 6001, 6500),
                // 2, 3001~5000
                (ValueType::Integer, 2, 3001, 4000),
                (ValueType::Integer, 2, 4001, 5000),
                // 3, 2001~3500
                (ValueType::Boolean, 3, 2001, 3000),
                (ValueType::Boolean, 3, 3001, 3500),
                // 4. 1001~2500
                (ValueType::Float, 4, 1001, 2000),
                (ValueType::Float, 4, 2001, 2500),
            ], vec![
                Some((1, 4500, 4501)),
                None, None,
                Some((2, 4001, 4002)),
                None, None,
                None, None, None
            ]),
        ];
        #[rustfmt::skip]
        let expected_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from(
            [
                // 1, 1~6500
                (1, vec![
                    generate_data_block(ValueType::Unsigned, vec![(3, 1002)]),
                    generate_data_block(ValueType::Unsigned, vec![(1003, 2002)]),
                    generate_data_block(ValueType::Unsigned, vec![(2003, 3002)]),
                    generate_data_block(ValueType::Unsigned, vec![(3003, 4002)]),
                    generate_data_block(ValueType::Unsigned, vec![(4003, 4499), (4502, 5004)]),
                    generate_data_block(ValueType::Unsigned, vec![(5005, 6004)]),
                    generate_data_block(ValueType::Unsigned, vec![(6005, 6500)]),
                ]),
                // 2, 1~5000
                (2, vec![
                    generate_data_block(ValueType::Integer, vec![(1, 1000)]),
                    generate_data_block(ValueType::Integer, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Integer, vec![(2001, 2500), (2503, 3002)]),
                    generate_data_block(ValueType::Integer, vec![(3003, 4000), (4003, 4004)]),
                    generate_data_block(ValueType::Integer, vec![(4005, 5000)]),
                ]),
                // 3, 1~3500
                (3, vec![
                    generate_data_block(ValueType::Boolean, vec![(1, 1000)]),
                    generate_data_block(ValueType::Boolean, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Boolean, vec![(2001, 3000)]),
                    generate_data_block(ValueType::Boolean, vec![(3001, 3500)]),
                ]),
                // 4, 1~2500
                (4, vec![
                    generate_data_block(ValueType::Float, vec![(1, 1000)]),
                    generate_data_block(ValueType::Float, vec![(1001, 2000)]),
                    generate_data_block(ValueType::Float, vec![(2001, 2500)]),
                ]),
            ]
        );

        let dir = "/tmp/test/compaction/4";
        let database = "dba".to_string();
        let opt = create_options(dir.to_string());
        let dir = opt.storage.tsm_dir(&database, 1);
        if !file_manager::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }

        let mut column_files = Vec::new();
        for (tsm_sequence, tsm_desc, tombstone_desc) in data_desc.iter() {
            let mut tsm_writer = tsm::new_tsm_writer(&dir, *tsm_sequence, false, 0)
                .await
                .unwrap();
            for arg in tsm_desc.iter() {
                tsm_writer
                    .write_block(arg.1, &generate_data_block(arg.0, vec![(arg.2, arg.3)]))
                    .await
                    .unwrap();
            }
            tsm_writer.write_index().await.unwrap();
            tsm_writer.finish().await.unwrap();
            let mut tsm_tombstone = TsmTombstone::open(&dir, *tsm_sequence).await.unwrap();
            for t in tombstone_desc.iter().flatten() {
                tsm_tombstone
                    .add_range(&[t.0][..], &TimeRange::new(t.1, t.2))
                    .await
                    .unwrap();
            }

            tsm_tombstone.flush().await.unwrap();
            column_files.push(Arc::new(ColumnFile::new(
                *tsm_sequence,
                2,
                TimeRange::new(tsm_writer.min_ts(), tsm_writer.max_ts()),
                tsm_writer.size(),
                false,
                tsm_writer.path(),
            )));
        }

        let next_file_id = 4_u64;

        let (compact_req, kernel) =
            prepare_compact_req_and_kernel(database, opt, next_file_id, column_files);

        let version_edit = run_compaction_job(compact_req, kernel)
            .await
            .unwrap()
            .unwrap();

        check_column_file(dir, version_edit, expected_data).await;
    }
}
