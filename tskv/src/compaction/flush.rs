use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    iter::Peekable,
    path::{Path, PathBuf},
    rc::Rc,
    slice,
    sync::Arc,
};

use models::{utils as model_utils, FieldId, SeriesId, Timestamp, ValueType};
use parking_lot::{Mutex, RwLock};
use regex::internal::Input;
use snafu::{NoneError, ResultExt};
use tokio::{
    select,
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot,
        oneshot::Sender,
    },
};
use trace::{debug, error, info, warn};

use crate::{
    compaction::FlushReq,
    context::GlobalContext,
    error::{self, Result},
    kv_option::Options,
    memcache::{ColumnBlock, FieldVal, MemCache, MemEntry, SeriesColumnBlocksIterator},
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::{LevelInfo, Version},
    tsm::{self, DataBlock, TsmWriter},
    version_set::VersionSet,
    TseriesFamilyId,
};

struct FlushingBlock {
    pub field_id: FieldId,
    pub data_block: DataBlock,
    pub is_delta: bool,
}

pub struct FlushTask {
    mem_caches: Vec<Arc<RwLock<MemCache>>>,
    ts_family_id: TseriesFamilyId,
    global_context: Arc<GlobalContext>,
    path_tsm: PathBuf,
    path_delta: PathBuf,
}

impl FlushTask {
    pub fn new(
        mem_caches: Vec<Arc<RwLock<MemCache>>>,
        ts_family_id: TseriesFamilyId,
        global_context: Arc<GlobalContext>,
        path_tsm: PathBuf,
        path_delta: PathBuf,
    ) -> Self {
        Self {
            mem_caches,
            ts_family_id,
            global_context,
            path_tsm,
            path_delta,
        }
    }

    pub async fn run(
        self,
        version: Arc<Version>,
        version_edits: &mut Vec<VersionEdit>,
    ) -> Result<()> {
        info!(
            "Flush: Running flush job on ts_family: {} with {} MemCaches, collecting informations.",
            version.ts_family_id,
            self.mem_caches.len(),
        );
        let (mut high_seq, mut low_seq) = (0, u64::MAX);
        let mut total_memcache_size = 0_u64;

        let mut flushing_mems = Vec::with_capacity(self.mem_caches.len());
        for mem in self.mem_caches.iter() {
            flushing_mems.push(mem.write());
        }
        let mut compactor = FlushCompactIterator::new(flushing_mems.len(), version.max_level_ts);
        for mem in flushing_mems.iter() {
            let seq_no = mem.seq_no();
            high_seq = seq_no.max(high_seq);
            low_seq = seq_no.min(low_seq);
            total_memcache_size += mem.cache_size();

            compactor.append_upstream_iterator(mem.iter_series_column_blocks());
        }

        if total_memcache_size == 0 {
            return Ok(());
        }

        let mut compact_metas = self.write_block_set(&mut compactor, 0)?;
        let mut max_level_ts = version.max_level_ts;
        let mut edit = VersionEdit::new();
        for cm in compact_metas.iter_mut() {
            cm.low_seq = low_seq;
            cm.high_seq = high_seq;
            max_level_ts = max_level_ts.max(cm.max_ts);
        }
        for cm in compact_metas {
            edit.add_file(cm, max_level_ts);
        }
        version_edits.push(edit);

        drop(compactor);
        for mem in flushing_mems.iter_mut() {
            mem.flushed = true;
        }

        Ok(())
    }

    fn write_block_set(
        &self,
        compactor: &mut FlushCompactIterator,
        max_file_size: u64,
    ) -> Result<Vec<CompactMeta>> {
        let mut tsm_writer: Option<TsmWriter> = None;
        let mut delta_writer: Option<TsmWriter> = None;
        while let Some((field_id, tsm_blks, dlt_blks)) = compactor.next() {
            if !tsm_blks.is_empty() && tsm_writer.is_none() {
                let writer = tsm::new_tsm_writer(
                    self.path_tsm.clone(),
                    self.global_context.file_id_next(),
                    false,
                    max_file_size,
                )?;
                info!("Flush: File {}(tsm) been created.", writer.sequence());
                tsm_writer = Some(writer);
            }
            if !dlt_blks.is_empty() && delta_writer.is_none() {
                let writer = tsm::new_tsm_writer(
                    self.path_delta.clone(),
                    self.global_context.file_id_next(),
                    true,
                    max_file_size,
                )?;
                info!("Flush: File {}(delta) been created.", writer.sequence());
                delta_writer = Some(writer);
            }
            for data_block in tsm_blks {
                if let Some(writer) = tsm_writer.as_mut() {
                    writer
                        .write_block(field_id, &data_block)
                        .context(error::WriteTsmSnafu)?;
                }
            }
            for data_block in dlt_blks {
                if let Some(writer) = delta_writer.as_mut() {
                    writer
                        .write_block(field_id, &data_block)
                        .context(error::WriteTsmSnafu)?;
                }
            }
        }
        if let Some(writer) = tsm_writer.as_mut() {
            writer.write_index().context(error::WriteTsmSnafu)?;
            writer.flush().context(error::WriteTsmSnafu)?;
            info!(
                "Flush: File: {} write finished ({} B).",
                writer.sequence(),
                writer.size()
            );
        }
        if let Some(writer) = delta_writer.as_mut() {
            writer.write_index().context(error::WriteTsmSnafu)?;
            writer.flush().context(error::WriteTsmSnafu)?;
            info!(
                "Flush: File: {} write finished ({} B).",
                writer.sequence(),
                writer.size()
            );
        }

        let mut compact_metas = vec![];
        if let Some(writer) = tsm_writer {
            compact_metas.push(CompactMeta::new(
                writer.sequence(),
                writer.size(),
                self.ts_family_id,
                1,
                writer.min_ts(),
                writer.max_ts(),
                false,
            ));
        }
        if let Some(writer) = delta_writer {
            compact_metas.push(CompactMeta::new(
                writer.sequence(),
                writer.size(),
                self.ts_family_id,
                0,
                writer.min_ts(),
                writer.max_ts(),
                true,
            ));
        }

        Ok(compact_metas)
    }
}

pub async fn run_flush_memtable_job(
    reqs: Arc<Mutex<Vec<FlushReq>>>,
    global_context: Arc<GlobalContext>,
    version_set: Arc<RwLock<VersionSet>>,
    summary_task_sender: UnboundedSender<SummaryTask>,
    compact_task_sender: UnboundedSender<TseriesFamilyId>,
) -> Result<()> {
    let mut tsf_caches: HashMap<TseriesFamilyId, Vec<Arc<RwLock<MemCache>>>> = HashMap::new();
    {
        let mut reqs = reqs.lock();
        info!("Flush: Running flush job on {} MemCaches", reqs.len());
        if reqs.len() == 0 {
            return Ok(());
        }
        for req in reqs.iter() {
            for (tf, mem) in &req.mems {
                let mem_vec = tsf_caches.entry(*tf).or_insert(Vec::new());
                mem_vec.push(mem.clone());
            }
        }
        reqs.clear();
    }

    let mut edits: Vec<VersionEdit> = vec![];
    for (tsf_id, caches) in tsf_caches.iter() {
        if let Some(tsf) = version_set.read().get_tsfamily_by_tf_id(*tsf_id) {
            if caches.is_empty() {
                continue;
            }

            // todo: build path by vnode data

            let tsf_rlock = tsf.read();
            let storage_opt = tsf_rlock.storage_opt();
            let database = tsf_rlock.database();
            let path_tsm = storage_opt.tsm_dir(&database, *tsf_id);
            let path_delta = storage_opt.delta_dir(&database, *tsf_id);
            drop(tsf_rlock);

            FlushTask::new(
                caches.clone(),
                *tsf_id,
                global_context.clone(),
                path_tsm,
                path_delta,
            )
            .run(tsf.read().version(), &mut edits)
            .await?;

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

struct FlushCompactIterator<'a> {
    iterators: Vec<Peekable<SeriesColumnBlocksIterator<'a>>>,
    max_level_ts: i64,
    data_block_size: usize,

    iterators_count: usize,
    curr_sid: Option<SeriesId>,
    last_sid: Option<SeriesId>,
    /// Holds currently flushing series data.
    /// It's the same length as the MemCache partions'.
    flushing_schema_col_blocks: Vec<(Rc<Vec<u32>>, Rc<Vec<ColumnBlock>>)>,
    /// Holds currently flushing series' all column_id.
    flushing_schema_col_ids: Vec<u32>,
    /// Maps column_id to index in flushing_schema_col_ids.
    flushing_schema_col_map: HashMap<u32, usize>,
    /// Holds currently flushing column_id.
    flushing_schema_col_id: u32,
    /// Holds the next flushing column_id's index in flushing_schema_col_ids.
    flushing_schema_next_col_index: usize,
}

impl<'a> FlushCompactIterator<'a> {
    pub fn new(capacity: usize, max_level_ts: i64) -> Self {
        Self {
            iterators: Vec::with_capacity(capacity),
            max_level_ts,
            data_block_size: 1000,

            iterators_count: 0,
            curr_sid: None,
            last_sid: None,
            flushing_schema_col_blocks: vec![],
            flushing_schema_col_ids: vec![],
            flushing_schema_col_map: HashMap::new(),
            flushing_schema_col_id: 0,
            flushing_schema_next_col_index: 0,
        }
    }

    pub fn append_upstream_iterator(&mut self, iterator: SeriesColumnBlocksIterator<'a>) {
        self.iterators.push(iterator.peekable());
        self.iterators_count += 1;
    }
}

impl<'a> Iterator for FlushCompactIterator<'a> {
    type Item = (FieldId, Vec<DataBlock>, Vec<DataBlock>);

    fn next(&mut self) -> Option<Self::Item> {
        debug!("====================");
        if self.flushing_schema_col_ids.is_empty() {
            debug!("Selecting next series_id.");
            // 1. Get next Vec<(Vec<ColumnId>, Vec<ColumnBlock>)> for next SeriesId from each MemCache.
            let mut skipped = 0_usize;
            self.flushing_schema_col_blocks.truncate(0);
            loop {
                debug!("---------------------");
                if skipped >= self.iterators_count {
                    self.curr_sid = None;
                    break;
                }
                for iter in self.iterators.iter_mut() {
                    if let Some((sid, schema, blks)) = iter.peek() {
                        if let Some(curr_sid) = self.curr_sid {
                            if *sid != curr_sid {
                                debug!("Skipped series_id: {}", sid);
                                skipped += 1;
                                continue;
                            }
                        } else {
                            debug!("Selected next series_id: '{}'", sid);
                            self.curr_sid = Some(*sid);
                            self.last_sid = Some(*sid);
                        }
                        debug!("Pushed '{}' blocks", blks.len());
                        self.flushing_schema_col_blocks
                            .push((schema.clone(), blks.clone()));
                        iter.next();
                    } else {
                        debug!("An iterator ran over.");
                        skipped += 1;
                        continue;
                    }
                }
            }

            debug!("Selected series_id: '{:?}'.", self.last_sid);

            // 2. If no Vec<(Vec<ColumnId>, Vec<ColumnBlock>)> selected, break.
            if self.flushing_schema_col_blocks.is_empty() {
                debug!("Selected '0' flushing data, return None.");
                return None;
            }

            debug!(
                "Selected '{}' flushing data.",
                self.flushing_schema_col_blocks.len()
            );

            // 3. Merge Vec<(Vec<ColumnId>, Vec<ColumnBlock>)>.
            // 3.1. Merge schemas:
            let mut all_schema_cols: HashSet<u32> = HashSet::new();
            for (schema, _) in self.flushing_schema_col_blocks.iter() {
                if schema.is_empty() {
                    continue;
                }
                for col in schema.iter() {
                    all_schema_cols.insert(*col);
                }
            }
            self.flushing_schema_col_ids = all_schema_cols.into_iter().collect();
            self.flushing_schema_col_ids.sort();
            debug!(
                "Selected schema columns: {:?}",
                self.flushing_schema_col_ids
            );
            for (i, col) in self.flushing_schema_col_ids.iter().enumerate() {
                self.flushing_schema_col_map.insert(*col, i);
            }
            self.flushing_schema_col_id = *self.flushing_schema_col_ids.first().unwrap();
            self.flushing_schema_next_col_index = 1;
        } else {
            debug!("Selecting current series_id: '{:?}'.", self.last_sid);
            self.flushing_schema_col_id =
                self.flushing_schema_col_ids[self.flushing_schema_next_col_index];
            self.flushing_schema_next_col_index += 1;
        }

        debug!(
            "Flushing column_id: '{}' at '{}'",
            self.flushing_schema_col_id,
            self.flushing_schema_next_col_index - 1
        );

        if self.flushing_schema_next_col_index >= self.flushing_schema_col_ids.len() {
            self.flushing_schema_next_col_index = 0;
            self.flushing_schema_col_ids.clear();
            self.flushing_schema_col_map.clear();
        }

        // 3.2. Merge columns:
        // 3.2.1. Fetch ColumnBlockReaders for current column id.
        let mut col_blk_value_iters = Vec::new();
        let mut value_type = ValueType::Unknown;
        for (schema, column_blocks) in self.flushing_schema_col_blocks.iter() {
            if let Ok(col_idx) = schema.binary_search(&self.flushing_schema_col_id) {
                let col = column_blocks.get(col_idx).unwrap();
                if value_type == ValueType::Unknown {
                    value_type = col.value_type()
                }
                col_blk_value_iters.push(col.iter());
            }
        }

        if value_type == ValueType::Unknown {
            value_type = ValueType::Float;
        }

        debug!(
            "Flushing column_type for column_id({}: {})",
            self.flushing_schema_col_id, value_type
        );
        // 3.2.2. Merge ColumnBlocks from ColumnBlockReaders into NullableDataBlock.
        let mut tsm_blocks: Vec<DataBlock> = Vec::new();
        let mut delta_blocks: Vec<DataBlock> = Vec::new();
        let mut tsm_block = DataBlock::new(self.data_block_size, value_type);
        let mut delta_block = DataBlock::new(self.data_block_size, value_type);
        let mut next_row_ts: Timestamp;
        let mut next_row_idx: usize;
        let mut next_row_value: Option<&FieldVal>;
        let mut exists_next_row: bool;
        let iter_count = col_blk_value_iters.len();
        let mut skipped = 0_usize;
        loop {
            debug!("---------------------");
            if skipped >= iter_count {
                debug!(
                    "Flushing finished for column_id: '{}'",
                    self.flushing_schema_col_id
                );
                break;
            }
            next_row_idx = 0_usize;
            next_row_ts = Timestamp::MAX;
            next_row_value = None;
            exists_next_row = false;

            // Get minimum timestamp for a row.
            // iter_1 = [(1, 1), (1, None), (2, 1), (3, 1)]
            // iter_2 = [(1, None), (1, 2), (2, 2), (3, None), (4, 2)]
            // iter_3 = [(1, 3), (2, None), (3, 3)]
            // => iter = [(1, 3), (2, 2), (3, 3), (4, 2)]
            let mut curr_timestamp;
            for (i, iter) in col_blk_value_iters.iter_mut().enumerate() {
                // let mut next_ts = next_ts_vec[i];
                // Get last inserted minimum timestamp and non-none field_value for a column.
                curr_timestamp = None;
                loop {
                    if let Some((ts, field_val)) = iter.peek() {
                        debug!(
                            "Selecting for minumum timestamp and non-none field_value: {}-{:?}",
                            ts, field_val
                        );
                        // Compare current timestamp with last.
                        if let Some(t) = curr_timestamp {
                            if t == **ts {
                                // Same, update and go to next.
                                if field_val.is_some() {
                                    next_row_value = *field_val;
                                }
                                if iter.next() {
                                    continue;
                                } else {
                                    if next_row_ts >= t {
                                        next_row_ts = t;
                                        next_row_idx = i;
                                    }
                                    exists_next_row = true;
                                }
                            } else {
                                // Different, compare and break.
                                if next_row_ts >= t {
                                    next_row_ts = t;
                                    next_row_idx = i;
                                }
                                exists_next_row = true;
                                break;
                            }
                        } else {
                            // Initial, go to next.
                            let t = **ts;
                            curr_timestamp = Some(t);
                            if field_val.is_some() {
                                next_row_value = *field_val;
                            }
                            if iter.next() {
                                continue;
                            } else {
                                if next_row_ts >= t {
                                    next_row_ts = t;
                                    next_row_idx = i;
                                }
                                exists_next_row = true;
                            }
                        }
                    } else {
                        // Final, break.
                        skipped += 1;
                        break;
                    }
                }

                iter.prev();
            }

            if !exists_next_row {
                debug!("Flushing finished by 'next_row_not_exists'.");
                break;
            }

            // Iterator that fetched row value go to next.
            let next_row = col_blk_value_iters.get_mut(next_row_idx).unwrap();
            next_row.next();

            // If timestamp is less than or equal to Version::max_level_ts,
            // value should be put in delta files.
            if next_row_ts > self.max_level_ts {
                debug!(
                    "Append tsm next_row: ({}, {:?})",
                    next_row_ts, next_row_value
                );
                if let Some(v) = next_row_value {
                    tsm_block.insert(&v.data_value(next_row_ts));
                }
                if tsm_block.len() as usize >= self.data_block_size {
                    tsm_blocks.push(tsm_block);
                    tsm_block = DataBlock::new(self.data_block_size, value_type);
                }
            } else {
                debug!(
                    "Append delta next_row: ({}, {:?})",
                    next_row_ts, next_row_value
                );
                if let Some(v) = next_row_value {
                    delta_block.insert(&v.data_value(next_row_ts));
                }
                if delta_block.len() as usize >= self.data_block_size {
                    delta_blocks.push(delta_block);
                    delta_block = DataBlock::new(self.data_block_size, value_type);
                }
            }
        }
        if tsm_block.len() > 0 {
            tsm_blocks.push(tsm_block);
        }
        if delta_block.len() > 0 {
            delta_blocks.push(delta_block);
        }

        let field_id =
            model_utils::unite_id(self.flushing_schema_col_id as u64, self.last_sid.unwrap());

        debug!(
            "Flush for column_id({}) finished, final field_id is '{}', selected '{}' tsm_blocks and '{}' delta_blocks",
            self.flushing_schema_col_id, field_id, tsm_blocks.len(), delta_blocks.len(),
        );

        return Some((field_id, tsm_blocks, delta_blocks));
    }
}

#[cfg(test)]
pub mod test {
    use models::{utils as model_utils, Timestamp};

    use crate::compaction::flush::FlushCompactIterator;
    use crate::memcache::test::stringify_series_cache_blocks;
    use crate::memcache::{DataType, FieldVal};
    use crate::{
        compaction::FlushReq,
        context::GlobalContext,
        kv_option::Options,
        memcache::{test::put_rows_to_cache, MemCache},
        tseries_family::FLUSH_REQ,
        tsm::test::check_data_block,
        version_set::VersionSet,
    };

    #[test]
    fn test_flush() {
        let mut caches = vec![
            MemCache::new(1, 1024, 0),
            MemCache::new(1, 1024, 0),
            MemCache::new(1, 1024, 0),
        ];
        put_rows_to_cache(&mut caches[0], 1, 1, vec![0, 1, 2], (3, 4), false);
        put_rows_to_cache(&mut caches[0], 1, 2, vec![0, 1, 3], (1, 2), false);
        put_rows_to_cache(&mut caches[0], 1, 3, vec![0, 1, 2, 3], (5, 5), true);
        put_rows_to_cache(&mut caches[0], 1, 3, vec![0, 1, 2, 3], (5, 6), false);

        put_rows_to_cache(&mut caches[1], 2, 1, vec![0, 1, 2], (9, 10), false);
        put_rows_to_cache(&mut caches[1], 2, 2, vec![0, 1, 3], (7, 8), false);
        put_rows_to_cache(&mut caches[1], 2, 3, vec![0, 1, 2, 3], (11, 11), true);
        put_rows_to_cache(&mut caches[1], 2, 3, vec![0, 1, 2, 3], (11, 12), false);

        put_rows_to_cache(&mut caches[2], 3, 1, vec![0, 1, 2], (15, 16), false);
        put_rows_to_cache(&mut caches[2], 3, 2, vec![0, 1, 3], (13, 14), false);
        put_rows_to_cache(&mut caches[2], 3, 3, vec![0, 1, 2, 3], (17, 17), true);
        put_rows_to_cache(&mut caches[2], 3, 3, vec![0, 1, 2, 3], (17, 18), false);

        let max_level_ts = 10;

        #[rustfmt::skip]
        let expected_data: Vec<(u32, u64, Vec<DataType>, Vec<DataType>)> = vec![
            // | === SeriesId: 1 === |
            // Ts:    1,    2,    3,    4,    5,    5, 6
            // Col_0: 1,    2,    3,    4,    None, 5, 6
            // Col_1: 1,    2,    3,    4,    None, 5, 6
            // Col_2: None, None, 3,    4,    None, 5, 6
            // Col_3: 1,    2,    None, None, None, 5, 6
            (0, 1, vec![], vec![
                DataType::F64(1, 1.0), DataType::F64(2, 2.0), DataType::F64(3, 3.0), DataType::F64(4, 4.0), DataType::F64(5, 5.0), DataType::F64(6, 6.0),
            ]),
            (1, 1, vec![], vec![
                DataType::F64(1, 1.0), DataType::F64(2, 2.0), DataType::F64(3, 3.0), DataType::F64(4, 4.0), DataType::F64(5, 5.0), DataType::F64(6, 6.0),
            ],),
            (2, 1, vec![], vec![
                DataType::F64(3, 3.0), DataType::F64(4, 4.0), DataType::F64(5, 5.0), DataType::F64(6, 6.0),
            ]),
            (3, 1, vec![], vec![
                DataType::F64(1, 1.0), DataType::F64(2, 2.0), DataType::F64(5, 5.0), DataType::F64(6, 6.0),
            ]),
            // | === SeriesId: 2 === |
            // Ts:    7,    8,    9,    10,   11,   11, 12
            // Col_0: 7,    8,    9,    10,   None, 11, 12
            // Col_1: 7,    8,    9,    10,   None, 11, 12
            // Col_2: None, None, 9,    10,   None, 11, 12
            // Col_3: 7,    8,    None, None, None, 11, 12
            (0, 2, vec![DataType::F64(11, 11.0), DataType::F64(12, 12.0)], vec![
                DataType::F64(7, 7.0), DataType::F64(8, 8.0), DataType::F64(9, 9.0), DataType::F64(10, 10.0),
            ]),
            (1, 2, vec![DataType::F64(11, 11.0), DataType::F64(12, 12.0)], vec![
                DataType::F64(7, 7.0), DataType::F64(8, 8.0), DataType::F64(9, 9.0), DataType::F64(10, 10.0),
            ],),
            (2, 2, vec![DataType::F64(11, 11.0), DataType::F64(12, 12.0)], vec![
                DataType::F64(9, 9.0), DataType::F64(10, 10.0),
            ]),
            (3, 2, vec![DataType::F64(11, 11.0), DataType::F64(12, 12.0)], vec![
                DataType::F64(7, 7.0), DataType::F64(8, 8.0),
            ]),
            // | === SeriesId: 3 === |
            // Ts:    13,   14,   15,   16,   16,   17, 18
            // Col_0: 13,   14,   15,   16,   None, 17, 18
            // Col_1: 13,   14,   15,   16,   None, 17, 18
            // Col_2: None, None, 15,   16,   None, 17, 18
            // Col_3: 13,   14,   None, None, None, 17, 18
            (0, 3, vec![
                DataType::F64(13, 13.0), DataType::F64(14, 14.0), DataType::F64(15, 15.0),
                DataType::F64(16, 16.0), DataType::F64(17, 17.0), DataType::F64(18, 18.0),
            ], vec![]),
            (1, 3, vec![
                DataType::F64(13, 13.0), DataType::F64(14, 14.0), DataType::F64(15, 15.0),
                DataType::F64(16, 16.0), DataType::F64(17, 17.0), DataType::F64(18, 18.0),
            ], vec![]),
            (2, 3, vec![
                DataType::F64(15, 15.0), DataType::F64(16, 16.0), DataType::F64(17, 17.0), DataType::F64(18, 18.0),
            ], vec![]),
            (3, 3, vec![
                DataType::F64(13, 13.0), DataType::F64(14, 14.0), DataType::F64(17, 17.0), DataType::F64(18, 18.0),
            ], vec![]),
        ];

        let mut flush_compact_iterator = FlushCompactIterator::new(3, max_level_ts);
        flush_compact_iterator.append_upstream_iterator(caches[0].iter_series_column_blocks());
        flush_compact_iterator.append_upstream_iterator(caches[1].iter_series_column_blocks());
        flush_compact_iterator.append_upstream_iterator(caches[2].iter_series_column_blocks());

        for (i, data) in expected_data.iter().enumerate() {
            let (fid, tsm_blks, delta_blks) = flush_compact_iterator.next().unwrap();
            let (col_id, sid) = model_utils::split_id(fid);
            assert_eq!(data.0, col_id);
            assert_eq!(data.1, sid);
            if data.2.is_empty() {
                assert!(tsm_blks.is_empty());
            } else {
                assert_eq!(tsm_blks.len(), 1);
                check_data_block(&tsm_blks[0], &data.2);
            }
            if data.3.is_empty() {
                assert!(delta_blks.is_empty());
            } else {
                assert_eq!(delta_blks.len(), 1);
                check_data_block(&delta_blks[0], &data.3);
            }
        }
    }
}
