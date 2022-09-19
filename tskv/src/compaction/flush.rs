use std::{
    cmp::max,
    collections::{HashMap, HashSet},
    iter::Peekable,
    path::{Path, PathBuf},
    rc::Rc,
    slice,
    sync::Arc,
};

use models::utils::split_code_type_id;
use models::{
    utils as model_utils, FieldId, FieldInfo, RwLockRef, SeriesId, SeriesKey, Timestamp, ValueType,
};
use parking_lot::{Mutex, RwLock};
use regex::internal::Input;
use snafu::{NoneError, OptionExt, ResultExt, Snafu};
use tokio::{
    select,
    sync::{
        mpsc::{self, UnboundedSender},
        oneshot,
        oneshot::Sender,
    },
};
use trace::{debug, error, info, warn};
use utils;

use crate::index::IndexResult;
use crate::tsm::coder_instence::CodeType;
use crate::{
    compaction::FlushReq,
    context::GlobalContext,
    error::{self, Result},
    kv_option::Options,
    memcache::{DataType, FieldVal, MemCache, MemEntry, SeriesData, SeriesDataIterator},
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::{LevelInfo, Version},
    tsm::{self, DataBlock, TsmWriter},
    version_set::VersionSet,
    Error, TseriesFamilyId,
};

struct FlushingBlock {
    pub field_id: FieldId,
    pub data_block: DataBlock,
    pub is_delta: bool,
}

pub struct FlushTask {
    mem_caches: Vec<Arc<RwLock<MemCache>>>,
    ts_family_id: TseriesFamilyId,
    db_name: String,
    global_context: Arc<GlobalContext>,
    path_tsm: PathBuf,
    path_delta: PathBuf,
}

impl FlushTask {
    pub fn new(
        mem_caches: Vec<Arc<RwLock<MemCache>>>,
        ts_family_id: TseriesFamilyId,
        db_name: String,
        global_context: Arc<GlobalContext>,
        path_tsm: PathBuf,
        path_delta: PathBuf,
    ) -> Self {
        Self {
            mem_caches,
            ts_family_id,
            db_name,
            global_context,
            path_tsm,
            path_delta,
        }
    }

    pub async fn run(
        self,
        version: Arc<Version>,
        version_edits: &mut Vec<VersionEdit>,
        version_set: Arc<RwLock<VersionSet>>,
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

            compactor.append_upstream_iterator(mem.iter_series_data());
        }

        if total_memcache_size == 0 {
            return Ok(());
        }

        let mut compact_metas = self.write_block_set(&mut compactor, 0, version_set.clone())?;
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
        version_set: Arc<RwLock<VersionSet>>,
    ) -> Result<Vec<CompactMeta>> {
        let mut tsm_writer: Option<TsmWriter> = None;
        let mut delta_writer: Option<TsmWriter> = None;
        while let Some(series_flushing_data) = compactor.next() {
            let series_id = match compactor.last_sid {
                None => return Err(Error::InvalidSeriesId),
                Some(v) => v,
            };

            let schema = self.get_table_schema(series_id, version_set.clone())?;
            let field_id_code_type_map = {
                let mut res_map = HashMap::new();
                for i in schema.iter() {
                    let code_type_id = i.code_type();
                    let (ts_code_type, val_code_type) = split_code_type_id(code_type_id);
                    res_map.insert(
                        i.field_id(),
                        (
                            CodeType::try_from(ts_code_type).unwrap(),
                            CodeType::try_from(val_code_type).unwrap(),
                        ),
                    );
                }
                res_map
            };

            for (field_id, dlt_blks, tsm_blks) in series_flushing_data {
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

                let (ts_code_type, val_code_type) = match field_id_code_type_map.get(&field_id) {
                    None => {
                        warn!("failed get code type for field id : {}", field_id);
                        (CodeType::Unknown, CodeType::Unknown)
                    }
                    Some(v) => *v,
                };
                for data_block in tsm_blks {
                    if let Some(writer) = tsm_writer.as_mut() {
                        writer
                            .write_block(field_id, &data_block, ts_code_type, val_code_type)
                            .context(error::WriteTsmSnafu)?;
                    }
                }
                for data_block in dlt_blks {
                    if let Some(writer) = delta_writer.as_mut() {
                        writer
                            .write_block(field_id, &data_block, ts_code_type, val_code_type)
                            .context(error::WriteTsmSnafu)?;
                    }
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

    fn get_table_schema(
        &self,
        series_id: SeriesId,
        version_set: Arc<RwLock<VersionSet>>,
    ) -> Result<Vec<FieldInfo>> {
        if let Some(db) = version_set.read().get_db(&self.db_name) {
            let series_key = match db.read().get_series_key(series_id) {
                Ok(series_key) => {
                    if let Some(series_key_unwrap) = series_key {
                        series_key_unwrap
                    } else {
                        warn!(
                            "table not found for series id : {}, get empty schema",
                            series_id
                        );
                        return Ok(vec![]);
                    }
                }
                Err(e) => return Err(Error::IndexErr { source: e }),
            };

            return match db.read().get_table_schema(series_key.table()) {
                Ok(schema) => {
                    if let Some(schema_unwrap) = schema {
                        Ok(schema_unwrap)
                    } else {
                        warn!(
                            "table schema not found for table name : {}, get empty schema",
                            series_key.table()
                        );
                        return Ok(vec![]);
                    }
                }
                Err(e) => Err(Error::IndexErr { source: e }),
            };
        } else {
            warn!(
                "database not found for database name : {}, get empty schema",
                self.db_name
            );
            return Ok(vec![]);
        }
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
                database,
                global_context.clone(),
                path_tsm,
                path_delta,
            )
            .run(tsf.read().version(), &mut edits, version_set.clone())
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
    iterators: Vec<Peekable<SeriesDataIterator<'a>>>,
    max_level_ts: i64,
    data_block_size: usize,

    iterators_count: usize,
    curr_sid: Option<SeriesId>,
    last_sid: Option<SeriesId>,
    flushing_series_datas: Vec<RwLockRef<SeriesData>>,
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
            flushing_series_datas: vec![],
        }
    }

    pub fn append_upstream_iterator(&mut self, iterator: SeriesDataIterator<'a>) {
        self.iterators.push(iterator.peekable());
        self.iterators_count += 1;
    }

    pub fn next_series_id(&mut self) {
        debug!("Selecting next series_id.");
        let mut skipped = 0_usize;
        self.curr_sid = None;
        self.last_sid = None;
        self.flushing_series_datas.truncate(0);
        loop {
            if skipped >= self.iterators_count {
                // Current series_id found, or all iterators finished
                self.curr_sid = None;
                break;
            }
            for iter in self.iterators.iter_mut() {
                if let Some((sid, series_data)) = iter.peek() {
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
                    self.flushing_series_datas.push(series_data.clone());
                    iter.next();
                } else {
                    skipped += 1;
                    continue;
                }
            }
        }

        debug!("Selected series_id: '{:?}'.", self.last_sid);
    }

    fn sort_dedup(v: &mut Vec<(Timestamp, FieldVal)>) {
        v.sort_by_key(|a| a.0);
        utils::dedup_front_by_key(v, |a| a.0);
    }

    /// Transform v into two Vec<DataBlock>, witch are delta blocks and tsm blocks.
    fn into_splited_data_blocks(
        value_type: ValueType,
        v: Vec<(Timestamp, FieldVal)>,
        max_level_ts: Timestamp,
        data_block_size: usize,
    ) -> (Vec<DataBlock>, Vec<DataBlock>) {
        let mut delta_blocks = Vec::new();
        let mut delta_blk = DataBlock::new(data_block_size, value_type);
        let mut tsm_blocks = Vec::new();
        let mut tsm_blk = DataBlock::new(data_block_size, value_type);
        for (ts, v) in v {
            if ts > max_level_ts {
                tsm_blk.insert(&v.data_value(ts));
                if tsm_blk.len() as usize >= data_block_size {
                    tsm_blocks.push(tsm_blk);
                    tsm_blk = DataBlock::new(data_block_size, value_type);
                }
            } else {
                delta_blk.insert(&v.data_value(ts));
                if delta_blk.len() as usize >= data_block_size {
                    delta_blocks.push(delta_blk);
                    delta_blk = DataBlock::new(data_block_size, value_type);
                }
            }
        }
        if tsm_blk.len() > 0 {
            tsm_blocks.push(tsm_blk);
        }
        if delta_blk.len() > 0 {
            delta_blocks.push(delta_blk);
        }

        (delta_blocks, tsm_blocks)
    }
}

impl<'a> Iterator for FlushCompactIterator<'a> {
    type Item = Vec<(FieldId, Vec<DataBlock>, Vec<DataBlock>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // 1. Get next Vec<SeriesData> for next SeriesId from each MemCache.
        self.next_series_id();
        if self.last_sid.is_none() {
            return None;
        }

        // 2. Merge Vec<SeriesData> of the next SeriesId.
        if self.flushing_series_datas.is_empty() {
            // TODO: Return something
            return Some(vec![]);
        }

        let mut schema_columns_value_type_map: HashMap<u32, ValueType> = HashMap::new();
        let mut column_values_map: HashMap<u32, Vec<(Timestamp, FieldVal)>> = HashMap::new();

        // Iterates [ MemCache ] -> next_series_id -> [ SeriesData ]
        for series_data in self.flushing_series_datas.iter() {
            // Iterates SeriesData -> [ RowGroups{ schema_id, schema, [ RowData ] } ]
            for (sch_id, sch_cols, rows) in series_data.read().flat_groups() {
                // Iterates [ RowData ]
                for row in rows.iter() {
                    // Iterates RowData -> [ Option<FieldVal>, column_id ]
                    for (val, col) in row.fields.iter().zip(sch_cols.iter()) {
                        if let Some(v) = val {
                            schema_columns_value_type_map
                                .entry(*col)
                                .or_insert_with(|| v.value_type());
                            column_values_map
                                .entry(*col)
                                .or_insert_with(|| Vec::new())
                                .push((row.ts, v.clone()));
                        }
                    }
                }
            }
        }

        let mut ret_vec: Vec<(FieldId, Vec<DataBlock>, Vec<DataBlock>)> =
            Vec::with_capacity(column_values_map.len());
        let mut ret_vec_col_id = Vec::with_capacity(ret_vec.capacity());
        for (col, mut values) in column_values_map.into_iter() {
            Self::sort_dedup(&mut values);
            if let Some(typ) = schema_columns_value_type_map.get(&col) {
                let (dlt, tsm) = Self::into_splited_data_blocks(
                    *typ,
                    values,
                    self.max_level_ts,
                    self.data_block_size,
                );
                let field_id = model_utils::unite_id(col as u64, self.last_sid.unwrap());
                ret_vec.push((field_id, dlt, tsm));
                ret_vec_col_id.push(col);
            }
        }
        ret_vec.sort_by_key(|a| a.0);

        return Some(ret_vec);
    }
}

#[cfg(test)]
pub mod test {
    use models::{utils as model_utils, Timestamp};
    use utils::dedup_front_by_key;

    use crate::memcache::test::put_rows_to_cache;
    use crate::{
        compaction::{flush::FlushCompactIterator, FlushReq},
        context::GlobalContext,
        kv_option::Options,
        memcache::{DataType, FieldVal, MemCache},
        tseries_family::FLUSH_REQ,
        tsm::test::check_data_block,
        version_set::VersionSet,
    };

    #[test]
    fn test_sort_dedup() {
        #[rustfmt::skip]
        let mut data = vec![
            (1, 11), (1, 12), (2, 21), (3, 3), (2, 22), (4, 41), (4, 42)
        ];
        data.sort_by_key(|a| a.0);
        println!("{:?}", &data);
        assert_eq!(
            &data,
            &vec![(1, 11), (1, 12), (2, 21), (2, 22), (3, 3), (4, 41), (4, 42)]
        );
        dedup_front_by_key(&mut data, |a| a.0);
        println!("{:?}", &data);
        assert_eq!(&data, &vec![(1, 12), (2, 22), (3, 3), (4, 42)]);
    }

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
        let expected_data: Vec<Vec<(u32, u64, Vec<DataType>, Vec<DataType>)>> = vec![
            // | === SeriesId: 1 === |
            // Ts:    1,    2,    3,    4,    5,    5, 6
            // Col_0: 1,    2,    3,    4,    None, 5, 6
            // Col_1: 1,    2,    3,    4,    None, 5, 6
            // Col_2: None, None, 3,    4,    None, 5, 6
            // Col_3: 1,    2,    None, None, None, 5, 6
            vec![
                (0, 1, vec![
                    DataType::F64(1, 1.0), DataType::F64(2, 2.0), DataType::F64(3, 3.0), DataType::F64(4, 4.0),
                    DataType::F64(5, 5.0), DataType::F64(6, 6.0),
                ], vec![]),
                (1, 1, vec![
                    DataType::F64(1, 1.0), DataType::F64(2, 2.0), DataType::F64(3, 3.0), DataType::F64(4, 4.0),
                    DataType::F64(5, 5.0), DataType::F64(6, 6.0),
                ], vec![],),
                (2, 1, vec![
                    DataType::F64(3, 3.0), DataType::F64(4, 4.0), DataType::F64(5, 5.0), DataType::F64(6, 6.0),
                ], vec![]),
                (3, 1, vec![
                    DataType::F64(1, 1.0), DataType::F64(2, 2.0), DataType::F64(5, 5.0), DataType::F64(6, 6.0),
                ], vec![]),
            ],
            // | === SeriesId: 2 === |
            // Ts:    7,    8,    9,    10,   11,   11, 12
            // Col_0: 7,    8,    9,    10,   None, 11, 12
            // Col_1: 7,    8,    9,    10,   None, 11, 12
            // Col_2: None, None, 9,    10,   None, 11, 12
            // Col_3: 7,    8,    None, None, None, 11, 12
            vec![
                (0, 2, vec![
                    DataType::F64(7, 7.0), DataType::F64(8, 8.0), DataType::F64(9, 9.0), DataType::F64(10, 10.0),
                ], vec![DataType::F64(11, 11.0), DataType::F64(12, 12.0)],),
                (1, 2, vec![
                    DataType::F64(7, 7.0), DataType::F64(8, 8.0), DataType::F64(9, 9.0), DataType::F64(10, 10.0),
                ], vec![DataType::F64(11, 11.0), DataType::F64(12, 12.0)],),
                (2, 2, vec![
                    DataType::F64(9, 9.0), DataType::F64(10, 10.0),
                ], vec![DataType::F64(11, 11.0), DataType::F64(12, 12.0)]),
                (3, 2, vec![
                    DataType::F64(7, 7.0), DataType::F64(8, 8.0),
                ], vec![DataType::F64(11, 11.0), DataType::F64(12, 12.0)]),
            ],
            // | === SeriesId: 3 === |
            // Ts:    13,   14,   15,   16,   16,   17, 18
            // Col_0: 13,   14,   15,   16,   None, 17, 18
            // Col_1: 13,   14,   15,   16,   None, 17, 18
            // Col_2: None, None, 15,   16,   None, 17, 18
            // Col_3: 13,   14,   None, None, None, 17, 18
            vec![
                (0, 3, vec![], vec![
                    DataType::F64(13, 13.0), DataType::F64(14, 14.0), DataType::F64(15, 15.0),
                    DataType::F64(16, 16.0), DataType::F64(17, 17.0), DataType::F64(18, 18.0),
                ]),
                (1, 3, vec![], vec![
                    DataType::F64(13, 13.0), DataType::F64(14, 14.0), DataType::F64(15, 15.0),
                    DataType::F64(16, 16.0), DataType::F64(17, 17.0), DataType::F64(18, 18.0),
                ]),
                (2, 3, vec![], vec![
                    DataType::F64(15, 15.0), DataType::F64(16, 16.0), DataType::F64(17, 17.0), DataType::F64(18, 18.0),
                ]),
                (3, 3, vec![], vec![
                    DataType::F64(13, 13.0), DataType::F64(14, 14.0), DataType::F64(17, 17.0), DataType::F64(18, 18.0),
                ]),
            ]
        ];

        let mut flush_compact_iterator = FlushCompactIterator::new(3, max_level_ts);
        flush_compact_iterator.append_upstream_iterator(caches[0].iter_series_data());
        flush_compact_iterator.append_upstream_iterator(caches[1].iter_series_data());
        flush_compact_iterator.append_upstream_iterator(caches[2].iter_series_data());

        for (i, data) in expected_data.iter().enumerate() {
            let ret_vec = flush_compact_iterator.next().unwrap();
            for (
                (fid, delta_blks, tsm_blks),
                (data_col_id, data_sid, data_delta_blks, data_tsm_blks),
            ) in ret_vec.into_iter().zip(data.iter())
            {
                let (col_id, sid) = model_utils::split_id(fid);
                assert_eq!(*data_col_id, col_id);
                assert_eq!(*data_sid, sid);
                if data_tsm_blks.is_empty() {
                    assert!(tsm_blks.is_empty());
                } else {
                    assert_eq!(tsm_blks.len(), 1);
                    check_data_block(&tsm_blks[0], data_tsm_blks);
                }
                if data_delta_blks.is_empty() {
                    assert!(delta_blks.is_empty());
                } else {
                    assert_eq!(delta_blks.len(), 1);
                    check_data_block(&delta_blks[0], data_delta_blks);
                }
            }
        }
    }
}
