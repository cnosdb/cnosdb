use std::{
    cmp::max,
    collections::HashMap,
    iter::Peekable,
    path::{Path, PathBuf},
    rc::Rc,
    slice,
    sync::Arc,
};

use models::{
    utils as model_utils, FieldId, FieldInfo, RwLockRef, SeriesId, SeriesKey, Timestamp, ValueType,
};
use parking_lot::{Mutex, RwLock};
use regex::internal::Input;
use snafu::{NoneError, OptionExt, ResultExt};
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
    database::Database,
    error::{self, Error, Result},
    index::IndexResult,
    kv_option::Options,
    memcache::{DataType, FieldVal, MemCache, MemEntry, SeriesData},
    summary::{CompactMeta, SummaryTask, VersionEdit},
    tseries_family::{LevelInfo, Version},
    tsm::{
        self,
        codec::{DataBlockEncoding, Encoding},
        DataBlock, TsmWriter,
    },
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
        path_tsm: impl AsRef<Path>,
        path_delta: impl AsRef<Path>,
    ) -> Self {
        Self {
            mem_caches,
            ts_family_id,
            global_context,
            path_tsm: path_tsm.as_ref().into(),
            path_delta: path_delta.as_ref().into(),
        }
    }

    pub fn run(self, version: Arc<Version>, version_edits: &mut Vec<VersionEdit>) -> Result<()> {
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
        let mut flushing_mems_data: HashMap<u64, Vec<Arc<RwLock<SeriesData>>>> = HashMap::new();
        for mem in flushing_mems.iter() {
            let seq_no = mem.seq_no();
            high_seq = seq_no.max(high_seq);
            low_seq = seq_no.min(low_seq);
            total_memcache_size += mem.cache_size();

            for (series_id, series_data) in mem.read_series_data() {
                flushing_mems_data
                    .entry(series_id)
                    .or_insert_with(|| Vec::with_capacity(flushing_mems.len()))
                    .push(series_data);
            }
        }

        if total_memcache_size == 0 {
            return Ok(());
        }

        let mut max_level_ts = version.max_level_ts;
        let mut compact_metas = self.flush_mem_caches(
            flushing_mems_data,
            max_level_ts,
            tsm::MAX_BLOCK_VALUES as usize,
        )?;
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

        for mem in flushing_mems.iter_mut() {
            mem.flushed = true;
        }

        Ok(())
    }

    /// Merges caches data and write them into a `.tsm` file and a `.delta` file
    /// (Sometimes one of the two file type.), returns `CompactMeta`s of the wrote files.
    fn flush_mem_caches(
        &self,
        mut caches_data: HashMap<SeriesId, Vec<Arc<RwLock<SeriesData>>>>,
        max_level_ts: Timestamp,
        data_block_size: usize,
    ) -> Result<Vec<CompactMeta>> {
        let mut delta_writer: Option<TsmWriter> = None;
        let mut tsm_writer: Option<TsmWriter> = None;

        for (sid, series_datas) in caches_data.iter_mut() {
            // todo : improve get table schema
            // let schema: Vec<FieldInfo> = self.get_table_schema(*sid)?;
            let schema: Vec<FieldInfo> = vec![];
            let field_id_code_type_map: HashMap<FieldId, u8> =
                HashMap::from_iter(schema.iter().map(|f| (f.field_id(), f.code_type())));
            let mut schema_columns_value_type_map: HashMap<u32, ValueType> = HashMap::new();
            let mut column_values_map: HashMap<u32, Vec<(Timestamp, FieldVal)>> = HashMap::new();

            // Iterates [ MemCache ] -> next_series_id -> [ SeriesData ]
            for series_data in series_datas.iter_mut() {
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
                                    .or_insert_with(Vec::new)
                                    .push((row.ts, v.clone()));
                            }
                        }
                    }
                }
            }

            // Merge the collected data.
            let merged_series_data = Self::merge_series_data(
                *sid,
                column_values_map,
                schema_columns_value_type_map,
                max_level_ts,
                data_block_size,
            );

            // Write the merged data into files.
            for (field_id, dlt_blks, tsm_blks) in merged_series_data {
                let encoding =
                    DataBlockEncoding(field_id_code_type_map.get(&field_id).copied().unwrap_or(0));
                if !dlt_blks.is_empty() {
                    if delta_writer.is_none() {
                        let writer = self.new_writer(true)?;
                        info!("Flush: File {}(delta) been created.", writer.sequence());
                        delta_writer = Some(writer);
                    };
                    let writer = delta_writer.as_mut().unwrap();
                    for mut data_block in dlt_blks {
                        data_block.set_encodings(encoding);
                        writer
                            .write_block(field_id, &data_block)
                            .context(error::WriteTsmSnafu)?;
                    }
                }
                if !tsm_blks.is_empty() {
                    if tsm_writer.is_none() {
                        let writer = self.new_writer(false)?;
                        info!("Flush: File {}(tsm) been created.", writer.sequence());
                        tsm_writer = Some(writer);
                    }
                    let writer = tsm_writer.as_mut().unwrap();
                    for mut data_block in tsm_blks {
                        data_block.set_encodings(encoding);
                        writer
                            .write_block(field_id, &data_block)
                            .context(error::WriteTsmSnafu)?;
                    }
                }
            }
        }

        // Flush the wrote files.
        self.finish_flush_mem_caches(delta_writer, tsm_writer)
    }

    /// For the collected data, sort and dedup by timestamp, and then split by max_level_ts.
    /// Returns [ ( FieldId, Delta_DataBlocks, Tsm_DataBlocks) ]
    fn merge_series_data(
        series_id: SeriesId,
        column_values: HashMap<u32, Vec<(Timestamp, FieldVal)>>,
        column_types: HashMap<u32, ValueType>,
        max_level_ts: Timestamp,
        data_block_size: usize,
    ) -> Vec<(FieldId, Vec<DataBlock>, Vec<DataBlock>)> {
        let mut cols_data: Vec<(FieldId, Vec<DataBlock>, Vec<DataBlock>)> =
            Vec::with_capacity(column_values.len());

        for (col, mut values) in column_values.into_iter() {
            if let Some(typ) = column_types.get(&col) {
                values.sort_by_key(|a| a.0);
                utils::dedup_front_by_key(&mut values, |a| a.0);

                let field_id = model_utils::unite_id(col as u64, series_id);
                let mut delta_blocks = Vec::new();
                let mut tsm_blocks = Vec::new();
                let mut tsm_blk = DataBlock::new(data_block_size, *typ);
                let mut delta_blk = DataBlock::new(data_block_size, *typ);
                for (ts, v) in values {
                    if ts > max_level_ts {
                        tsm_blk.insert(&v.data_value(ts));
                        if tsm_blk.len() as usize >= data_block_size {
                            tsm_blocks.push(tsm_blk);
                            tsm_blk = DataBlock::new(data_block_size, *typ);
                        }
                    } else {
                        delta_blk.insert(&v.data_value(ts));
                        if delta_blk.len() as usize >= data_block_size {
                            delta_blocks.push(delta_blk);
                            delta_blk = DataBlock::new(data_block_size, *typ);
                        }
                    }
                }
                if !delta_blk.is_empty() {
                    delta_blocks.push(delta_blk);
                }
                if !tsm_blk.is_empty() {
                    tsm_blocks.push(tsm_blk);
                }
                cols_data.push((field_id, delta_blocks, tsm_blocks));
            }
        }

        // Sort by FieldId
        cols_data.sort_by_key(|a| a.0);
        cols_data
    }

    fn new_writer(&self, is_delta: bool) -> Result<TsmWriter> {
        let dir = if is_delta {
            &self.path_delta
        } else {
            &self.path_tsm
        };
        tsm::new_tsm_writer(dir, self.global_context.file_id_next(), is_delta, 0)
    }

    /// Flush writers (if it exists) and then generate `CompactMeta`s.
    fn finish_flush_mem_caches(
        &self,
        mut delta_writer: Option<TsmWriter>,
        mut tsm_writer: Option<TsmWriter>,
    ) -> Result<Vec<CompactMeta>> {
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

        // Sort by File id.
        compact_metas.sort_by_key(|c| c.file_id);

        Ok(compact_metas)
    }
}

pub fn run_flush_memtable_job(
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
        if caches.is_empty() {
            continue;
        }
        let tsf_warp = version_set.read().get_tsfamily_by_tf_id(*tsf_id);
        if let Some(tsf) = tsf_warp {
            // todo: build path by vnode data
            let tsf_rlock = tsf.read();
            let storage_opt = tsf_rlock.storage_opt();
            let version = tsf_rlock.version();
            let database = tsf_rlock.database();
            drop(tsf_rlock);
            let path_tsm = storage_opt.tsm_dir(&database, *tsf_id);
            let path_delta = storage_opt.delta_dir(&database, *tsf_id);

            FlushTask::new(
                caches.clone(),
                *tsf_id,
                global_context.clone(),
                path_tsm,
                path_delta,
            )
            .run(version, &mut edits)?;

            if let Err(e) = compact_task_sender.send(*tsf_id) {
                error!("{}", e);
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

#[cfg(test)]
pub mod flush_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;

    use models::{utils as model_utils, FieldId, Timestamp};
    use parking_lot::RwLock;
    use utils::dedup_front_by_key;

    use crate::memcache::test::put_rows_to_cache;
    use crate::summary::{CompactMeta, VersionEdit};
    use crate::tseries_family::{LevelInfo, Version};
    use crate::tsm::tsm_reader_tests::read_and_check;
    use crate::tsm::{codec::DataBlockEncoding, DataBlock, TsmReader};
    use crate::{
        compaction::FlushReq,
        context::GlobalContext,
        kv_option::Options,
        memcache::{DataType, FieldVal, MemCache},
        tseries_family::FLUSH_REQ,
        version_set::VersionSet,
    };
    use crate::{file_manager, file_utils};

    use super::FlushTask;

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

    #[tokio::test]
    async fn test_flush() {
        let config = config::get_config("../config/config.toml");
        trace::init_default_global_tracing(&config.log.path, "tskv.log", "debug");

        let dir = PathBuf::from_str("/tmp/test/flush/test_flush").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let tsm_dir = dir.join("tsm");
        let delta_dir = dir.join("delta");

        let mut caches = vec![
            MemCache::new(1, 16, 0),
            MemCache::new(1, 16, 0),
            MemCache::new(1, 16, 0),
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

        // | === SeriesId: 1 === |
        // Ts:    1,    2,    3,    4,    5,    5, 6
        // Col_0: 1,    2,    3,    4,    None, 5, 6
        // Col_1: 1,    2,    3,    4,    None, 5, 6
        // Col_2: None, None, 3,    4,    None, 5, 6
        // Col_3: 1,    2,    None, None, None, 5, 6
        // | === SeriesId: 2 === |
        // Ts:    7,    8,    9,    10
        // Col_0: 7,    8,    9,    10
        // Col_1: 7,    8,    9,    10
        // Col_2: None, None, 9,    10
        // Col_3: 7,    8,    None, None
        #[rustfmt::skip]
        let expected_delta_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from([
            (model_utils::unite_id(0, 1), vec![DataBlock::F64{ts: vec![1, 2, 3, 4, 5, 6], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(1, 1), vec![DataBlock::F64{ts: vec![1, 2, 3, 4, 5, 6], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(2, 1), vec![DataBlock::F64{ts: vec![3, 4, 5, 6], val: vec![3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(3, 1), vec![DataBlock::F64{ts: vec![1, 2, 5, 6], val: vec![1.0, 2.0, 5.0, 6.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(0, 2), vec![DataBlock::F64{ts: vec![7, 8, 9, 10], val: vec![7.0, 8.0, 9.0, 10.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(1, 2), vec![DataBlock::F64{ts: vec![7, 8, 9, 10], val: vec![7.0, 8.0, 9.0, 10.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(2, 2), vec![DataBlock::F64{ts: vec![9, 10], val: vec![9.0, 10.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(3, 2), vec![DataBlock::F64{ts: vec![7, 8], val: vec![7.0, 8.0], enc: DataBlockEncoding::default()}]),
        ]);

        // | === SeriesId: 2 === |
        // Ts:    11,   11, 12
        // Col_0: None, 11, 12
        // Col_1: None, 11, 12
        // Col_2: None, 11, 12
        // Col_3: None, 11, 12
        // | === SeriesId: 3 === |
        // Ts:    13,   14,   15,   16,   16,   17, 18
        // Col_0: 13,   14,   15,   16,   None, 17, 18
        // Col_1: 13,   14,   15,   16,   None, 17, 18
        // Col_2: None, None, 15,   16,   None, 17, 18
        // Col_3: 13,   14,   None, None, None, 17, 18
        #[rustfmt::skip]
        let expected_tsm_data: HashMap<FieldId, Vec<DataBlock>> = HashMap::from([
            (model_utils::unite_id(0, 2), vec![DataBlock::F64{ts: vec![11, 12], val: vec![11.0, 12.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(1, 2), vec![DataBlock::F64{ts: vec![11, 12], val: vec![11.0, 12.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(2, 2), vec![DataBlock::F64{ts: vec![11, 12], val: vec![11.0, 12.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(3, 2), vec![DataBlock::F64{ts: vec![11, 12], val: vec![11.0, 12.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(0, 3), vec![DataBlock::F64{ts: vec![13, 14, 15, 16, 17, 18], val: vec![13.0, 14.0, 15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(1, 3), vec![DataBlock::F64{ts: vec![13, 14, 15, 16, 17, 18], val: vec![13.0, 14.0, 15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(2, 3), vec![DataBlock::F64{ts: vec![15, 16, 17, 18], val: vec![15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default()}]),
            (model_utils::unite_id(3, 3), vec![DataBlock::F64{ts: vec![13, 14, 17, 18], val: vec![13.0, 14.0, 17.0, 18.0], enc: DataBlockEncoding::default()}]),
        ]);

        let ts_family_id = 1;
        let database = "test_db".to_string();

        let caches = caches
            .into_iter()
            .map(|c| Arc::new(RwLock::new(c)))
            .collect();
        let global_context = Arc::new(GlobalContext::new());
        let options = Options::from(&config);
        #[rustfmt::skip]
        let version = Arc::new(Version {
            ts_family_id, database: database.clone(), storage_opt: options.storage.clone(),
            last_seq: 1, max_level_ts,
            levels_info: LevelInfo::init_levels(database, options.storage),
        });
        let flush_task = FlushTask::new(caches, 1, global_context, &tsm_dir, &delta_dir);
        let mut version_edits = vec![];
        flush_task.run(version, &mut version_edits).unwrap();

        assert_eq!(version_edits.len(), 1);
        let ve = version_edits.get(0).unwrap();
        assert_eq!(ve.max_level_ts, 18);
        assert_eq!(ve.add_files.len(), 2);
        assert!(ve.del_files.is_empty());

        let (mut tsm_reader, mut dlt_reader) = (None, None);
        for cm in ve.add_files.iter() {
            if cm.is_delta {
                assert_eq!(cm.file_size, 377);
                assert_eq!(cm.min_ts, 1);
                assert_eq!(cm.max_ts, 10);
                let file_path = file_utils::make_delta_file_name(&delta_dir, cm.file_id);
                dlt_reader = Some(TsmReader::open(file_path).unwrap())
            } else {
                assert_eq!(cm.file_size, 366);
                assert_eq!(cm.min_ts, 11);
                assert_eq!(cm.max_ts, 18);
                let file_path = file_utils::make_tsm_file_name(&tsm_dir, cm.file_id);
                tsm_reader = Some(TsmReader::open(file_path).unwrap())
            }
        }

        read_and_check(tsm_reader.as_ref().unwrap(), expected_tsm_data);
        read_and_check(dlt_reader.as_ref().unwrap(), expected_delta_data);
    }
}
