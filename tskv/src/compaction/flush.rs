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
        DataBlock, NullableDataBlock, TsmWriter,
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
    database: Option<Arc<RwLock<Database>>>,
    ts_family_id: TseriesFamilyId,
    global_context: Arc<GlobalContext>,
    path_tsm: PathBuf,
    path_delta: PathBuf,
}

impl FlushTask {
    pub fn new(
        mem_caches: Vec<Arc<RwLock<MemCache>>>,
        database: Option<Arc<RwLock<Database>>>,
        ts_family_id: TseriesFamilyId,
        global_context: Arc<GlobalContext>,
        path_tsm: impl AsRef<Path>,
        path_delta: impl AsRef<Path>,
    ) -> Self {
        Self {
            mem_caches,
            database,
            ts_family_id,
            global_context,
            path_tsm: path_tsm.as_ref().into(),
            path_delta: path_delta.as_ref().into(),
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
            let schema: Vec<FieldInfo> = self.get_table_schema(*sid)?;
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
                    Self::write_data(delta_writer.as_mut().unwrap(), field_id, dlt_blks, encoding)?;
                }
                if !tsm_blks.is_empty() {
                    if tsm_writer.is_none() {
                        let writer = self.new_writer(false)?;
                        info!("Flush: File {}(tsm) been created.", writer.sequence());
                        tsm_writer = Some(writer);
                    }
                    Self::write_data(tsm_writer.as_mut().unwrap(), field_id, tsm_blks, encoding)?;
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
    ) -> Vec<(FieldId, Vec<NullableDataBlock>, Vec<NullableDataBlock>)> {
        let mut cols_data: Vec<(FieldId, Vec<NullableDataBlock>, Vec<NullableDataBlock>)> =
            Vec::with_capacity(column_values.len());
        let mut cols_data_builder = Vec::with_capacity(column_values.len());
        let mut column_value_iters = Vec::with_capacity(column_values.len());
        for (col, mut values) in column_values.into_iter() {
            if let Some(typ) = column_types.get(&col) {
                values.sort_by_key(|a| a.0);
                utils::dedup_front_by_key(&mut values, |a| a.0);

                column_value_iters.push(values.into_iter().peekable());

                let field_id = model_utils::unite_id(col as u64, series_id);
                cols_data.push((field_id, Vec::new(), Vec::new()));
                // Temporarily put a default Encoding
                cols_data_builder.push((
                    NullableDataBlock::new(0, *typ, DataBlockEncoding::default()),
                    NullableDataBlock::new(0, *typ, DataBlockEncoding::default()),
                ));
            }
        }

        fn insert_old_and_create_new(
            blocks: &mut Vec<NullableDataBlock>,
            block: &mut NullableDataBlock,
        ) {
            if !block.is_empty() {
                let blk = std::mem::replace(
                    block,
                    NullableDataBlock::new(0, block.field_type(), block.encodings()),
                );
                blocks.push(blk);
            }
        }

        let mut row_ts;
        let mut skipped;
        loop {
            row_ts = Timestamp::MAX;
            skipped = 0_usize;
            for iter in column_value_iters.iter_mut() {
                if let Some((ts, _)) = iter.peek() {
                    row_ts = row_ts.min(*ts);
                } else {
                    skipped += 1;
                }
            }
            if skipped >= column_value_iters.len() {
                break;
            }
            println!("Selected row_ts: {}", row_ts);
            for ((iter, (_, dlt_blks, tsm_blks)), (dlt_blk_builder, tsm_blk_builder)) in
                column_value_iters
                    // Iterate [ column_id: [ ( Timestamp, Value ) ] ]
                    .iter_mut()
                    // Iterate [ column_id: ( FieldId, [ delta_block ], [ tsm_block ] ) ]
                    .zip(cols_data.iter_mut())
                    // Iterate [ column_id: ( delta_block_builder, tsm_block_builder ) ]
                    .zip(cols_data_builder.iter_mut())
            {
                if let Some((ts, val)) = iter.peek() {
                    let col_ts = *ts;
                    if col_ts == row_ts {
                        if row_ts <= max_level_ts {
                            dlt_blk_builder.append_value(col_ts, Some((*val).clone()));
                        } else {
                            tsm_blk_builder.append_value(col_ts, Some((*val).clone()));
                        }
                        iter.next();
                    } else {
                        if row_ts <= max_level_ts {
                            dlt_blk_builder.append_null(row_ts);
                        } else {
                            tsm_blk_builder.append_null(row_ts);
                        }
                    }
                    if dlt_blk_builder.len() > data_block_size {
                        insert_old_and_create_new(dlt_blks, dlt_blk_builder);
                    }
                    if tsm_blk_builder.len() > data_block_size {
                        insert_old_and_create_new(tsm_blks, tsm_blk_builder);
                    }
                }
            }
        }
        for ((_, dlt_blks, tsm_blks), (dlt_blk, tsm_blk)) in
            cols_data.iter_mut().zip(cols_data_builder.iter_mut())
        {
            insert_old_and_create_new(dlt_blks, dlt_blk);
            insert_old_and_create_new(tsm_blks, tsm_blk);
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

    fn write_data(
        writer: &mut TsmWriter,
        field_id: FieldId,
        data_blocks: Vec<NullableDataBlock>,
        encoding: DataBlockEncoding,
    ) -> Result<()> {
        for mut blk in data_blocks {
            blk.set_encodings(encoding);
            writer
                .write_block(field_id, &blk)
                .context(error::WriteTsmSnafu)?;
        }
        Ok(())
    }

    fn close_writer(&self, writer: &mut TsmWriter) -> Result<CompactMeta> {
        writer.write_index().context(error::WriteTsmSnafu)?;
        writer.flush().context(error::WriteTsmSnafu)?;
        info!(
            "Flush: File: {} write finished ({} B).",
            writer.sequence(),
            writer.size()
        );
        Ok(CompactMeta::new(
            writer.sequence(),
            writer.size(),
            self.ts_family_id,
            if writer.is_delta() { 0 } else { 1 },
            writer.min_ts(),
            writer.max_ts(),
            writer.is_delta(),
        ))
    }

    /// Flush writers (if exists) and then generate `CompactMeta`s.
    fn finish_flush_mem_caches(
        &self,
        mut delta_writer: Option<TsmWriter>,
        mut tsm_writer: Option<TsmWriter>,
    ) -> Result<Vec<CompactMeta>> {
        let mut compact_metas = Vec::with_capacity(2);
        tsm_writer
            .as_mut()
            .map(|w| self.close_writer(w))
            .transpose()?
            .map(|cm| {
                compact_metas.push(cm);
            });
        delta_writer
            .as_mut()
            .map(|w| self.close_writer(w))
            .transpose()?
            .map(|cm| {
                compact_metas.push(cm);
            });

        // Sort by File id.
        compact_metas.sort_by_key(|c| c.file_id);

        Ok(compact_metas)
    }

    fn get_table_schema(&self, series_id: SeriesId) -> Result<Vec<FieldInfo>> {
        match self.database.as_ref() {
            None => {
                warn!("database not found, get empty schema",);
                Ok(Vec::with_capacity(0))
            }
            Some(db) => {
                let schema_opt = db
                    .read()
                    .get_table_schema_by_series_id(series_id)
                    .context(error::IndexErrSnafu)?;
                Ok(schema_opt.unwrap_or_else(|| Vec::with_capacity(0)))
            }
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
            let db_instance = version_set.read().get_db(&database);
            let path_tsm = storage_opt.tsm_dir(&database, *tsf_id);
            let path_delta = storage_opt.delta_dir(&database, *tsf_id);
            drop(tsf_rlock);

            FlushTask::new(
                caches.clone(),
                db_instance,
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

#[cfg(test)]
pub mod flush_tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::str::FromStr;
    use std::sync::Arc;

    use config::Config;
    use models::{utils as model_utils, FieldId, SeriesId, Timestamp};
    use parking_lot::RwLock;
    use utils::dedup_front_by_key;

    use crate::{
        compaction::FlushReq,
        context::GlobalContext,
        kv_option::Options,
        memcache::test::put_rows_to_cache,
        memcache::{DataType, FieldVal, MemCache},
        summary::{CompactMeta, VersionEdit},
        tseries_family::FLUSH_REQ,
        tseries_family::{LevelInfo, Version},
        tsm::nullable_block_tests::check_nullable_data_block,
        tsm::{DataBlock, TsmReader},
        version_set::VersionSet,
        TseriesFamilyId,
    };
    use crate::{file_manager, file_utils, TimeRange};

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

    pub(crate) fn read_and_check(
        reader: &TsmReader,
        data_pattern: HashMap<FieldId, Vec<(Timestamp, Option<FieldVal>)>>,
    ) {
        for (fid, blk_pattern) in data_pattern.iter() {
            reader
                .index_iterator_opt(*fid)
                .next()
                .map(|idx_meta| idx_meta.block_iterator())
                .map(|blk_iter| {
                    for blk_meta in blk_iter {
                        let block = reader.get_data_block(&blk_meta).unwrap();
                        check_nullable_data_block(&block, blk_pattern);
                    }
                });
        }
    }

    async fn run_flush_task(
        config: &Config,
        caches: Vec<Arc<RwLock<MemCache>>>,
        database: String,
        ts_family_id: TseriesFamilyId,
        max_level_ts: Timestamp,
        delta_dir: impl AsRef<Path>,
        tsm_dir: impl AsRef<Path>,
    ) -> Vec<VersionEdit> {
        let global_context = Arc::new(GlobalContext::new());
        let options = Options::from(config);
        #[rustfmt::skip]
            let version = Arc::new(Version {
            ts_family_id, database: database.clone(), storage_opt: options.storage.clone(),
            last_seq: 1, max_level_ts,
            levels_info: LevelInfo::init_levels(database.clone(), options.storage.clone()),
        });
        let flush_task = FlushTask::new(caches, None, 1, global_context, &tsm_dir, &delta_dir);
        let mut version_edits = vec![];
        flush_task.run(version, &mut version_edits).await.unwrap();
        version_edits
    }

    #[allow(clippy::too_many_arguments)]
    async fn test_flush(
        config: &Config,
        base_dir: PathBuf,
        data_in_caches: Vec<Vec<(SeriesId, u32, Vec<u32>, (Timestamp, Timestamp), bool)>>,
        max_level_ts: Timestamp,
        expected_max_level_ts: Timestamp,
        expected_delta_data: HashMap<FieldId, Vec<(Timestamp, Option<FieldVal>)>>,
        expected_delta_file_size: u64,
        expected_delta_time_range: TimeRange,
        expected_tsm_data: HashMap<FieldId, Vec<(Timestamp, Option<FieldVal>)>>,
        expected_tsm_file_size: u64,
        expected_tsm_time_range: TimeRange,
    ) {
        let _ = std::fs::remove_dir_all(&base_dir);
        std::fs::create_dir_all(&base_dir).unwrap();
        let delta_dir = base_dir.join("delta");
        let tsm_dir = base_dir.join("tsm");

        let mut caches: Vec<Arc<RwLock<MemCache>>> = Vec::with_capacity(data_in_caches.len());
        for cache_data in data_in_caches {
            let mut cache = MemCache::new(1, 1024, 0);
            for (sid, schema_id, schema_column_ids, time_range, put_none) in cache_data {
                put_rows_to_cache(
                    &mut cache,
                    sid,
                    schema_id,
                    schema_column_ids,
                    time_range,
                    put_none,
                );
            }
            caches.push(Arc::new(RwLock::new(cache)));
        }

        let version_edits = run_flush_task(
            config,
            caches,
            "test_db".to_string(),
            1,
            max_level_ts,
            &delta_dir,
            &tsm_dir,
        )
        .await;
        assert_eq!(version_edits.len(), 1);
        let ve = version_edits.get(0).unwrap();
        assert_eq!(ve.max_level_ts, expected_max_level_ts);
        assert_eq!(ve.add_files.len(), 2);
        assert!(ve.del_files.is_empty());

        let (mut tsm_reader, mut dlt_reader) = (None, None);
        for cm in ve.add_files.iter() {
            if cm.is_delta {
                assert_eq!(cm.file_size, expected_delta_file_size);
                assert_eq!(cm.min_ts, expected_delta_time_range.min_ts);
                assert_eq!(cm.max_ts, expected_delta_time_range.max_ts);
                let file_path = file_utils::make_delta_file_name(&delta_dir, cm.file_id);
                dlt_reader = Some(TsmReader::open(file_path).unwrap())
            } else {
                assert_eq!(cm.file_size, expected_tsm_file_size);
                assert_eq!(cm.min_ts, expected_tsm_time_range.min_ts);
                assert_eq!(cm.max_ts, expected_tsm_time_range.max_ts);
                let file_path = file_utils::make_tsm_file_name(&tsm_dir, cm.file_id);
                tsm_reader = Some(TsmReader::open(file_path).unwrap())
            }
        }

        read_and_check(tsm_reader.as_ref().unwrap(), expected_tsm_data);
        read_and_check(dlt_reader.as_ref().unwrap(), expected_delta_data);
    }

    #[tokio::test]
    async fn test_flush_1() {
        //! # delta data in cache:
        //! ```txt
        //! | === SeriesId: 1 === |
        //! Ts:    1,    2,    3,    4,    5,    5, 6
        //! Col_0: 1,    2,    3,    4,    None, 5, 6
        //! Col_1: 1,    2,    3,    4,    None, 5, 6
        //! Col_2: None, None, 3,    4,    None, 5, 6
        //! Col_3: 1,    2,    None, None, None, 5, 6
        //! | === SeriesId: 2 === |
        //! Ts:    7,    8,    9,    10
        //! Col_0: 7,    8,    9,    10
        //! Col_1: 7,    8,    9,    10
        //! Col_2: None, None, 9,    10
        //! Col_3: 7,    8,    None, None
        //! ```
        //! # tsm data in cache:
        //! ```txt
        //! | === SeriesId: 2 === |
        //! Ts:    11,   11, 12
        //! Col_0: None, 11, 12
        //! Col_1: None, 11, 12
        //! Col_2: None, 11, 12
        //! Col_3: None, 11, 12
        //! | === SeriesId: 3 === |
        //! Ts:    13,   14,   15,   16,   16,   17, 18
        //! Col_0: 13,   14,   15,   16,   None, 17, 18
        //! Col_1: 13,   14,   15,   16,   None, 17, 18
        //! Col_2: None, None, 15,   16,   None, 17, 18
        //! Col_3: 13,   14,   None, None, None, 17, 18
        //! ```

        let config = config::get_config("../config/config.toml");
        trace::init_default_global_tracing(&config.log.path, "tskv.log", "debug");

        let base_dir = PathBuf::from_str("/tmp/test/flush/test_flush_1").unwrap();

        #[rustfmt::skip]
        let data_in_caches = vec![
            vec![(1, 1, vec![0, 1, 2], (3, 4), false),
                 (1, 2, vec![0, 1, 3], (1, 2), false),
                 (1, 3, vec![0, 1, 2, 3], (5, 5), true),
                 (1, 3, vec![0, 1, 2, 3], (5, 6), false)],
            vec![(2, 1, vec![0, 1, 2], (9, 10), false),
                 (2, 2, vec![0, 1, 3], (7, 8), false),
                 (2, 3, vec![0, 1, 2, 3], (11, 11), true),
                 (2, 3, vec![0, 1, 2, 3], (11, 12), false)],
            vec![(3, 1, vec![0, 1, 2], (15, 16), false),
                 (3, 2, vec![0, 1, 3], (13, 14), false),
                 (3, 3, vec![0, 1, 2, 3], (17, 17), true),
                 (3, 3, vec![0, 1, 2, 3], (17, 18), false)],
        ];
        let max_level_ts = 10;
        let expected_max_level_ts = 18;
        let expected_delta_time_range: TimeRange = (1, 10).into();
        let expected_tsm_time_range: TimeRange = (11, 18).into();
        #[rustfmt::skip]
        let expected_delta_data: HashMap<FieldId, Vec<(Timestamp, Option<FieldVal>)>> = HashMap::from([
            (model_utils::unite_id(0, 1), vec![
                (1, Some(FieldVal::Float(1.0))), (2, Some(FieldVal::Float(2.0))), (3, Some(FieldVal::Float(3.0))),
                (4, Some(FieldVal::Float(4.0))), (5, Some(FieldVal::Float(5.0))), (6, Some(FieldVal::Float(6.0))),
            ]),
            (model_utils::unite_id(1, 1), vec![
                (1, Some(FieldVal::Float(1.0))), (2, Some(FieldVal::Float(2.0))), (3, Some(FieldVal::Float(3.0))),
                (4, Some(FieldVal::Float(4.0))), (5, Some(FieldVal::Float(5.0))), (6, Some(FieldVal::Float(6.0))),
            ]),
            (model_utils::unite_id(2, 1), vec![
                (1, None), (2, None), (3, Some(FieldVal::Float(3.0))), (4, Some(FieldVal::Float(4.0))),
                (5, Some(FieldVal::Float(5.0))), (6, Some(FieldVal::Float(6.0))),
            ]),
            (model_utils::unite_id(3, 1), vec![
                (1, Some(FieldVal::Float(1.0))), (2, Some(FieldVal::Float(2.0))), (3, None),
                (4, None), (5, Some(FieldVal::Float(5.0))), (6, Some(FieldVal::Float(6.0))),
            ]),
            (model_utils::unite_id(0, 2), vec![
                (7, Some(FieldVal::Float(7.0))), (8, Some(FieldVal::Float(8.0))), (9, Some(FieldVal::Float(9.0))),
                (10, Some(FieldVal::Float(10.0))),
            ]),
            (model_utils::unite_id(1, 2), vec![
                (7, Some(FieldVal::Float(7.0))), (8, Some(FieldVal::Float(8.0))), (9, Some(FieldVal::Float(9.0))),
                (10, Some(FieldVal::Float(10.0))),
            ]),
            (model_utils::unite_id(2, 2), vec![
                (7, None), (8, None), (9, Some(FieldVal::Float(9.0))), (10, Some(FieldVal::Float(10.0))),
            ]),
            (model_utils::unite_id(3, 2), vec![
                (7, Some(FieldVal::Float(7.0))), (8, Some(FieldVal::Float(8.0))), (9, None), (10, None),
            ]),
        ]);
        #[rustfmt::skip]
        let expected_tsm_data: HashMap<FieldId, Vec<(Timestamp, Option<FieldVal>)>> = HashMap::from([
            (model_utils::unite_id(0, 2), vec![(11, Some(FieldVal::Float(11.0))), (12, Some(FieldVal::Float(12.0)))]),
            (model_utils::unite_id(1, 2), vec![(11, Some(FieldVal::Float(11.0))), (12, Some(FieldVal::Float(12.0)))]),
            (model_utils::unite_id(2, 2), vec![(11, Some(FieldVal::Float(11.0))), (12, Some(FieldVal::Float(12.0)))]),
            (model_utils::unite_id(3, 2), vec![(11, Some(FieldVal::Float(11.0))), (12, Some(FieldVal::Float(12.0)))]),
            (model_utils::unite_id(0, 3), vec![
                (13, Some(FieldVal::Float(13.0))), (14, Some(FieldVal::Float(14.0))), (15, Some(FieldVal::Float(15.0))),
                (16, Some(FieldVal::Float(16.0))), (17, Some(FieldVal::Float(17.0))), (18, Some(FieldVal::Float(18.0))),
            ]),
            (model_utils::unite_id(1, 3), vec![
                (13, Some(FieldVal::Float(13.0))), (14, Some(FieldVal::Float(14.0))), (15, Some(FieldVal::Float(15.0))),
                (16, Some(FieldVal::Float(16.0))), (17, Some(FieldVal::Float(17.0))), (18, Some(FieldVal::Float(18.0))),
            ]),
            (model_utils::unite_id(2, 3), vec![
                (13, None), (14, None), (15, Some(FieldVal::Float(15.0))), (16, Some(FieldVal::Float(16.0))),
                (17, Some(FieldVal::Float(17.0))), (18, Some(FieldVal::Float(18.0))),
            ]),
            (model_utils::unite_id(3, 3), vec![
                (13, Some(FieldVal::Float(13.0))), (14, Some(FieldVal::Float(14.0))), (15, None), (16, None),
                (17, Some(FieldVal::Float(17.0))), (18, Some(FieldVal::Float(18.0))),
            ]),
        ]);

        let expected_delta_file_size = 375_u64;
        let expected_tsm_file_size = 362_u64;

        test_flush(
            &config,
            base_dir,
            data_in_caches,
            max_level_ts,
            expected_max_level_ts,
            expected_delta_data,
            expected_delta_file_size,
            expected_delta_time_range,
            expected_tsm_data,
            expected_tsm_file_size,
            expected_tsm_time_range,
        )
        .await;
    }
}
