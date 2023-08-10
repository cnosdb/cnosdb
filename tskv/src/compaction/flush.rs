use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use models::codec::Encoding;
use models::{utils as model_utils, ColumnId, FieldId, SeriesId, Timestamp, ValueType};
use parking_lot::RwLock;
use snafu::ResultExt;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tokio::time::timeout;
use trace::{error, info, warn};
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::context::{GlobalContext, GlobalSequenceContext};
use crate::error::{self, Result};
use crate::memcache::{FieldVal, MemCache, SeriesData};
use crate::summary::{CompactMeta, CompactMetaBuilder, SummaryTask, VersionEdit};
use crate::tseries_family::Version;
use crate::tsm::codec::DataBlockEncoding;
use crate::tsm::{self, DataBlock, TsmWriter};
use crate::version_set::VersionSet;
use crate::{ColumnFileId, TseriesFamilyId};

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

    pub async fn run(
        self,
        version: Arc<Version>,
        version_edits: &mut Vec<VersionEdit>,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) -> Result<()> {
        let (mut high_seq, mut low_seq) = (0, u64::MAX);
        let mut total_memcache_size = 0_u64;

        let mut flushing_mems = Vec::with_capacity(self.mem_caches.len());
        for mem in self.mem_caches.iter() {
            flushing_mems.push(mem.read());
        }
        let mut flushing_mems_data: HashMap<SeriesId, Vec<Arc<RwLock<SeriesData>>>> =
            HashMap::new();
        let flushing_mems_len = flushing_mems.len();
        for mem in flushing_mems.into_iter() {
            let seq_no = mem.seq_no();
            high_seq = seq_no.max(high_seq);
            low_seq = seq_no.min(low_seq);
            total_memcache_size += mem.cache_size();

            for (series_id, series_data) in mem.read_series_data() {
                flushing_mems_data
                    .entry(series_id)
                    .or_insert_with(|| Vec::with_capacity(flushing_mems_len))
                    .push(series_data);
            }
        }

        if total_memcache_size == 0 {
            return Ok(());
        }

        let mut max_level_ts = version.max_level_ts;
        let mut column_file_metas = self
            .flush_mem_caches(
                flushing_mems_data,
                max_level_ts,
                tsm::MAX_BLOCK_VALUES as usize,
            )
            .await?;
        let mut edit = VersionEdit::new(self.ts_family_id);
        for (cm, _) in column_file_metas.iter_mut() {
            cm.low_seq = low_seq;
            cm.high_seq = high_seq;
            max_level_ts = max_level_ts.max(cm.max_ts);
        }
        for (cm, field_filter) in column_file_metas {
            file_metas.insert(cm.file_id, field_filter);
            edit.add_file(cm, max_level_ts);
        }
        version_edits.push(edit);

        Ok(())
    }

    /// Merges caches data and write them into a `.tsm` file and a `.delta` file
    /// (Sometimes one of the two file type.), returns `CompactMeta`s of the wrote files.
    async fn flush_mem_caches(
        &self,
        mut caches_data: HashMap<SeriesId, Vec<Arc<RwLock<SeriesData>>>>,
        max_level_ts: Timestamp,
        max_data_block_size: usize,
    ) -> Result<Vec<(CompactMeta, Arc<BloomFilter>)>> {
        let mut writer = WriterWrapper::new(self.ts_family_id, max_level_ts, max_data_block_size);

        let mut column_encoding_map: HashMap<ColumnId, Encoding> = HashMap::new();
        let mut column_values_map: HashMap<ColumnId, (ValueType, Vec<(Timestamp, FieldVal)>)> =
            HashMap::new();
        for (sid, series_datas) in caches_data.iter_mut() {
            column_encoding_map.clear();
            column_values_map.clear();

            // Iterates [ MemCache ] -> next_series_id -> [ SeriesData ]
            for series_data in series_datas.iter_mut() {
                // Iterates SeriesData -> [ RowGroups{ schema_id, schema, [ RowData ] } ]
                for (_sch_id, sch_cols, rows) in series_data.read().flat_groups() {
                    for i in sch_cols.columns().iter() {
                        column_encoding_map.insert(i.id, i.encoding);
                    }
                    // Iterates [ RowData ]
                    for row in rows.iter() {
                        // Iterates RowData -> [ Option<FieldVal>, column_id ]
                        for (val, col) in row.fields.iter().zip(sch_cols.fields().iter()) {
                            if let Some(v) = val {
                                let (_, col_vals) = column_values_map
                                    .entry(col.id)
                                    .or_insert_with(|| (v.value_type(), Vec::with_capacity(64)));
                                col_vals.push((row.ts, v.clone()));
                            }
                        }
                    }
                }
            }

            for (col_id, (value_type, values)) in column_values_map.iter_mut() {
                // Sort by timestamp.
                values.sort_by_key(|a| a.0);
                // Dedup by timestamp.
                utils::dedup_front_by_key(values, |a| a.0);

                let field_id = model_utils::unite_id(*col_id, *sid);
                let encoding = DataBlockEncoding::new(
                    Encoding::Default,
                    column_encoding_map.get(col_id).copied().unwrap_or_default(),
                );
                writer
                    .write_field(field_id, values, value_type, encoding, self)
                    .await?;
            }
        }

        // Flush the wrote files.
        writer.finish().await
    }

    async fn new_tsm_writer(&self, is_delta: bool) -> Result<TsmWriter> {
        let dir = if is_delta {
            &self.path_delta
        } else {
            &self.path_tsm
        };
        tsm::new_tsm_writer(dir, self.global_context.file_id_next(), is_delta, 0).await
    }
}

pub async fn run_flush_memtable_job(
    req: FlushReq,
    global_context: Arc<GlobalContext>,
    global_sequence_context: Arc<GlobalSequenceContext>,
    version_set: Arc<tokio::sync::RwLock<VersionSet>>,
    summary_task_sender: Sender<SummaryTask>,
    compact_task_sender: Option<Sender<CompactTask>>,
) -> Result<()> {
    info!(
        "Flush: Running flush job for ts_family {} with {} MemCaches",
        req.ts_family_id,
        req.mems.len()
    );

    let mut version_edits: Vec<VersionEdit> = vec![];
    let mut file_metas: HashMap<ColumnFileId, Arc<BloomFilter>> = HashMap::new();

    let get_tsf_result = version_set
        .read()
        .await
        .get_tsfamily_by_tf_id(req.ts_family_id)
        .await;
    if let Some(tsf) = get_tsf_result {
        // todo: build path by vnode data
        let (storage_opt, version, database) = {
            let tsf_rlock = tsf.read().await;
            tsf_rlock.update_last_modified().await;
            (
                tsf_rlock.storage_opt(),
                tsf_rlock.version(),
                tsf_rlock.database(),
            )
        };

        let path_tsm = storage_opt.tsm_dir(&database, req.ts_family_id);
        let path_delta = storage_opt.delta_dir(&database, req.ts_family_id);

        let flush_task = FlushTask::new(
            req.mems.clone(),
            req.ts_family_id,
            global_context.clone(),
            path_tsm,
            path_delta,
        );

        flush_task
            .run(version, &mut version_edits, &mut file_metas)
            .await?;

        tsf.read().await.update_last_modified().await;

        if let Some(sender) = compact_task_sender.as_ref() {
            let _ = sender.send(CompactTask::Normal(req.ts_family_id)).await;
        }
    }

    // If there are no data flushed but it's a force flush,
    // just write an empty VersionEdit with the max seq_no to the summary.
    if version_edits.is_empty() && req.force_flush {
        let mut ve = VersionEdit::new(req.ts_family_id);
        ve.has_seq_no = true;
        ve.seq_no = global_sequence_context.max_seq();
        version_edits.push(ve);
    }

    info!(
        "Flush: Run flush job for ts_family {} finished, version edits: {:?}",
        req.ts_family_id, version_edits
    );

    let (task_state_sender, task_state_receiver) = oneshot::channel();
    let task = SummaryTask::new(
        version_edits,
        Some(file_metas),
        Some(HashMap::from([(req.ts_family_id, req.mems)])),
        task_state_sender,
    );

    if let Err(e) = summary_task_sender.send(task).await {
        warn!("failed to send Summary task, {}", e);
    }

    if timeout(Duration::from_secs(10), task_state_receiver)
        .await
        .is_err()
    {
        error!("Failed recv summary call back, may case inconsistency of data temporarily");
    }

    Ok(())
}

struct WriterWrapper {
    ts_family_id: TseriesFamilyId,
    max_level_ts: Timestamp,
    max_data_block_size: usize,

    /// Buffers of level-0 and level-1 data blocks:
    ///
    /// Each variant of DataBlock will be insert to a hard-coded index of buffers:
    /// `[ [ Float, Integer, Unsigned, Boolean, Bytes ]; 2 ]`
    buffers: [[DataBlock; 5]; 2],
    /// Pointer to leve-0 and level-1 TSM writers.
    writers: [Option<TsmWriter>; 2],
}

impl WriterWrapper {
    pub fn new(
        ts_family_id: TseriesFamilyId,
        max_level_ts: Timestamp,
        max_data_block_size: usize,
    ) -> Self {
        let data_block_buffers = [
            DataBlock::new(0, ValueType::Float),
            DataBlock::new(0, ValueType::Integer),
            DataBlock::new(0, ValueType::Unsigned),
            DataBlock::new(0, ValueType::Boolean),
            DataBlock::new(0, ValueType::String),
        ];
        Self {
            ts_family_id,
            max_level_ts,
            max_data_block_size,

            buffers: [data_block_buffers.clone(), data_block_buffers],
            writers: [None, None],
        }
    }

    pub async fn write_field(
        &mut self,
        field_id: FieldId,
        values: &[(Timestamp, FieldVal)],
        value_type: &ValueType,
        encoding: DataBlockEncoding,
        flush_task: &FlushTask,
    ) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let buf_idx = match value_type {
            ValueType::Float => 0,
            ValueType::Integer => 1,
            ValueType::Unsigned => 2,
            ValueType::Boolean => 3,
            ValueType::String => 4,
            ValueType::Unknown => {
                error!("Flush: Unknown value type for field: {}", field_id);
                return Ok(());
            }
        };

        // Split values for level-0 and levle-1.
        let splited_values = match values.binary_search_by(|v| v.0.cmp(&self.max_level_ts)) {
            Ok(i) => {
                if i == values.len() - 1 {
                    [values, &[]]
                } else {
                    [&values[..=i], &values[(i + 1)..]]
                }
            }
            Err(i) => {
                if i == 0 {
                    [&[], values]
                } else if i < values.len() {
                    [&values[..i], &values[i..]]
                } else {
                    [values, &[]]
                }
            }
        };
        // Fill buffer and write to disk if buffer is full.
        for (level_idx, values) in splited_values.into_iter().enumerate() {
            for (ts, val) in values {
                let buffer = &mut self.buffers[level_idx][buf_idx];
                buffer.insert(val.data_value(*ts));
                if buffer.len() > self.max_data_block_size {
                    buffer.set_encoding(encoding);
                    Self::write_tsm(&mut self.writers, flush_task, level_idx, field_id, buffer)
                        .await?;
                    buffer.clear();
                }
            }
        }
        // Write the remaining data to disk.
        for (level, lvl_buffer) in self.buffers.iter_mut().enumerate() {
            let buffer = &mut lvl_buffer[buf_idx];
            if !buffer.is_empty() {
                buffer.set_encoding(encoding);
                Self::write_tsm(&mut self.writers, flush_task, level, field_id, buffer).await?;
                buffer.clear();
            }
        }

        Ok(())
    }

    async fn write_tsm(
        writers: &mut [Option<TsmWriter>; 2],
        flush_task: &FlushTask,
        level: usize,
        field_id: FieldId,
        data_block: &DataBlock,
    ) -> Result<usize> {
        let writer_opt = &mut writers[level];
        let writer = match writer_opt.as_mut() {
            Some(w) => w,
            None => {
                let writer = flush_task.new_tsm_writer(level == 0).await?;
                info!(
                    "Flush: File: {} been created (level={}).",
                    writer.sequence(),
                    level
                );
                writer_opt.insert(writer)
            }
        };
        writer
            .write_block(field_id, data_block)
            .await
            .context(error::WriteTsmSnafu)
    }

    pub async fn finish(&mut self) -> Result<Vec<(CompactMeta, Arc<BloomFilter>)>> {
        let mut column_file_metas = Vec::with_capacity(2);
        let compact_meta_builder = CompactMetaBuilder::new(self.ts_family_id);

        // While delta_writer is level-0, tsm_writer is level-1.
        for (level, writer) in self.writers.iter_mut().enumerate() {
            if let Some(w) = writer.as_mut() {
                w.write_index().await.context(error::WriteTsmSnafu)?;
                w.finish().await.context(error::WriteTsmSnafu)?;
                info!(
                    "Flush: File: {} write finished (level: {}, {} B).",
                    w.sequence(),
                    level,
                    w.size()
                );
                column_file_metas.push((
                    compact_meta_builder.build(
                        w.sequence(),
                        w.size(),
                        level as u32,
                        w.min_ts(),
                        w.max_ts(),
                    ),
                    Arc::new(w.bloom_filter_cloned()),
                ));
            }
        }

        // Sort by File id.
        column_file_metas.sort_by_key(|c| c.0.file_id);

        Ok(column_file_metas)
    }
}

#[cfg(test)]
pub mod flush_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use lru_cache::asynchronous::ShardedCache;
    use memory_pool::{GreedyMemoryPool, MemoryPoolRef};
    use minivec::mini_vec;
    use models::codec::Encoding;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{utils as model_utils, ColumnId, FieldId, Timestamp, ValueType};
    use parking_lot::RwLock;
    use utils::dedup_front_by_key;

    use super::FlushTask;
    use crate::compaction::flush::WriterWrapper;
    use crate::context::GlobalContext;
    use crate::file_utils;
    use crate::kv_option::Options;
    use crate::memcache::test::put_rows_to_cache;
    use crate::memcache::{FieldVal, MemCache};
    use crate::tseries_family::{LevelInfo, Version};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::tsm_reader_tests::read_and_check;
    use crate::tsm::{DataBlock, TsmReader};

    pub fn default_table_schema(ids: Vec<ColumnId>) -> TskvTableSchema {
        let fields = ids
            .iter()
            .map(|i| TableColumn {
                id: *i,
                name: i.to_string(),
                column_type: ColumnType::Field(ValueType::Unknown),
                encoding: Encoding::Default,
            })
            .collect();

        TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "".to_string(),
            fields,
        )
    }

    #[test]
    fn test_sort_dedup() {
        #[rustfmt::skip]
            let mut data = vec![
            (1, 11), (1, 12), (2, 21), (3, 3), (2, 22), (4, 41), (4, 42),
        ];
        data.sort_by_key(|a| a.0);
        assert_eq!(
            &data,
            &vec![(1, 11), (1, 12), (2, 21), (2, 22), (3, 3), (4, 41), (4, 42)]
        );
        dedup_front_by_key(&mut data, |a| a.0);
        assert_eq!(&data, &vec![(1, 12), (2, 22), (3, 3), (4, 42)]);
    }

    #[tokio::test]
    async fn test_flush() {
        let mut config = config::get_config_for_test();
        config.storage.path = "/tmp/test/flush/test_flush".to_string();
        config.log.path = "/tmp/test/flush/test_flush/logs".to_string();
        trace::init_default_global_tracing(&config.log.path, "tskv.log", "debug");

        let dir: PathBuf = config.storage.path.clone().into();
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let tsm_dir = dir.join("tsm");
        let delta_dir = dir.join("delta");
        let memory_pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let test_case = flush_test_case_1(&memory_pool, 10);

        let ts_family_id = 1;
        let database = Arc::new("test_db".to_string());
        let global_context = Arc::new(GlobalContext::new());
        let options = Options::from(&config);
        #[rustfmt::skip]
            let version = Arc::new(Version {
            ts_family_id,
            database: database.clone(),
            storage_opt: options.storage.clone(),
            last_seq: 1,
            max_level_ts: test_case.max_level_ts_before,
            levels_info: LevelInfo::init_levels(database, 0, options.storage),
            tsm_reader_cache: Arc::new(ShardedCache::with_capacity(1)),
        });
        let flush_task =
            FlushTask::new(test_case.caches(), 1, global_context, &tsm_dir, &delta_dir);
        let mut version_edits = vec![];
        let mut file_metas = HashMap::new();
        flush_task
            .run(version, &mut version_edits, &mut file_metas)
            .await
            .unwrap();

        assert_eq!(version_edits.len(), 1);
        let ve = version_edits.get(0).unwrap();
        assert_eq!(ve.max_level_ts, test_case.max_level_ts_after);
        assert_eq!(ve.add_files.len(), test_case.add_files_num());
        assert_eq!(
            ve.add_files.iter().filter(|f| f.level == 0).count(),
            test_case.level_0_files_num()
        );
        assert_eq!(
            ve.add_files.iter().filter(|f| f.level == 1).count(),
            test_case.level_1_files_num()
        );
        assert!(ve.del_files.is_empty());

        let (mut delta_i, mut tsm_i) = (0_usize, 0_usize);

        for cm in ve.add_files.iter() {
            let i = if cm.level == 0 {
                let i = delta_i;
                delta_i += 1;
                i
            } else {
                let i = tsm_i;
                tsm_i += 1;
                i
            };
            assert_eq!(
                cm.file_size,
                test_case.expected_file_size[cm.level as usize][i]
            );
            assert_eq!(cm.min_ts, test_case.expected_min_ts[cm.level as usize][i]);
            assert_eq!(cm.max_ts, test_case.expected_max_ts[cm.level as usize][i]);
            let file_path = if cm.is_delta {
                file_utils::make_delta_file_name(&delta_dir, cm.file_id)
            } else {
                file_utils::make_tsm_file_name(&tsm_dir, cm.file_id)
            };
            let tsm_reader = TsmReader::open(file_path).await.unwrap();
            read_and_check(&tsm_reader, &test_case.expected_data[cm.level as usize][i])
                .await
                .unwrap();
        }
    }

    struct FlushTestCase {
        caches: Vec<Arc<RwLock<MemCache>>>,
        max_level_ts_before: Timestamp,
        max_level_ts_after: Timestamp,
        expected_data: [Vec<HashMap<FieldId, Vec<DataBlock>>>; 2],
        expected_file_size: [Vec<u64>; 2],
        expected_min_ts: [Vec<Timestamp>; 2],
        expected_max_ts: [Vec<Timestamp>; 2],
    }

    impl FlushTestCase {
        pub fn caches(&self) -> Vec<Arc<RwLock<MemCache>>> {
            self.caches.to_vec()
        }

        pub fn add_files_num(&self) -> usize {
            self.expected_file_size[0].len() + self.expected_file_size[1].len()
        }

        pub fn level_0_files_num(&self) -> usize {
            self.expected_file_size[0].len()
        }

        pub fn level_1_files_num(&self) -> usize {
            self.expected_file_size[1].len()
        }
    }

    fn flush_test_case_1(memory_pool: &MemoryPoolRef, max_level_ts: Timestamp) -> FlushTestCase {
        let caches = vec![
            MemCache::new(1, 16, 2, 0, memory_pool),
            MemCache::new(1, 16, 2, 0, memory_pool),
            MemCache::new(1, 16, 2, 0, memory_pool),
        ];
        #[rustfmt::skip]
            let _skip_fmt = {
            put_rows_to_cache(&caches[0], 1, 1, default_table_schema(vec![0, 1, 2]), (3, 4), false);
            put_rows_to_cache(&caches[0], 1, 2, default_table_schema(vec![0, 1, 3]), (1, 2), false);
            put_rows_to_cache(&caches[0], 1, 3, default_table_schema(vec![0, 1, 2, 3]), (5, 5), true);
            put_rows_to_cache(&caches[0], 1, 3, default_table_schema(vec![0, 1, 2, 3]), (5, 6), false);
            put_rows_to_cache(&caches[1], 2, 1, default_table_schema(vec![0, 1, 2]), (9, 10), false);
            put_rows_to_cache(&caches[1], 2, 2, default_table_schema(vec![0, 1, 3]), (7, 8), false);
            put_rows_to_cache(&caches[1], 2, 3, default_table_schema(vec![0, 1, 2, 3]), (11, 11), true);
            put_rows_to_cache(&caches[1], 2, 3, default_table_schema(vec![0, 1, 2, 3]), (11, 12), false);
            put_rows_to_cache(&caches[2], 3, 1, default_table_schema(vec![0, 1, 2]), (15, 16), false);
            put_rows_to_cache(&caches[2], 3, 2, default_table_schema(vec![0, 1, 3]), (13, 14), false);
            put_rows_to_cache(&caches[2], 3, 3, default_table_schema(vec![0, 1, 2, 3]), (17, 17), true);
            put_rows_to_cache(&caches[2], 3, 3, default_table_schema(vec![0, 1, 2, 3]), (17, 18), false);
            "skip_fmt"
        };
        let caches = caches
            .into_iter()
            .map(|c| Arc::new(RwLock::new(c)))
            .collect();

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
            (model_utils::unite_id(0, 1), vec![DataBlock::F64 { ts: vec![1, 2, 3, 4, 5, 6], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(1, 1), vec![DataBlock::F64 { ts: vec![1, 2, 3, 4, 5, 6], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(2, 1), vec![DataBlock::F64 { ts: vec![3, 4, 5, 6], val: vec![3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(3, 1), vec![DataBlock::F64 { ts: vec![1, 2, 5, 6], val: vec![1.0, 2.0, 5.0, 6.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(0, 2), vec![DataBlock::F64 { ts: vec![7, 8, 9, 10], val: vec![7.0, 8.0, 9.0, 10.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(1, 2), vec![DataBlock::F64 { ts: vec![7, 8, 9, 10], val: vec![7.0, 8.0, 9.0, 10.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(2, 2), vec![DataBlock::F64 { ts: vec![9, 10], val: vec![9.0, 10.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(3, 2), vec![DataBlock::F64 { ts: vec![7, 8], val: vec![7.0, 8.0], enc: DataBlockEncoding::default() }]),
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
            (model_utils::unite_id(0, 2), vec![DataBlock::F64 { ts: vec![11, 12], val: vec![11.0, 12.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(1, 2), vec![DataBlock::F64 { ts: vec![11, 12], val: vec![11.0, 12.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(2, 2), vec![DataBlock::F64 { ts: vec![11, 12], val: vec![11.0, 12.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(3, 2), vec![DataBlock::F64 { ts: vec![11, 12], val: vec![11.0, 12.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(0, 3), vec![DataBlock::F64 { ts: vec![13, 14, 15, 16, 17, 18], val: vec![13.0, 14.0, 15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(1, 3), vec![DataBlock::F64 { ts: vec![13, 14, 15, 16, 17, 18], val: vec![13.0, 14.0, 15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(2, 3), vec![DataBlock::F64 { ts: vec![15, 16, 17, 18], val: vec![15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(3, 3), vec![DataBlock::F64 { ts: vec![13, 14, 17, 18], val: vec![13.0, 14.0, 17.0, 18.0], enc: DataBlockEncoding::default() }]),
        ]);

        FlushTestCase {
            caches,
            max_level_ts_before: max_level_ts,
            max_level_ts_after: 18,
            expected_data: [vec![expected_delta_data], vec![expected_tsm_data]],
            expected_file_size: [vec![377], vec![366]],
            expected_min_ts: [vec![1], vec![11]],
            expected_max_ts: [vec![10], vec![18]],
        }
    }

    #[tokio::test]
    async fn test_writer_wrapper() {
        let dir = PathBuf::from("/tmp/test/flush/test_write_wrapper");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let tsm_dir = dir.join("tsm");
        let delta_dir = dir.join("delta");

        let ts_family_id = 1;
        let max_level_ts = 10;
        let global_context = Arc::new(GlobalContext::new());
        let flush_task = FlushTask::new(
            vec![],
            ts_family_id,
            global_context.clone(),
            &tsm_dir,
            &delta_dir,
        );

        let col_enc: HashMap<ColumnId, Encoding> = HashMap::from([
            (1, Encoding::Default),
            (2, Encoding::Default),
            (3, Encoding::Default),
            (4, Encoding::Default),
            (5, Encoding::Default),
        ]);

        let mut writer = WriterWrapper::new(ts_family_id, max_level_ts, 1000);
        let mut expected_delta_data = HashMap::new();
        let mut expected_tsm_data = HashMap::new();
        for sid in [1, 2, 3] {
            #[rustfmt::skip]
            write_one_field(
                &mut writer, &flush_task, &col_enc, 10, model_utils::unite_id(1, sid), FieldVal::Float(1.0), &mut expected_delta_data, &mut expected_tsm_data,
            ).await;
            #[rustfmt::skip]
            write_one_field(
                &mut writer, &flush_task, &col_enc, 10, model_utils::unite_id(2, sid), FieldVal::Integer(1), &mut expected_delta_data, &mut expected_tsm_data,
            ).await;
            #[rustfmt::skip]
            write_one_field(
                &mut writer, &flush_task, &col_enc, 10, model_utils::unite_id(3, sid), FieldVal::Unsigned(1), &mut expected_delta_data, &mut expected_tsm_data,
            ).await;
            #[rustfmt::skip]
            write_one_field(
                &mut writer, &flush_task, &col_enc, 10, model_utils::unite_id(4, sid), FieldVal::Boolean(true), &mut expected_delta_data, &mut expected_tsm_data,
            ).await;
            #[rustfmt::skip]
            write_one_field(
                &mut writer, &flush_task, &col_enc, 10, model_utils::unite_id(5, sid), FieldVal::Bytes(mini_vec![1, 1]), &mut expected_delta_data, &mut expected_tsm_data,
            ).await;
        }

        let compact_metas = writer.finish().await.unwrap();
        assert_eq!(compact_metas.len(), 2);
        assert_eq!(compact_metas[0].0.level, 0);
        assert_eq!(compact_metas[1].0.level, 1);

        let delta_file_name =
            file_utils::make_delta_file_name(&delta_dir, compact_metas[0].0.file_id);
        let delta_reader = TsmReader::open(delta_file_name).await.unwrap();
        read_and_check(&delta_reader, &expected_delta_data)
            .await
            .unwrap();

        let tsm_file_name = file_utils::make_tsm_file_name(&tsm_dir, compact_metas[1].0.file_id);
        let tsm_reader = TsmReader::open(tsm_file_name).await.unwrap();
        read_and_check(&tsm_reader, &expected_tsm_data)
            .await
            .unwrap();
    }

    /// Write one field_value as delta value, then write as tsm value,
    /// Then flush to delta file and tsm file.
    async fn write_one_field(
        writer: &mut WriterWrapper,
        flush_task: &FlushTask,
        col_enc_map: &HashMap<ColumnId, Encoding>,
        max_level_ts: Timestamp,
        field_id: FieldId,
        field_val: FieldVal,
        expected_delta_data: &mut HashMap<FieldId, Vec<DataBlock>>,
        expected_tsm_data: &mut HashMap<FieldId, Vec<DataBlock>>,
    ) {
        let value_type = field_val.value_type();
        let (col_id, _) = model_utils::split_id(field_id);
        let encoding = DataBlockEncoding::new(
            Encoding::Default,
            col_enc_map.get(&col_id).copied().unwrap_or_default(),
        );

        let values = vec![
            (max_level_ts - 2, field_val.clone()),
            (max_level_ts - 1, field_val.clone()),
            (max_level_ts, field_val.clone()),
            (max_level_ts + 1, field_val.clone()),
            (max_level_ts + 2, field_val.clone()),
            (max_level_ts + 3, field_val.clone()),
        ];
        let mut delta_data_block = DataBlock::new(1, field_val.value_type());
        delta_data_block.insert(field_val.data_value(max_level_ts - 2));
        delta_data_block.insert(field_val.data_value(max_level_ts - 1));
        delta_data_block.insert(field_val.data_value(max_level_ts));
        expected_delta_data.insert(field_id, vec![delta_data_block]);

        let mut tsm_data_block = DataBlock::new(1, field_val.value_type());
        tsm_data_block.insert(field_val.data_value(max_level_ts + 1));
        tsm_data_block.insert(field_val.data_value(max_level_ts + 2));
        tsm_data_block.insert(field_val.data_value(max_level_ts + 3));
        expected_tsm_data.insert(field_id, vec![tsm_data_block]);

        writer
            .write_field(field_id, &values, &value_type, encoding, flush_task)
            .await
            .unwrap();
    }
}
