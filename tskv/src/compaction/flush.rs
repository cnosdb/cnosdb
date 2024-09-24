use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use models::codec::Encoding;
use models::{FieldId, SeriesId, Timestamp, ValueType};
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
use crate::memcache::{
    FieldVal, MemCache, SeriesData, SeriesDataFieldValues, SeriesDataFieldValuesBuilder,
};
use crate::summary::{CompactMeta, CompactMetaBuilder, SummaryTask, VersionEdit};
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
    path_delta: PathBuf,
}

impl FlushTask {
    pub fn new(
        mem_caches: Vec<Arc<RwLock<MemCache>>>,
        ts_family_id: TseriesFamilyId,
        global_context: Arc<GlobalContext>,
        path_delta: impl AsRef<Path>,
    ) -> Self {
        Self {
            mem_caches,
            ts_family_id,
            global_context,
            path_delta: path_delta.as_ref().into(),
        }
    }

    pub async fn run(
        self,
        version_edits: &mut Vec<VersionEdit>,
        file_metas: &mut HashMap<ColumnFileId, Arc<BloomFilter>>,
    ) -> Result<()> {
        let (mut high_seq, mut low_seq) = (0, u64::MAX);
        let mut total_memcache_size = 0_u64;

        let mut flushing_mems = Vec::with_capacity(self.mem_caches.len());
        for mem in self.mem_caches.iter() {
            flushing_mems.push(mem.read());
        }
        // TODO(zipper, delta_compaction): this changed the order of fields in a tsm file.
        // For old tsm file , field_ids are not ordered, so cannot use delta-compaction cache.
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

        if let Some((mut cm, field_filter)) = self
            .flush_mem_caches(flushing_mems_data, tsm::MAX_BLOCK_VALUES as usize)
            .await?
        {
            let mut edit = VersionEdit::new(self.ts_family_id);

            cm.low_seq = low_seq;
            cm.high_seq = high_seq;

            file_metas.insert(cm.file_id, field_filter);
            edit.add_file(cm);

            version_edits.push(edit);
        }

        Ok(())
    }

    /// Merges caches data and write them into a `.tsm` file and a `.delta` file
    /// (Sometimes one of the two file type.), returns `CompactMeta`s of the wrote files.
    async fn flush_mem_caches(
        &self,
        caches_data: HashMap<SeriesId, Vec<Arc<RwLock<SeriesData>>>>,
        max_data_block_size: usize,
    ) -> Result<Option<(CompactMeta, Arc<BloomFilter>)>> {
        let mut writer = WriterWrapper::new(self.ts_family_id, max_data_block_size);

        let mut field_values: Vec<SeriesDataFieldValues> = Vec::with_capacity(caches_data.len());
        for series_datas in caches_data.values() {
            // Iterates [ MemCache ] -> next_series_id -> [ SeriesData ]
            for series_data in series_datas {
                // Build lazy field value extractor.
                if let Some(builder) = SeriesDataFieldValuesBuilder::try_new(series_data.read()) {
                    field_values.extend(builder.build());
                }
            }
        }
        field_values.sort_unstable();

        let mut values: Vec<(Timestamp, FieldVal)> = Vec::new();
        let mut last_field_id = None::<FieldId>;
        let mut value_type = ValueType::Unknown;
        let mut value_encoding = Encoding::Unknown;
        for fv in field_values.iter() {
            if let Some(last_fid) = last_field_id {
                if fv.field_id() != last_fid {
                    // All cached series data of previous field_id has been iterated.
                    if !values.is_empty() {
                        // Flush the values of last field to tsm file.
                        values.sort_by_key(|(ts, _)| *ts);
                        utils::dedup_front_by_key(&mut values, |(ts, _)| *ts);
                        let encoding = DataBlockEncoding::new(Encoding::Default, value_encoding);
                        writer
                            .write_field(last_fid, &values, value_type, encoding, self)
                            .await?;
                        values.clear();
                    }
                    // Set current field_id and append the values for new field.
                    last_field_id = Some(fv.field_id());
                }
            } else {
                // First loop, set current field_id and append the values.
                last_field_id = Some(fv.field_id());
            }
            // Append the value of the same field.
            let (vt, ve): (ValueType, Encoding) = fv.get(&mut values);
            if vt != ValueType::Unknown {
                // Sometimes data in a mem_cache is missing several columns, then fv.get() returns Unknown.
                value_type = vt;
            }
            if ve != Encoding::Unknown {
                value_encoding = ve;
            }
        }
        if !values.is_empty() {
            // The values of last field not flushed.
            if let Some(last_fid) = last_field_id {
                values.sort_by_key(|(ts, _)| *ts);
                utils::dedup_front_by_key(&mut values, |(ts, _)| *ts);
                let encoding = DataBlockEncoding::new(Encoding::Default, value_encoding);
                writer
                    .write_field(last_fid, &values, value_type, encoding, self)
                    .await?;
            }
        }
        drop(values);

        // Flush the wrote files.
        writer.finish().await
    }

    async fn new_tsm_writer(&self) -> Result<TsmWriter> {
        tsm::new_tsm_writer(
            &self.path_delta,
            self.global_context.file_id_next(),
            true,
            0,
        )
        .await
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
        let (storage_opt, database) = {
            let tsf_rlock = tsf.read().await;
            tsf_rlock.update_last_modified().await;
            (tsf_rlock.storage_opt(), tsf_rlock.database())
        };

        let path_delta = storage_opt.delta_dir(&database, req.ts_family_id);

        let flush_task = FlushTask::new(
            req.mems.clone(),
            req.ts_family_id,
            global_context.clone(),
            path_delta,
        );

        if let Err(e) = flush_task.run(&mut version_edits, &mut file_metas).await {
            error!(
                "Flush: Failed to flush ts_family {}, {}",
                req.ts_family_id, e
            );
        }

        tsf.read().await.update_last_modified().await;

        if let Some(sender) = compact_task_sender.as_ref() {
            let _ = sender.send(CompactTask::Delta(req.ts_family_id)).await;
        }
    }

    // If there are no data flushed, but it's a force flush,
    // advance the global seq_no context and return, do not need to update the summary.
    if version_edits.is_empty() && req.force_flush {
        let global_max_seq_no = global_sequence_context.max_seq();
        let mut vnode_id_max_seq_no = HashMap::with_capacity(1);
        vnode_id_max_seq_no.insert(req.ts_family_id, global_max_seq_no);
        global_sequence_context.next_stage(None, Some(vnode_id_max_seq_no));
        return Ok(());
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
    max_data_block_size: usize,

    /// Buffers of level-0 and level-1 data blocks:
    ///
    /// Each variant of DataBlock will be insert to a hard-coded index of buffers:
    /// `[ [ Float, Integer, Unsigned, Boolean, Bytes ]; 2 ]`
    buffers: [DataBlock; 5],
    /// Pointer to leve-0 and level-1 TSM writers.
    writer: Option<TsmWriter>,
}

impl WriterWrapper {
    pub fn new(ts_family_id: TseriesFamilyId, max_data_block_size: usize) -> Self {
        let data_block_buffers = [
            DataBlock::new(0, ValueType::Float),
            DataBlock::new(0, ValueType::Integer),
            DataBlock::new(0, ValueType::Unsigned),
            DataBlock::new(0, ValueType::Boolean),
            DataBlock::new(0, ValueType::String),
        ];
        Self {
            ts_family_id,
            max_data_block_size,

            buffers: data_block_buffers,
            writer: None,
        }
    }

    pub async fn write_field(
        &mut self,
        field_id: FieldId,
        values: &[(Timestamp, FieldVal)],
        value_type: ValueType,
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

        // flush all data to delta level
        // Fill buffer and write to disk if buffer is full.
        for (ts, val) in values {
            let buffer = &mut self.buffers[buf_idx];
            buffer.insert(val.data_value(*ts));
            if buffer.len() > self.max_data_block_size {
                buffer.set_encoding(encoding);
                Self::write_tsm(&mut self.writer, flush_task, field_id, buffer).await?;
                buffer.clear();
            }
        }
        // Write the remaining data to disk.
        let buffer = &mut self.buffers[buf_idx];
        if !buffer.is_empty() {
            buffer.set_encoding(encoding);
            Self::write_tsm(&mut self.writer, flush_task, field_id, buffer).await?;
            buffer.clear();
        }

        Ok(())
    }

    async fn write_tsm(
        writer_opt: &mut Option<TsmWriter>,
        flush_task: &FlushTask,
        field_id: FieldId,
        data_block: &DataBlock,
    ) -> Result<usize> {
        let writer = match writer_opt.as_mut() {
            Some(w) => w,
            None => {
                let writer = flush_task.new_tsm_writer().await?;
                info!("Flush: File: {} been created (level=0).", writer.sequence(),);
                writer_opt.insert(writer)
            }
        };
        writer
            .write_block(field_id, data_block)
            .await
            .context(error::WriteTsmSnafu)
    }

    pub async fn finish(&mut self) -> Result<Option<(CompactMeta, Arc<BloomFilter>)>> {
        let mut compact_meta_opt = None;

        // While delta_writer is level-0, tsm_writer is level-1.
        let compact_meta_builder = CompactMetaBuilder::new(self.ts_family_id);
        if let Some(w) = self.writer.as_mut() {
            w.write_index().await.context(error::WriteTsmSnafu)?;
            w.finish().await.context(error::WriteTsmSnafu)?;
            info!(
                "Flush: File: {} write finished (level: 0, {} B).",
                w.sequence(),
                w.size()
            );
            compact_meta_opt = Some((
                compact_meta_builder.build(w.sequence(), w.size(), 0_u32, w.min_ts(), w.max_ts()),
                Arc::new(w.bloom_filter_cloned()),
            ));
        }

        Ok(compact_meta_opt)
    }
}

#[cfg(test)]
pub mod flush_tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use memory_pool::{GreedyMemoryPool, MemoryPoolRef};
    use minivec::{mini_vec, MiniVec};
    use models::codec::Encoding;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{utils as model_utils, ColumnId, FieldId, Timestamp, ValueType};
    use parking_lot::RwLock;
    use utils::dedup_front_by_key;

    use super::FlushTask;
    use crate::compaction::flush::WriterWrapper;
    use crate::context::GlobalContext;
    use crate::file_utils;
    use crate::memcache::test::{put_rows_to_cache, PutNone};
    use crate::memcache::{FieldVal, MemCache};
    use crate::tsm::codec::DataBlockEncoding;
    use crate::tsm::test::read_and_check;
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
        {
            let mut data = vec![(1, 11), (1, 12), (2, 21), (3, 3), (2, 22), (4, 41), (4, 42)];
            data.sort_by_key(|a| a.0);
            assert_eq!(
                &data,
                &vec![(1, 11), (1, 12), (2, 21), (2, 22), (3, 3), (4, 41), (4, 42)]
            );
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(&data, &vec![(1, 12), (2, 22), (3, 3), (4, 42)]);
        }
        {
            // Test dedup-front for list with no duplicated key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (2, "b2".into()),
                (3, "c3".into()),
                (4, "d4".into()),
            ];
            data.sort_by_key(|a| a.0);
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(
                &data,
                &vec![
                    (1, "a1".into()),
                    (2, "b2".into()),
                    (3, "c3".into()),
                    (4, "d4".into()),
                ]
            );
        }
        {
            // Test dedup-front for list with only one key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (1, "a2".into()),
                (1, "a3".into()),
                (1, "a4".into()),
            ];
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(&data, &vec![(1, "a4".into()),]);
        }
        {
            // Test dedup-front for list with shuffled multiply duplicated key.
            let mut data: Vec<(i32, MiniVec<u8>)> = vec![
                (1, "a1".into()),
                (1, "b1".into()),
                (2, "c2".into()),
                (3, "d3".into()),
                (2, "e2".into()),
                (4, "e4".into()),
                (4, "f4".into()),
            ];
            data.sort_by_key(|a| a.0);
            dedup_front_by_key(&mut data, |a| a.0);
            assert_eq!(
                &data,
                &vec![
                    (1, "b1".into()),
                    (2, "e2".into()),
                    (3, "d3".into()),
                    (4, "f4".into()),
                ]
            );
        }
    }

    #[tokio::test]
    async fn test_flush() {
        trace::init_default_global_tracing("/tmp/test/flush/log", "tskv.log", "debug");
        {
            let case = flush_test_case_1(10);
            case.run("case_1", "/tmp/test/flush/test_flush/1".into())
                .await;
        }
        {
            let case = flush_test_case_2(10);
            case.run("case_2", "/tmp/test/flush/test_flush/2".into())
                .await;
        }
    }

    struct FlushTestCase {
        caches: Vec<Arc<RwLock<MemCache>>>,
        max_level_ts_before: Timestamp,
        max_level_ts_after: Timestamp,
        /// Expected delta file data of each compact metas.
        expected_delta_data: Vec<Vec<(FieldId, Vec<DataBlock>)>>,
        expected_file_size: Vec<u64>,
        expected_min_ts: Vec<Timestamp>,
        expected_max_ts: Vec<Timestamp>,
    }

    impl FlushTestCase {
        pub fn caches(&self) -> Vec<Arc<RwLock<MemCache>>> {
            self.caches.to_vec()
        }

        pub fn add_files_num(&self) -> usize {
            self.expected_file_size.len()
        }

        pub async fn run(&self, case_name: &str, base_dir: PathBuf) {
            let mut config = config::get_config_for_test();
            config.storage.path = base_dir.to_str().unwrap().to_string();
            config.log.path = base_dir.join("log").to_str().unwrap().to_string();

            let _ = std::fs::remove_dir_all(&base_dir);
            std::fs::create_dir_all(&base_dir).unwrap();
            let delta_dir = base_dir.join("delta");

            let global_context = Arc::new(GlobalContext::new());
            let flush_task = FlushTask::new(self.caches(), 1, global_context, &delta_dir);
            let mut version_edits = vec![];
            let mut file_metas = HashMap::new();
            flush_task
                .run(&mut version_edits, &mut file_metas)
                .await
                .unwrap();

            assert_eq!(version_edits.len(), 1, "In case {}", case_name);
            let ve = version_edits.first().unwrap();
            assert_eq!(
                ve.max_level_ts, self.max_level_ts_after,
                "In case {}",
                case_name
            );
            assert_eq!(
                ve.add_files.len(),
                self.add_files_num(),
                "In case {}",
                case_name
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
                    cm.file_size, self.expected_file_size[i],
                    "In case {}",
                    case_name
                );
                assert_eq!(cm.min_ts, self.expected_min_ts[i], "In case {}", case_name);
                assert_eq!(cm.max_ts, self.expected_max_ts[i], "In case {}", case_name);
                let file_path = file_utils::make_delta_file_name(&delta_dir, cm.file_id);
                let tsm_reader = TsmReader::open(file_path).await.unwrap();
                read_and_check(&tsm_reader, &self.expected_delta_data[i]).await;
            }
        }
    }

    fn flush_test_case_1(max_level_ts: Timestamp) -> FlushTestCase {
        let memory_pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let caches = vec![
            MemCache::new(1, 16, 2, 0, &memory_pool),
            MemCache::new(1, 16, 2, 0, &memory_pool),
            MemCache::new(1, 16, 2, 0, &memory_pool),
        ];
        // |     === Cache: 0 ===     |
        // | ===== | = Series: 1 | = Series: 1
        // | ===== | = Schema: 1 | = Schema: 1
        // | Col_0 | 3, 4        | 1, 2, 3
        // | Col_1 | 3, 4        | 1, 2, 3
        // | Col_2 | 3, 4        | 1, 2, 4
        // | Col_3 | _, _        | _, _, _
        // |     === Cache: 1 ===     |
        // | ===== | = Series: 1  | = Series: 1
        // | ===== | = Schema: 2  | = Schema: 2
        // | Col_0 | 5, 6         | 6, 7, 8
        // | Col_1 | 5, 6         | 6, 7, 8
        // | Col_2 | 5, 6         | 6, 7, 8
        // | Col_3 | 5, 6         | 6, 7, 8
        #[rustfmt::skip]
        let _skip_fmt = {
            put_rows_to_cache(&caches[0], 1, 1, default_table_schema(vec![0, 1, 2, 3]), (3, 4), PutNone::Some([3_usize].into()));
            put_rows_to_cache(&caches[0], 1, 1, default_table_schema(vec![0, 1, 2, 3]), (1, 3), PutNone::Some([3_usize].into()));
            put_rows_to_cache(&caches[1], 1, 1, default_table_schema(vec![0, 1, 2, 3]), (5, 6), PutNone::False);
            put_rows_to_cache(&caches[1], 1, 1, default_table_schema(vec![0, 1, 2, 3]), (6, 8), PutNone::False);
            "skip_fmt"
        };
        let caches = caches
            .into_iter()
            .map(|c| Arc::new(RwLock::new(c)))
            .collect();

        // | === SeriesId: 1 === |
        // Ts:    1,    2,    3,    4,    5,    6,    7,    8
        // Col_0: 1,    2,    3,    4,    5,    6,    7,    8
        // Col_1: 1,    2,    3,    4,    5,    6,    7,    8
        // Col_2: 1,    2,    3,    4,    5,    6,    7,    8
        // Col_3: None, None, None, None, 5,    6,    7,    8
        #[rustfmt::skip]
        let mut expected_delta_data: Vec<(FieldId, Vec<DataBlock>)> = vec![
            (model_utils::unite_id(0, 1), vec![DataBlock::F64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(1, 1), vec![DataBlock::F64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(2, 1), vec![DataBlock::F64 { ts: vec![1, 2, 3, 4, 5, 6, 7, 8], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(3, 1), vec![DataBlock::F64 { ts: vec![5, 6, 7, 8], val: vec![5.0, 6.0, 7.0, 8.0], enc: DataBlockEncoding::default() }]),
        ];
        expected_delta_data.sort_by_key(|a| a.0);

        FlushTestCase {
            caches,
            max_level_ts_before: max_level_ts,
            max_level_ts_after: 8,
            expected_delta_data: vec![expected_delta_data],
            expected_file_size: vec![131512],
            expected_min_ts: vec![1],
            expected_max_ts: vec![8],
        }
    }

    fn flush_test_case_2(max_level_ts: Timestamp) -> FlushTestCase {
        let memory_pool: MemoryPoolRef = Arc::new(GreedyMemoryPool::new(1024 * 1024));
        let caches = vec![
            MemCache::new(1, 16, 2, 0, &memory_pool),
            MemCache::new(1, 16, 2, 0, &memory_pool),
            MemCache::new(1, 16, 2, 0, &memory_pool),
        ];
        // | === Cache: 0 === |
        // | = | === Series: 1 | === Series: 1 | === Series: 1
        // | = | === Schema: 1 | === Schema: 2 | === Schema: 3
        // | = | Col_0: 3, 4   | Col_0: 1, 2   | Col_0: 5, 5, 6
        // | = | Col_1: 3, 4   | Col_1: 1, 2   | Col_1: 5, 5, 6
        // | = | Col_2: 3, 4   |               | Col_2: 5, 5, 6
        // | = |                 Col_3: 1, 2   | Col_3: 5, 5, 6
        // | === Cache: 1 === |
        // | = | === Series: 2 | === Series: 2 | === Series: 2
        // | = | === Schema: 1 | === Schema: 2 | === Schema: 3
        // | = | Col_0: 9, 10  | Col_0: 7, 8   | Col_0: 11, 11, 12
        // | = | Col_1: 9, 10  | Col_1: 7, 8   | Col_1: 11, 11, 12
        // | = | Col_2: 9, 10  |               | Col_2: 11, 11, 12
        // | = |                 Col_3: 7, 8   | Col_3: 11, 11, 12
        // | === Cache: 2 === |
        // | = | === Series: 3 | === Series: 3 | === Series: 3
        // | = | === Schema: 1 | === Schema: 2 | === Schema: 3
        // | = | Col_0: 15, 16 | Col_0: 13, 14 | Col_0: 17, 17, 18
        // | = | Col_1: 15, 16 | Col_1: 13, 14 | Col_1: 17, 17, 18
        // | = | Col_2: 15, 16 |               | Col_2: 17, 17, 18
        // | = |                 Col_3: 13, 14 | Col_3: 17, 17, 18
        #[rustfmt::skip]
        let _skip_fmt = {
            put_rows_to_cache(&caches[0], 1, 1, default_table_schema(vec![0, 1, 2]), (3, 4), PutNone::False);
            put_rows_to_cache(&caches[0], 1, 2, default_table_schema(vec![0, 1, 3]), (1, 2), PutNone::False);
            put_rows_to_cache(&caches[0], 1, 3, default_table_schema(vec![0, 1, 2, 3]), (5, 5), PutNone::True);
            put_rows_to_cache(&caches[0], 1, 3, default_table_schema(vec![0, 1, 2, 3]), (5, 6), PutNone::False);
            put_rows_to_cache(&caches[1], 2, 1, default_table_schema(vec![0, 1, 2]), (9, 10), PutNone::False);
            put_rows_to_cache(&caches[1], 2, 2, default_table_schema(vec![0, 1, 3]), (7, 8), PutNone::False);
            put_rows_to_cache(&caches[1], 2, 3, default_table_schema(vec![0, 1, 2, 3]), (11, 11), PutNone::True);
            put_rows_to_cache(&caches[1], 2, 3, default_table_schema(vec![0, 1, 2, 3]), (11, 12), PutNone::False);
            put_rows_to_cache(&caches[2], 3, 1, default_table_schema(vec![0, 1, 2]), (15, 16), PutNone::False);
            put_rows_to_cache(&caches[2], 3, 2, default_table_schema(vec![0, 1, 3]), (13, 14), PutNone::False);
            put_rows_to_cache(&caches[2], 3, 3, default_table_schema(vec![0, 1, 2, 3]), (17, 17), PutNone::True);
            put_rows_to_cache(&caches[2], 3, 3, default_table_schema(vec![0, 1, 2, 3]), (17, 18), PutNone::False);
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
        // Ts:    7,    8,    9,    10,   11, 12
        // Col_0: 7,    8,    9,    10,   11, 12
        // Col_1: 7,    8,    9,    10,   11, 12
        // Col_2: None, None, 9,    10,   11, 12
        // Col_3: 7,    8,    None, None, 11, 12
        // | === SeriesId: 3 === |
        // Ts:    13,   14,   15,   16,   16,   17, 18
        // Col_0: 13,   14,   15,   16,   None, 17, 18
        // Col_1: 13,   14,   15,   16,   None, 17, 18
        // Col_2: None, None, 15,   16,   None, 17, 18
        // Col_3: 13,   14,   None, None, None, 17, 18
        #[rustfmt::skip]
        let mut expected_delta_data: Vec<(FieldId, Vec<DataBlock>)> = vec![
            (model_utils::unite_id(0, 1), vec![DataBlock::F64 { ts: vec![1, 2, 3, 4, 5, 6], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(1, 1), vec![DataBlock::F64 { ts: vec![1, 2, 3, 4, 5, 6], val: vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(2, 1), vec![DataBlock::F64 { ts: vec![3, 4, 5, 6], val: vec![3.0, 4.0, 5.0, 6.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(3, 1), vec![DataBlock::F64 { ts: vec![1, 2, 5, 6], val: vec![1.0, 2.0, 5.0, 6.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(0, 2), vec![DataBlock::F64 { ts: vec![7, 8, 9, 10, 11, 12], val: vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(1, 2), vec![DataBlock::F64 { ts: vec![7, 8, 9, 10, 11, 12], val: vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(2, 2), vec![DataBlock::F64 { ts: vec![9, 10, 11, 12], val: vec![9.0, 10.0, 11.0, 12.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(3, 2), vec![DataBlock::F64 { ts: vec![7, 8, 11, 12], val: vec![7.0, 8.0, 11.0, 12.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(0, 3), vec![DataBlock::F64 { ts: vec![13, 14, 15, 16, 17, 18], val: vec![13.0, 14.0, 15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(1, 3), vec![DataBlock::F64 { ts: vec![13, 14, 15, 16, 17, 18], val: vec![13.0, 14.0, 15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(2, 3), vec![DataBlock::F64 { ts: vec![15, 16, 17, 18], val: vec![15.0, 16.0, 17.0, 18.0], enc: DataBlockEncoding::default() }]),
            (model_utils::unite_id(3, 3), vec![DataBlock::F64 { ts: vec![13, 14, 17, 18], val: vec![13.0, 14.0, 17.0, 18.0], enc: DataBlockEncoding::default() }]),
        ];
        expected_delta_data.sort_by_key(|a| a.0);

        FlushTestCase {
            caches,
            max_level_ts_before: max_level_ts,
            max_level_ts_after: 18,
            expected_delta_data: vec![expected_delta_data],
            expected_file_size: vec![132330],
            expected_min_ts: vec![1],
            expected_max_ts: vec![18],
        }
    }

    #[tokio::test]
    async fn test_writer_wrapper() {
        let dir = PathBuf::from("/tmp/test/flush/test_write_wrapper");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let delta_dir = dir.join("delta");

        let ts_family_id = 1;
        let max_level_ts = 10;
        let global_context = Arc::new(GlobalContext::new());
        let flush_task = FlushTask::new(vec![], ts_family_id, global_context.clone(), &delta_dir);

        let col_enc: HashMap<ColumnId, Encoding> = HashMap::from([
            (1, Encoding::Default),
            (2, Encoding::Default),
            (3, Encoding::Default),
            (4, Encoding::Default),
            (5, Encoding::Default),
        ]);

        let mut writer = WriterWrapper::new(ts_family_id, 1000);
        let mut expected_delta_data = Vec::new();
        for sid in [1, 2, 3] {
            #[rustfmt::skip]
            write_one_field(&mut writer, &flush_task, &col_enc, max_level_ts,
                model_utils::unite_id(1, sid), FieldVal::Float(1.0), &mut expected_delta_data).await;
            #[rustfmt::skip]
            write_one_field(&mut writer, &flush_task, &col_enc, max_level_ts,
                model_utils::unite_id(2, sid), FieldVal::Integer(1), &mut expected_delta_data).await;
            #[rustfmt::skip]
            write_one_field(&mut writer, &flush_task, &col_enc, max_level_ts,
                model_utils::unite_id(3, sid), FieldVal::Unsigned(1), &mut expected_delta_data).await;
            #[rustfmt::skip]
            write_one_field(&mut writer, &flush_task, &col_enc, max_level_ts,
                model_utils::unite_id(4, sid), FieldVal::Boolean(true), &mut expected_delta_data).await;
            #[rustfmt::skip]
            write_one_field(&mut writer, &flush_task, &col_enc, max_level_ts,
                model_utils::unite_id(5, sid), FieldVal::Bytes(mini_vec![1, 1]), &mut expected_delta_data).await;
        }
        expected_delta_data.sort_by_key(|a| a.0);

        let compact_meta = writer.finish().await.unwrap();
        assert!(compact_meta.is_some());
        let (compact_meta, _) = compact_meta.unwrap();
        assert_eq!(compact_meta.level, 0);

        let delta_file_name = file_utils::make_delta_file_name(&delta_dir, compact_meta.file_id);
        let delta_reader = TsmReader::open(delta_file_name).await.unwrap();
        read_and_check(&delta_reader, &expected_delta_data).await;
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
        expected_delta_data: &mut Vec<(FieldId, Vec<DataBlock>)>,
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
        delta_data_block.insert(field_val.data_value(max_level_ts + 1));
        delta_data_block.insert(field_val.data_value(max_level_ts + 2));
        delta_data_block.insert(field_val.data_value(max_level_ts + 3));
        expected_delta_data.push((field_id, vec![delta_data_block]));

        writer
            .write_field(field_id, &values, value_type, encoding, flush_task)
            .await
            .unwrap();
    }
}
