use std::cmp::max;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::oneshot;
use tokio::time::timeout;
use trace::{error, info, warn};
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::context::GlobalContext;
use crate::error::TskvResult;
use crate::file_system::async_filesystem::LocalFileSystem;
use crate::mem_cache::memcache::MemCache;
use crate::summary::{CompactMetaBuilder, SummaryTask, VersionEdit};
use crate::tsm::writer::TsmWriter;
use crate::{TsKvContext, TseriesFamilyId};

pub struct FlushTask {
    owner: String,
    tsf_id: TseriesFamilyId,
    mem_caches: Vec<Arc<RwLock<MemCache>>>,

    tsm_writer: TsmWriter,
    delta_writer: TsmWriter,
}

impl FlushTask {
    pub async fn new(
        owner: String,
        tsf_id: TseriesFamilyId,
        mem_caches: Vec<Arc<RwLock<MemCache>>>,
        global_context: Arc<GlobalContext>,
        path_tsm: impl AsRef<Path>,
        path_delta: impl AsRef<Path>,
    ) -> TskvResult<Self> {
        let file_id = global_context.file_id_next();
        let tsm_writer = TsmWriter::open(&path_tsm, file_id, 0, false).await?;
        let file_id = global_context.file_id_next();
        let delta_writer = TsmWriter::open(&path_delta, file_id, 0, true).await?;

        Ok(Self {
            owner,
            tsf_id,
            mem_caches,
            tsm_writer,
            delta_writer,
        })
    }

    pub fn clear_files(&mut self) {
        if let Err(err) = LocalFileSystem::remove_if_exists(self.tsm_writer.path()) {
            info!(
                "delete flush tsm file: {:?} failed: {}",
                self.tsm_writer.path(),
                err
            );
        }

        if let Err(err) = LocalFileSystem::remove_if_exists(self.delta_writer.path()) {
            info!(
                "delete flush tsm file: {:?} failed: {}",
                self.delta_writer.path(),
                err
            );
        }
    }

    pub async fn run(
        &mut self,
        max_level_ts: i64,
        high_seq_no: u64,
    ) -> TskvResult<(VersionEdit, HashMap<u64, Arc<BloomFilter>>)> {
        let mut files_meta = HashMap::new();
        let mut tsm_writer_is_used = false;
        let mut delta_writer_is_used = false;
        let mut version_edit =
            VersionEdit::new_update_vnode(self.tsf_id, self.owner.clone(), high_seq_no);
        for memcache in self.mem_caches.iter() {
            let (group, delta_group) = memcache.read().to_chunk_group(max_level_ts)?;
            if !group.is_empty() {
                tsm_writer_is_used = true;
                self.tsm_writer.write_data(group).await?;
            }

            if !delta_group.is_empty() {
                delta_writer_is_used = true;
                self.delta_writer.write_data(delta_group).await?;
            }
        }

        let mut max_level_ts = max_level_ts;
        let compact_meta_builder = CompactMetaBuilder::new(self.tsf_id);
        if tsm_writer_is_used {
            self.tsm_writer.finish().await?;
            files_meta.insert(
                self.tsm_writer.file_id(),
                Arc::new(self.tsm_writer.series_bloom_filter().clone()),
            );
            let tsm_meta = compact_meta_builder.build(
                self.tsm_writer.file_id(),
                self.tsm_writer.size(),
                1,
                self.tsm_writer.min_ts(),
                self.tsm_writer.max_ts(),
            );
            max_level_ts = max(max_level_ts, tsm_meta.max_ts);
            version_edit.add_file(tsm_meta, max_level_ts);
        } else {
            let path = self.tsm_writer.path();
            let result = LocalFileSystem::remove_if_exists(path);
            info!("Flush: remove unsed file: {:?}, {:?}", path, result);
        }

        if delta_writer_is_used {
            self.delta_writer.finish().await?;
            files_meta.insert(
                self.delta_writer.file_id(),
                Arc::new(self.delta_writer.series_bloom_filter().clone()),
            );

            let delta_meta = compact_meta_builder.build(
                self.delta_writer.file_id(),
                self.delta_writer.size(),
                0,
                self.delta_writer.min_ts(),
                self.delta_writer.max_ts(),
            );

            max_level_ts = max(max_level_ts, delta_meta.max_ts);
            version_edit.add_file(delta_meta, max_level_ts);
        } else {
            let path = self.delta_writer.path();
            let result = LocalFileSystem::remove_if_exists(path);
            info!("Flush: remove unsed file: {:?}, {:?}", path, result);
        }

        Ok((version_edit, files_meta))
    }
}

pub async fn flush_memtable(
    req: &FlushReq,
    ctx: Arc<TsKvContext>,
    mems: Vec<Arc<RwLock<MemCache>>>,
) -> TskvResult<()> {
    let (mut high_seq_no, mut low_seq_no) = (0, u64::MAX);
    for mem in mems.iter() {
        high_seq_no = high_seq_no.max(mem.read().seq_no());
        low_seq_no = low_seq_no.min(mem.read().min_seq_no());
    }

    info!(
        "Flush: running  {} seq: [{}-{}], memcache count: {}",
        req,
        low_seq_no,
        high_seq_no,
        mems.len()
    );

    // todo: build path by vnode data
    let (storage_opt, max_level_ts) = {
        let tsf_rlock = req.ts_family.read().await;
        tsf_rlock.update_last_modified().await;
        (tsf_rlock.storage_opt(), tsf_rlock.version().max_level_ts())
    };

    let path_tsm = storage_opt.tsm_dir(&req.owner, req.tf_id);
    let path_delta = storage_opt.delta_dir(&req.owner, req.tf_id);
    let mut flush_task = FlushTask::new(
        req.owner.clone(),
        req.tf_id,
        mems.clone(),
        ctx.global_ctx.clone(),
        path_tsm,
        path_delta,
    )
    .await?;

    let result = flush_task.run(max_level_ts, high_seq_no).await;
    let (version_edit, files_meta) = match result {
        Ok((ve, files_meta)) => (ve, files_meta),
        Err(err) => {
            flush_task.clear_files();
            return Err(err);
        }
    };

    info!(
        "Flush: completed: owner: {} tsf_id: {}, version edit: {:?}",
        req.owner, req.tf_id, version_edit
    );

    let (task_state_sender, task_state_receiver) = oneshot::channel();
    let task = SummaryTask::new(
        req.ts_family.clone(),
        version_edit,
        Some(files_meta),
        Some(mems),
        task_state_sender,
    );

    if let Err(e) = ctx.summary_task_sender.send(task).await {
        warn!(
            "Flush: failed to send summary task for tsf_id: {}: {e}",
            req.tf_id
        );
    }

    if timeout(Duration::from_secs(10), task_state_receiver)
        .await
        .is_err()
    {
        error!(
            "Flush: failed to receive summary task result for tsf_id: {}",
            req.tf_id
        );
    }

    if req.trigger_compact {
        let _ = ctx
            .compact_task_sender
            .send(CompactTask { tsf_id: req.tf_id })
            .await;
    }

    Ok(())
}

#[cfg(test)]
pub mod flush_tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow_schema::TimeUnit;
    use cache::ShardedAsyncCache;
    use memory_pool::{GreedyMemoryPool, MemoryPool};
    use minivec::MiniVec;
    use models::codec::Encoding;
    use models::field_value::FieldVal;
    use models::predicate::domain::TimeRange;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesKey, ValueType};
    use parking_lot::lock_api::RwLock;
    use utils::dedup_front_by_key;

    use crate::compaction::flush::FlushTask;
    use crate::context::GlobalContext;
    use crate::mem_cache::memcache::MemCache;
    use crate::mem_cache::row_data::{OrderedRowsData, RowData};
    use crate::mem_cache::series_data::RowGroup;
    use crate::tsfamily::level_info::LevelInfo;
    use crate::tsfamily::version::Version;
    use crate::tsm::data_block::{DataBlock, MutableColumn};
    use crate::tsm::reader::TsmReader;
    use crate::tsm::writer::TsmWriter;
    use crate::Options;

    fn f64_column(data: Vec<f64>) -> MutableColumn {
        let mut col = MutableColumn::empty(TableColumn::new(
            4,
            "f_col_1".to_string(),
            ColumnType::Field(ValueType::Float),
            Encoding::default(),
        ))
        .unwrap();
        for datum in data {
            col.push(Some(FieldVal::Float(datum))).unwrap()
        }
        col
    }

    fn ts_column(data: Vec<i64>) -> MutableColumn {
        let mut col = MutableColumn::empty(TableColumn::new(
            1,
            "time".to_string(),
            ColumnType::Time(TimeUnit::Nanosecond),
            Encoding::default(),
        ))
        .unwrap();
        for datum in data {
            col.push(Some(FieldVal::Integer(datum))).unwrap()
        }
        col
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
    async fn test_flush_run() {
        let dir = "/tmp/test/flush/1";
        let _ = std::fs::remove_dir_all(dir);
        std::fs::create_dir_all(dir).unwrap();
        let mut global_config = config::tskv::get_config_for_test();
        global_config.storage.path = dir.to_string();
        let opt = Arc::new(Options::from(&global_config));

        let database = Arc::new("cnosdb.test".to_string());

        #[rustfmt::skip]
            let levels = [
            LevelInfo::init(database.clone(), 0, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 1, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 2, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 3, 0, opt.storage.clone()),
            LevelInfo::init(database.clone(), 4, 0, opt.storage.clone()),
        ];
        let tsm_reader_cache = Arc::new(ShardedAsyncCache::create_lru_sharded_cache(16));

        let version = Version::new(
            1,
            database.clone(),
            opt.storage.clone(),
            1,
            levels,
            5,
            tsm_reader_cache,
        );
        let sid = 1;
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024));
        let mem_cache = MemCache::new(1, 1000, 2, 1, &memory_pool);
        #[rustfmt::skip]
            let mut schema_1 = TskvTableSchema::new(
            "test_tenant".to_string(), "test_db".to_string(), "test_table".to_string(),
            vec![
                TableColumn::new_time_column(1, TimeUnit::Nanosecond),
                TableColumn::new_tag_column(2, "tag_col_1".to_string()),
                TableColumn::new_tag_column(3, "tag_col_2".to_string()),
                TableColumn::new(4, "f_col_1".to_string(), ColumnType::Field(ValueType::Float), Default::default()),
            ],
        );
        schema_1.schema_version = 1;
        let mut rows = OrderedRowsData::new();
        rows.insert(RowData {
            ts: 1,
            fields: vec![Some(FieldVal::Float(1.0))],
        });
        rows.insert(RowData {
            ts: 3,
            fields: vec![Some(FieldVal::Float(3.0))],
        });
        rows.insert(RowData {
            ts: 6,
            fields: vec![Some(FieldVal::Float(6.0))],
        });
        rows.insert(RowData {
            ts: 9,
            fields: vec![Some(FieldVal::Float(9.0))],
        });
        #[rustfmt::skip]
            let row_group_1 = RowGroup {
            schema: Arc::new(schema_1),
            range: TimeRange::new(1, 3),
            rows,
            size: 10,
        };

        mem_cache
            .write_group(sid, SeriesKey::default(), 1, row_group_1.clone())
            .unwrap();

        let mem_caches = vec![Arc::new(RwLock::new(mem_cache))];
        let path_tsm = PathBuf::from("/tmp/test/flush/tsm1");
        let path_delta = PathBuf::from("/tmp/test/flush/tsm2");
        let mut flush_task = FlushTask::new(
            database.to_string(),
            1,
            mem_caches,
            Arc::new(GlobalContext::new()),
            path_tsm.clone(),
            path_delta.clone(),
        )
        .await
        .unwrap();

        let (edit, _) = flush_task.run(version.max_level_ts(), 100).await.unwrap();

        let tsm_info = edit.add_files.first().unwrap();
        let delta_info = edit.add_files.get(1).unwrap();

        let mut schema = TskvTableSchema::new(
            "test_tenant".to_string(),
            "test_db".to_string(),
            "test_table".to_string(),
            vec![
                TableColumn::new(
                    1,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "tag_col_1".to_string(),
                    ColumnType::Tag,
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "tag_col_2".to_string(),
                    ColumnType::Tag,
                    Encoding::default(),
                ),
                TableColumn::new(
                    4,
                    "f_col_1".to_string(),
                    ColumnType::Field(ValueType::Float),
                    Encoding::default(),
                ),
            ],
        );

        schema.schema_version = 1;
        let schema = Arc::new(schema);
        let data1 = DataBlock::new(
            schema.clone(),
            ts_column(vec![6, 9]),
            vec![f64_column(vec![6.0, 9.0])],
        );

        let data2 = DataBlock::new(
            schema.clone(),
            ts_column(vec![1, 3]),
            vec![f64_column(vec![1.0, 3.0])],
        );

        {
            let tsm_writer = TsmWriter::open(&path_tsm, tsm_info.file_id, 100, false)
                .await
                .unwrap();
            let tsm_reader = TsmReader::open(tsm_writer.path()).await.unwrap();
            let tsm_data = tsm_reader.read_datablock(1, 0).await.unwrap();
            assert_eq!(tsm_data, data1);
        }

        {
            let delta_writer = TsmWriter::open(&path_delta, delta_info.file_id, 100, true)
                .await
                .unwrap();
            let delta_reader = TsmReader::open(delta_writer.path()).await.unwrap();
            let delta_data = delta_reader.read_datablock(1, 0).await.unwrap();
            assert_eq!(delta_data, data2);
        }
    }
}
