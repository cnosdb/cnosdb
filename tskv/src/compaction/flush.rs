use std::cmp::max;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::oneshot;
use tokio::time::timeout;
use trace::{error, info, warn};
use utils::BloomFilter;

use crate::compaction::{CompactTask, FlushReq};
use crate::context::GlobalContext;
use crate::error::Result;
use crate::memcache::MemCache;
use crate::summary::{CompactMetaBuilder, SummaryTask, VersionEdit};
use crate::tseries_family::Version;
use crate::tsm::writer::TsmWriter;
use crate::{ColumnFileId, Error, TsKvContext, TseriesFamilyId};

pub struct FlushTask {
    ts_family_id: TseriesFamilyId,
    mem_caches: Vec<Arc<RwLock<MemCache>>>,
    low_seq_no: u64,
    high_seq_no: u64,
    global_context: Arc<GlobalContext>,
    path_tsm: PathBuf,
    path_delta: PathBuf,
}

impl FlushTask {
    pub fn new(
        ts_family_id: TseriesFamilyId,
        mem_caches: Vec<Arc<RwLock<MemCache>>>,
        low_seq_no: u64,
        high_seq_no: u64,
        global_context: Arc<GlobalContext>,
        path_tsm: impl AsRef<Path>,
        path_delta: impl AsRef<Path>,
    ) -> Self {
        Self {
            ts_family_id,
            mem_caches,
            low_seq_no,
            high_seq_no,
            global_context,
            path_tsm: path_tsm.as_ref().into(),
            path_delta: path_delta.as_ref().into(),
        }
    }

    pub async fn run(
        self,
        version: Arc<Version>,
    ) -> Result<(VersionEdit, HashMap<u64, Arc<BloomFilter>>)> {
        let mut tsm_writer = None;
        let mut delta_writer = None;
        let mut file_metas = HashMap::new();

        for memcache in self.mem_caches {
            let (group, delta_group) = memcache.read().to_chunk_group(version.clone())?;
            if tsm_writer.is_none() && !group.is_empty() {
                tsm_writer = Some(
                    TsmWriter::open(&self.path_tsm, self.global_context.file_id_next(), 0, false)
                        .await?,
                );
            }
            if delta_writer.is_none() && !delta_group.is_empty() {
                delta_writer = Some(
                    TsmWriter::open(
                        &self.path_delta,
                        self.global_context.file_id_next(),
                        0,
                        true,
                    )
                    .await?,
                );
            }
            if let Some(tsm_writer) = tsm_writer.as_mut() {
                tsm_writer.write_data(group).await?;
            }
            if let Some(delta_writer) = delta_writer.as_mut() {
                delta_writer.write_data(delta_group).await?;
            }
        }

        let compact_meta_builder = CompactMetaBuilder::new(self.ts_family_id);
        let mut max_level_ts = version.max_level_ts();
        let mut edit = VersionEdit::new(self.ts_family_id);

        if let Some(mut tsm_writer) = tsm_writer {
            tsm_writer.finish().await?;
            file_metas.insert(
                tsm_writer.file_id(),
                Arc::new(tsm_writer.series_bloom_filter().clone()),
            );
            let tsm_meta = compact_meta_builder.build(
                tsm_writer.file_id(),
                tsm_writer.size() as u64,
                1,
                tsm_writer.min_ts(),
                tsm_writer.max_ts(),
            );
            max_level_ts = max(max_level_ts, tsm_meta.max_ts);
            edit.add_file(tsm_meta, max_level_ts);
        }

        if let Some(mut delta_writer) = delta_writer {
            delta_writer.finish().await?;
            file_metas.insert(
                delta_writer.file_id(),
                Arc::new(delta_writer.series_bloom_filter().clone()),
            );

            let delta_meta = compact_meta_builder.build(
                delta_writer.file_id(),
                delta_writer.size() as u64,
                0,
                delta_writer.min_ts(),
                delta_writer.max_ts(),
            );

            max_level_ts = max(max_level_ts, delta_meta.max_ts);
            edit.add_file(delta_meta, max_level_ts);
        }

        Ok((edit, file_metas))
    }
}

pub async fn run_flush_memtable_job(
    req: FlushReq,
    ctx: Arc<TsKvContext>,
    trigger_compact: bool,
) -> Result<Option<VersionEdit>> {
    let req_str = format!("{req}");
    info!("Flush: running: {req_str}");

    let mut version_edit = None;
    let mut file_metas: HashMap<ColumnFileId, Arc<BloomFilter>> = HashMap::new();

    let tsf = ctx
        .version_set
        .read()
        .await
        .get_tsfamily_by_tf_id(req.ts_family_id)
        .await
        .ok_or(Error::VnodeNotFound {
            vnode_id: req.ts_family_id,
        })?;

    // todo: build path by vnode data
    let (storage_opt, version, database) = {
        let tsf_rlock = tsf.read().await;
        tsf_rlock.update_last_modified().await;
        (
            tsf_rlock.storage_opt(),
            tsf_rlock.version(),
            tsf_rlock.tenant_database(),
        )
    };

    let path_tsm = storage_opt.tsm_dir(&database, req.ts_family_id);
    let path_delta = storage_opt.delta_dir(&database, req.ts_family_id);

    let flush_task = FlushTask::new(
        req.ts_family_id,
        req.mems.clone(),
        req.low_seq_no,
        req.high_seq_no,
        ctx.global_ctx.clone(),
        path_tsm,
        path_delta,
    );

    if let Ok((ve, fm)) = flush_task.run(version.clone()).await {
        let _ = version_edit.insert(ve);
        file_metas = fm;
    }

    tsf.read().await.update_last_modified().await;
    if trigger_compact {
        let _ = ctx
            .compact_task_sender
            .send(CompactTask::Vnode(req.ts_family_id))
            .await;
    }

    // If there are no data to be flushed but it's a force flush,
    // just write an empty VersionEdit with the max seq_no to the summary.
    if version_edit.is_none() && req.force_flush {
        let mut ve = VersionEdit::new(req.ts_family_id);
        ve.has_seq_no = true;
        ve.seq_no = 0; // Fixme
        let _ = version_edit.insert(ve);
    }

    info!(
        "Flush: completed: {req_str}, version edit: {:?}",
        version_edit
    );

    if let Some(ref ve) = version_edit {
        let (task_state_sender, task_state_receiver) = oneshot::channel();
        let task = SummaryTask::new(
            tsf.clone(),
            ve.clone(),
            Some(file_metas),
            Some(req.mems),
            task_state_sender,
        );

        if let Err(e) = ctx.summary_task_sender.send(task).await {
            warn!("Flush: failed to send summary task for {req_str}: {e}",);
        }

        if timeout(Duration::from_secs(10), task_state_receiver)
            .await
            .is_err()
        {
            error!("Flush: failed to receive summary task result in 10 seconds for {req_str}",);
        }
    }

    Ok(version_edit)
}

#[cfg(test)]
pub mod flush_tests {
    use minivec::MiniVec;
    use models::codec::Encoding;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{ColumnId, ValueType};
    use utils::dedup_front_by_key;

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
        // todo!("test_flush");
    }
}
