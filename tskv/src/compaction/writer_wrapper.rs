use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use utils::BloomFilter;

use crate::compaction::{CompactReq, CompactTask, CompactingBlock};
use crate::context::GlobalContext;
use crate::summary::CompactMeta;
use crate::tsm::writer::TsmWriter;
use crate::{ColumnFileId, LevelId, TskvResult, VersionEdit};

pub struct WriterWrapper {
    // Init values.
    context: Arc<GlobalContext>,
    compact_task: CompactTask,
    out_level: LevelId,
    tsm_dir: PathBuf,

    max_level_ts: i64,

    // Temporary values.
    tsm_writer: Option<TsmWriter>,

    // Result values.
    version_edit: VersionEdit,
    file_metas: HashMap<ColumnFileId, Arc<BloomFilter>>,
}

impl WriterWrapper {
    pub async fn new(request: &CompactReq, context: Arc<GlobalContext>) -> TskvResult<Self> {
        let vnode_id = request.compact_task.vnode_id();
        let storage_opt = request.version.storage_opt();
        let tsm_dir = storage_opt.tsm_dir(request.version.owner().as_str(), vnode_id);
        Ok(Self {
            context,
            compact_task: request.compact_task,
            out_level: request.out_level,
            tsm_dir,
            max_level_ts: request.version.max_level_ts(),

            tsm_writer: None,

            version_edit: VersionEdit::new(vnode_id),
            file_metas: HashMap::new(),
        })
    }

    pub async fn close(
        mut self,
    ) -> TskvResult<(VersionEdit, HashMap<ColumnFileId, Arc<BloomFilter>>)> {
        if let Some(mut tsm_writer) = self.tsm_writer {
            tsm_writer.finish().await?;

            trace::info!(
                "Compaction({}): File: {} write finished (level: {}, {} B).",
                self.compact_task,
                tsm_writer.file_id(),
                self.out_level,
                tsm_writer.size()
            );

            let file_id = tsm_writer.file_id();
            let cm = CompactMeta {
                file_id,
                file_size: tsm_writer.size(),
                tsf_id: self.compact_task.vnode_id(),
                level: self.out_level,
                min_ts: tsm_writer.min_ts(),
                max_ts: tsm_writer.max_ts(),
                is_delta: false,
            };
            self.version_edit.add_file(cm, self.max_level_ts);
            let bloom_filter = tsm_writer.into_series_bloom_filter();
            self.file_metas.insert(file_id, Arc::new(bloom_filter));
        }

        Ok((self.version_edit, self.file_metas))
    }

    pub async fn writer(&mut self) -> TskvResult<&mut TsmWriter> {
        if self.tsm_writer.is_none() {
            let file_id = self.context.file_id_next();
            let tsm_writer = TsmWriter::open(&self.tsm_dir, file_id, 0, false).await?;
            trace::info!(
                "Compaction({}): File: {file_id} been created (level: {}).",
                self.compact_task,
                self.out_level,
            );
            self.tsm_writer = Some(tsm_writer);
        }
        Ok(self.tsm_writer.as_mut().unwrap())
    }

    /// Write CompactingBlock to TsmWriter, fill file_metas and version_edit.
    pub async fn write(&mut self, blk: CompactingBlock) -> TskvResult<()> {
        self.writer().await?.write_compacting_block(blk).await
    }
}
