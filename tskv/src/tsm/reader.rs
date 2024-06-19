use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::TskvTableSchemaRef;
use models::schema::TIME_FIELD_NAME;
use models::SeriesId;
use snafu::OptionExt;

use crate::error::{CommonSnafu, ReadTsmSnafu, TskvResult};
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::file::stream_reader::FileStreamReader;
use crate::file_system::FileSystem;
use crate::tsm::chunk::Chunk;
use crate::tsm::chunk_group::{ChunkGroup, ChunkGroupMeta};
use crate::tsm::data_block::{DataBlock, MutableColumn};
use crate::tsm::footer::Footer;
use crate::tsm::page::{Page, PageMeta, PageWriteSpec};
use crate::tsm::{ColumnGroupID, TsmTombstone, FOOTER_SIZE};
use crate::{file_utils, TskvError};

pub struct TsmMetaData {
    footer: Arc<Footer>,
    chunk_group_meta: Arc<ChunkGroupMeta>,
    chunk_group: BTreeMap<String, Arc<ChunkGroup>>,
    chunk: BTreeMap<SeriesId, Arc<Chunk>>,
}

impl TsmMetaData {
    pub fn new(
        footer: Arc<Footer>,
        chunk_group_meta: Arc<ChunkGroupMeta>,
        chunk_group: BTreeMap<String, Arc<ChunkGroup>>,
        chunk: BTreeMap<SeriesId, Arc<Chunk>>,
    ) -> Self {
        Self {
            footer,
            chunk_group_meta,
            chunk_group,
            chunk,
        }
    }

    pub fn footer(&self) -> Arc<Footer> {
        self.footer.clone()
    }

    pub fn chunk_group_meta(&self) -> Arc<ChunkGroupMeta> {
        self.chunk_group_meta.clone()
    }

    pub fn chunk_group(&self) -> &BTreeMap<String, Arc<ChunkGroup>> {
        &self.chunk_group
    }

    pub fn chunk(&self) -> &BTreeMap<SeriesId, Arc<Chunk>> {
        &self.chunk
    }

    pub fn table_schema(&self, table_name: &str) -> Option<TskvTableSchemaRef> {
        self.chunk_group_meta.table_schema(table_name)
    }

    pub fn table_schema_by_sid(&self, series_id: SeriesId) -> Option<TskvTableSchemaRef> {
        let table_name = self.table_name(series_id)?;
        self.table_schema(table_name)
    }

    pub fn table_name(&self, series_id: SeriesId) -> Option<&str> {
        for (table_name, series_map) in self.chunk_group.iter() {
            if series_map
                .chunks()
                .iter()
                .any(|c| c.series_id() == series_id)
            {
                return Some(table_name.as_ref());
            }
        }
        None
    }
}

pub struct TsmReader {
    file_id: u64,
    reader: Box<FileStreamReader>,
    tsm_meta: Arc<TsmMetaData>,
    tombstone: Arc<TsmTombstone>,
}

impl TsmReader {
    pub async fn open(tsm_path: impl AsRef<Path>) -> TskvResult<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let file_system = LocalFileSystem::new(LocalFileType::ThreadPool);
        let reader = file_system
            .open_file_reader(&path)
            .await
            .map_err(|e| TskvError::FileSystemError { source: e })?;

        let file_id = file_utils::get_tsm_file_id_by_path(&path)?;

        let footer = Arc::new(read_footer(&reader).await?);
        let chunk_group_meta = Arc::new(read_chunk_group_meta(&reader, &footer).await?);
        let chunk_group = read_chunk_groups(&reader, &chunk_group_meta).await?;
        let chunk = read_chunk(&reader, &chunk_group).await?;

        let tombstone_path = path.parent().unwrap_or_else(|| Path::new("/"));
        let tombstone = Arc::new(TsmTombstone::open(tombstone_path, file_id).await?);

        let tsm_meta = Arc::new(TsmMetaData::new(
            footer,
            chunk_group_meta,
            chunk_group,
            chunk,
        ));

        Ok(Self {
            // file_location: path,
            file_id,
            reader,
            tsm_meta,
            tombstone,
        })
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn footer(&self) -> &Footer {
        &self.tsm_meta.footer
    }

    pub fn has_tombstone(&self) -> bool {
        !self.tombstone.is_empty()
    }

    pub fn chunk_group_meta(&self) -> &ChunkGroupMeta {
        &self.tsm_meta.chunk_group_meta
    }

    pub fn chunk_group(&self) -> &BTreeMap<String, Arc<ChunkGroup>> {
        &self.tsm_meta.chunk_group
    }

    pub fn chunk(&self) -> &BTreeMap<SeriesId, Arc<Chunk>> {
        &self.tsm_meta.chunk
    }

    pub fn tsm_meta_data(&self) -> Arc<TsmMetaData> {
        self.tsm_meta.clone()
    }

    pub fn tombstone(&self) -> Arc<TsmTombstone> {
        self.tombstone.clone()
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_range: TimeRange,
    ) -> TskvResult<BTreeMap<SeriesId, Vec<(ColumnGroupID, Vec<PageMeta>)>>> {
        let meta = self.tsm_meta.clone();
        let mut map = BTreeMap::new();
        for series_id in series_ids {
            let mut column_groups = vec![];
            if meta.footer().maybe_series_exist(series_id) {
                if let Some(chunk) = meta.chunk().get(series_id) {
                    for (id, column_group) in chunk.column_group() {
                        let page_time_range = column_group.time_range();
                        let mut pages = vec![];
                        for page in column_group.pages() {
                            if page_time_range.overlaps(&time_range) {
                                pages.push(page.meta().clone());
                            }
                        }
                        if !pages.is_empty() {
                            column_groups.push((*id, pages));
                        }
                    }
                }
            }
            map.insert(*series_id, column_groups);
        }
        Ok(map)
    }

    pub async fn read_page(&self, page_spec: &PageWriteSpec) -> TskvResult<Page> {
        read_page(&self.reader, page_spec).await
    }

    pub async fn read_series_pages(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> TskvResult<Vec<Page>> {
        let chunk = self.chunk();
        let reader = &self.reader;
        if let Some(chunk) = chunk.get(&series_id) {
            for (id, column_group) in chunk.column_group() {
                if *id != column_group_id {
                    continue;
                }
                let mut res_page = Vec::with_capacity(column_group.pages().len());
                for page in column_group.pages() {
                    let page = read_page(reader, page).await?;
                    res_page.push(page);
                }
                return Ok(res_page);
            }
        }
        Ok(vec![])
    }

    pub async fn read_datablock_raw(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> TskvResult<Vec<u8>> {
        let chunk = self.chunk();
        if let Some(chunk) = chunk.get(&series_id) {
            for (id, column_group) in chunk.column_group() {
                if *id != column_group_id {
                    continue;
                }
                let mut res_column_group = vec![0u8; column_group.size() as usize];
                self.reader
                    .read_at(column_group.pages_offset() as usize, &mut res_column_group)
                    .await
                    .map_err(|e| {
                        ReadTsmSnafu {
                            reason: e.to_string(),
                        }
                        .build()
                    })?;
                return Ok(res_column_group);
            }
        }
        Ok(vec![])
    }

    pub async fn read_datablock(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> TskvResult<DataBlock> {
        let column_group = self.read_series_pages(series_id, column_group_id).await?;
        let schema = self
            .tsm_meta
            .table_schema_by_sid(series_id)
            .context(CommonSnafu {
                reason: format!("table schema for series id : {} not found", series_id),
            })?;
        let data_block = decode_pages(column_group, schema)?;

        Ok(data_block)
    }

    pub fn table_schema(&self, table_name: &str) -> Option<TskvTableSchemaRef> {
        self.tsm_meta.chunk_group_meta.table_schema(table_name)
    }
}

pub fn decode_buf_to_pages(
    chunk: Arc<Chunk>,
    column_group_id: ColumnGroupID,
    pages_buf: &[u8],
) -> TskvResult<Vec<Page>> {
    let column_group = chunk
        .column_group()
        .get(&column_group_id)
        .context(CommonSnafu {
            reason: format!(
                "column group for column group id : {} not found",
                column_group_id
            ),
        })?;
    let mut pages = Vec::with_capacity(column_group.pages().len());

    for page in column_group.pages() {
        let offset = (page.offset() - column_group.pages_offset()) as usize;
        let end = offset + page.size as usize;
        let page_buf = pages_buf.get(offset..end).context(CommonSnafu {
            reason: "page_buf get error".to_string(),
        })?;
        let page = Page {
            meta: page.meta.clone(),
            bytes: Bytes::from(page_buf.to_vec()),
        };
        let page_result = page.crc_validation()?;
        pages.push(page_result);
    }
    Ok(pages)
}

impl Debug for TsmReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TsmReader")
            .field("file_id", &self.file_id)
            .field("footer", &self.tsm_meta.footer)
            .field("chunk_group_meta", &self.tsm_meta.chunk_group_meta)
            .field("chunk_group", &self.tsm_meta.chunk_group)
            .field("chunk", &self.tsm_meta.chunk)
            .finish()
    }
}

pub async fn read_footer(reader: &FileStreamReader) -> TskvResult<Footer> {
    if reader.len() < FOOTER_SIZE {
        return Err(ReadTsmSnafu {
            reason: "file is too small".to_string(),
        }
        .build());
    };
    let pos = reader.len() - FOOTER_SIZE;
    let mut buffer = vec![0u8; FOOTER_SIZE];
    reader.read_at(pos, &mut buffer).await.map_err(|e| {
        ReadTsmSnafu {
            reason: e.to_string(),
        }
        .build()
    })?;
    Footer::deserialize(&buffer)
}

pub async fn read_chunk_group_meta(
    reader: &FileStreamReader,
    footer: &Footer,
) -> TskvResult<ChunkGroupMeta> {
    let pos = footer.table().chunk_group_offset();
    let mut buffer = vec![0u8; footer.table().chunk_group_size() as usize];
    reader
        .read_at(pos as usize, &mut buffer)
        .await
        .map_err(|e| {
            ReadTsmSnafu {
                reason: e.to_string(),
            }
            .build()
        })?; // read chunk group meta
    let specs = ChunkGroupMeta::deserialize(&buffer)?;
    Ok(specs)
}

pub async fn read_chunk_groups(
    reader: &FileStreamReader,
    chunk_group_meta: &ChunkGroupMeta,
) -> TskvResult<BTreeMap<String, Arc<ChunkGroup>>> {
    let mut specs = BTreeMap::new();
    for chunk in chunk_group_meta.tables().values() {
        let pos = chunk.chunk_group_offset();
        let mut buffer = vec![0u8; chunk.chunk_group_size() as usize];
        reader
            .read_at(pos as usize, &mut buffer)
            .await
            .map_err(|e| {
                ReadTsmSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?; // read chunk group meta
        let group = Arc::new(ChunkGroup::deserialize(&buffer)?);
        specs.insert(chunk.name().to_string(), group);
    }
    Ok(specs)
}

pub async fn read_chunk(
    reader: &FileStreamReader,
    chunk_group: &BTreeMap<String, Arc<ChunkGroup>>,
) -> TskvResult<BTreeMap<SeriesId, Arc<Chunk>>> {
    let mut chunks = BTreeMap::new();
    for group in chunk_group.values() {
        for chunk_spec in group.chunks() {
            let pos = chunk_spec.chunk_offset();
            let mut buffer = vec![0u8; chunk_spec.chunk_size() as usize];
            reader
                .read_at(pos as usize, &mut buffer)
                .await
                .map_err(|e| {
                    ReadTsmSnafu {
                        reason: e.to_string(),
                    }
                    .build()
                })?;
            let chunk = Arc::new(Chunk::deserialize(&buffer)?);
            chunks.insert(chunk_spec.series_id(), chunk);
        }
    }
    Ok(chunks)
}

async fn read_page(reader: &FileStreamReader, page_spec: &PageWriteSpec) -> TskvResult<Page> {
    let pos = page_spec.offset();
    let mut buffer = vec![0u8; page_spec.size() as usize];
    reader
        .read_at(pos as usize, &mut buffer)
        .await
        .map_err(|e| {
            ReadTsmSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
    let page = Page {
        meta: page_spec.meta().clone(),
        bytes: Bytes::from(buffer),
    };
    page.crc_validation()
}

pub fn decode_pages(pages: Vec<Page>, table_schema: TskvTableSchemaRef) -> TskvResult<DataBlock> {
    let mut time_column = MutableColumn::empty_with_cap(table_schema.time_column(), 0)?;

    let mut other_columns = Vec::new();

    for page in pages {
        let column = page.to_column()?;
        if page.desc().name == TIME_FIELD_NAME {
            time_column = column;
        } else {
            other_columns.push(column);
        }
    }

    Ok(DataBlock::new(table_schema, time_column, other_columns))
}

pub fn decode_pages_buf(
    pages_buf: &[u8],
    chunk: Arc<Chunk>,
    column_group_id: ColumnGroupID,
    table_schema: TskvTableSchemaRef,
) -> TskvResult<DataBlock> {
    let pages = decode_buf_to_pages(chunk, column_group_id, pages_buf)?;
    let data_block = decode_pages(pages, table_schema)?;
    Ok(data_block)
}
