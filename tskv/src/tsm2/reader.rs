use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use models::predicate::domain::TimeRange;
use models::schema::{TskvTableSchemaRef, TIME_FIELD};
use models::SeriesId;

use crate::error::Result;
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::file::stream_reader::FileStreamReader;
use crate::file_system::FileSystem;
use crate::tsm::TsmTombstone;
use crate::tsm2::page::{Chunk, ChunkGroup, ChunkGroupMeta, Footer, Page, PageMeta, PageWriteSpec};
use crate::tsm2::writer::{Column, DataBlock2};
use crate::tsm2::{ColumnGroupID, FOOTER_SIZE};
use crate::{file_utils, Error};

pub struct TSM2MetaData {
    footer: Arc<Footer>,
    chunk_group_meta: Arc<ChunkGroupMeta>,
    chunk_group: BTreeMap<String, Arc<ChunkGroup>>,
    chunk: BTreeMap<SeriesId, Arc<Chunk>>,
}

impl TSM2MetaData {
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
            if series_map.chunks.iter().any(|c| c.series_id == series_id) {
                return Some(table_name.as_ref());
            }
        }
        None
    }
}

pub struct TSM2Reader {
    file_location: PathBuf,
    file_id: u64,
    reader: Box<FileStreamReader>,
    tsm_meta: Arc<TSM2MetaData>,
    tombstone: Arc<TsmTombstone>,
}

impl TSM2Reader {
    pub async fn open(tsm_path: impl AsRef<Path>) -> Result<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let file_system = LocalFileSystem::new(LocalFileType::ThreadPool);
        let reader = file_system
            .open_file_reader(&path)
            .await
            .map_err(|e| Error::FileSystemError { source: e })?;

        let file_id = file_utils::get_tsm_file_id_by_path(&path)?;

        let footer = Arc::new(read_footer(&reader).await?);
        let chunk_group_meta = Arc::new(read_chunk_group_meta(&reader, &footer).await?);
        let chunk_group = read_chunk_groups(&reader, &chunk_group_meta).await?;
        let chunk = read_chunk(&reader, &chunk_group).await?;

        let tombstone_path = path.parent().unwrap_or_else(|| Path::new("/"));
        let tombstone = Arc::new(TsmTombstone::open(tombstone_path, file_id).await?);

        let tsm_meta = Arc::new(TSM2MetaData::new(
            footer,
            chunk_group_meta,
            chunk_group,
            chunk,
        ));

        Ok(Self {
            file_location: path,
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

    pub fn tsm_meta_data(&self) -> Arc<TSM2MetaData> {
        self.tsm_meta.clone()
    }

    pub fn tombstone(&self) -> Arc<TsmTombstone> {
        self.tombstone.clone()
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_range: TimeRange,
    ) -> Result<BTreeMap<SeriesId, Vec<(ColumnGroupID, Vec<PageMeta>)>>> {
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

    // pub async fn read_pages(
    //     &mut self,
    //     series_ids: &[SeriesId],
    //     column_id: &[ColumnId],
    // ) -> Result<Vec<Page>> {
    //     let mut res = Vec::new();
    //     let meta = self.tsm_meta.clone();
    //     let footer = meta.footer();
    //     let bloom_filter = footer.series().bloom_filter();
    //     let reader = self.reader.clone();
    //     for sid in series_ids {
    //         if !bloom_filter.contains(sid.as_bytes()) {
    //             continue;
    //         }
    //         let chunk = self.chunk().get(sid).ok_or(Error::CommonError {
    //             reason: format!("chunk for series id : {} not found", sid),
    //         })?;
    //         for pages in chunk.pages() {
    //             if column_id.contains(&pages.meta.column.id) {
    //                 let page = read_page(reader.clone(), pages).await?;
    //                 res.push(page);
    //             }
    //         }
    //     }
    //     Ok(res)
    // }

    pub async fn read_page(&self, page_spec: &PageWriteSpec) -> Result<Page> {
        read_page(&self.reader, page_spec).await
    }

    pub async fn read_series_pages(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> Result<Vec<Page>> {
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
    ) -> Result<Vec<u8>> {
        let chunk = self.chunk();
        if let Some(chunk) = chunk.get(&series_id) {
            for (id, column_group) in chunk.column_group() {
                if *id != column_group_id {
                    continue;
                }
                let mut res_column_group = vec![0u8; column_group.size() as usize];
                self.reader
                    .read_at(column_group.pages_offset() as usize, &mut res_column_group)
                    .await?;
                return Ok(res_column_group);
            }
        }
        Ok(vec![])
    }

    pub async fn read_datablock(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> Result<DataBlock2> {
        let column_group = self.read_series_pages(series_id, column_group_id).await?;
        let schema = self
            .tsm_meta
            .table_schema_by_sid(series_id)
            .ok_or(Error::CommonError {
                reason: format!("table schema for series id : {} not found", series_id),
            })?;
        let data_block = decode_pages(column_group, schema)?;

        Ok(data_block)
    }

    fn get_table_name(&self, series_id: SeriesId) -> Option<&str> {
        self.tsm_meta.table_name(series_id)
    }

    pub fn table_schema(&self, table_name: &str) -> Option<TskvTableSchemaRef> {
        self.tsm_meta.chunk_group_meta.table_schema(table_name)
    }
}

pub fn decode_buf_to_pages(
    chunk: Arc<Chunk>,
    column_group_id: ColumnGroupID,
    pages_buf: &[u8],
) -> Result<Vec<Page>> {
    let column_group = chunk
        .column_group()
        .get(&column_group_id)
        .ok_or(Error::CommonError {
            reason: format!(
                "column group for column group id : {} not found",
                column_group_id
            ),
        })?;
    let mut pages = Vec::with_capacity(column_group.pages().len());

    for page in column_group.pages() {
        let offset = (page.offset() - column_group.pages_offset()) as usize;
        let end = offset + page.size;
        let page_buf = pages_buf.get(offset..end).ok_or(Error::CommonError {
            reason: "page_buf get error".to_string(),
        })?;
        let page = Page {
            meta: page.meta.clone(),
            bytes: Bytes::from(page_buf.to_vec()),
        };
        pages.push(page);
    }
    Ok(pages)
}

impl Debug for TSM2Reader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TSMReader")
            .field("file_id", &self.file_id)
            .field("footer", &self.tsm_meta.footer)
            .field("chunk_group_meta", &self.tsm_meta.chunk_group_meta)
            .field("chunk_group", &self.tsm_meta.chunk_group)
            .field("chunk", &self.tsm_meta.chunk)
            .finish()
    }
}

pub async fn read_footer(reader: &FileStreamReader) -> Result<Footer> {
    let pos = reader.len() - FOOTER_SIZE;
    let mut buffer = vec![0u8; FOOTER_SIZE];
    reader.read_at(pos, &mut buffer).await?;
    Footer::deserialize(&buffer)
}

pub async fn read_chunk_group_meta(
    reader: &FileStreamReader,
    footer: &Footer,
) -> Result<ChunkGroupMeta> {
    let pos = footer.table.chunk_group_offset() as usize;
    let mut buffer = vec![0u8; footer.table.chunk_group_size()];
    reader.read_at(pos, &mut buffer).await?; // read chunk group meta
    let specs = ChunkGroupMeta::deserialize(&buffer)?;
    Ok(specs)
}

pub async fn read_chunk_groups(
    reader: &FileStreamReader,
    chunk_group_meta: &ChunkGroupMeta,
) -> Result<BTreeMap<String, Arc<ChunkGroup>>> {
    let mut specs = BTreeMap::new();
    for chunk in chunk_group_meta.tables().values() {
        let pos = chunk.chunk_group_offset() as usize;
        let mut buffer = vec![0u8; chunk.chunk_group_size()];
        reader.read_at(pos, &mut buffer).await?; // read chunk group meta
        let group = Arc::new(ChunkGroup::deserialize(&buffer)?);
        specs.insert(chunk.name().to_string(), group);
    }
    Ok(specs)
}

pub async fn read_chunk(
    reader: &FileStreamReader,
    chunk_group: &BTreeMap<String, Arc<ChunkGroup>>,
) -> Result<BTreeMap<SeriesId, Arc<Chunk>>> {
    let mut chunks = BTreeMap::new();
    for group in chunk_group.values() {
        for chunk_spec in group.chunks() {
            let pos = chunk_spec.chunk_offset() as usize;
            let mut buffer = vec![0u8; chunk_spec.chunk_size()];
            reader.read_at(pos, &mut buffer).await?;
            let chunk = Arc::new(Chunk::deserialize(&buffer)?);
            chunks.insert(chunk_spec.series_id, chunk);
        }
    }
    Ok(chunks)
}

async fn read_page(reader: &FileStreamReader, page_spec: &PageWriteSpec) -> Result<Page> {
    let pos = page_spec.offset() as usize;
    let mut buffer = vec![0u8; page_spec.size()];
    reader.read_at(pos, &mut buffer).await?;
    let page = Page {
        meta: page_spec.meta().clone(),
        bytes: Bytes::from(buffer),
    };
    Ok(page)
}

pub fn decode_pages(pages: Vec<Page>, table_schema: TskvTableSchemaRef) -> Result<DataBlock2> {
    let mut time_column_desc = table_schema.time_column();
    let mut time_column = Column::empty_with_cap(time_column_desc.column_type.clone(), 0)?;

    let mut other_columns_desc = Vec::new();
    let mut other_columns = Vec::new();

    for page in pages {
        let column = page.to_column()?;
        if page.desc().name == TIME_FIELD {
            time_column_desc = page.desc().clone();
            time_column = column;
        } else {
            other_columns_desc.push(page.desc().clone());
            other_columns.push(column);
        }
    }

    Ok(DataBlock2::new(
        table_schema,
        time_column,
        time_column_desc,
        other_columns,
        other_columns_desc,
    ))
}

pub fn decode_pages_buf(
    pages_buf: &[u8],
    chunk: Arc<Chunk>,
    column_group_id: ColumnGroupID,
    table_schema: TskvTableSchemaRef,
) -> Result<DataBlock2> {
    let pages = decode_buf_to_pages(chunk, column_group_id, pages_buf)?;
    let data_block = decode_pages(pages, table_schema)?;
    Ok(data_block)
}
