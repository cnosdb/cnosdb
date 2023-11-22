use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::parquet::data_type::AsBytes;
use models::codec::Encoding;
use models::predicate::domain::TimeRange;
use models::schema::{ColumnType, TableColumn, TskvTableSchemaRef, TIME_FIELD};
use models::{ColumnId, SeriesId};
use parking_lot::RwLock;

use crate::error::Result;
use crate::file_system::file::async_file::AsyncFile;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::tsm::TsmTombstone;
use crate::tsm2::page::{Chunk, ChunkGroup, ChunkGroupMeta, Footer, Page, PageMeta, PageWriteSpec};
use crate::tsm2::writer::{Column, DataBlock2};
use crate::tsm2::FOOTER_SIZE;
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

#[derive(Clone)]
pub struct TSM2Reader {
    file_location: PathBuf,
    file_id: u64,
    reader: Arc<AsyncFile>,
    tsm_meta: Arc<TSM2MetaData>,
    tombstone: Option<Arc<RwLock<TsmTombstone>>>,
}

impl TSM2Reader {
    pub async fn open(tsm_path: impl AsRef<Path>) -> Result<Self> {
        let path = tsm_path.as_ref().to_path_buf();
        let file_id = file_utils::get_tsm_file_id_by_path(&path)?;
        let reader = Arc::new(file_manager::open_file(&path).await?);
        let footer = Arc::new(read_footer(reader.clone()).await?);
        let chunk_group_meta = Arc::new(read_chunk_group_meta(reader.clone(), &footer).await?);
        let chunk_group = read_chunk_groups(reader.clone(), &chunk_group_meta).await?;
        let chunk = read_chunk(reader.clone(), &chunk_group).await?;
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
            tombstone: None,
        })
    }

    pub fn reader(&self) -> Arc<AsyncFile> {
        self.reader.clone()
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    fn footer(&self) -> &Footer {
        &self.tsm_meta.footer
    }

    fn chunk_group_meta(&self) -> &ChunkGroupMeta {
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

    pub fn tombstone(&self) -> Option<Arc<RwLock<TsmTombstone>>> {
        self.tombstone.clone()
    }

    pub async fn statistics(
        &self,
        series_ids: &[SeriesId],
        time_range: TimeRange,
    ) -> Result<BTreeMap<SeriesId, Vec<PageMeta>>> {
        let meta = self.tsm_meta.clone();
        let mut map = BTreeMap::new();
        for series_id in series_ids {
            let mut pages = vec![];
            if meta.footer().is_series_exist(series_id) {
                if let Some(chunk) = meta.chunk().get(series_id) {
                    // chunks.push(chunk);
                    for page in chunk.pages() {
                        if page.meta.time_range.overlaps(&time_range) {
                            pages.push(page.meta().clone());
                        }
                    }
                }
            }
            map.insert(*series_id, pages);
        }
        Ok(map)
    }

    pub async fn read_pages(
        &mut self,
        series_ids: &[SeriesId],
        column_id: &[ColumnId],
    ) -> Result<Vec<Page>> {
        let mut res = Vec::new();
        let meta = self.tsm_meta.clone();
        let footer = meta.footer();
        let bloom_filter = footer.series().bloom_filter();
        let reader = self.reader.clone();
        for sid in series_ids {
            if !bloom_filter.contains(sid.as_bytes()) {
                continue;
            }
            let chunk = self.chunk().get(sid).ok_or(Error::CommonError {
                reason: format!("chunk for series id : {} not found", sid),
            })?;
            for pages in chunk.pages() {
                if column_id.contains(&pages.meta.column.id) {
                    let page = read_page(reader.clone(), pages).await?;
                    res.push(page);
                }
            }
        }
        Ok(res)
    }

    pub async fn read_page(&self, page_spec: &PageWriteSpec) -> Result<Page> {
        read_page(self.reader.clone(), page_spec).await
    }

    pub async fn read_series_pages(&self, series_id: SeriesId) -> Result<Vec<Page>> {
        let chunk = self.chunk();
        let mut res = Vec::new();
        let reader = self.reader.clone();
        if let Some(chunk) = chunk.get(&series_id) {
            for page in chunk.pages() {
                let page = read_page(reader.clone(), page).await?;
                res.push(page);
            }
        }
        Ok(res)
    }

    pub async fn read_datablock_raw(&self, series_id: SeriesId) -> Result<Vec<u8>> {
        let chunk = self.chunk();
        if let Some(chunk) = chunk.get(&series_id) {
            let mut res = Vec::with_capacity(chunk.size() as usize);
            self.reader
                .read_at(chunk.all_page_begin_offset(), &mut res)
                .await?;
            return Ok(res);
        }
        Ok(vec![])
    }

    pub async fn read_datablock(&self, series_id: SeriesId) -> Result<DataBlock2> {
        let pages = self.read_series_pages(series_id).await?;
        let schema = self
            .tsm_meta
            .table_schema_by_sid(series_id)
            .ok_or(Error::CommonError {
                reason: format!("table schema for series id : {} not found", series_id),
            })?;
        let data_block = decode_pages(pages, schema)?;
        Ok(data_block)
    }

    fn get_table_name(&self, series_id: SeriesId) -> Option<&str> {
        self.tsm_meta.table_name(series_id)
    }

    pub fn table_schema(&self, table_name: &str) -> Option<TskvTableSchemaRef> {
        self.tsm_meta.chunk_group_meta.table_schema(table_name)
    }
}

pub fn decode_buf_to_pages(chunk: Arc<Chunk>, pages_buf: &[u8]) -> Result<Vec<Page>> {
    let mut pages = Vec::with_capacity(chunk.pages().len());
    for page in chunk.pages() {
        let offset = page.offset() - chunk.all_page_begin_offset();
        let page_buf = pages_buf
            .get(offset as usize..page.size)
            .ok_or(Error::CommonError {
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

pub async fn read_footer(reader: Arc<AsyncFile>) -> Result<Footer> {
    let pos = reader.len() - (FOOTER_SIZE as u64);
    let mut buffer = vec![0u8; FOOTER_SIZE];
    reader.read_at(pos, &mut buffer).await?;
    Footer::deserialize(&buffer)
}

pub async fn read_chunk_group_meta(
    reader: Arc<AsyncFile>,
    footer: &Footer,
) -> Result<ChunkGroupMeta> {
    let pos = footer.table.chunk_group_offset();
    let mut buffer = vec![0u8; footer.table.chunk_group_size()];
    reader.read_at(pos, &mut buffer).await?; // read chunk group meta
    let specs = ChunkGroupMeta::deserialize(&buffer)?;
    Ok(specs)
}

pub async fn read_chunk_groups(
    reader: Arc<AsyncFile>,
    chunk_group_meta: &ChunkGroupMeta,
) -> Result<BTreeMap<String, Arc<ChunkGroup>>> {
    let mut specs = BTreeMap::new();
    for chunk in chunk_group_meta.tables().values() {
        let pos = chunk.chunk_group_offset();
        let mut buffer = vec![0u8; chunk.chunk_group_size()];
        reader.read_at(pos, &mut buffer).await?; // read chunk group meta
        let group = Arc::new(ChunkGroup::deserialize(&buffer)?);
        specs.insert(chunk.name().to_string(), group);
    }
    Ok(specs)
}

pub async fn read_chunk(
    reader: Arc<AsyncFile>,
    chunk_group: &BTreeMap<String, Arc<ChunkGroup>>,
) -> Result<BTreeMap<SeriesId, Arc<Chunk>>> {
    let mut chunks = BTreeMap::new();
    for group in chunk_group.values() {
        for chunk_spec in group.chunks() {
            let pos = chunk_spec.chunk_offset();
            let mut buffer = vec![0u8; chunk_spec.chunk_size()];
            reader.read_at(pos, &mut buffer).await?;
            let chunk = Arc::new(Chunk::deserialize(&buffer)?);
            chunks.insert(chunk_spec.series_id, chunk);
        }
    }
    Ok(chunks)
}

async fn read_page(reader: Arc<AsyncFile>, page_spec: &PageWriteSpec) -> Result<Page> {
    let pos = page_spec.offset();
    let mut buffer = vec![0u8; page_spec.size()];
    reader.read_at(pos, &mut buffer).await?;
    let page = Page {
        meta: page_spec.meta().clone(),
        bytes: Bytes::from(buffer),
    };
    Ok(page)
}

pub fn decode_pages(pages: Vec<Page>, table_schema: TskvTableSchemaRef) -> Result<DataBlock2> {
    let mut time_column_desc = TableColumn::new(
        0,
        TIME_FIELD.to_string(),
        ColumnType::Time(TimeUnit::Nanosecond),
        Encoding::Default,
    );
    let mut time_column = Column::empty(time_column_desc.column_type.clone());

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
    table_schema: TskvTableSchemaRef,
) -> Result<DataBlock2> {
    let pages = decode_buf_to_pages(chunk, pages_buf)?;
    let data_block = decode_pages(pages, table_schema)?;
    Ok(data_block)
}
