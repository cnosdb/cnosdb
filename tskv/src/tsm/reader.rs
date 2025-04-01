use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Formatter};
use std::path::Path;
use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::compute::filter_record_batch;
use arrow_array::types::{
    Int64Type, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, PrimitiveArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use models::codec::Encoding;
use models::predicate::domain::{TimeRange, TimeRanges};
use models::schema::tskv_table_schema::{PhysicalCType, TskvTableSchemaRef};
use models::{PhysicalDType, SeriesId, SeriesKey};
use snafu::{location, Backtrace, GenerateImplicitData, Location, OptionExt, ResultExt};

use crate::error::{ArrowSnafu, CommonSnafu, DecodeSnafu, ReadTsmSnafu, TskvResult, TsmPageSnafu};
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::file::stream_reader::FileStreamReader;
use crate::file_system::FileSystem;
use crate::tsm::chunk::Chunk;
use crate::tsm::chunk_group::{ChunkGroup, ChunkGroupMeta};
use crate::tsm::codec::{
    get_bool_codec, get_encoding, get_f64_codec, get_i64_codec, get_str_codec, get_ts_codec,
    get_u64_codec,
};
use crate::tsm::footer::{Footer, TsmVersion};
use crate::tsm::page::{Page, PageMeta, PageStatistics, PageWriteSpec};
use crate::tsm::{ColumnGroupID, TsmTombstone, FOOTER_SIZE};
use crate::{file_utils, ColumnFileId, TskvError};

#[derive(Clone)]
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

    pub fn update_tag_value(&mut self, series: &HashMap<SeriesId, SeriesKey>) -> TskvResult<()> {
        for (series_id, key) in series.iter() {
            if let Some(chunk) = self.chunk.get(series_id) {
                let new_chunk = chunk.update_series(key.clone());
                self.chunk.insert(*series_id, Arc::new(new_chunk));
            }
        }
        Ok(())
    }
}

pub struct TsmReader {
    file_id: ColumnFileId,
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
        let mut target = Vec::new();
        let buffer = read_tsm_meta_buffer(&reader, &footer).await?;
        let tsm_meta_buffer = {
            match footer.version() {
                TsmVersion::V1 => buffer.as_slice(),
                TsmVersion::V2 => {
                    let encoding = get_encoding(&buffer);
                    let codec = get_str_codec(encoding);
                    codec.decode(&buffer, &mut target).context(DecodeSnafu)?;
                    assert_eq!(target.len(), 1);
                    target[0].as_slice()
                }
            }
        };

        let chunk_group_meta = read_chunk_group_meta(tsm_meta_buffer, &footer).await?;
        let chunk_group = read_chunk_groups(tsm_meta_buffer, &chunk_group_meta).await?;
        let chunk = read_chunk(tsm_meta_buffer, &chunk_group).await?;

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

    pub async fn read_adjacent_pages(
        &self,
        pages_specs: &[PageWriteSpec],
    ) -> TskvResult<Vec<Page>> {
        let pos = pages_specs[0].offset() as usize;
        let total_size: usize = pages_specs.iter().map(|p| p.size() as usize).sum();
        let mut buffer = vec![0u8; total_size];
        self.reader.read_at(pos, &mut buffer).await.map_err(|e| {
            ReadTsmSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;

        let bytes = Bytes::from(buffer);
        let mut pages = Vec::with_capacity(pages_specs.len());
        let mut offset = 0;
        for page_spec in pages_specs {
            let size = page_spec.size() as usize;
            let page = Page {
                meta: page_spec.meta().clone(),
                bytes: bytes.slice(offset..offset + size),
            };
            page.crc_validation()?;
            pages.push(page);
            offset += size;
        }
        Ok(pages)
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

    pub async fn read_record_batch(
        &self,
        series_id: SeriesId,
        column_group_id: ColumnGroupID,
    ) -> TskvResult<RecordBatch> {
        let column_group = self.read_series_pages(series_id, column_group_id).await?;
        let schema = self
            .tsm_meta
            .table_schema_by_sid(series_id)
            .context(CommonSnafu {
                reason: format!("table schema for series id : {} not found", series_id),
            })?;
        let record_batch = decode_pages(
            column_group,
            schema.meta(),
            Some((self.tombstone.clone(), series_id)),
        )?;
        Ok(record_batch)
    }

    pub async fn add_tombstone_and_compact_to_tmp(
        &self,
        time_range: TimeRange,
    ) -> TskvResult<TimeRanges> {
        self.tombstone
            .add_range_and_compact_to_tmp(time_range)
            .await
    }

    /// Replace current tombstone file with compact_tmp tombstone file.
    pub async fn replace_tombstone_with_compact_tmp(&self) -> TskvResult<()> {
        self.tombstone.replace_with_compact_tmp().await
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

pub async fn read_tsm_meta_buffer(
    reader: &FileStreamReader,
    footer: &Footer,
) -> TskvResult<Vec<u8>> {
    let pos = footer.series().chunk_offset() as usize;
    let size = reader.len() - FOOTER_SIZE - pos;
    let mut buffer = vec![0u8; size];
    reader.read_at(pos, &mut buffer).await.map_err(|e| {
        ReadTsmSnafu {
            reason: e.to_string(),
        }
        .build()
    })?;
    Ok(buffer)
}

pub async fn read_chunk_group_meta(
    buffer: &[u8],
    footer: &Footer,
) -> TskvResult<Arc<ChunkGroupMeta>> {
    let pos = footer.table().chunk_group_offset() as usize;
    let size = footer.table().chunk_group_size() as usize;
    let serialize_buffer = &buffer[pos..pos + size];
    let specs = ChunkGroupMeta::deserialize(serialize_buffer)?;
    Ok(Arc::new(specs))
}

pub async fn read_chunk_groups(
    buffer: &[u8],
    chunk_group_meta: &ChunkGroupMeta,
) -> TskvResult<BTreeMap<String, Arc<ChunkGroup>>> {
    let mut specs = BTreeMap::new();
    for chunk in chunk_group_meta.tables().values() {
        let pos = chunk.chunk_group_offset() as usize;
        let size = chunk.chunk_group_size() as usize;
        let serialize_buffer = &buffer[pos..pos + size];
        let group = Arc::new(ChunkGroup::deserialize(serialize_buffer)?);
        specs.insert(chunk.name().to_string(), group);
    }
    Ok(specs)
}

pub async fn read_chunk(
    buffer: &[u8],
    chunk_group: &BTreeMap<String, Arc<ChunkGroup>>,
) -> TskvResult<BTreeMap<SeriesId, Arc<Chunk>>> {
    let mut chunks = BTreeMap::new();
    for group in chunk_group.values() {
        for chunk_spec in group.chunks() {
            let pos = chunk_spec.chunk_offset() as usize;
            let size = chunk_spec.chunk_size() as usize;
            let serialize_buffer = &buffer[pos..pos + size];
            let chunk = Arc::new(Chunk::deserialize(serialize_buffer)?);
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
pub fn decode_pages(
    pages: Vec<Page>,
    schema_meta: HashMap<String, String>,
    tomb: Option<(Arc<TsmTombstone>, SeriesId)>,
) -> TskvResult<RecordBatch> {
    let mut target_arrays = Vec::with_capacity(pages.len());

    let fields = pages
        .iter()
        .map(|page| Field::from(&page.meta.column))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new_with_metadata(fields, schema_meta));

    if let Some((tomb, series_id)) = tomb {
        // deal time page
        let time_page = pages
            .iter()
            .find(|f| f.meta.column.column_type.is_time())
            .context(CommonSnafu {
                reason: "time field not found".to_string(),
            })?;
        let (time_array, time_range, time) = get_time_page_meta(time_page)?;
        let filters = tomb.get_all_fields_excluded_time_range(&time_range);
        let time_null_bits = {
            if filters.is_empty() {
                None
            } else {
                let null_bitset = update_nullbits_by_time_range(&time_array, &filters, time_page)?;
                Some(null_bitset)
            }
        };

        target_arrays.push(time);

        // deal field page
        for page in pages
            .iter()
            .filter(|p| p.meta.column.column_type.is_field())
        {
            let filters =
                tomb.get_column_overlapped_time_ranges(series_id, page.meta.column.id, &time_range);
            let array = {
                if filters.is_empty() {
                    page.to_arrow_array()?
                } else {
                    let null_bitset = update_nullbits_by_time_range(&time_array, &filters, page)?;
                    updated_nullbuffer(page.to_arrow_array()?, null_bitset)?
                }
            };
            target_arrays.push(array);
        }
        let mut record_batch = RecordBatch::try_new(schema, target_arrays).context(ArrowSnafu)?;
        if let Some(time_column_have_null) = time_null_bits {
            let boolean_array = BooleanArray::new(time_column_have_null, None);
            record_batch =
                filter_record_batch(&record_batch, &boolean_array).context(ArrowSnafu)?;
        }
        Ok(record_batch)
    } else {
        for page in pages {
            let array = page.to_arrow_array()?;
            target_arrays.push(array);
        }
        let record_batch = RecordBatch::try_new(schema, target_arrays).context(ArrowSnafu)?;
        Ok(record_batch)
    }
}

pub fn get_time_page_meta(
    time_page: &Page,
) -> TskvResult<(PrimitiveArray<Int64Type>, TimeRange, ArrayRef)> {
    let time_array_ref = time_page.to_arrow_array()?;
    let time_array = match time_array_ref.data_type() {
        DataType::Timestamp(_time_unit, _) => {
            let array_data = time_array_ref.to_data();
            if array_data.buffers().is_empty() {
                return Err(TskvError::CommonError {
                    reason: "Timestamp array does not have a value buffer".to_string(),
                    location: location!(),
                    backtrace: Backtrace::generate(),
                });
            }
            let values_buffer: Buffer = array_data.buffers()[0].clone();
            let int64_data = ArrayData::builder(DataType::Int64)
                .len(array_data.len())
                .add_buffer(values_buffer)
                .nulls(array_data.nulls().cloned())
                .build()
                .map_err(|e| TskvError::CommonError {
                    reason: format!("Failed to build Int64Array: {}", e),
                    location: location!(),
                    backtrace: Backtrace::generate(),
                })?;
            Int64Array::from(int64_data)
        }
        _ => {
            return Err(TskvError::UnsupportedDataType {
                dt: time_array_ref.data_type().to_string(),
                location: location!(),
                backtrace: Backtrace::generate(),
            })
        }
    };
    let time_range = match time_page.meta.statistics {
        PageStatistics::I64(ref stats) => {
            let min = stats.min().ok_or_else(|| {
                CommonSnafu {
                    reason: "Missing min value in time column statistics".to_string(),
                }
                .build()
            })?;
            let max = stats.max().ok_or_else(|| {
                CommonSnafu {
                    reason: "Missing max value in time column statistics".to_string(),
                }
                .build()
            })?;
            TimeRange::new(min, max)
        }
        _ => {
            return Err(CommonSnafu {
                reason: "time column data type error".to_string(),
            }
            .build())
        }
    };
    Ok((time_array, time_range, time_array_ref))
}

pub fn decode_pages_buf(
    pages_buf: &[u8],
    chunk: Arc<Chunk>,
    column_group_id: ColumnGroupID,
    table_schema: TskvTableSchemaRef,
) -> TskvResult<RecordBatch> {
    let pages = decode_buf_to_pages(chunk, column_group_id, pages_buf)?;
    let data_block = decode_pages(pages, table_schema.meta(), None)?;
    Ok(data_block)
}

fn update_nullbits_by_time_range(
    time_array_ref: &PrimitiveArray<Int64Type>,
    time_ranges: &Vec<TimeRange>,
    page: &Page,
) -> TskvResult<BooleanBuffer> {
    let mut null_bitset = page.null_bitset();

    for time_range in time_ranges {
        let start_index = time_array_ref
            .values()
            .binary_search(&time_range.min_ts)
            .unwrap_or_else(|x| x);
        let end_index = time_array_ref
            .values()
            .binary_search(&time_range.max_ts)
            .map(|x| x + 1)
            .unwrap_or_else(|x| x);
        for i in start_index..end_index {
            null_bitset.set_bit(i, false);
        }
    }
    Ok(null_bitset.finish())
}

pub fn data_buf_to_arrow_array(page: &Page) -> TskvResult<ArrayRef> {
    let data_buffer = page.data_buffer();
    let encoding = get_encoding(data_buffer);
    let page_null_buffer = NullBuffer::new(page.null_bitset().finish_cloned());

    let array = match page.meta().column.column_type.to_physical_type() {
        PhysicalCType::Time(precision) => {
            decode_to_arrow_timestamp(data_buffer, encoding, &page_null_buffer, precision)?
        }
        PhysicalCType::Field(PhysicalDType::Integer) => {
            let encoding = get_encoding(data_buffer);
            let codec = get_i64_codec(encoding);
            codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .map_err(|e| TskvError::Decode {
                    source: e,
                    location: location!(),
                    backtrace: Backtrace::generate(),
                })?
        }
        PhysicalCType::Field(PhysicalDType::Float) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_f64_codec(encoding);
            ts_codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .map_err(|e| TskvError::Decode {
                    source: e,
                    location: location!(),
                    backtrace: Backtrace::generate(),
                })?
        }
        PhysicalCType::Field(PhysicalDType::Unsigned) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_u64_codec(encoding);
            ts_codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .map_err(|e| TskvError::Decode {
                    source: e,
                    location: location!(),
                    backtrace: Backtrace::generate(),
                })?
        }
        PhysicalCType::Field(PhysicalDType::Boolean) => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_bool_codec(encoding);
            ts_codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .map_err(|e| TskvError::Decode {
                    source: e,
                    location: location!(),
                    backtrace: Backtrace::generate(),
                })?
        }
        PhysicalCType::Field(PhysicalDType::String) | PhysicalCType::Tag => {
            let encoding = get_encoding(data_buffer);
            let ts_codec = get_str_codec(encoding);
            ts_codec
                .decode_to_array(data_buffer, &page_null_buffer)
                .map_err(|e| TskvError::Decode {
                    source: e,
                    location: location!(),
                    backtrace: Backtrace::generate(),
                })?
        }
        PhysicalCType::Field(PhysicalDType::Unknown) => {
            return Err(TskvError::UnsupportedDataType {
                dt: "unknown".to_string(),
                location: location!(),
                backtrace: Backtrace::generate(),
            })
        }
    };
    Ok(array)
}

fn decode_to_arrow_timestamp(
    data: &[u8],
    encoding: Encoding,
    null_bitset: &NullBuffer,
    precision: TimeUnit,
) -> TskvResult<ArrayRef> {
    let codec = get_ts_codec(encoding);
    let array_ref = codec
        .decode_to_array(data, null_bitset)
        .context(DecodeSnafu)?;
    let array = array_ref
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            TsmPageSnafu {
                reason: "Arrow array is not Int64Array".to_string(),
            }
            .build()
        })?;

    let timestamp_array: ArrayRef = match precision {
        TimeUnit::Second => Arc::new(array.reinterpret_cast::<TimestampSecondType>()),
        TimeUnit::Millisecond => Arc::new(array.reinterpret_cast::<TimestampMillisecondType>()),
        TimeUnit::Microsecond => Arc::new(array.reinterpret_cast::<TimestampMicrosecondType>()),
        TimeUnit::Nanosecond => Arc::new(array.reinterpret_cast::<TimestampNanosecondType>()),
    };

    Ok(timestamp_array)
}

pub(crate) fn updated_nullbuffer(
    array_ref: ArrayRef,
    updated_nulls: BooleanBuffer,
) -> TskvResult<ArrayRef> {
    let data_type = array_ref.data_type();
    let data = array_ref.to_data();
    let nulls = updated_nulls.into_inner();
    let new_data = match data_type {
        DataType::UInt64 => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(UInt64Array::from(d)) as ArrayRef),
        DataType::Int64 => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(Int64Array::from(d)) as ArrayRef),
        DataType::Float64 => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(Float64Array::from(d)) as ArrayRef),
        DataType::Boolean => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(BooleanArray::from(d)) as ArrayRef),
        DataType::Utf8 => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| Arc::new(StringArray::from(d)) as ArrayRef),
        DataType::Timestamp(time_unit, _) => ArrayData::builder(data_type.clone())
            .len(data.len())
            .buffers(data.buffers().to_vec())
            .null_bit_buffer(Some(nulls))
            .build()
            .map(|d| match time_unit {
                TimeUnit::Second => Arc::new(TimestampSecondArray::from(d)) as ArrayRef,
                TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(d)) as ArrayRef,
                TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(d)) as ArrayRef,
                TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(d)) as ArrayRef,
            }),
        _ => {
            return Err(TskvError::UnsupportedDataType {
                dt: data_type.to_string(),
                location: location!(),
                backtrace: Backtrace::generate(),
            })
        }
    };
    new_data.map_err(|e| TskvError::Arrow {
        source: e,
        location: location!(),
        backtrace: Backtrace::generate(),
    })
}
