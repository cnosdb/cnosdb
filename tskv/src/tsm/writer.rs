use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap};
use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::RecordBatch;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::{TableColumn, TskvTableSchemaRef};
use models::{SeriesId, SeriesKey};
use snafu::{OptionExt, ResultExt};
use utils::BloomFilter;

use crate::compaction::CompactingBlock;
use crate::error::{CommonSnafu, IOSnafu, ModelSnafu};
use crate::file_system::async_filesystem::{LocalFileSystem, LocalFileType};
use crate::file_system::file::stream_writer::FileStreamWriter;
use crate::file_system::FileSystem;
use crate::file_utils::{make_delta_file, make_tsm_file};
use crate::tsm::chunk::{Chunk, ChunkStatics, ChunkWriteSpec};
use crate::tsm::chunk_group::{ChunkGroup, ChunkGroupMeta, ChunkGroupWriteSpec};
use crate::tsm::column_group::ColumnGroup;
use crate::tsm::footer::{Footer, SeriesMeta, TableMeta, TsmVersion};
use crate::tsm::page::{Page, PageStatistics, PageWriteSpec};
use crate::tsm::{ColumnGroupID, BLOOM_FILTER_BITS};
use crate::{ColumnFileId, TskvError, TskvResult};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum State {
    Initialised,
    Started,
    Finished,
}

const TSM_MAGIC: [u8; 4] = 0x12CDA16_u32.to_be_bytes();
const TSM_BUFFER_SIZE: usize = 16 * 1024 * 1024;
pub struct TsmWriter {
    file_id: ColumnFileId,
    min_ts: i64,
    max_ts: i64,
    size: u64,
    max_size: u64,
    path: PathBuf,

    series_bloom_filter: BloomFilter,
    // todo: table object id bloom filter
    // table_bloom_filter: BloomFilter,
    writer: Box<FileStreamWriter>,
    table_schemas: HashMap<String, TskvTableSchemaRef>,

    /// <table < series, Chunk>>
    page_specs: BTreeMap<String, BTreeMap<SeriesId, Chunk>>,
    /// <table, ChunkGroup>
    chunk_specs: BTreeMap<String, ChunkGroup>,
    /// [ChunkGroupWriteSpec]
    chunk_group_specs: ChunkGroupMeta,
    footer: Footer,
    state: State,
}

//MutableRecordBatch
impl TsmWriter {
    pub async fn open(
        path_buf: &impl AsRef<Path>,
        file_id: ColumnFileId,
        max_size: u64,
        id_delta: bool,
    ) -> TskvResult<Self> {
        let file_path = if id_delta {
            make_delta_file(path_buf, file_id)
        } else {
            make_tsm_file(path_buf, file_id)
        };
        let file_system = LocalFileSystem::new(LocalFileType::ThreadPool);
        let file = file_system
            .open_file_writer(&file_path, TSM_BUFFER_SIZE)
            .await
            .map_err(|e| TskvError::FileSystemError { source: e })?;
        let writer = Self::new(file_path, file, file_id, max_size);
        Ok(writer)
    }
    fn new(
        path: PathBuf,
        writer: Box<FileStreamWriter>,
        file_id: ColumnFileId,
        max_size: u64,
    ) -> Self {
        Self {
            file_id,
            max_ts: i64::MIN,
            min_ts: i64::MAX,
            size: 0,
            max_size,
            path,
            series_bloom_filter: BloomFilter::new(BLOOM_FILTER_BITS),
            writer,
            table_schemas: Default::default(),
            page_specs: Default::default(),
            chunk_specs: Default::default(),
            chunk_group_specs: Default::default(),
            footer: Footer::empty(TsmVersion::V1),
            state: State::Initialised,
        }
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn min_ts(&self) -> i64 {
        self.min_ts
    }

    pub fn max_ts(&self) -> i64 {
        self.max_ts
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn series_bloom_filter(&self) -> &BloomFilter {
        &self.series_bloom_filter
    }

    pub fn into_series_bloom_filter(self) -> BloomFilter {
        self.series_bloom_filter
    }

    pub fn is_finished(&self) -> bool {
        self.state == State::Finished
    }

    pub async fn write_header(&mut self) -> TskvResult<usize> {
        let size = self
            .writer
            .write_vec([IoSlice::new(TSM_MAGIC.as_slice())].as_mut_slice())
            .await
            .context(IOSnafu)?;
        self.state = State::Started;
        self.size += size as u64;
        Ok(size)
    }

    /// todo: write footer
    pub async fn write_footer(&mut self) -> TskvResult<usize> {
        let buf = self.footer.serialize()?;
        let size = self.writer.write(&buf).await.context(IOSnafu)?;
        self.size += size as u64;
        Ok(size)
    }

    pub async fn write_chunk_group(&mut self) -> TskvResult<()> {
        for (table, group) in &self.chunk_specs {
            let chunk_group_offset = self.writer.len() as u64;
            let buf = group.serialize()?;
            let chunk_group_size = self.writer.write(&buf).await.context(IOSnafu)? as u64;
            self.size += chunk_group_size;
            let chunk_group_spec = ChunkGroupWriteSpec {
                table_schema: self.table_schemas.get(table).unwrap().clone(),
                chunk_group_offset,
                chunk_group_size,
                time_range: group.time_range(),
                // The number of chunks in the group.
                count: 0,
            };
            self.chunk_group_specs.push(chunk_group_spec);
        }
        Ok(())
    }

    pub async fn write_chunk_group_specs(&mut self, series: SeriesMeta) -> TskvResult<()> {
        let chunk_group_specs_offset = self.writer.len() as u64;
        let buf = self.chunk_group_specs.serialize()?;
        let chunk_group_specs_size = self.writer.write(&buf).await.context(IOSnafu)?;
        self.size += chunk_group_specs_size as u64;
        let time_range = self.chunk_group_specs.time_range();
        self.footer.set_time_range(time_range);
        self.footer.set_table_meta(TableMeta::new(
            chunk_group_specs_offset,
            chunk_group_specs_size as u64,
        ));
        self.footer.set_series(series);
        Ok(())
    }

    pub async fn write_chunk(&mut self) -> TskvResult<SeriesMeta> {
        let chunk_offset = self.writer.len() as u64;
        for (table, group) in &self.page_specs {
            for (series, chunk) in group {
                let chunk_offset = self.writer.len() as u64;
                let buf = chunk.serialize()?;
                let chunk_size = self.writer.write(&buf).await.context(IOSnafu)? as u64;
                self.size += chunk_size;
                let time_range = chunk.time_range();
                self.min_ts = min(self.min_ts, time_range.min_ts);
                self.max_ts = max(self.max_ts, time_range.max_ts);
                let chunk_spec = ChunkWriteSpec::new(
                    *series,
                    chunk_offset,
                    chunk_size,
                    ChunkStatics::new(*time_range),
                );
                self.chunk_specs
                    .entry(table.clone())
                    .or_default()
                    .push(chunk_spec);
                self.series_bloom_filter.insert(&series.to_be_bytes());
            }
        }
        let chunk_size = self.writer.len() as u64 - chunk_offset;
        let series = SeriesMeta::new(
            self.series_bloom_filter.bytes().to_vec(),
            chunk_offset,
            chunk_size,
        );
        Ok(series)
    }

    fn create_column_group(
        &mut self,
        schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: &SeriesKey,
    ) -> ColumnGroup {
        let chunks = self.page_specs.entry(schema.name.clone()).or_default();
        let chunk = chunks.entry(series_id).or_insert(Chunk::new(
            schema.name.clone(),
            series_id,
            series_key.clone(),
        ));

        ColumnGroup::new(chunk.next_column_group_id())
    }

    pub async fn write_record_batch(
        &mut self,
        series_id: SeriesId,
        series_key: SeriesKey,
        table_schema: TskvTableSchemaRef,
        record_batch: RecordBatch,
    ) -> TskvResult<()> {
        if self.state == State::Finished {
            return Err(CommonSnafu {
                reason: "TsmWriter has been finished".to_string(),
            }
            .build());
        }
        let columns = record_batch
            .schema()
            .fields
            .iter()
            .map(|field| TableColumn::try_from(field.clone()).context(ModelSnafu))
            .collect::<TskvResult<Vec<TableColumn>>>()?;

        let pages = record_batch
            .columns()
            .iter()
            .cloned()
            .zip(columns.into_iter())
            .collect::<Vec<(_, _)>>()
            .into_iter()
            .map(|(array, col_desc)| Page::arrow_array_to_page(array, col_desc))
            .collect::<TskvResult<Vec<Page>>>()?;

        let time_range = match pages
            .iter()
            .find(|page| page.meta.column.column_type.is_time())
            .context(CommonSnafu {
                reason: "time column not found".to_string(),
            })?
            .meta
            .statistics
        {
            PageStatistics::I64(ref v) => {
                let min = v.min().ok_or_else(|| {
                    CommonSnafu {
                        reason: "time column min not found".to_string(),
                    }
                    .build()
                })?;
                let max = v.max().ok_or_else(|| {
                    CommonSnafu {
                        reason: "time column max not found".to_string(),
                    }
                    .build()
                })?;
                TimeRange::new(min, max)
            }
            _ => {
                return Err(CommonSnafu {
                    reason: "time column type error".to_string(),
                }
                .build());
            }
        };

        self.write_pages(table_schema, series_id, series_key, pages, time_range)
            .await?;
        Ok(())
    }

    pub async fn write_pages(
        &mut self,
        schema: TskvTableSchemaRef,
        series_id: SeriesId,
        series_key: SeriesKey,
        pages: Vec<Page>,
        time_range: TimeRange,
    ) -> TskvResult<()> {
        if self.state == State::Initialised {
            self.write_header().await?;
        }

        let mut column_group = self.create_column_group(schema.clone(), series_id, &series_key);

        let table_name = schema.name.clone();
        for page in pages {
            let offset = self.writer.len() as u64;
            let size = self.writer.write(&page.bytes).await.context(IOSnafu)?;
            self.size += size as u64;
            let spec = PageWriteSpec {
                offset,
                size: size as u64,
                meta: page.meta,
            };
            column_group.push(spec);
        }
        self.insert_schema(schema.clone());
        column_group.time_range_merge(&time_range);
        self.page_specs
            .entry(table_name.clone())
            .or_default()
            .entry(series_id)
            .or_insert(Chunk::new(schema.name.clone(), series_id, series_key))
            .push(column_group.into())?;
        Ok(())
    }

    pub async fn write_raw(
        &mut self,
        schema: TskvTableSchemaRef,
        meta: Arc<Chunk>,
        column_group_id: ColumnGroupID,
        raw: Vec<u8>,
    ) -> TskvResult<()> {
        if self.state == State::Initialised {
            self.write_header().await?;
        }

        let mut new_column_group =
            self.create_column_group(schema.clone(), meta.series_id(), meta.series_key());

        let mut offset = self.writer.len() as u64;
        let size = self.writer.write(&raw).await.context(IOSnafu)?;
        self.size += size as u64;

        let column_group = meta
            .column_group()
            .get(&column_group_id)
            .context(CommonSnafu {
                reason: format!("column group not found: {}", column_group_id),
            })?;
        for spec in column_group.pages() {
            let spec = PageWriteSpec {
                offset,
                size: spec.size,
                meta: spec.meta.clone(),
            };
            offset += spec.size;
            new_column_group.push(spec);
        }
        self.insert_schema(schema.clone());
        new_column_group.time_range_merge(column_group.time_range());
        let series_id = meta.series_id();
        let series_key = meta.series_key().clone();
        let table_name = schema.name.clone();
        self.page_specs
            .entry(table_name.clone())
            .or_default()
            .entry(series_id)
            .or_insert(Chunk::new(table_name, series_id, series_key))
            .push(new_column_group.into())?;

        Ok(())
    }

    fn insert_schema(&mut self, schema: TskvTableSchemaRef) {
        if let Some(table) = self.table_schemas.get(&schema.name) {
            if table.schema_version < schema.schema_version {
                self.table_schemas.insert(schema.name.clone(), schema);
            }
        } else {
            self.table_schemas.insert(schema.name.clone(), schema);
        }
    }

    pub async fn write_compacting_block(
        &mut self,
        compacting_block: CompactingBlock,
    ) -> TskvResult<()> {
        match compacting_block {
            CompactingBlock::Decoded {
                record_batch,
                series_id,
                series_key,
                table_schema,
                ..
            } => {
                self.write_record_batch(series_id, series_key, table_schema, record_batch)
                    .await?
            }
            CompactingBlock::Encoded {
                table_schema,
                series_id,
                series_key,
                time_range,
                record_batch,
                ..
            } => {
                self.write_pages(
                    table_schema,
                    series_id,
                    series_key,
                    record_batch,
                    time_range,
                )
                .await?
            }
            CompactingBlock::Raw {
                table_schema,
                meta,
                column_group_id,
                raw,
                ..
            } => {
                self.write_raw(table_schema, meta, column_group_id, raw)
                    .await?
            }
        }

        if self.max_size != 0 && self.size > self.max_size {
            self.finish().await?;
        }

        Ok(())
    }

    pub async fn finish(&mut self) -> TskvResult<()> {
        let series_meta = self.write_chunk().await?;
        self.write_chunk_group().await?;
        self.write_chunk_group_specs(series_meta).await?;
        self.write_footer().await?;
        self.writer.flush().await.context(IOSnafu)?;
        self.state = State::Finished;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::datatypes::TimeUnit;
    use arrow_array::{ArrayRef, Int64Array, RecordBatch, TimestampNanosecondArray};
    use models::codec::Encoding;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesKey, ValueType};

    use crate::tsm::reader::{decode_pages, TsmReader};
    use crate::tsm::writer::TsmWriter;

    fn i64_column(data: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(data))
    }

    fn ts_column(data: Vec<i64>) -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(data))
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let data1 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                ts_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
            ],
        )
        .unwrap();

        let path = "/tmp/test/tsm";
        let mut tsm_writer = TsmWriter::open(&PathBuf::from(path), 1, 0, false)
            .await
            .unwrap();
        tsm_writer
            .write_record_batch(1, SeriesKey::default(), schema.clone(), data1.clone())
            .await
            .unwrap();
        tsm_writer.finish().await.unwrap();
        let tsm_reader = TsmReader::open(tsm_writer.path).await.unwrap();
        let pages2 = tsm_reader.read_series_pages(1, 0).await.unwrap();
        let data2 = decode_pages(pages2, schema.meta(), None).unwrap();
        assert_eq!(data1, data2);
    }

    #[tokio::test]
    async fn test_write_and_read_2() {
        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let data1 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                ts_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
            ],
        )
        .unwrap();

        let path = "/tmp/test/tsm2";
        let mut tsm_writer = TsmWriter::open(&PathBuf::from(path), 1, 0, false)
            .await
            .unwrap();
        tsm_writer
            .write_record_batch(1, SeriesKey::default(), schema.clone(), data1.clone())
            .await
            .unwrap();
        tsm_writer.finish().await.unwrap();
        let tsm_reader = TsmReader::open(tsm_writer.path).await.unwrap();
        let pages2 = tsm_reader.read_series_pages(1, 0).await.unwrap();
        let data2 = decode_pages(pages2, schema.meta(), None).unwrap();
        assert_eq!(data1, data2);
    }

    #[tokio::test]
    async fn test_write_and_read_3() {
        let schema = TskvTableSchema::new(
            "cnosdb".to_string(),
            "public".to_string(),
            "test0".to_string(),
            vec![
                TableColumn::new(
                    0,
                    "time".to_string(),
                    ColumnType::Time(TimeUnit::Nanosecond),
                    Encoding::default(),
                ),
                TableColumn::new(
                    1,
                    "f1".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    2,
                    "f2".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
                TableColumn::new(
                    3,
                    "f3".to_string(),
                    ColumnType::Field(ValueType::Integer),
                    Encoding::default(),
                ),
            ],
        );
        let schema = Arc::new(schema);
        let data1 = RecordBatch::try_new(
            schema.to_record_data_schema(),
            vec![
                ts_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
            ],
        )
        .unwrap();

        let path = "/tmp/test/tsm3";
        let mut tsm_writer = TsmWriter::open(&PathBuf::from(path), 1, 0, false)
            .await
            .unwrap();
        tsm_writer
            .write_record_batch(1, SeriesKey::default(), schema.clone(), data1.clone())
            .await
            .unwrap();
        tsm_writer.finish().await.unwrap();
        let tsm_reader = TsmReader::open(tsm_writer.path).await.unwrap();
        let raw2 = tsm_reader.read_datablock_raw(1, 0).await.unwrap();
        let chunk = tsm_reader.chunk();
        //println!("{:?}", chunk);
        if let Some(meta) = chunk.get(&(1_u32)) {
            let path2 = "/tmp/test/tsm4";
            let mut tsm_writer2 = TsmWriter::open(&PathBuf::from(path2), 1, 0, false)
                .await
                .unwrap();
            tsm_writer2
                .write_raw(schema, meta.clone(), 0, raw2.clone())
                .await
                .unwrap();
            tsm_writer2.finish().await.unwrap();
            let tsm_reader2 = TsmReader::open(tsm_writer2.path).await.unwrap();
            let raw3 = tsm_reader2.read_datablock_raw(1, 0).await.unwrap();
            assert_eq!(raw2, raw3);
        } else {
            panic!("meta not found");
        }
    }
}
