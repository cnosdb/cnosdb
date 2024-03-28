use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap};
use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use models::codec::Encoding;
use models::predicate::domain::TimeRange;
use models::schema::TskvTableSchemaRef;
use models::{SeriesId, SeriesKey};
use snafu::ResultExt;
use utils::BloomFilter;

use crate::compaction::CompactingBlock;
use crate::error::IOSnafu;
use crate::file_system::file::cursor::FileCursor;
use crate::file_system::file::IFile;
use crate::file_system::file_manager;
use crate::file_utils::{make_delta_file, make_tsm_file};
use crate::tsm::chunk::{Chunk, ChunkStatics, ChunkWriteSpec};
use crate::tsm::chunk_group::{ChunkGroup, ChunkGroupMeta, ChunkGroupWriteSpec};
use crate::tsm::column_group::ColumnGroup;
use crate::tsm::data_block::DataBlock;
use crate::tsm::footer::{Footer, SeriesMeta, TableMeta};
use crate::tsm::page::{Page, PageWriteSpec};
use crate::tsm::{TsmWriteData, BLOOM_FILTER_BITS};
use crate::{TskvError, TskvResult};

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum State {
    Initialised,
    Started,
    Finished,
}

pub enum Version {
    V1,
}

pub struct WriteOptions {
    version: Version,
    write_statistics: bool,
    encode: Encoding,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            version: Version::V1,
            write_statistics: true,
            encode: Encoding::Null,
        }
    }
}

const HEADER_LEN: u64 = 5;
const TSM_MAGIC: [u8; 4] = 0x12CDA16_u32.to_be_bytes();
const VERSION: [u8; 1] = [1];

pub struct TsmWriter {
    file_id: u64,
    min_ts: i64,
    max_ts: i64,
    size: u64,
    max_size: u64,
    path: PathBuf,

    series_bloom_filter: BloomFilter,
    // todo: table object id bloom filter
    // table_bloom_filter: BloomFilter,
    writer: FileCursor,
    options: WriteOptions,
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
        file_id: u64,
        max_size: u64,
        is_delta: bool,
    ) -> TskvResult<Self> {
        let file_path = if is_delta {
            make_delta_file(path_buf, file_id)
        } else {
            make_tsm_file(path_buf, file_id)
        };
        let file_cursor = file_manager::create_file(&file_path).await?;
        let writer = Self::new(file_path, file_cursor.into(), file_id, max_size);
        Ok(writer)
    }
    fn new(path: PathBuf, writer: FileCursor, file_id: u64, max_size: u64) -> Self {
        Self {
            file_id,
            max_ts: i64::MIN,
            min_ts: i64::MAX,
            size: 0,
            max_size,
            path,
            series_bloom_filter: BloomFilter::new(BLOOM_FILTER_BITS),
            writer,
            options: Default::default(),
            table_schemas: Default::default(),
            page_specs: Default::default(),
            chunk_specs: Default::default(),
            chunk_group_specs: Default::default(),
            footer: Default::default(),
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

    pub fn is_finished(&self) -> bool {
        self.state == State::Finished
    }

    pub async fn write_header(&mut self) -> TskvResult<usize> {
        let size = self
            .writer
            .write_vec(
                [
                    IoSlice::new(TSM_MAGIC.as_slice()),
                    IoSlice::new(VERSION.as_slice()),
                ]
                .as_mut_slice(),
            )
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
            let chunk_group_offset = self.writer.pos();
            let buf = group.serialize()?;
            let chunk_group_size = self.writer.write(&buf).await? as u64;
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
        let chunk_group_specs_offset = self.writer.pos();
        let buf = self.chunk_group_specs.serialize()?;
        let chunk_group_specs_size = self.writer.write(&buf).await? as u64;
        self.size += chunk_group_specs_size;
        let time_range = self.chunk_group_specs.time_range();
        let footer = Footer {
            version: 2_u8,
            time_range,
            table: TableMeta::new(chunk_group_specs_offset, chunk_group_specs_size),
            series,
        };
        self.footer = footer;
        Ok(())
    }

    pub async fn write_chunk(&mut self) -> TskvResult<SeriesMeta> {
        let chunk_offset = self.writer.pos();
        for (table, group) in &self.page_specs {
            for (series, chunk) in group {
                let chunk_offset = self.writer.pos();
                let buf = chunk.serialize()?;
                let chunk_size = self.writer.write(&buf).await? as u64;
                self.size += chunk_size;
                let time_range = chunk.time_range();
                self.min_ts = min(self.min_ts, time_range.min_ts);
                self.max_ts = max(self.max_ts, time_range.max_ts);
                let chunk_spec = ChunkWriteSpec {
                    series_id: *series,
                    chunk_offset,
                    chunk_size,
                    statics: ChunkStatics {
                        time_range: *time_range,
                    },
                };
                self.chunk_specs
                    .entry(table.clone())
                    .or_default()
                    .push(chunk_spec);
                self.series_bloom_filter.insert(&series.to_be_bytes());
            }
        }
        let chunk_size = self.writer.pos() - chunk_offset;
        let series = SeriesMeta::new(
            self.series_bloom_filter.bytes().to_vec(),
            chunk_offset,
            chunk_size,
        );
        Ok(series)
    }
    pub async fn write_data(&mut self, groups: TsmWriteData) -> TskvResult<()> {
        // write page data
        for (_, group) in groups {
            for (series, (series_buf, datablock)) in group {
                self.write_datablock(series, series_buf, datablock).await?;
            }
        }
        Ok(())
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

    pub async fn write_datablock(
        &mut self,
        series_id: SeriesId,
        series_key: SeriesKey,
        datablock: DataBlock,
    ) -> TskvResult<()> {
        if self.state == State::Finished {
            return Err(TskvError::CommonError {
                reason: "TsmWriter has been finished".to_string(),
            });
        }

        let time_range = datablock.time_range()?;
        let schema = datablock.schema();
        let pages = datablock.block_to_page()?;

        self.write_pages(schema, series_id, series_key, pages, time_range)
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

        let table = schema.name.clone();
        for page in pages {
            let offset = self.writer.pos();
            let size = self.writer.write(&page.bytes).await? as u64;
            self.size += size;
            let spec = PageWriteSpec {
                offset,
                size,
                meta: page.meta,
            };
            column_group.push(spec);
        }
        self.table_schemas.insert(table.clone(), schema.clone());
        column_group.time_range_merge(&time_range);
        self.page_specs
            .entry(table.clone())
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
        column_group_id: u64,
        raw: Vec<u8>,
    ) -> TskvResult<()> {
        if self.state == State::Initialised {
            self.write_header().await?;
        }

        let mut new_column_group =
            self.create_column_group(schema.clone(), meta.series_id(), meta.series_key());

        let mut offset = self.writer.pos();
        let size = self.writer.write(&raw).await? as u64;
        self.size += size;

        let table = schema.name.to_string();
        let column_group =
            meta.column_group()
                .get(&column_group_id)
                .ok_or(TskvError::CommonError {
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
            self.table_schemas.insert(table.clone(), schema.clone());
        }
        new_column_group.time_range_merge(column_group.time_range());
        let series_id = meta.series_id();
        let series_key = meta.series_key().clone();
        self.page_specs
            .entry(table.clone())
            .or_default()
            .entry(series_id)
            .or_insert(Chunk::new(table, series_id, series_key))
            .push(new_column_group.into())?;

        Ok(())
    }

    pub async fn write_compacting_block(
        &mut self,
        compacting_block: CompactingBlock,
    ) -> TskvResult<()> {
        match compacting_block {
            CompactingBlock::Decoded {
                data_block,
                series_id,
                series_key,
                ..
            } => {
                self.write_datablock(series_id, series_key, data_block)
                    .await?
            }
            CompactingBlock::Encoded {
                table_schema,
                series_id,
                series_key,
                time_range,
                data_block,
                ..
            } => {
                self.write_pages(table_schema, series_id, series_key, data_block, time_range)
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
        self.writer.file_ref().sync_data().await?;
        self.state = State::Finished;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::datatypes::TimeUnit;
    use models::codec::Encoding;
    use models::field_value::FieldVal;
    use models::predicate::domain::TimeRange;
    use models::schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::{SeriesKey, ValueType};

    use crate::tsm::data_block::MutableColumn;
    use crate::tsm::reader::TsmReader;
    use crate::tsm::writer::{DataBlock, TsmWriter};

    fn i64_column(data: Vec<i64>) -> MutableColumn {
        let mut col = MutableColumn::empty(TableColumn::new(
            1,
            "f1".to_string(),
            ColumnType::Field(ValueType::Integer),
            Encoding::default(),
        ))
        .unwrap();
        for datum in data {
            col.push(Some(FieldVal::Integer(datum))).unwrap()
        }
        col
    }

    fn ts_column(data: Vec<i64>) -> MutableColumn {
        let mut col = MutableColumn::empty(TableColumn::new(
            0,
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
        let data1 = DataBlock::new(
            schema.clone(),
            ts_column(vec![1, 2, 3]),
            vec![
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
                i64_column(vec![1, 2, 3]),
            ],
        );

        let path = "/tmp/test/tsm";
        let mut tsm_writer = TsmWriter::open(&PathBuf::from(path), 1, 0, false)
            .await
            .unwrap();
        tsm_writer
            .write_datablock(1, SeriesKey::default(), data1.clone())
            .await
            .unwrap();
        tsm_writer.finish().await.unwrap();
        let tsm_reader = TsmReader::open(tsm_writer.path).await.unwrap();
        let data2 = tsm_reader.read_datablock(1, 0).await.unwrap();
        assert_eq!(data1, data2);
        let time_range = data2.time_range().unwrap();
        assert_eq!(time_range, TimeRange::new(1, 3));
        println!("time range: {:?}", time_range);
    }
}
