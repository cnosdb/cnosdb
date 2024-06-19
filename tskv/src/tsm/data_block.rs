use std::sync::Arc;

use models::column_data::{ColumnData, PrimaryColumnData};
use models::field_value::FieldVal;
use models::predicate::domain::TimeRange;
use models::schema::tskv_table_schema::{TableColumn, TskvTableSchemaRef};
use models::{ColumnId, SeriesId};
use utils::bitset::BitSet;

use crate::error::{CommonSnafu, DataBlockSnafu};
use crate::tsm::page::Page;
use crate::tsm::TsmTombstone;
use crate::{TskvError, TskvResult};

#[derive(Debug, Clone, PartialEq)]
pub struct DataBlock {
    schema: TskvTableSchemaRef,
    ts: MutableColumn,
    cols: Vec<MutableColumn>,
}

enum Merge {
    SelfTs(usize),
    OtherTs(usize),
    Equal(usize, usize),
}

impl DataBlock {
    pub fn new(schema: TskvTableSchemaRef, ts: MutableColumn, cols: Vec<MutableColumn>) -> Self {
        DataBlock { schema, ts, cols }
    }

    pub fn schema(&self) -> TskvTableSchemaRef {
        self.schema.clone()
    }

    pub fn block_to_page(&self) -> TskvResult<Vec<Page>> {
        let mut pages = Vec::with_capacity(self.cols.len() + 1);
        pages.push(Page::col_to_page(&self.ts)?);
        for col in self.cols.iter() {
            pages.push(Page::col_to_page(col)?);
        }
        Ok(pages)
    }

    pub fn merge(&mut self, other: DataBlock) -> TskvResult<DataBlock> {
        self.schema_check(&other)?;

        let schema = if self.schema.schema_version > other.schema.schema_version {
            self.schema.clone()
        } else {
            other.schema.clone()
        };
        let (sort_index, time_array) = self.sort_index_and_time_col(&other)?;
        let mut columns = Vec::new();
        for field in schema.fields() {
            let column_self = self.column(field.id);
            let column_other = other.column(field.id);
            let mut merge_column = MutableColumn::empty_with_cap(field, time_array.len())?;
            for idx in sort_index.iter() {
                match idx {
                    Merge::SelfTs(index) => {
                        if let Some(column_self) = column_self {
                            merge_column.push(column_self.column_data.get(*index))?;
                        } else {
                            merge_column.push(None)?;
                        }
                    }
                    Merge::OtherTs(index) => {
                        if let Some(column_other) = column_other {
                            merge_column.push(column_other.column_data.get(*index))?;
                        } else {
                            merge_column.push(None)?;
                        }
                    }
                    Merge::Equal(index_self, index_other) => {
                        let field_self = if let Some(column_self) = column_self {
                            column_self.column_data.get(*index_self)
                        } else {
                            None
                        };
                        let field_other = if let Some(column_other) = column_other {
                            column_other.column_data.get(*index_other)
                        } else {
                            None
                        };
                        if field_self.is_some() {
                            merge_column.push(field_self)?;
                        } else {
                            merge_column.push(field_other)?;
                        }
                    }
                }
            }
            columns.push(merge_column);
        }

        let mut ts_col =
            MutableColumn::empty_with_cap(self.ts.column_desc().clone(), time_array.len())?;

        for value in time_array {
            ts_col.push(Some(FieldVal::Integer(value)))?
        }

        let datablock = DataBlock::new(schema, ts_col, columns);

        // todo: split datablock to blocks
        // let mut blocks = Vec::with_capacity(time_array.len() / Self::BLOCK_SIZE + 1);
        // blocks.push(datablock);

        Ok(datablock)
    }

    fn sort_index_and_time_col(&self, other: &DataBlock) -> TskvResult<(Vec<Merge>, Vec<i64>)> {
        let mut sort_index = Vec::with_capacity(self.len() + other.len());
        let mut time_array = Vec::new();
        let (mut index_self, mut index_other) = (0_usize, 0_usize);
        let (self_len, other_len) = (self.len(), other.len());
        while index_self < self_len && index_other < other_len {
            match (self.ts.data(), other.ts.data()) {
                (PrimaryColumnData::I64(ref data1, ..), PrimaryColumnData::I64(ref data2, ..)) => {
                    match data1[index_self].cmp(&data2[index_other]) {
                        std::cmp::Ordering::Less => {
                            sort_index.push(Merge::SelfTs(index_self));
                            time_array.push(data1[index_self]);
                            index_self += 1;
                        }
                        std::cmp::Ordering::Greater => {
                            sort_index.push(Merge::OtherTs(index_other));
                            time_array.push(data2[index_other]);
                            index_other += 1;
                        }
                        std::cmp::Ordering::Equal => {
                            sort_index.push(Merge::Equal(index_self, index_other));
                            time_array.push(data1[index_self]);
                            index_self += 1;
                            index_other += 1;
                        }
                    }
                }
                _ => {
                    return Err(DataBlockSnafu {
                        reason: "Time column does not support except i64 physical data type"
                            .to_string(),
                    }
                    .build())
                }
            }
        }

        match (self.ts.data(), &other.ts.data()) {
            (PrimaryColumnData::I64(ref data1, ..), PrimaryColumnData::I64(ref data2, ..)) => {
                while index_self < self_len {
                    sort_index.push(Merge::SelfTs(index_self));
                    time_array.push(data1[index_self]);
                    index_self += 1;
                }
                while index_other < other_len {
                    sort_index.push(Merge::OtherTs(index_other));
                    time_array.push(data2[index_other]);
                    index_other += 1;
                }
            }
            _ => {
                return Err(DataBlockSnafu {
                    reason: "Time column does not support except i64 physical data type"
                        .to_string(),
                }
                .build())
            }
        }
        Ok((sort_index, time_array))
    }

    pub fn len(&self) -> usize {
        self.ts.valid().len()
    }

    pub fn is_empty(&self) -> bool {
        self.ts.valid().is_empty()
    }

    pub fn schema_check(&self, other: &DataBlock) -> TskvResult<()> {
        if self.schema.name != other.schema.name
            || self.schema.db != other.schema.db
            || self.schema.tenant != other.schema.tenant
        {
            return Err(CommonSnafu {
                reason: format!(
                    "schema name not match in datablock merge, self: {}.{}.{}, other: {}.{}.{}",
                    self.schema.tenant,
                    self.schema.db,
                    self.schema.name,
                    other.schema.tenant,
                    other.schema.db,
                    other.schema.name
                ),
            }
            .build());
        }
        Ok(())
    }

    pub fn column(&self, id: ColumnId) -> Option<&MutableColumn> {
        for (index, col) in self.cols.iter().enumerate() {
            if col.column_desc().id == id {
                return Some(&self.cols[index]);
            }
        }
        None
    }

    pub fn chunk(&self, start: usize, end: usize) -> TskvResult<DataBlock> {
        if start > end || end > self.len() {
            return Err(CommonSnafu {
                reason: "start or end index out of range".to_string(),
            }
            .build());
        }

        let ts_column = self.ts.chunk(start, end)?;
        let other_colums = self
            .cols
            .iter()
            .map(|column| column.chunk(start, end))
            .collect::<TskvResult<Vec<_>>>()?;
        let datablock = DataBlock::new(self.schema.clone(), ts_column, other_colums);

        Ok(datablock)
    }

    pub fn filter_by_tomb(
        &mut self,
        tombstone: Arc<TsmTombstone>,
        series_id: SeriesId,
    ) -> TskvResult<()> {
        let time_range = self.time_range()?;
        self.cols
            .iter_mut()
            .try_for_each(|column: &mut MutableColumn| -> TskvResult<()> {
                let time_ranges = tombstone.get_overlapped_time_ranges(
                    series_id,
                    column.column_desc().id,
                    &time_range,
                );
                for time_range in time_ranges {
                    let index_begin = self
                        .ts
                        .data()
                        .binary_search_for_i64_col(time_range.min_ts)
                        .map_err(|e| TskvError::ColumnDataError { source: e })?
                        .unwrap_or_else(|index| index);
                    let index_end = self
                        .ts
                        .data()
                        .binary_search_for_i64_col(time_range.max_ts)
                        .map_err(|e| TskvError::ColumnDataError { source: e })?
                        .map(|index| index + 1)
                        .unwrap_or_else(|index| index);
                    if index_begin == index_end {
                        continue;
                    }
                    column.mut_valid().clear_bits(index_begin, index_end);
                }
                Ok(())
            })?;
        Ok(())
    }

    pub fn time_range(&self) -> TskvResult<TimeRange> {
        match self.ts.column_data.primary_data {
            PrimaryColumnData::I64(_, min, max) => Ok(TimeRange {
                min_ts: min,
                max_ts: max,
            }),
            _ => Err(DataBlockSnafu {
                reason: "Time column does not support except i64 physical data type".to_string(),
            }
            .build()),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutableColumn {
    column_desc: TableColumn,
    column_data: ColumnData,
}

impl MutableColumn {
    pub fn empty(column_desc: TableColumn) -> TskvResult<MutableColumn> {
        let column_type = column_desc.column_type.to_physical_data_type();
        let data =
            ColumnData::new(column_type).map_err(|e| TskvError::ColumnDataError { source: e })?;
        let column = Self {
            column_desc,
            column_data: data,
        };
        Ok(column)
    }

    pub fn empty_with_cap(column_desc: TableColumn, cap: usize) -> TskvResult<MutableColumn> {
        let column_type = column_desc.column_type.to_physical_data_type();
        let data = ColumnData::with_cap(column_type, cap)
            .map_err(|e| TskvError::ColumnDataError { source: e })?;
        let column = Self {
            column_desc,
            column_data: data,
        };
        Ok(column)
    }

    pub fn chunk(&self, start: usize, end: usize) -> TskvResult<MutableColumn> {
        let column = self
            .column_data
            .chunk(start, end)
            .map_err(|e| TskvError::ColumnDataError { source: e })?;
        Ok(MutableColumn {
            column_desc: self.column_desc.clone(),
            column_data: column,
        })
    }

    pub fn valid(&self) -> &BitSet {
        &self.column_data.valid
    }

    pub fn mut_valid(&mut self) -> &mut BitSet {
        &mut self.column_data.valid
    }

    pub fn data(&self) -> &PrimaryColumnData {
        &self.column_data.primary_data
    }

    pub fn column_desc(&self) -> &TableColumn {
        &self.column_desc
    }

    pub fn push(&mut self, value: Option<FieldVal>) -> TskvResult<()> {
        self.column_data
            .push(value)
            .map_err(|e| TskvError::ColumnDataError { source: e })
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow::datatypes::TimeUnit;
    use models::codec::Encoding;
    use models::field_value::FieldVal;
    use models::predicate::domain::TimeRange;
    use models::schema::tskv_table_schema::{ColumnType, TableColumn, TskvTableSchema};
    use models::ValueType;

    use crate::file_system::async_filesystem::LocalFileSystem;
    use crate::file_system::FileSystem;
    use crate::tsm::data_block::{DataBlock, MutableColumn};
    use crate::tsm::TsmTombstone;

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
    async fn test_datablock_to_pages() {
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
        let pages = data1.block_to_page().unwrap();
        assert_eq!(4, pages.len());
        for page in pages.iter() {
            let result = page.crc_validation();
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_datablock_merge() {
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
            ],
        );
        let schema = Arc::new(schema);
        let mut data1 = DataBlock::new(
            schema.clone(),
            ts_column(vec![1, 2, 3]),
            vec![i64_column(vec![1, 2, 3])],
        );
        let data2 = DataBlock::new(
            schema.clone(),
            ts_column(vec![4, 5, 6]),
            vec![i64_column(vec![4, 5, 6])],
        );
        let data3 = data1.merge(data2).unwrap();
        let data4 = DataBlock::new(
            schema.clone(),
            ts_column(vec![1, 2, 3, 4, 5, 6]),
            vec![i64_column(vec![1, 2, 3, 4, 5, 6])],
        );
        assert_eq!(data4, data3);
    }

    #[tokio::test]
    async fn test_datablock_sort() {
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
            ts_column(vec![1, 3, 5, 7]),
            vec![
                i64_column(vec![1, 3, 5]),
                i64_column(vec![1, 3, 5]),
                i64_column(vec![1, 3, 5]),
            ],
        );
        let data2 = DataBlock::new(
            schema.clone(),
            ts_column(vec![2, 4, 6, 7]),
            vec![
                i64_column(vec![2, 4, 6]),
                i64_column(vec![2, 4, 6]),
                i64_column(vec![2, 4, 6]),
            ],
        );
        let (_, time_array) = data1.sort_index_and_time_col(&data2).unwrap();
        let expected_time_array = vec![1, 2, 3, 4, 5, 6, 7];
        assert_eq!(expected_time_array, time_array);
    }

    #[tokio::test]
    async fn test_datablock_chunk() {
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
            ts_column(vec![1, 2, 3, 4, 5, 6]),
            vec![
                i64_column(vec![1, 2, 3, 4, 5, 6]),
                i64_column(vec![1, 2, 3, 4, 5, 6]),
                i64_column(vec![1, 2, 3, 4, 5, 6]),
            ],
        );
        let data2 = data1.chunk(1, 5).unwrap();
        let data3 = DataBlock::new(
            schema.clone(),
            ts_column(vec![2, 3, 4, 5]),
            vec![
                i64_column(vec![2, 3, 4, 5]),
                i64_column(vec![2, 3, 4, 5]),
                i64_column(vec![2, 3, 4, 5]),
            ],
        );

        assert_eq!(data3, data2);
    }

    #[tokio::test]
    async fn test_datablock_fliter_by_tomb() {
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
            ],
        );
        let schema = Arc::new(schema);
        let mut data1 = DataBlock::new(
            schema.clone(),
            ts_column(vec![1, 2, 3, 4, 5, 6]),
            vec![i64_column(vec![1, 2, 3, 4, 5, 6])],
        );
        let dir = PathBuf::from("/tmp/test/writter/1".to_string());
        let _ = std::fs::remove_dir_all(&dir);
        if !LocalFileSystem::try_exists(&dir) {
            std::fs::create_dir_all(&dir).unwrap();
        }
        let mut tombstone = TsmTombstone::open(&dir, 1).await.unwrap();
        tombstone
            .add_range(&[(0, 1)], &TimeRange::new(1, 4))
            .await
            .unwrap();
        let arctombstone = Arc::new(tombstone);
        data1.filter_by_tomb(arctombstone, 0).unwrap();
        let expect_bitset = vec![false, false, false, false, true, true, false, false];
        assert_eq!(expect_bitset, data1.column(1).unwrap().valid().to_vec());
    }
}
